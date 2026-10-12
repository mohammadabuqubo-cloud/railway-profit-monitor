
import os
import re
import json
import time
import hmac
import hashlib
import logging
import threading
import urllib.parse
import urllib.request
import urllib.error

from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET


# ============================================================
# MAQ BINANCE RAILWAY BOT - DH COMPATIBLE V3
#
# Supports:
# - Original DH TradingView strategy messages
# - BUY|BTCUSDT and SELL|BTCUSDT
# - Automatic symbol normalization
# - Binance Futures market orders
# - Opposite-signal position reversal
# - Variable-controlled initial stop loss
# - Variable-controlled breakeven
# - Variable-controlled partial take profit
# - Variable-controlled final take profit
# - Optional portfolio profit target
#
# EMPTY INITIAL_STOP_LOSS_ROI_PCT = DISABLED
#
# WARNING:
# This bot can place leveraged Futures orders.
# Test with Binance Testnet before live deployment.
# ============================================================


# ============================================================
# LOGGING AND FLASK
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ-BINANCE-BOT")

app = Flask(__name__)


# ============================================================
# ENVIRONMENT HELPERS
# ============================================================

def env_str(name, default=""):
    value = os.getenv(name)

    if value is None:
        return default

    return str(value).strip()


def env_float(name, default=0.0):
    value = env_str(name)

    if value == "":
        return float(default)

    return float(value)


def env_int(name, default=0):
    value = env_str(name)

    if value == "":
        return int(default)

    return int(value)


def optional_percentage(name):
    """
    Empty or zero means disabled.
    Positive values are percentages.
    """
    value = env_str(name)

    if value == "":
        return 0.0

    try:
        result = float(value)
    except ValueError as exc:
        raise RuntimeError(
            f"{name} must be a number or empty"
        ) from exc

    if result < 0 or result > 100:
        raise RuntimeError(
            f"{name} must be between 0 and 100"
        )

    return result


# ============================================================
# RAILWAY VARIABLES
# ============================================================

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")

BINANCE_TESTNET = (
    env_str("BINANCE_TESTNET", "false").lower()
    == "true"
)

ORDER_MARGIN_USDT = env_float(
    "ORDER_MARGIN_USDT", 50
)

ENTRY_LEVERAGE = env_int(
    "ENTRY_LEVERAGE", 16
)

POLL_INTERVAL_SECONDS = max(
    5,
    env_int("POLL_INTERVAL_SECONDS", 5)
)

# Empty means disabled.
INITIAL_STOP_LOSS_ROI_PCT = optional_percentage(
    "INITIAL_STOP_LOSS_ROI_PCT"
)

BREAKEVEN_TRIGGER_ROI_PCT = optional_percentage(
    "BREAKEVEN_TRIGGER_ROI_PCT"
)

PARTIAL_TP_ROI_PCT = optional_percentage(
    "PARTIAL_TP_ROI_PCT"
)

PARTIAL_CLOSE_PCT = optional_percentage(
    "PARTIAL_CLOSE_PCT"
)

FINAL_TP_ROI_PCT = optional_percentage(
    "FINAL_TP_ROI_PCT"
)

PORTFOLIO_PROFIT_USD = env_float(
    "PORTFOLIO_PROFIT_USD", 0
)

WEBHOOK_SECRET = env_str("WEBHOOK_SECRET")

# Default to disabled until explicitly enabled.
ENABLE_LIVE_ORDERS = (
    env_str("ENABLE_LIVE_ORDERS", "false").lower()
    == "true"
)


# ============================================================
# CONFIG VALIDATION
# ============================================================

if ORDER_MARGIN_USDT <= 0:
    raise RuntimeError(
        "ORDER_MARGIN_USDT must be positive"
    )

if ENTRY_LEVERAGE < 1 or ENTRY_LEVERAGE > 125:
    raise RuntimeError(
        "ENTRY_LEVERAGE must be between 1 and 125"
    )

if (
    PARTIAL_TP_ROI_PCT > 0
    and PARTIAL_CLOSE_PCT <= 0
):
    raise RuntimeError(
        "PARTIAL_CLOSE_PCT must be positive when partial TP is enabled"
    )

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    log.warning(
        "Binance API credentials are missing"
    )


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    testnet=BINANCE_TESTNET
)

BINANCE_FUTURES_BASE = (
    "https://testnet.binancefuture.com"
    if BINANCE_TESTNET
    else "https://fapi.binance.com"
)


# ============================================================
# GLOBAL STATE
# ============================================================

position_states = {}
last_errors = {}

last_snapshot = {
    "last_run": None,
    "positions": [],
    "total_unrealized_profit": 0.0
}

exchange_info_cache = {
    "loaded_at": 0.0,
    "symbols": {}
}

BOT_ALGO_PREFIX = "MAQSL"

entry_lock = threading.Lock()
monitor_lock = threading.Lock()
exchange_info_lock = threading.Lock()

monitor_started = False


# ============================================================
# UTILITIES
# ============================================================

def is_rate_limit_error(exc):
    message = str(exc).lower()

    return (
        "-1003" in message
        or "too many requests" in message
        or "rate limit" in message
    )


def normalize_symbol(raw_symbol):
    symbol = str(raw_symbol).strip().upper()

    if ":" in symbol:
        symbol = symbol.split(":")[-1]

    if symbol.endswith(".P"):
        symbol = symbol[:-2]

    symbol = (
        symbol.replace("/", "")
        .replace("-", "")
        .replace("_", "")
    )

    if not re.fullmatch(r"[A-Z0-9]{5,30}", symbol):
        raise ValueError(
            f"Invalid Futures symbol: {symbol}"
        )

    return symbol


def position_key(position):
    return (
        position["symbol"]
        + ":"
        + position.get("positionSide", "BOTH")
    )


def round_down_to_step(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))

    return float(
        (value / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step
    )


# ============================================================
# BINANCE EXCHANGE INFORMATION
# ============================================================

def refresh_exchange_info():
    now = time.time()

    if (
        exchange_info_cache["symbols"]
        and now - exchange_info_cache["loaded_at"] < 21600
    ):
        return

    with exchange_info_lock:

        now = time.time()

        if (
            exchange_info_cache["symbols"]
            and now - exchange_info_cache["loaded_at"] < 21600
        ):
            return

        response = client.futures_exchange_info()

        exchange_info_cache["symbols"] = {
            item["symbol"]: item
            for item in response["symbols"]
        }

        exchange_info_cache["loaded_at"] = now


def get_symbol_info(symbol):
    refresh_exchange_info()

    return exchange_info_cache["symbols"].get(
        symbol
    )


def validate_symbol(symbol):
    info = get_symbol_info(symbol)

    if info is None:
        raise ValueError(
            f"Unknown Binance Futures symbol: {symbol}"
        )

    if info.get("status") != "TRADING":
        raise ValueError(
            f"Symbol is not trading: {symbol}"
        )

    return info


def get_filter(symbol, filter_type):
    info = validate_symbol(symbol)

    for item in info.get("filters", []):
        if item.get("filterType") == filter_type:
            return item

    raise ValueError(
        f"Missing {filter_type} for {symbol}"
    )


def get_quantity_step(symbol):
    filt = get_filter(symbol, "LOT_SIZE")

    return float(filt["stepSize"])


def get_market_quantity_step(symbol):
    info = validate_symbol(symbol)

    for item in info.get("filters", []):
        if item.get("filterType") == "MARKET_LOT_SIZE":
            step = float(item.get("stepSize", 0))
            if step > 0:
                return step

    return get_quantity_step(symbol)


def get_tick_size(symbol):
    filt = get_filter(symbol, "PRICE_FILTER")

    return float(filt["tickSize"])


def round_quantity(symbol, quantity):
    return round_down_to_step(
        quantity,
        get_market_quantity_step(symbol)
    )


def round_price(symbol, price):
    tick = Decimal(str(get_tick_size(symbol)))
    value = Decimal(str(price))

    result = (
        (value / tick).to_integral_value(
            rounding=ROUND_DOWN
        ) * tick
    )

    return format(result, "f")


# ============================================================
# BINANCE POSITIONS
# ============================================================

def get_open_positions():
    positions = client.futures_position_information()

    return [
        p for p in positions
        if abs(float(p["positionAmt"])) > 0
    ]


def get_symbol_positions(symbol):
    return client.futures_position_information(
        symbol=symbol
    )


def get_current_position(symbol):
    positions = [
        p for p in get_symbol_positions(symbol)
        if abs(float(p["positionAmt"])) > 0
    ]

    if len(positions) > 1:
        raise RuntimeError(
            f"Multiple open position sides for {symbol}; "
            "Hedge Mode requires separate handling"
        )

    return positions[0] if positions else None


def position_margin(position):
    margin = float(
        position.get("positionInitialMargin", 0)
        or 0
    )

    if margin > 0:
        return margin

    notional = abs(
        float(position.get("notional", 0) or 0)
    )

    leverage = float(
        position.get("leverage", 1) or 1
    )

    return notional / max(leverage, 1)


def calculate_roi(position):
    margin = position_margin(position)

    if margin <= 0:
        return 0.0

    profit = float(
        position.get(
            "unRealizedProfit",
            position.get("unrealizedProfit", 0)
        ) or 0
    )

    return profit / margin * 100


# ============================================================
# BINANCE CONDITIONAL ALGO API
# ============================================================

def binance_algo_request(method, path, params=None):

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError(
            "Missing Binance credentials"
        )

    params = dict(params or {})

    params["timestamp"] = int(
        time.time() * 1000
    )

    params["recvWindow"] = 5000

    query = urllib.parse.urlencode(params)

    signature = hmac.new(
        BINANCE_API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    payload = query + "&signature=" + signature

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    method = method.upper()

    if method == "POST":

        req = urllib.request.Request(
            BINANCE_FUTURES_BASE + path,
            data=payload.encode(),
            headers=headers,
            method="POST"
        )

    else:

        req = urllib.request.Request(
            BINANCE_FUTURES_BASE
            + path
            + "?"
            + payload,
            headers=headers,
            method=method
        )

    try:
        with urllib.request.urlopen(
            req,
            timeout=15
        ) as response:

            body = response.read().decode()

            return json.loads(body) if body else {}

    except urllib.error.HTTPError as exc:

        body = exc.read().decode(
            errors="replace"
        )

        raise RuntimeError(
            f"Binance Algo API {exc.code}: {body}"
        ) from exc


def get_open_algo_orders(symbol):
    response = binance_algo_request(
        "GET",
        "/fapi/v1/openAlgoOrders",
        {
            "symbol": symbol,
            "algoType": "CONDITIONAL"
        }
    )

    return (
        response
        if isinstance(response, list)
        else []
    )


def cancel_bot_stops(symbol, position_side="BOTH"):

    for order in get_open_algo_orders(symbol):

        client_id = str(
            order.get("clientAlgoId", "")
        )

        if not client_id.startswith(BOT_ALGO_PREFIX):
            continue

        order_side = order.get(
            "positionSide", "BOTH"
        )

        if (
            position_side != "BOTH"
            and order_side != position_side
        ):
            continue

        algo_id = order.get("algoId")

        if algo_id:
            binance_algo_request(
                "DELETE",
                "/fapi/v1/algoOrder",
                {
                    "symbol": symbol,
                    "algoId": algo_id
                }
            )

            log.info(
                "Cancelled MAQ stop: %s %s",
                symbol,
                algo_id
            )


def create_protective_stop(position, price, reason):

    amount = float(position["positionAmt"])

    if amount == 0:
        return False

    symbol = position["symbol"]

    side = (
        "SELL"
        if amount > 0
        else "BUY"
    )

    position_side = position.get(
        "positionSide", "BOTH"
    )

    trigger_price = round_price(
        symbol, price
    )

    client_id = (
        f"{BOT_ALGO_PREFIX}"
        f"_{reason[:3]}"
        f"_{int(time.time() * 1000)}"
    )

    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "triggerPrice": trigger_price,
        "workingType": "MARK_PRICE",
        "closePosition": "true",
        "clientAlgoId": client_id
    }

    if position_side != "BOTH":
        params["positionSide"] = position_side

    # Place the replacement first, so an existing
    # protective stop is not removed before the
    # new order has been accepted.
    response = binance_algo_request(
        "POST",
        "/fapi/v1/algoOrder",
        params
    )

    # Cancel older MAQ stops, preserving the new one.
    try:
        for order in get_open_algo_orders(symbol):

            old_id = str(
                order.get("clientAlgoId", "")
            )

            if (
                old_id.startswith(BOT_ALGO_PREFIX)
                and old_id != client_id
                and (
                    position_side == "BOTH"
                    or order.get("positionSide", "BOTH")
                    == position_side
                )
            ):
                binance_algo_request(
                    "DELETE",
                    "/fapi/v1/algoOrder",
                    {
                        "symbol": symbol,
                        "algoId": order["algoId"]
                    }
                )

    except Exception:
        log.exception(
            "Old stop cleanup failed for %s",
            symbol
        )

    log.info(
        "%s stop created for %s at %s",
        reason,
        symbol,
        trigger_price
    )

    return response


# ============================================================
# INITIAL STOP LOSS
# ============================================================

def calculate_initial_stop_price(position):

    if INITIAL_STOP_LOSS_ROI_PCT <= 0:
        return None

    entry = float(position["entryPrice"])

    leverage = float(
        position.get("leverage", 1) or 1
    )

    amount = float(position["positionAmt"])

    fraction = (
        INITIAL_STOP_LOSS_ROI_PCT
        / 100
        / max(leverage, 1)
    )

    if amount > 0:
        return entry * (1 - fraction)

    return entry * (1 + fraction)


def arm_initial_stop(position):

    # Empty Railway variable means disabled.
    if INITIAL_STOP_LOSS_ROI_PCT <= 0:
        return False

    price = calculate_initial_stop_price(
        position
    )

    if price is None:
        return False

    return bool(
        create_protective_stop(
            position,
            price,
            "INITIAL"
        )
    )


# ============================================================
# BREAKEVEN
# ============================================================

def move_stop_to_breakeven(position):

    if BREAKEVEN_TRIGGER_ROI_PCT <= 0:
        return False

    entry = float(position["entryPrice"])

    if entry <= 0:
        return False

    return bool(
        create_protective_stop(
            position,
            entry,
            "BREAKEVEN"
        )
    )


# ============================================================
# MARKET CLOSE
# ============================================================

def close_position_quantity(position, quantity):

    amount = float(position["positionAmt"])

    if amount == 0:
        return None

    symbol = position["symbol"]

    quantity = round_quantity(
        symbol,
        min(abs(amount), abs(quantity))
    )

    if quantity <= 0:
        return None

    side = (
        SIDE_SELL
        if amount > 0
        else SIDE_BUY
    )

    position_side = position.get(
        "positionSide", "BOTH"
    )

    params = {
        "symbol": symbol,
        "side": side,
        "type": ORDER_TYPE_MARKET,
        "quantity": quantity
    }

    if position_side == "BOTH":
        params["reduceOnly"] = True
    else:
        params["positionSide"] = position_side

    result = client.futures_create_order(
        **params
    )

    log.warning(
        "Closed %s quantity %s",
        symbol,
        quantity
    )

    return result


def close_position(position):
    return close_position_quantity(
        position,
        abs(float(position["positionAmt"]))
    )


# ============================================================
# PARTIAL TAKE PROFIT
# ============================================================

def execute_partial_take_profit(position):

    if (
        PARTIAL_TP_ROI_PCT <= 0
        or PARTIAL_CLOSE_PCT <= 0
    ):
        return False

    quantity = (
        abs(float(position["positionAmt"]))
        * PARTIAL_CLOSE_PCT
        / 100
    )

    result = close_position_quantity(
        position,
        quantity
    )

    return bool(result)


# ============================================================
# POSITION STATE
# ============================================================

def get_position_state(position):

    key = position_key(position)

    state = position_states.get(key)

    entry = float(position["entryPrice"])

    if (
        state is None
        or abs(state["entry_price"] - entry) > 1e-10
    ):

        state = {
            "entry_price": entry,
            "initial_stop_armed": False,
            "breakeven_armed": False,
            "partial_tp_done": False
        }

        position_states[key] = state

    return state


# ============================================================
# ENTRY ORDER
# ============================================================

def get_futures_price(symbol):

    response = client.futures_symbol_ticker(
        symbol=symbol
    )

    return float(response["price"])


def calculate_entry_quantity(symbol):

    price = get_futures_price(symbol)

    notional = (
        ORDER_MARGIN_USDT
        * ENTRY_LEVERAGE
    )

    quantity = round_quantity(
        symbol,
        notional / price
    )

    if quantity <= 0:
        raise RuntimeError(
            f"Invalid quantity for {symbol}"
        )

    return quantity


def wait_for_position(symbol, timeout=10):

    start = time.time()

    while time.time() - start < timeout:

        position = get_current_position(symbol)

        if position:
            return position

        time.sleep(0.5)

    return None


def wait_for_close(symbol, timeout=10):

    start = time.time()

    while time.time() - start < timeout:

        if get_current_position(symbol) is None:
            return True

        time.sleep(0.5)

    return False


def process_entry_signal(signal, raw_symbol):

    signal = signal.upper().strip()
    symbol = normalize_symbol(raw_symbol)

    if signal not in ("BUY", "SELL"):
        raise ValueError(
            "Signal must be BUY or SELL"
        )

    validate_symbol(symbol)

    if not ENABLE_LIVE_ORDERS:
        log.warning(
            "DRY RUN: %s %s",
            signal,
            symbol
        )

        return {
            "status": "dry_run",
            "signal": signal,
            "symbol": symbol
        }

    with entry_lock:

        current = get_current_position(symbol)

        if current:

            amount = float(
                current["positionAmt"]
            )

            current_signal = (
                "BUY"
                if amount > 0
                else "SELL"
            )

            if current_signal == signal:

                return {
                    "status": "ignored",
                    "reason": "already_in_position",
                    "symbol": symbol,
                    "signal": signal
                }

            log.warning(
                "Reversing %s from %s to %s",
                symbol,
                current_signal,
                signal
            )

            # Close first; do not open the new position
            # until the old one is confirmed closed.
            close_position(current)

            if not wait_for_close(symbol):
                raise RuntimeError(
                    f"Position failed to close: {symbol}"
                )

            cancel_bot_stops(
                symbol,
                current.get("positionSide", "BOTH")
            )

            position_states.pop(
                position_key(current),
                None
            )

        client.futures_change_leverage(
            symbol=symbol,
            leverage=ENTRY_LEVERAGE
        )

        quantity = calculate_entry_quantity(
            symbol
        )

        side = (
            SIDE_BUY
            if signal == "BUY"
            else SIDE_SELL
        )

        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=quantity
        )

        log.warning(
            "OPENED %s %s quantity=%s",
            signal,
            symbol,
            quantity
        )

        position = wait_for_position(
            symbol
        )

        stop_armed = False

        if position:

            state = get_position_state(
                position
            )

            # Initial stop only when explicitly enabled.
            if INITIAL_STOP_LOSS_ROI_PCT > 0:

                stop_armed = arm_initial_stop(
                    position
                )

                state["initial_stop_armed"] = bool(
                    stop_armed
                )

            else:

                log.warning(
                    "Initial stop disabled for %s",
                    symbol
                )

        else:

            log.error(
                "Entry order sent but position "
                "could not be confirmed for %s",
                symbol
            )

        return {
            "status": "executed",
            "symbol": symbol,
            "signal": signal,
            "order_id": order.get("orderId"),
            "initial_stop_enabled":
                INITIAL_STOP_LOSS_ROI_PCT > 0,
            "initial_stop_armed": bool(stop_armed),
            "position_confirmed": bool(position)
        }


# ============================================================
# RISK MONITOR
# ============================================================

def check_positions():

    global last_snapshot

    with entry_lock:

        positions = get_open_positions()

        active_keys = {
            position_key(p)
            for p in positions
        }

        for key in list(position_states):

            if key not in active_keys:
                position_states.pop(
                    key,
                    None
                )

        total_profit = 0.0
        snapshot = []

        for position in positions:

            symbol = position["symbol"]
            key = position_key(position)

            state = get_position_state(
                position
            )

            roi = calculate_roi(position)

            unrealized = float(
                position.get(
                    "unRealizedProfit",
                    position.get(
                        "unrealizedProfit", 0
                    )
                ) or 0
            )

            total_profit += unrealized

            log.info(
                "%s ROI %.2f%%",
                symbol,
                roi
            )

            # ======================================
            # INITIAL STOP LOSS
            # ======================================

            if (
                INITIAL_STOP_LOSS_ROI_PCT > 0
                and roi <= -INITIAL_STOP_LOSS_ROI_PCT
            ):

                log.warning(
                    "STOP LOSS %s ROI %.2f%%",
                    symbol,
                    roi
                )

                close_position(position)

                continue

            # ======================================
            # FINAL TAKE PROFIT
            # ======================================

            if (
                FINAL_TP_ROI_PCT > 0
                and roi >= FINAL_TP_ROI_PCT
            ):

                log.warning(
                    "FINAL TP %s ROI %.2f%%",
                    symbol,
                    roi
                )

                close_position(position)

                continue

            # ======================================
            # BREAKEVEN
            # ======================================

            if (
                BREAKEVEN_TRIGGER_ROI_PCT > 0
                and roi >= BREAKEVEN_TRIGGER_ROI_PCT
                and not state["breakeven_armed"]
            ):

                if move_stop_to_breakeven(
                    position
                ):

                    state["breakeven_armed"] = True
                    state["initial_stop_armed"] = False

            # ======================================
            # INITIAL STOP REARM
            # ======================================

            elif (
                INITIAL_STOP_LOSS_ROI_PCT > 0
                and not state["initial_stop_armed"]
                and not state["breakeven_armed"]
            ):

                if arm_initial_stop(position):

                    state["initial_stop_armed"] = True

            # ======================================
            # PARTIAL TAKE PROFIT
            # ======================================

            if (
                PARTIAL_TP_ROI_PCT > 0
                and PARTIAL_CLOSE_PCT > 0
                and roi >= PARTIAL_TP_ROI_PCT
                and not state["partial_tp_done"]
            ):

                if execute_partial_take_profit(
                    position
                ):

                    state["partial_tp_done"] = True

                    # After partial TP, breakeven is
                    # optional and independently
                    # controlled by its Railway variable.
                    if BREAKEVEN_TRIGGER_ROI_PCT > 0:

                        remaining = get_current_position(
                            symbol
                        )

                        if remaining:

                            if move_stop_to_breakeven(
                                remaining
                            ):

                                state["breakeven_armed"] = True
                                state["initial_stop_armed"] = False

            snapshot.append({
                "symbol": symbol,
                "roi_pct": round(roi, 2),
                "unrealized_profit": unrealized,
                "initial_stop_armed":
                    state["initial_stop_armed"],
                "breakeven_armed":
                    state["breakeven_armed"],
                "partial_tp_done":
                    state["partial_tp_done"]
            })

        # ==========================================
        # PORTFOLIO PROFIT TARGET
        # ==========================================

        if (
            PORTFOLIO_PROFIT_USD > 0
            and total_profit >= PORTFOLIO_PROFIT_USD
        ):

            log.warning(
                "Portfolio target reached: %.2f",
                total_profit
            )

            for position in positions:

                close_position(position)

            position_states.clear()

        last_snapshot = {
            "last_run": time.time(),
            "positions": snapshot,
            "total_unrealized_profit": round(
                total_profit, 4
            )
        }


# ============================================================
# MONITOR LOOP
# ============================================================

def monitor_loop():

    log.info(
        "MAQ DH Compatible V3 started"
    )

    log.info(
        "Initial SL: %s",
        (
            f"{INITIAL_STOP_LOSS_ROI_PCT}%"
            if INITIAL_STOP_LOSS_ROI_PCT > 0
            else "DISABLED"
        )
    )

    log.info(
        "Breakeven: %s",
        (
            f"{BREAKEVEN_TRIGGER_ROI_PCT}%"
            if BREAKEVEN_TRIGGER_ROI_PCT > 0
            else "DISABLED"
        )
    )

    log.info(
        "Live orders: %s",
        ENABLE_LIVE_ORDERS
    )

    backoff = 60

    while True:

        try:

            if ENABLE_LIVE_ORDERS:

                check_positions()

            backoff = 60

            time.sleep(
                POLL_INTERVAL_SECONDS
            )

        except Exception as exc:

            log.exception(
                "Risk monitor error"
            )

            if is_rate_limit_error(exc):

                time.sleep(backoff)

                backoff = min(
                    backoff * 2,
                    600
                )

            else:

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )


# ============================================================
# ORIGINAL DH TRADINGVIEW MESSAGE PARSER
# ============================================================

def parse_tradingview_message(raw):

    raw = str(raw or "").strip()

    if not raw:
        raise ValueError(
            "Empty TradingView message"
        )

    # MAQ format: BUY|BTCUSDT
    match = re.fullmatch(
        r"\s*(BUY|SELL)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?\s*",
        raw,
        re.IGNORECASE
    )

    if match:

        return (
            match.group(1).upper(),
            normalize_symbol(
                match.group(2)
            )
        )

    # Original DH order-fill alert.
    #
    # DH: (Strategy) Super SmartSuper Trend:
    # order sell @ 1000 filled on BINANCE:BTCUSDT.P.
    # New strategy position is -1000

    match = re.search(
        r"\border\s+(buy|sell)"
        r"\s+@\s+[^\r\n]*?"
        r"\bfilled\s+on\s+"
        r"([A-Za-z0-9:._/\-]+)",
        raw,
        re.IGNORECASE
    )

    if match:

        return (
            match.group(1).upper(),
            normalize_symbol(
                match.group(2).rstrip(".")
            )
        )

    raise ValueError(
        "Unsupported TradingView alert message"
    )


def parse_tradingview_request(req):

    if req.is_json:

        data = req.get_json(
            silent=True
        )

        if isinstance(data, dict):

            signal = (
                data.get("signal")
                or data.get("action")
            )

            symbol = (
                data.get("symbol")
                or data.get("ticker")
            )

            if signal and symbol:

                signal = str(
                    signal
                ).upper().strip()

                return (
                    signal,
                    normalize_symbol(symbol)
                )

            for field in (
                "message",
                "text",
                "alert"
            ):

                if isinstance(
                    data.get(field),
                    str
                ):

                    return parse_tradingview_message(
                        data[field]
                    )

        elif isinstance(data, str):

            return parse_tradingview_message(
                data
            )

    return parse_tradingview_message(
        req.get_data(
            as_text=True
        )
    )


# ============================================================
# WEBHOOK SECURITY
# ============================================================

def check_webhook_secret(req):

    if not WEBHOOK_SECRET:
        return True

    supplied = req.args.get(
        "secret", ""
    )

    if not supplied:
        supplied = req.headers.get(
            "X-Webhook-Secret", ""
        )

    return hmac.compare_digest(
        supplied,
        WEBHOOK_SECRET
    )


# ============================================================
# WEB ROUTES
# ============================================================

@app.route("/", methods=["GET"])
def health():

    return jsonify({
        "status": "ok",
        "service": "MAQ DH Compatible V3",
        "live_orders": ENABLE_LIVE_ORDERS
    })


@app.route("/status", methods=["GET"])
def status():

    return jsonify({
        "service": "MAQ DH Compatible V3",
        "live_orders": ENABLE_LIVE_ORDERS,
        "testnet": BINANCE_TESTNET,
        "margin_usdt": ORDER_MARGIN_USDT,
        "leverage": ENTRY_LEVERAGE,
        "risk_settings": {
            "initial_stop_loss_roi_pct":
                INITIAL_STOP_LOSS_ROI_PCT,
            "initial_stop_enabled":
                INITIAL_STOP_LOSS_ROI_PCT > 0,
            "breakeven_trigger_roi_pct":
                BREAKEVEN_TRIGGER_ROI_PCT,
            "partial_tp_roi_pct":
                PARTIAL_TP_ROI_PCT,
            "partial_close_pct":
                PARTIAL_CLOSE_PCT,
            "final_tp_roi_pct":
                FINAL_TP_ROI_PCT,
            "portfolio_profit_usd":
                PORTFOLIO_PROFIT_USD
        },
        "snapshot": last_snapshot,
        "errors": last_errors
    })


@app.route("/webhook", methods=["POST"])
def webhook():

    if not check_webhook_secret(request):

        return jsonify({
            "status": "rejected",
            "error": "Invalid webhook secret"
        }), 401

    try:

        signal, symbol = parse_tradingview_request(
            request
        )

        log.info(
            "TradingView received: %s %s",
            signal,
            symbol
        )

        result = process_entry_signal(
            signal,
            symbol
        )

        return jsonify(result), 200

    except ValueError as exc:

        log.warning(
            "Invalid alert: %s",
            exc
        )

        return jsonify({
            "status": "error",
            "error": str(exc)
        }), 400

    except Exception as exc:

        log.exception(
            "Webhook execution error"
        )

        return jsonify({
            "status": "error",
            "error": str(exc)
        }), (
            429 if is_rate_limit_error(exc)
            else 500
        )


# ============================================================
# START BACKGROUND MONITOR
# ============================================================

def start_monitor():

    global monitor_started

    with monitor_lock:

        if monitor_started:
            return

        monitor_started = True

        thread = threading.Thread(
            target=monitor_loop,
            daemon=True,
            name="MAQ-Risk-Monitor"
        )

        thread.start()


start_monitor()


# ============================================================
# RUN APPLICATION
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv("PORT", "8080")
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
