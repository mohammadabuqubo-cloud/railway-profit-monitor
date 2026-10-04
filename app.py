import os
import time
import json
import hmac
import hashlib
import logging
import threading
import urllib.parse
import urllib.request
import urllib.error

from decimal import Decimal, ROUND_DOWN, ROUND_UP

from flask import Flask, jsonify, request
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("MAQ-BINANCE-V2")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# ENV HELPERS
# ============================================================

def env_float(name, default):
    raw = os.environ.get(name)

    if raw is None or str(raw).strip() == "":
        return float(default)

    return float(raw)


def env_int(name, default):
    raw = os.environ.get(name)

    if raw is None or str(raw).strip() == "":
        return int(default)

    return int(raw)


def env_str(name, default=""):
    raw = os.environ.get(name)

    if raw is None:
        return default

    return str(raw).strip()


# ============================================================
# BINANCE CONFIG
# ============================================================

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")

BINANCE_TESTNET = (
    env_str("BINANCE_TESTNET", "false").lower()
    == "true"
)


# ============================================================
# ENTRY CONFIG
#
# THESE TWO REMAIN CONTROLLED FROM RAILWAY
# ============================================================

ORDER_MARGIN_USDT = env_float(
    "ORDER_MARGIN_USDT",
    10
)

ENTRY_LEVERAGE = env_int(
    "ENTRY_LEVERAGE",
    10
)


# ============================================================
# LOCKED RISK RULES
#
# IMPORTANT:
#
# These are intentionally NOT read from Railway environment
# variables in this version.
#
# This prevents an old/wrong Railway variable from changing
# TP1 / BE / SL behavior.
# ============================================================

INITIAL_STOP_LOSS_ROI_PCT = 20.0

BREAKEVEN_TRIGGER_ROI_PCT = 10.0

PARTIAL_TP_ROI_PCT = 20.0

PARTIAL_CLOSE_PCT = 50.0

FINAL_TP_ROI_PCT = 60.0

PORTFOLIO_PROFIT_USD = 0.0


# ============================================================
# MONITOR CONFIG
# ============================================================

POLL_INTERVAL_SECONDS = max(
    5,
    env_int(
        "POLL_INTERVAL_SECONDS",
        5
    )
)

ENTRY_GRACE_SECONDS = 5

EXCHANGE_INFO_CACHE_SECONDS = 21600


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
# RUNTIME STATE
# ============================================================

BOT_ALGO_PREFIX = "MAQSL"

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

exchange_info_lock = threading.Lock()

entry_lock = threading.Lock()

monitor_start_lock = threading.Lock()

monitor_started = False


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def normalize_symbol(raw_symbol):

    symbol = str(raw_symbol).upper().strip()

    if ":" in symbol:
        symbol = symbol.split(":")[-1]

    if symbol.endswith(".P"):
        symbol = symbol[:-2]

    symbol = symbol.replace("/", "")
    symbol = symbol.replace("-", "")
    symbol = symbol.replace("_", "")

    return symbol


# ============================================================
# SIGNED BINANCE ALGO REQUEST
# ============================================================

def binance_algo_request(method, path, params=None):

    params = dict(params or {})

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query = urllib.parse.urlencode(params)

    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    signed_query = (
        query
        + "&signature="
        + signature
    )

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type":
            "application/x-www-form-urlencoded"
    }

    method = method.upper()

    if method == "POST":

        url = BINANCE_FUTURES_BASE + path

        req = urllib.request.Request(
            url,
            data=signed_query.encode("utf-8"),
            headers=headers,
            method="POST"
        )

    else:

        url = (
            BINANCE_FUTURES_BASE
            + path
            + "?"
            + signed_query
        )

        req = urllib.request.Request(
            url,
            headers=headers,
            method=method
        )

    try:

        with urllib.request.urlopen(
            req,
            timeout=15
        ) as response:

            text = response.read().decode("utf-8")

            if not text:
                return {}

            return json.loads(text)

    except urllib.error.HTTPError as exc:

        body = exc.read().decode(
            "utf-8",
            errors="replace"
        )

        raise RuntimeError(
            f"Binance Algo HTTP {exc.code}: {body}"
        ) from exc


# ============================================================
# EXCHANGE INFO
# ============================================================

def refresh_exchange_info_cache(force=False):

    now = time.time()

    age = (
        now
        - exchange_info_cache["loaded_at"]
    )

    if (
        not force
        and exchange_info_cache["symbols"]
        and age < EXCHANGE_INFO_CACHE_SECONDS
    ):
        return

    with exchange_info_lock:

        info = client.futures_exchange_info()

        exchange_info_cache["symbols"] = {
            item["symbol"]: item
            for item in info.get("symbols", [])
        }

        exchange_info_cache["loaded_at"] = (
            time.time()
        )


def get_symbol_info(symbol):

    refresh_exchange_info_cache()

    return exchange_info_cache[
        "symbols"
    ].get(symbol)


def validate_futures_symbol(symbol):

    info = get_symbol_info(symbol)

    if info is None:

        raise ValueError(
            f"{symbol} is not a Binance Futures symbol"
        )

    status = str(
        info.get("status", "")
    ).upper()

    if status and status != "TRADING":

        raise ValueError(
            f"{symbol} is not currently trading"
        )

    return info


# ============================================================
# PRECISION
# ============================================================

def get_quantity_step_size(symbol):

    info = get_symbol_info(symbol)

    if info:

        for f in info["filters"]:

            if f["filterType"] == "LOT_SIZE":

                return Decimal(
                    str(f["stepSize"])
                )

    return Decimal("0.001")


def get_tick_size(symbol):

    info = get_symbol_info(symbol)

    if info:

        for f in info["filters"]:

            if f["filterType"] == "PRICE_FILTER":

                return Decimal(
                    str(f["tickSize"])
                )

    return Decimal("0.00000001")


def round_quantity_down(symbol, qty):

    step = get_quantity_step_size(symbol)

    value = Decimal(str(abs(qty)))

    rounded = (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step

    return float(rounded)


def round_stop_price(
    symbol,
    price,
    position_amt
):

    tick = get_tick_size(symbol)

    value = Decimal(str(price))

    # LONG stop:
    # round down so stop isn't accidentally moved closer.
    #
    # SHORT stop:
    # round up for the same reason.

    rounding_mode = (
        ROUND_DOWN
        if position_amt > 0
        else ROUND_UP
    )

    rounded = (
        value / tick
    ).to_integral_value(
        rounding=rounding_mode
    ) * tick

    return format(rounded, "f")


# ============================================================
# POSITION HELPERS
# ============================================================

def position_key(p):

    return (
        f"{p['symbol']}:"
        f"{p.get('positionSide', 'BOTH')}"
    )


def get_open_positions():

    positions = (
        client.futures_position_information()
    )

    return [
        p
        for p in positions
        if abs(
            float(p["positionAmt"])
        ) > 0
    ]


def get_any_symbol_position(symbol):

    positions = (
        client.futures_position_information(
            symbol=symbol
        )
    )

    for p in positions:

        if abs(
            float(p["positionAmt"])
        ) > 0:

            return p

    return None


def get_current_position(
    symbol,
    position_side="BOTH"
):

    positions = (
        client.futures_position_information(
            symbol=symbol
        )
    )

    for p in positions:

        if (
            p["symbol"] == symbol
            and
            p.get(
                "positionSide",
                "BOTH"
            ) == position_side
            and
            abs(
                float(p["positionAmt"])
            ) > 0
        ):

            return p

    return None


# ============================================================
# ROI CALCULATION — V2
#
# IMPORTANT FIX
#
# We do NOT calculate the trading trigger from Binance's
# positionInitialMargin anymore.
#
# LONG:
# ((mark - entry) / entry) * leverage * 100
#
# SHORT:
# ((entry - mark) / entry) * leverage * 100
#
# Example:
#
# 10x leverage
# +2% price move
# = approximately +20% ROI
# ============================================================

def calculate_roi_pct(p):

    entry = float(
        p.get("entryPrice", 0) or 0
    )

    mark = float(
        p.get("markPrice", 0) or 0
    )

    leverage = float(
        p.get("leverage", 1) or 1
    )

    amount = float(
        p.get("positionAmt", 0) or 0
    )

    if (
        entry <= 0
        or mark <= 0
        or leverage <= 0
        or amount == 0
    ):

        return 0.0

    if amount > 0:

        price_return_pct = (
            (mark - entry)
            / entry
        ) * 100.0

    else:

        price_return_pct = (
            (entry - mark)
            / entry
        ) * 100.0

    roi = (
        price_return_pct
        * leverage
    )

    return roi


# ============================================================
# STATE
# ============================================================

def create_fresh_state(p):

    key = position_key(p)

    state = {
        "entry_price":
            float(p["entryPrice"]),

        "opened_at":
            time.time(),

        "initial_stop_armed":
            False,

        "breakeven_armed":
            False,

        "partial_tp_done":
            False
    }

    position_states[key] = state

    return state


def get_or_create_state(p):

    key = position_key(p)

    current_entry = float(
        p["entryPrice"]
    )

    state = position_states.get(key)

    if state is None:

        return create_fresh_state(p)

    old_entry = float(
        state.get(
            "entry_price",
            0
        )
    )

    tolerance = max(
        abs(current_entry) * 0.000001,
        0.0000000001
    )

    if abs(
        current_entry - old_entry
    ) > tolerance:

        log.warning(
            "NEW ENTRY DETECTED | %s | old=%s new=%s",
            key,
            old_entry,
            current_entry
        )

        return create_fresh_state(p)

    return state


# ============================================================
# CLOSE POSITION
# ============================================================

def close_position_quantity(
    p,
    quantity,
    reason
):

    symbol = p["symbol"]

    amount = float(
        p["positionAmt"]
    )

    if amount == 0:
        return False

    quantity = round_quantity_down(
        symbol,
        quantity
    )

    if quantity <= 0:
        return False

    close_side = (
        SIDE_SELL
        if amount > 0
        else SIDE_BUY
    )

    position_side = p.get(
        "positionSide",
        "BOTH"
    )

    params = {
        "symbol": symbol,
        "side": close_side,
        "type": ORDER_TYPE_MARKET,
        "quantity": quantity
    }

    if position_side == "BOTH":

        params["reduceOnly"] = True

    else:

        params["positionSide"] = (
            position_side
        )

    log.warning(
        "CLOSE REASON=%s | %s | qty=%s | side=%s",
        reason,
        symbol,
        quantity,
        close_side
    )

    try:

        result = (
            client.futures_create_order(
                **params
            )
        )

        log.warning(
            "POSITION CLOSE EXECUTED | %s | reason=%s",
            symbol,
            reason
        )

        return result

    except Exception as exc:

        last_errors[
            position_key(p)
        ] = str(exc)

        log.exception(
            "POSITION CLOSE FAILED | %s",
            symbol
        )

        return False


def close_position(
    p,
    reason
):

    return close_position_quantity(
        p,
        abs(
            float(p["positionAmt"])
        ),
        reason
    )


# ============================================================
# ALGO STOP MANAGEMENT
# ============================================================

def get_open_algo_orders(symbol):

    result = binance_algo_request(
        "GET",
        "/fapi/v1/openAlgoOrders",
        {
            "symbol": symbol,
            "algoType": "CONDITIONAL"
        }
    )

    return (
        result
        if isinstance(result, list)
        else []
    )


def cancel_algo_order(
    symbol,
    algo_id
):

    return binance_algo_request(
        "DELETE",
        "/fapi/v1/algoOrder",
        {
            "symbol": symbol,
            "algoId": algo_id
        }
    )


def cancel_bot_stops(
    symbol,
    position_side="BOTH"
):

    try:

        orders = get_open_algo_orders(
            symbol
        )

        for order in orders:

            client_id = str(
                order.get(
                    "clientAlgoId",
                    ""
                )
            )

            if not client_id.startswith(
                BOT_ALGO_PREFIX
            ):
                continue

            order_position_side = (
                order.get(
                    "positionSide",
                    "BOTH"
                )
            )

            if (
                position_side != "BOTH"
                and
                order_position_side
                != position_side
            ):
                continue

            algo_id = order.get("algoId")

            if algo_id:

                cancel_algo_order(
                    symbol,
                    algo_id
                )

                log.info(
                    "BOT STOP CANCELLED | %s | algoId=%s",
                    symbol,
                    algo_id
                )

        return True

    except Exception as exc:

        log.exception(
            "STOP CANCEL FAILED | %s",
            symbol
        )

        return False


# ============================================================
# PROTECTIVE STOP
# ============================================================

def create_protective_stop(
    p,
    stop_price,
    reason
):

    symbol = p["symbol"]

    amount = float(
        p["positionAmt"]
    )

    if amount == 0:
        return False

    position_side = p.get(
        "positionSide",
        "BOTH"
    )

    mark = float(
        p.get("markPrice", 0) or 0
    )

    entry = float(
        p.get("entryPrice", 0) or 0
    )

    rounded_stop = round_stop_price(
        symbol,
        stop_price,
        amount
    )

    numeric_stop = float(
        rounded_stop
    )

    close_side = (
        "SELL"
        if amount > 0
        else "BUY"
    )

    log.warning(
        "ARM STOP | %s | reason=%s | "
        "entry=%s | mark=%s | trigger=%s",
        symbol,
        reason,
        entry,
        mark,
        rounded_stop
    )

    # ========================================================
    # SAFETY CHECK
    # ========================================================

    if mark > 0:

        if (
            amount > 0
            and numeric_stop >= mark
        ):

            log.error(
                "STOP NOT ARMED | %s | "
                "LONG trigger=%s >= mark=%s",
                symbol,
                numeric_stop,
                mark
            )

            return False

        if (
            amount < 0
            and numeric_stop <= mark
        ):

            log.error(
                "STOP NOT ARMED | %s | "
                "SHORT trigger=%s <= mark=%s",
                symbol,
                numeric_stop,
                mark
            )

            return False

    cancel_bot_stops(
        symbol,
        position_side
    )

    client_algo_id = (
        f"{BOT_ALGO_PREFIX}_"
        f"{reason[:4]}_"
        f"{int(time.time())}"
    )

    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": close_side,
        "type": "STOP_MARKET",
        "triggerPrice": rounded_stop,
        "workingType": "MARK_PRICE",
        "closePosition": "true",
        "clientAlgoId": client_algo_id
    }

    if position_side != "BOTH":

        params["positionSide"] = (
            position_side
        )

    try:

        result = binance_algo_request(
            "POST",
            "/fapi/v1/algoOrder",
            params
        )

        log.warning(
            "STOP ARMED | %s | reason=%s | trigger=%s",
            symbol,
            reason,
            rounded_stop
        )

        return result

    except Exception as exc:

        last_errors[
            position_key(p)
        ] = str(exc)

        log.exception(
            "STOP CREATION FAILED | %s",
            symbol
        )

        return False


# ============================================================
# INITIAL STOP
# ============================================================

def calculate_initial_stop_price(p):

    entry = float(
        p["entryPrice"]
    )

    leverage = float(
        p.get("leverage", 1) or 1
    )

    amount = float(
        p["positionAmt"]
    )

    adverse_price_fraction = (
        INITIAL_STOP_LOSS_ROI_PCT
        / 100.0
        / leverage
    )

    if amount > 0:

        stop = (
            entry
            * (
                1.0
                - adverse_price_fraction
            )
        )

    else:

        stop = (
            entry
            * (
                1.0
                + adverse_price_fraction
            )
        )

    log.info(
        "INITIAL STOP CALC | %s | "
        "entry=%s | leverage=%sx | "
        "SL ROI=-%.2f%% | stop=%s",
        p["symbol"],
        entry,
        leverage,
        INITIAL_STOP_LOSS_ROI_PCT,
        stop
    )

    return stop


def arm_initial_stop(p):

    return create_protective_stop(
        p,
        calculate_initial_stop_price(p),
        "INITIAL"
    )


# ============================================================
# BREAKEVEN
# ============================================================

def move_stop_to_breakeven(p):

    entry = float(
        p["entryPrice"]
    )

    return create_protective_stop(
        p,
        entry,
        "BREAKEVEN"
    )


# ============================================================
# ENTRY QUANTITY
# ============================================================

def get_market_price(symbol):

    result = (
        client.futures_symbol_ticker(
            symbol=symbol
        )
    )

    return float(
        result["price"]
    )


def set_leverage(symbol):

    result = (
        client.futures_change_leverage(
            symbol=symbol,
            leverage=ENTRY_LEVERAGE
        )
    )

    log.warning(
        "LEVERAGE SET | %s | %sx",
        symbol,
        ENTRY_LEVERAGE
    )

    return result


def calculate_entry_quantity(symbol):

    price = get_market_price(symbol)

    notional = (
        ORDER_MARGIN_USDT
        * ENTRY_LEVERAGE
    )

    raw_qty = (
        notional
        / price
    )

    qty = round_quantity_down(
        symbol,
        raw_qty
    )

    if qty <= 0:

        raise ValueError(
            f"Quantity calculated as zero for {symbol}"
        )

    return (
        qty,
        price,
        notional
    )


# ============================================================
# OPEN POSITION
# ============================================================

def open_market_position(
    symbol,
    signal
):

    qty, price, notional = (
        calculate_entry_quantity(symbol)
    )

    side = (
        SIDE_BUY
        if signal == "BUY"
        else SIDE_SELL
    )

    log.warning(
        "OPENING POSITION | %s | %s | "
        "qty=%s | market≈%s | "
        "margin=$%.2f | leverage=%sx | "
        "notional≈$%.2f",
        signal,
        symbol,
        qty,
        price,
        ORDER_MARGIN_USDT,
        ENTRY_LEVERAGE,
        notional
    )

    result = (
        client.futures_create_order(
            symbol=symbol,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )
    )

    log.warning(
        "ENTRY EXECUTED | %s | %s | orderId=%s",
        signal,
        symbol,
        result.get("orderId")
    )

    return result


# ============================================================
# WAIT FOR POSITION
# ============================================================

def wait_for_position(
    symbol,
    timeout=8
):

    start = time.time()

    while (
        time.time() - start
        < timeout
    ):

        p = get_any_symbol_position(
            symbol
        )

        if p:
            return p

        time.sleep(0.25)

    return None


def wait_for_position_close(
    symbol,
    timeout=10
):

    start = time.time()

    while (
        time.time() - start
        < timeout
    ):

        p = get_any_symbol_position(
            symbol
        )

        if p is None:
            return True

        time.sleep(0.5)

    return False


# ============================================================
# PROCESS SIGNAL
# ============================================================

def process_entry_signal(
    signal,
    raw_symbol
):

    signal = str(
        signal
    ).upper().strip()

    symbol = normalize_symbol(
        raw_symbol
    )

    if signal not in (
        "BUY",
        "SELL"
    ):

        raise ValueError(
            "Signal must be BUY or SELL"
        )

    validate_futures_symbol(
        symbol
    )

    with entry_lock:

        current = get_any_symbol_position(
            symbol
        )

        # ====================================================
        # EXISTING POSITION
        # ====================================================

        if current:

            amount = float(
                current["positionAmt"]
            )

            current_direction = (
                "BUY"
                if amount > 0
                else "SELL"
            )

            # SAME DIRECTION
            if current_direction == signal:

                log.warning(
                    "SIGNAL IGNORED | %s | %s | "
                    "same direction already open",
                    signal,
                    symbol
                )

                return {
                    "status": "ignored",
                    "reason":
                        "same_direction_position_exists",
                    "symbol": symbol,
                    "signal": signal
                }

            # OPPOSITE DIRECTION
            log.warning(
                "REVERSAL SIGNAL | %s | %s | "
                "closing existing %s",
                signal,
                symbol,
                current_direction
            )

            cancel_bot_stops(
                symbol,
                current.get(
                    "positionSide",
                    "BOTH"
                )
            )

            closed = close_position(
                current,
                f"OPPOSITE_{signal}_SIGNAL"
            )

            if not closed:

                raise RuntimeError(
                    f"Could not close existing {symbol}"
                )

            if not wait_for_position_close(
                symbol
            ):

                raise RuntimeError(
                    f"{symbol} did not close in time"
                )

            position_states.pop(
                position_key(current),
                None
            )

        # ====================================================
        # OPEN NEW POSITION
        # ====================================================

        set_leverage(symbol)

        order = open_market_position(
            symbol,
            signal
        )

        new_position = wait_for_position(
            symbol
        )

        stop_armed = False

        if new_position:

            state = create_fresh_state(
                new_position
            )

            stop_armed = bool(
                arm_initial_stop(
                    new_position
                )
            )

            state[
                "initial_stop_armed"
            ] = stop_armed

        else:

            log.error(
                "POSITION NOT FOUND AFTER ENTRY | %s",
                symbol
            )

        return {
            "status": "executed",
            "signal": signal,
            "symbol": symbol,
            "orderId":
                order.get("orderId"),
            "margin_usdt":
                ORDER_MARGIN_USDT,
            "leverage":
                ENTRY_LEVERAGE,
            "initial_stop_armed":
                stop_armed
        }


# ============================================================
# BACKGROUND WEBHOOK PROCESSOR
# ============================================================

def process_signal_background(
    signal,
    symbol
):

    try:

        log.warning(
            "BACKGROUND PROCESS START | %s | %s",
            signal,
            symbol
        )

        result = process_entry_signal(
            signal,
            symbol
        )

        log.warning(
            "BACKGROUND PROCESS COMPLETE | %s",
            result
        )

    except Exception as exc:

        last_errors[
            f"ENTRY:{normalize_symbol(symbol)}"
        ] = str(exc)

        log.exception(
            "BACKGROUND SIGNAL FAILED | %s | %s",
            signal,
            symbol
        )


# ============================================================
# RISK MONITOR
# ============================================================

def check_positions():

    global last_snapshot

    positions = get_open_positions()

    active_keys = {
        position_key(p)
        for p in positions
    }

    # Clean stale state
    for key in list(
        position_states.keys()
    ):

        if key not in active_keys:

            log.info(
                "POSITION GONE | %s | clearing state",
                key
            )

            position_states.pop(
                key,
                None
            )

    snapshot = []

    total_unrealized = 0.0

    for p in positions:

        symbol = p["symbol"]

        key = position_key(p)

        state = get_or_create_state(p)

        roi = calculate_roi_pct(p)

        unrealized = float(
            p.get(
                "unRealizedProfit",
                0
            )
            or p.get(
                "unrealizedProfit",
                0
            )
            or 0
        )

        total_unrealized += (
            unrealized
        )

        age = (
            time.time()
            - float(
                state.get(
                    "opened_at",
                    time.time()
                )
            )
        )

        # ====================================================
        # VERY IMPORTANT DEBUG LINE
        # ====================================================

        log.info(
            "ROI CHECK | %s | "
            "current=%.2f%% | "
            "SL=-%.2f%% | "
            "BE=+%.2f%% | "
            "TP1=+%.2f%% | "
            "TP1_CLOSE=%.0f%% | "
            "FINAL=+%.2f%% | "
            "age=%.1fs",
            symbol,
            roi,
            INITIAL_STOP_LOSS_ROI_PCT,
            BREAKEVEN_TRIGGER_ROI_PCT,
            PARTIAL_TP_ROI_PCT,
            PARTIAL_CLOSE_PCT,
            FINAL_TP_ROI_PCT,
            age
        )

        # ====================================================
        # HARD STOP
        # ====================================================

        if (
            age >= ENTRY_GRACE_SECONDS
            and
            roi <= -INITIAL_STOP_LOSS_ROI_PCT
        ):

            log.warning(
                "SL TRIGGER | %s | ROI=%.2f%%",
                symbol,
                roi
            )

            if close_position(
                p,
                "HARD_SOFTWARE_STOP"
            ):

                cancel_bot_stops(
                    symbol,
                    p.get(
                        "positionSide",
                        "BOTH"
                    )
                )

                position_states.pop(
                    key,
                    None
                )

            continue

        # ====================================================
        # FINAL TP +60%
        # ====================================================

        if (
            roi >= FINAL_TP_ROI_PCT
        ):

            log.warning(
                "FINAL TP TRIGGER | %s | "
                "ROI=%.2f%% >= %.2f%%",
                symbol,
                roi,
                FINAL_TP_ROI_PCT
            )

            if close_position(
                p,
                "FINAL_TP_60_ROI"
            ):

                cancel_bot_stops(
                    symbol,
                    p.get(
                        "positionSide",
                        "BOTH"
                    )
                )

                position_states.pop(
                    key,
                    None
                )

            continue

        # ====================================================
        # PARTIAL TP +20%
        #
        # THIS CONDITION IS EXPLICIT.
        #
        # A ROI OF +0.15 CANNOT PASS THIS CONDITION.
        # ====================================================

        if (
            roi >= 20.0
            and
            not state["partial_tp_done"]
        ):

            log.warning(
                "TP1 TRIGGER | %s | "
                "ROI=%.2f%% >= 20.00%%",
                symbol,
                roi
            )

            current_qty = abs(
                float(
                    p["positionAmt"]
                )
            )

            qty_to_close = (
                current_qty
                * 0.50
            )

            result = (
                close_position_quantity(
                    p,
                    qty_to_close,
                    "PARTIAL_TP_20_ROI_CLOSE_50"
                )
            )

            if result:

                state[
                    "partial_tp_done"
                ] = True

                log.warning(
                    "TP1 COMPLETE | %s | "
                    "50%% closed at ROI %.2f%%",
                    symbol,
                    roi
                )

                time.sleep(0.75)

                remaining = (
                    get_current_position(
                        symbol,
                        p.get(
                            "positionSide",
                            "BOTH"
                        )
                    )
                )

                if remaining:

                    if move_stop_to_breakeven(
                        remaining
                    ):

                        state[
                            "breakeven_armed"
                        ] = True

                        state[
                            "initial_stop_armed"
                        ] = False

                        log.warning(
                            "TP1 -> BREAKEVEN | %s",
                            symbol
                        )

            continue

        # ====================================================
        # BREAKEVEN +10%
        #
        # This does NOT close anything.
        #
        # It only replaces the initial stop with entry price.
        # ====================================================

        if (
            roi >= 10.0
            and
            not state["breakeven_armed"]
        ):

            log.warning(
                "BE TRIGGER | %s | "
                "ROI=%.2f%% >= 10.00%%",
                symbol,
                roi
            )

            if move_stop_to_breakeven(p):

                state[
                    "breakeven_armed"
                ] = True

                state[
                    "initial_stop_armed"
                ] = False

                log.warning(
                    "BREAKEVEN ARMED | %s | entry=%s",
                    symbol,
                    p["entryPrice"]
                )

        # ====================================================
        # ENSURE INITIAL STOP EXISTS
        # ====================================================

        elif (
            not state[
                "initial_stop_armed"
            ]
            and
            not state[
                "breakeven_armed"
            ]
        ):

            if arm_initial_stop(p):

                state[
                    "initial_stop_armed"
                ] = True

        snapshot.append(
            {
                "symbol":
                    symbol,

                "positionAmt":
                    float(
                        p["positionAmt"]
                    ),

                "entryPrice":
                    float(
                        p["entryPrice"]
                    ),

                "markPrice":
                    float(
                        p.get(
                            "markPrice",
                            0
                        )
                        or 0
                    ),

                "leverage":
                    float(
                        p.get(
                            "leverage",
                            0
                        )
                        or 0
                    ),

                "roiPct":
                    round(
                        roi,
                        3
                    ),

                "unrealizedProfit":
                    unrealized,

                "initialStopArmed":
                    state[
                        "initial_stop_armed"
                    ],

                "breakevenArmed":
                    state[
                        "breakeven_armed"
                    ],

                "partialTpDone":
                    state[
                        "partial_tp_done"
                    ]
            }
        )

    last_snapshot = {
        "last_run":
            time.time(),

        "positions":
            snapshot,

        "total_unrealized_profit":
            round(
                total_unrealized,
                8
            )
    }


# ============================================================
# MONITOR LOOP
# ============================================================

def monitor_loop():

    log.warning(
        "================================================"
    )

    log.warning(
        "MAQ BINANCE BOT V2 STARTED"
    )

    log.warning(
        "ENTRY | margin=$%.2f | leverage=%sx",
        ORDER_MARGIN_USDT,
        ENTRY_LEVERAGE
    )

    log.warning(
        "LOCKED RISK RULES | "
        "SL=-20%% | BE=+10%% | "
        "TP1=+20%% ROI / CLOSE 50%% | "
        "FINAL=+60%%"
    )

    log.warning(
        "ROI ENGINE = PRICE MOVE x ACTUAL LEVERAGE"
    )

    log.warning(
        "POLL=%ss",
        POLL_INTERVAL_SECONDS
    )

    log.warning(
        "================================================"
    )

    while True:

        try:

            check_positions()

        except Exception:

            log.exception(
                "MONITOR ERROR"
            )

        time.sleep(
            POLL_INTERVAL_SECONDS
        )


# ============================================================
# HEALTH
# ============================================================

@app.route("/")
def health():

    return jsonify(
        {
            "status": "ok",
            "service": "MAQ Binance Bot V2",
            "webhook": "async",
            "risk_rules": {
                "SL": -20,
                "BE": 10,
                "TP1": 20,
                "TP1_close_pct": 50,
                "FINAL": 60
            }
        }
    )


# ============================================================
# STATUS
# ============================================================

@app.route("/status")
def status():

    return jsonify(
        {
            "version":
                "MAQ-BINANCE-V2",

            "entry": {
                "margin_usdt":
                    ORDER_MARGIN_USDT,

                "leverage":
                    ENTRY_LEVERAGE,

                "notional_usdt":
                    ORDER_MARGIN_USDT
                    * ENTRY_LEVERAGE
            },

            "risk": {
                "initial_stop_roi":
                    -20,

                "breakeven_roi":
                    10,

                "partial_tp_roi":
                    20,

                "partial_close_pct":
                    50,

                "final_tp_roi":
                    60,

                "portfolio_exit":
                    False
            },

            "snapshot":
                last_snapshot,

            "states":
                position_states,

            "errors":
                last_errors
        }
    )


# ============================================================
# WEBHOOK
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def tradingview_webhook():

    try:

        signal = None
        symbol = None

        # JSON
        if request.is_json:

            data = (
                request.get_json(
                    silent=True
                )
                or {}
            )

            signal = data.get(
                "signal"
            )

            symbol = data.get(
                "symbol"
            )

        # RAW TEXT
        if not signal or not symbol:

            raw = request.get_data(
                as_text=True
            ).strip()

            if "|" in raw:

                signal, symbol = (
                    raw.split(
                        "|",
                        1
                    )
                )

        if not signal or not symbol:

            return jsonify(
                {
                    "status": "error",
                    "error":
                        "Expected BUY|BTCUSDT or SELL|BTCUSDT"
                }
            ), 400

        signal = str(
            signal
        ).upper().strip()

        symbol = str(
            symbol
        ).upper().strip()

        if signal not in (
            "BUY",
            "SELL"
        ):

            return jsonify(
                {
                    "status": "error",
                    "error":
                        "Signal must be BUY or SELL"
                }
            ), 400

        normalized = normalize_symbol(
            symbol
        )

        log.warning(
            "WEBHOOK ACCEPTED | %s | %s -> %s",
            signal,
            symbol,
            normalized
        )

        # ====================================================
        # BACKGROUND PROCESSING
        #
        # TradingView receives HTTP 200 immediately.
        # ====================================================

        thread = threading.Thread(
            target=process_signal_background,
            args=(
                signal,
                symbol
            ),
            daemon=True,
            name=(
                f"TV-{signal}-{normalized}"
            )
        )

        thread.start()

        return jsonify(
            {
                "status": "accepted",
                "signal": signal,
                "symbol": normalized,
                "processing": "background"
            }
        ), 200

    except Exception as exc:

        log.exception(
            "WEBHOOK ERROR"
        )

        return jsonify(
            {
                "status": "error",
                "error": str(exc)
            }
        ), 500


# ============================================================
# START MONITOR
# ============================================================

def start_monitor():

    global monitor_started

    with monitor_start_lock:

        if monitor_started:
            return

        monitor_started = True

        thread = threading.Thread(
            target=monitor_loop,
            daemon=True,
            name="MAQ-RISK-MONITOR"
        )

        thread.start()


start_monitor()


# ============================================================
# START APP
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            8080
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
