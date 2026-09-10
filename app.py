import os
import time
import logging
import threading
import hmac
import hashlib
import base64
import json
import urllib.request
import urllib.error

from decimal import Decimal, ROUND_DOWN, ROUND_UP

from flask import Flask, jsonify

from binance.client import Client
from binance.enums import (
    SIDE_BUY,
    SIDE_SELL,
    ORDER_TYPE_MARKET,
)

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("profit-monitor")

# =========================================================
# FLASK
# =========================================================

app = Flask(__name__)

# =========================================================
# CONFIG - RAILWAY ENVIRONMENT VARIABLES
# =========================================================

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")

USE_TESTNET = (
    os.environ.get("BINANCE_TESTNET", "false").lower() == "true"
)

POLL_INTERVAL_SECONDS = int(
    os.environ.get("POLL_INTERVAL_SECONDS", "5")
)

# ---------------------------------------------------------
# POSITION MANAGEMENT
# ---------------------------------------------------------

# Initial emergency stop:
# Example: 20 means close/protect at approximately -20% ROI.
INITIAL_STOP_LOSS_ROI_PCT = float(
    os.environ.get("INITIAL_STOP_LOSS_ROI_PCT", "20")
)

# Once ROI reaches this value, stop moves to breakeven.
BREAKEVEN_TRIGGER_ROI_PCT = float(
    os.environ.get("BREAKEVEN_TRIGGER_ROI_PCT", "10")
)

# First take-profit level.
PARTIAL_TP_ROI_PCT = float(
    os.environ.get("PARTIAL_TP_ROI_PCT", "20")
)

# Percentage of POSITION to close at first TP.
PARTIAL_CLOSE_PCT = float(
    os.environ.get("PARTIAL_CLOSE_PCT", "50")
)

# Final take-profit.
FINAL_TP_ROI_PCT = float(
    os.environ.get("FINAL_TP_ROI_PCT", "60")
)

# Close all positions when TOTAL floating PnL reaches this USD amount.
# Set to 0 to disable.
PORTFOLIO_PROFIT_USD = float(
    os.environ.get("PORTFOLIO_PROFIT_USD", "0")
)

# =========================================================
# WUNDERTRADING CONFIG
# =========================================================

WUNDER_API_KEY = os.environ.get("WUNDER_API_KEY")
WUNDER_API_SECRET = os.environ.get("WUNDER_API_SECRET")

WUNDER_PROFILE_CODE = os.environ.get(
    "WUNDER_PROFILE_CODE"
)

WUNDER_BASE_URL = "https://wundertrading.com"
WUNDER_RECV_WINDOW = "60000"

# =========================================================
# MASTER TRADING CONFIG
# =========================================================

WT_MASTER_AMOUNT = float(
    os.environ.get("WT_MASTER_AMOUNT", "50")
)

WT_MASTER_LEVERAGE = int(
    os.environ.get("WT_MASTER_LEVERAGE", "16")
)

WT_PAIRS_RAW = os.environ.get(
    "WT_PAIRS",
    ""
)

WT_PAIRS = [
    pair.strip().upper()
    for pair in WT_PAIRS_RAW.split(",")
    if pair.strip()
]

# =========================================================
# WARNINGS
# =========================================================

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    log.warning(
        "BINANCE_API_KEY / BINANCE_API_SECRET not set."
    )

if not WUNDER_API_KEY or not WUNDER_API_SECRET:
    log.warning(
        "WUNDER_API_KEY / WUNDER_API_SECRET not configured."
    )

if not WUNDER_PROFILE_CODE:
    log.warning(
        "WUNDER_PROFILE_CODE not configured."
    )

if not WT_PAIRS:
    log.warning(
        "WT_PAIRS is empty."
    )

# =========================================================
# BINANCE CLIENT
# =========================================================

client = Client(
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    testnet=USE_TESTNET,
)

# =========================================================
# STATE
# =========================================================

latest_snapshot = {
    "positions": [],
    "total_unrealized_profit": 0.0,
    "last_run": None,
}

# State for each currently open position.
#
# Example:
#
# BTCUSDT:BOTH:
# {
#     "entry_price": 50000,
#     "initial_stop_armed": True,
#     "breakeven_armed": True,
#     "partial_tp_done": True
# }

position_states = {}

wunder_status = {
    "configured": bool(
        WUNDER_API_KEY
        and WUNDER_API_SECRET
        and WUNDER_PROFILE_CODE
    ),
    "connected": False,
    "last_test": None,
    "http_status": None,
    "message": "Not tested yet",
}

# =========================================================
# WUNDERTRADING AUTHENTICATION
# =========================================================

def generate_wunder_signature(
    method,
    path,
    timestamp,
    recv_window,
    body=""
):

    payload = "\n".join([
        method.upper(),
        path,
        timestamp,
        recv_window,
        body,
    ])

    signature = base64.b64encode(
        hmac.new(
            WUNDER_API_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")

    return signature


# =========================================================
# WUNDERTRADING REQUEST
# =========================================================

def wunder_request(
    method,
    path,
    body=None,
):

    if not WUNDER_API_KEY or not WUNDER_API_SECRET:
        raise RuntimeError(
            "WunderTrading API credentials are not configured."
        )

    method = method.upper()

    if body is None:
        body_string = ""
    else:
        body_string = json.dumps(
            body,
            separators=(",", ":"),
        )

    timestamp = str(
        int(time.time() * 1000)
    )

    signature = generate_wunder_signature(
        method,
        path,
        timestamp,
        WUNDER_RECV_WINDOW,
        body_string,
    )

    headers = {
        "Accept": "application/json",
        "X-API-Key": WUNDER_API_KEY,
        "X-Signature": signature,
        "X-Timestamp": timestamp,
        "X-Recv-Window": WUNDER_RECV_WINDOW,
    }

    request_data = None

    if body_string:

        request_data = body_string.encode(
            "utf-8"
        )

        headers["Content-Type"] = "application/json"

    url = WUNDER_BASE_URL + path

    req = urllib.request.Request(
        url=url,
        data=request_data,
        headers=headers,
        method=method,
    )

    try:

        with urllib.request.urlopen(
            req,
            timeout=20,
        ) as response:

            raw = response.read().decode(
                "utf-8"
            )

            status_code = response.status

            if raw:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = {"raw": raw}
            else:
                data = None

            return status_code, data

    except urllib.error.HTTPError as e:

        error_body = e.read().decode(
            "utf-8",
            errors="replace",
        )

        try:
            error_data = json.loads(
                error_body
            )
        except Exception:
            error_data = {
                "raw": error_body
            }

        raise RuntimeError(
            f"WunderTrading HTTP {e.code}: {error_data}"
        )

    except urllib.error.URLError as e:

        raise RuntimeError(
            f"WunderTrading connection error: {e}"
        )


# =========================================================
# WUNDERTRADING TEST
# =========================================================

def test_wunder_connection():

    path = "/open_api/strategies/live"

    try:

        status_code, data = wunder_request(
            "GET",
            path,
        )

        wunder_status["configured"] = True
        wunder_status["connected"] = (
            status_code == 200
        )

        wunder_status["last_test"] = time.time()
        wunder_status["http_status"] = status_code

        wunder_status["message"] = (
            "WunderTrading API authentication successful"
        )

        return {
            "success": True,
            "http_status": status_code,
        }

    except Exception as e:

        wunder_status["connected"] = False
        wunder_status["last_test"] = time.time()
        wunder_status["message"] = str(e)

        return {
            "success": False,
            "message": str(e),
        }


# =========================================================
# WUNDER PROFILES
# =========================================================

def get_wunder_profiles():

    path = "/open_api/api_profiles"

    status_code, data = wunder_request(
        "GET",
        path,
    )

    return {
        "success": True,
        "http_status": status_code,
        "data": data,
    }


# =========================================================
# CREATE WUNDER TRADE
# =========================================================

def create_wunder_trade(
    pair,
    side,
):

    if side not in [
        "long",
        "short",
    ]:

        raise ValueError(
            "Side must be long or short."
        )

    if pair not in WT_PAIRS:

        raise ValueError(
            f"{pair} is not configured in WT_PAIRS."
        )

    if WT_MASTER_AMOUNT <= 0:

        raise ValueError(
            "WT_MASTER_AMOUNT must be greater than zero."
        )

    # Set Binance Futures leverage
    client.futures_change_leverage(
        symbol=pair,
        leverage=WT_MASTER_LEVERAGE,
    )

    log.info(
        "Binance leverage set | %s | %sx",
        pair,
        WT_MASTER_LEVERAGE,
    )

    path = "/open_api/strategies/trade"

    client_id = (
        f"railway-{pair.lower()}-"
        f"{side}-"
        f"{int(time.time() * 1000)}"
    )

    if len(client_id) < 32:

        client_id += (
            "x"
            * (
                32
                - len(client_id)
            )
        )

    if len(client_id) > 64:

        client_id = client_id[:64]

    payload = {
        "clientId": client_id,
        "exchangeCode": "BINANCE_FUTURES",
        "pairCode": pair,
        "profilesCodes": [
            WUNDER_PROFILE_CODE
        ],
        "side": side,
        "orderType": "market",
        "amountPerTrade": WT_MASTER_AMOUNT,
        "amountPerTradeType": "quote",
        "leverage": WT_MASTER_LEVERAGE,
    }

    status_code, data = wunder_request(
        "POST",
        path,
        payload,
    )

    return {
        "success": True,
        "http_status": status_code,
        "pair": pair,
        "side": side,
        "amount": WT_MASTER_AMOUNT,
        "leverage": WT_MASTER_LEVERAGE,
        "data": data,
    }


# =========================================================
# POSITION KEY
# =========================================================

def position_key(p):

    symbol = p["symbol"]

    position_side = p.get(
        "positionSide",
        "BOTH",
    )

    return f"{symbol}:{position_side}"


# =========================================================
# TICK SIZE
# =========================================================

def get_tick_size(symbol):

    try:

        info = client.futures_exchange_info()

        for s in info["symbols"]:

            if s["symbol"] == symbol:

                for f in s["filters"]:

                    if f["filterType"] == "PRICE_FILTER":

                        return float(
                            f["tickSize"]
                        )

    except Exception:

        log.exception(
            "Failed to get tick size for %s",
            symbol,
        )

    return None


# =========================================================
# QUANTITY STEP SIZE
# =========================================================

def get_quantity_step_size(symbol):

    try:

        info = client.futures_exchange_info()

        for s in info["symbols"]:

            if s["symbol"] == symbol:

                # Prefer MARKET_LOT_SIZE for market orders.
                for f in s["filters"]:

                    if f["filterType"] == "MARKET_LOT_SIZE":

                        return float(
                            f["stepSize"]
                        )

                for f in s["filters"]:

                    if f["filterType"] == "LOT_SIZE":

                        return float(
                            f["stepSize"]
                        )

    except Exception:

        log.exception(
            "Failed to get quantity step size for %s",
            symbol,
        )

    return None


# =========================================================
# ROUND PRICE
# =========================================================

def round_price_to_tick(
    price,
    tick_size,
    side,
):

    if not tick_size:
        return price

    price_decimal = Decimal(
        str(price)
    )

    tick_decimal = Decimal(
        str(tick_size)
    )

    ticks = (
        price_decimal
        / tick_decimal
    )

    if side == SIDE_SELL:

        rounded_ticks = ticks.quantize(
            Decimal("1"),
            rounding=ROUND_DOWN,
        )

    else:

        rounded_ticks = ticks.quantize(
            Decimal("1"),
            rounding=ROUND_UP,
        )

    return float(
        rounded_ticks
        * tick_decimal
    )


# =========================================================
# ROUND QUANTITY
# =========================================================

def round_quantity_down(
    quantity,
    step_size,
):

    if not step_size:
        return quantity

    qty_decimal = Decimal(
        str(quantity)
    )

    step_decimal = Decimal(
        str(step_size)
    )

    steps = (
        qty_decimal
        / step_decimal
    ).quantize(
        Decimal("1"),
        rounding=ROUND_DOWN,
    )

    return float(
        steps
        * step_decimal
    )


# =========================================================
# POSITION MARGIN
# =========================================================

def position_margin(p):

    # Binance may expose position initial margin directly.
    for field in [
        "positionInitialMargin",
        "initialMargin",
    ]:

        try:

            value = float(
                p.get(
                    field,
                    0,
                )
                or 0
            )

            if value > 0:
                return value

        except Exception:
            pass

    # Fallback calculation.
    leverage = float(
        p.get(
            "leverage",
            1,
        )
        or 1
    )

    if leverage <= 0:
        leverage = 1

    notional = abs(
        float(
            p.get(
                "notional",
                0,
            )
            or 0
        )
    )

    if notional <= 0:

        notional = abs(
            float(
                p["positionAmt"]
            )
            * float(
                p["markPrice"]
            )
        )

    return (
        notional
        / leverage
    )


# =========================================================
# CALCULATE ROI
# =========================================================

def calculate_roi_pct(p):

    unrealized = float(
        p.get(
            "unRealizedProfit",
            0,
        )
        or 0
    )

    margin = position_margin(p)

    if margin <= 0:
        return 0.0

    return (
        unrealized
        / margin
        * 100.0
    )


# =========================================================
# GET CURRENT POSITION
# =========================================================

def get_current_position(
    symbol,
    position_side="BOTH",
):

    try:

        positions = (
            client
            .futures_position_information(
                symbol=symbol
            )
        )

        for p in positions:

            if (
                p.get(
                    "positionSide",
                    "BOTH",
                )
                == position_side
                and
                float(
                    p["positionAmt"]
                ) != 0
            ):

                return p

    except Exception:

        log.exception(
            "Failed to refresh position %s",
            symbol,
        )

    return None


# =========================================================
# CLOSE POSITION QUANTITY
# =========================================================

def close_position_quantity(
    symbol,
    current_position_amt,
    quantity,
    reason,
    position_side="BOTH",
):

    if quantity <= 0:
        return None

    close_side = (
        SIDE_SELL
        if current_position_amt > 0
        else SIDE_BUY
    )

    step_size = get_quantity_step_size(
        symbol
    )

    quantity = round_quantity_down(
        abs(quantity),
        step_size,
    )

    if quantity <= 0:

        log.warning(
            "Close quantity rounded to zero for %s",
            symbol,
        )

        return None

    try:

        params = {
            "symbol": symbol,
            "side": close_side,
            "type": ORDER_TYPE_MARKET,
            "quantity": quantity,
        }

        if position_side == "BOTH":

            params["reduceOnly"] = True

        else:

            params["positionSide"] = position_side

        order = client.futures_create_order(
            **params
        )

        log.info(
            "CLOSED %s | Qty %.8f | %s",
            symbol,
            quantity,
            reason,
        )

        return order

    except Exception:

        log.exception(
            "Failed closing quantity for %s",
            symbol,
        )

        return None


# =========================================================
# CLOSE ENTIRE POSITION
# =========================================================

def close_position(
    symbol,
    position_amt,
    reason,
    position_side="BOTH",
):

    return close_position_quantity(
        symbol=symbol,
        current_position_amt=position_amt,
        quantity=abs(position_amt),
        reason=reason,
        position_side=position_side,
    )


# =========================================================
# CANCEL EXISTING PROTECTIVE STOP LOSSES
# =========================================================

def cancel_existing_stop_losses(
    symbol,
    close_side,
    position_side="BOTH",
):

    try:

        orders = client.futures_get_open_orders(
            symbol=symbol
        )

        for order in orders:

            order_type = str(
                order.get(
                    "type",
                    "",
                )
            ).upper()

            order_side = order.get(
                "side"
            )

            order_position_side = order.get(
                "positionSide",
                "BOTH",
            )

            reduce_only = bool(
                order.get(
                    "reduceOnly",
                    False,
                )
            )

            close_position_flag = bool(
                order.get(
                    "closePosition",
                    False,
                )
            )

            is_stop = (
                order_type
                in [
                    "STOP",
                    "STOP_MARKET",
                ]
            )

            correct_side = (
                order_side
                == close_side
            )

            correct_position = (
                position_side == "BOTH"
                or
                order_position_side == position_side
            )

            is_protective = (
                reduce_only
                or
                close_position_flag
                or
                position_side != "BOTH"
            )

            if (
                is_stop
                and
                correct_side
                and
                correct_position
                and
                is_protective
            ):

                client.futures_cancel_order(
                    symbol=symbol,
                    orderId=order[
                        "orderId"
                    ],
                )

                log.info(
                    "Cancelled old SL | %s | Order %s",
                    symbol,
                    order["orderId"],
                )

    except Exception:

        log.exception(
            "Failed cancelling stop loss for %s",
            symbol,
        )


# =========================================================
# CREATE PROTECTIVE STOP
# =========================================================

def create_protective_stop(
    p,
    stop_price,
    reason,
):

    symbol = p["symbol"]

    position_amt = float(
        p["positionAmt"]
    )

    position_side = p.get(
        "positionSide",
        "BOTH",
    )

    if position_amt == 0:
        return False

    close_side = (
        SIDE_SELL
        if position_amt > 0
        else SIDE_BUY
    )

    tick_size = get_tick_size(
        symbol
    )

    stop_price = round_price_to_tick(
        stop_price,
        tick_size,
        close_side,
    )

    try:

        cancel_existing_stop_losses(
            symbol,
            close_side,
            position_side,
        )

        params = {
            "symbol": symbol,
            "side": close_side,
            "type": "STOP_MARKET",
            "stopPrice": stop_price,
            "quantity": abs(
                position_amt
            ),
            "workingType": "MARK_PRICE",
        }

        if position_side == "BOTH":

            params["reduceOnly"] = True

        else:

            params["positionSide"] = position_side

        order = client.futures_create_order(
            **params
        )

        log.info(
            "%s | %s | Qty %.8f | Stop %.8f",
            reason,
            symbol,
            abs(position_amt),
            stop_price,
        )

        return True

    except Exception:

        log.exception(
            "Failed creating protective stop for %s",
            symbol,
        )

        return False


# =========================================================
# INITIAL -20% ROI STOP PRICE
# =========================================================

def calculate_initial_stop_price(p):

    entry_price = float(
        p["entryPrice"]
    )

    position_amt = float(
        p["positionAmt"]
    )

    leverage = float(
        p.get(
            "leverage",
            1,
        )
        or 1
    )

    if entry_price <= 0:
        return None

    if leverage <= 0:
        leverage = 1

    # Example:
    #
    # -20% ROI at 16x leverage:
    #
    # 20 / 16 = approximately 1.25% adverse price move.
    #
    price_move_pct = (
        INITIAL_STOP_LOSS_ROI_PCT
        / leverage
    )

    price_move_fraction = (
        price_move_pct
        / 100.0
    )

    if position_amt > 0:

        # LONG
        stop_price = (
            entry_price
            * (
                1.0
                - price_move_fraction
            )
        )

    else:

        # SHORT
        stop_price = (
            entry_price
            * (
                1.0
                + price_move_fraction
            )
        )

    return stop_price


# =========================================================
# ARM INITIAL STOP LOSS
# =========================================================

def arm_initial_stop_loss(p):

    stop_price = calculate_initial_stop_price(
        p
    )

    if not stop_price:
        return False

    return create_protective_stop(
        p,
        stop_price,
        (
            f"INITIAL SL "
            f"-{INITIAL_STOP_LOSS_ROI_PCT:.1f}% ROI"
        ),
    )


# =========================================================
# MOVE STOP TO BREAKEVEN
# =========================================================

def move_stop_to_breakeven(p):

    entry_price = float(
        p["entryPrice"]
    )

    # Prefer Binance's breakEvenPrice if available.
    try:

        break_even_price = float(
            p.get(
                "breakEvenPrice",
                0,
            )
            or 0
        )

    except Exception:

        break_even_price = 0

    if break_even_price <= 0:

        break_even_price = entry_price

    return create_protective_stop(
        p,
        break_even_price,
        "BREAKEVEN SL",
    )


# =========================================================
# PARTIAL CLOSE
# =========================================================

def execute_partial_take_profit(
    p,
):

    symbol = p["symbol"]

    position_amt = float(
        p["positionAmt"]
    )

    position_side = p.get(
        "positionSide",
        "BOTH",
    )

    if position_amt == 0:
        return False

    close_quantity = (
        abs(position_amt)
        * (
            PARTIAL_CLOSE_PCT
            / 100.0
        )
    )

    order = close_position_quantity(
        symbol=symbol,
        current_position_amt=position_amt,
        quantity=close_quantity,
        reason=(
            f"PARTIAL TP "
            f"{PARTIAL_CLOSE_PCT:.1f}% "
            f"at +{PARTIAL_TP_ROI_PCT:.1f}% ROI"
        ),
        position_side=position_side,
    )

    if not order:
        return False

    # Give Binance a moment to update the remaining quantity.
    time.sleep(0.75)

    remaining_position = get_current_position(
        symbol,
        position_side,
    )

    if remaining_position:

        # Recreate breakeven stop using the NEW smaller quantity.
        move_stop_to_breakeven(
            remaining_position
        )

    return True


# =========================================================
# CHECK POSITIONS
# =========================================================

def check_positions():

    try:

        positions = (
            client
            .futures_position_information()
        )

    except Exception:

        log.exception(
            "Failed to fetch positions"
        )

        return

    open_positions = [
        p
        for p in positions
        if float(
            p["positionAmt"]
        ) != 0
    ]

    active_keys = {
        position_key(p)
        for p in open_positions
    }

    # Remove state when position is completely closed.
    for key in list(
        position_states.keys()
    ):

        if key not in active_keys:

            position_states.pop(
                key,
                None,
            )

    total_unrealized = 0.0
    snapshot = []

    # =====================================================
    # INDIVIDUAL POSITION MANAGEMENT
    # =====================================================

    for p in open_positions:

        symbol = p["symbol"]

        position_amt = float(
            p["positionAmt"]
        )

        position_side = p.get(
            "positionSide",
            "BOTH",
        )

        entry_price = float(
            p["entryPrice"]
        )

        mark_price = float(
            p["markPrice"]
        )

        leverage = float(
            p.get(
                "leverage",
                1,
            )
            or 1
        )

        unrealized = float(
            p["unRealizedProfit"]
        )

        total_unrealized += unrealized

        roi_pct = calculate_roi_pct(
            p
        )

        key = position_key(
            p
        )

        # -----------------------------------------------
        # CREATE STATE FOR NEW POSITION
        # -----------------------------------------------

        if key not in position_states:

            position_states[key] = {
                "entry_price": entry_price,
                "initial_stop_armed": False,
                "breakeven_armed": False,
                "partial_tp_done": False,
            }

        state = position_states[
            key
        ]

        # -----------------------------------------------
        # DETECT A NEW TRADE ON SAME SYMBOL
        # -----------------------------------------------

        old_entry_price = float(
            state.get(
                "entry_price",
                0,
            )
        )

        if (
            old_entry_price > 0
            and
            abs(
                entry_price
                - old_entry_price
            )
            > 0.00000001
        ):

            log.info(
                "New position detected | %s | Entry changed %.8f -> %.8f",
                symbol,
                old_entry_price,
                entry_price,
            )

            state = {
                "entry_price": entry_price,
                "initial_stop_armed": False,
                "breakeven_armed": False,
                "partial_tp_done": False,
            }

            position_states[
                key
            ] = state

        # -----------------------------------------------
        # 1. INITIAL STOP LOSS
        # -----------------------------------------------

        if (
            not state[
                "initial_stop_armed"
            ]
            and
            not state[
                "breakeven_armed"
            ]
        ):

            success = arm_initial_stop_loss(
                p
            )

            if success:

                state[
                    "initial_stop_armed"
                ] = True

        # -----------------------------------------------
        # BACKUP HARD -20% ROI EXIT
        #
        # The exchange STOP_MARKET should normally fire
        # first. This is a second protection layer.
        # -----------------------------------------------

        if (
            roi_pct
            <= -INITIAL_STOP_LOSS_ROI_PCT
        ):

            close_position(
                symbol,
                position_amt,
                (
                    f"HARD SL "
                    f"{roi_pct:.2f}% ROI"
                ),
                position_side,
            )

            continue

        # -----------------------------------------------
        # 2. FINAL TP +60%
        #
        # Check BEFORE partial TP so if ROI jumps directly
        # above +60%, the bot closes everything.
        # -----------------------------------------------

        if (
            roi_pct
            >= FINAL_TP_ROI_PCT
        ):

            close_position(
                symbol,
                position_amt,
                (
                    f"FINAL TP "
                    f"+{roi_pct:.2f}% ROI"
                ),
                position_side,
            )

            continue

        # -----------------------------------------------
        # 3. BREAKEVEN AT +10%
        # -----------------------------------------------

        if (
            roi_pct
            >= BREAKEVEN_TRIGGER_ROI_PCT
            and
            not state[
                "breakeven_armed"
            ]
        ):

            success = move_stop_to_breakeven(
                p
            )

            if success:

                state[
                    "breakeven_armed"
                ] = True

                log.info(
                    "BREAKEVEN ACTIVATED | %s | ROI %.2f%%",
                    symbol,
                    roi_pct,
                )

        # -----------------------------------------------
        # 4. CLOSE 50% AT +20%
        # -----------------------------------------------

        if (
            roi_pct
            >= PARTIAL_TP_ROI_PCT
            and
            not state[
                "partial_tp_done"
            ]
        ):

            success = execute_partial_take_profit(
                p
            )

            if success:

                state[
                    "partial_tp_done"
                ] = True

                state[
                    "breakeven_armed"
                ] = True

                log.info(
                    "PARTIAL TP COMPLETE | %s | "
                    "%.1f%% of position closed",
                    symbol,
                    PARTIAL_CLOSE_PCT,
                )

        # -----------------------------------------------
        # SNAPSHOT
        # -----------------------------------------------

        snapshot.append({

            "symbol":
                symbol,

            "positionSide":
                position_side,

            "positionAmt":
                position_amt,

            "entryPrice":
                entry_price,

            "markPrice":
                mark_price,

            "leverage":
                leverage,

            "unrealizedProfit":
                round(
                    unrealized,
                    4,
                ),

            "roiPct":
                round(
                    roi_pct,
                    2,
                ),

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
                ],

        })

    # =====================================================
    # SNAPSHOT UPDATE
    # =====================================================

    latest_snapshot[
        "positions"
    ] = snapshot

    latest_snapshot[
        "total_unrealized_profit"
    ] = round(
        total_unrealized,
        4,
    )

    latest_snapshot[
        "last_run"
    ] = time.time()

    # =====================================================
    # PORTFOLIO PROFIT EXIT
    #
    # 0 = DISABLED
    # =====================================================

    if (
        PORTFOLIO_PROFIT_USD > 0
        and
        total_unrealized
        >= PORTFOLIO_PROFIT_USD
        and
        open_positions
    ):

        log.info(
            "PORTFOLIO TP HIT | Total +$%.2f",
            total_unrealized,
        )

        for p in open_positions:

            position_amt = float(
                p["positionAmt"]
            )

            if position_amt != 0:

                close_position(
                    p["symbol"],
                    position_amt,
                    (
                        f"PORTFOLIO TP "
                        f"+${total_unrealized:.2f}"
                    ),
                    p.get(
                        "positionSide",
                        "BOTH",
                    ),
                )


# =========================================================
# MONITOR LOOP
# =========================================================

def monitor_loop():

    log.info(
        "STARTING MONITOR | "
        "Initial SL -%.1f%% ROI | "
        "BE +%.1f%% ROI | "
        "Partial TP +%.1f%% ROI / %.1f%% position | "
        "Final TP +%.1f%% ROI | "
        "Portfolio TP $%.2f | "
        "Poll %ss",
        INITIAL_STOP_LOSS_ROI_PCT,
        BREAKEVEN_TRIGGER_ROI_PCT,
        PARTIAL_TP_ROI_PCT,
        PARTIAL_CLOSE_PCT,
        FINAL_TP_ROI_PCT,
        PORTFOLIO_PROFIT_USD,
        POLL_INTERVAL_SECONDS,
    )

    while True:

        try:

            check_positions()

        except Exception:

            log.exception(
                "Unexpected monitor-loop error"
            )

        time.sleep(
            POLL_INTERVAL_SECONDS
        )


# =========================================================
# HEALTH
# =========================================================

@app.route(
    "/",
    methods=["GET"],
)
def health():

    return jsonify({

        "status":
            "ok",

        "service":
            "binance-profit-monitor",

        "wundertrading_configured":
            bool(
                WUNDER_API_KEY
                and
                WUNDER_API_SECRET
                and
                WUNDER_PROFILE_CODE
            ),

    }), 200


# =========================================================
# STATUS
# =========================================================

@app.route(
    "/status",
    methods=["GET"],
)
def status():

    return jsonify({

        "config": {

            "initial_stop_loss_roi_pct":
                INITIAL_STOP_LOSS_ROI_PCT,

            "breakeven_trigger_roi_pct":
                BREAKEVEN_TRIGGER_ROI_PCT,

            "partial_tp_roi_pct":
                PARTIAL_TP_ROI_PCT,

            "partial_close_pct":
                PARTIAL_CLOSE_PCT,

            "final_tp_roi_pct":
                FINAL_TP_ROI_PCT,

            "portfolio_profit_usd":
                PORTFOLIO_PROFIT_USD,

            "poll_interval_seconds":
                POLL_INTERVAL_SECONDS,

            "wt_master_amount":
                WT_MASTER_AMOUNT,

            "wt_master_leverage":
                WT_MASTER_LEVERAGE,

            "wt_pairs":
                WT_PAIRS,
        },

        "snapshot":
            latest_snapshot,

        "position_states":
            position_states,

        "wundertrading":
            wunder_status,

    }), 200


# =========================================================
# WUNDER TEST
# =========================================================

@app.route(
    "/wunder-test",
    methods=["GET"],
)
def wunder_test():

    result = test_wunder_connection()

    if result.get(
        "success"
    ):

        return jsonify(
            result
        ), 200

    return jsonify(
        result
    ), 500


# =========================================================
# WUNDER PROFILES
# =========================================================

@app.route(
    "/wunder-profiles",
    methods=["GET"],
)
def wunder_profiles():

    try:

        return jsonify(
            get_wunder_profiles()
        ), 200

    except Exception as e:

        return jsonify({

            "success":
                False,

            "message":
                str(e),

        }), 500


# =========================================================
# CREATE ONE TRADE
# =========================================================

@app.route(
    "/trade/<pair>/<side>",
    methods=["POST"],
)
def trade_pair(
    pair,
    side,
):

    try:

        result = create_wunder_trade(
            pair.upper(),
            side.lower(),
        )

        return jsonify(
            result
        ), 200

    except Exception as e:

        log.exception(
            "Trade failed"
        )

        return jsonify({

            "success":
                False,

            "message":
                str(e),

        }), 500


# =========================================================
# START MONITOR
# =========================================================

_thread = threading.Thread(
    target=monitor_loop,
    daemon=True,
)

_thread.start()


# =========================================================
# START FLASK
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            8080,
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
