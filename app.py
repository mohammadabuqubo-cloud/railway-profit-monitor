import os
import time
import logging
import threading
import hmac
import hashlib
import json
import urllib.parse
import urllib.request
import urllib.error
from decimal import Decimal, ROUND_DOWN

from flask import Flask, jsonify
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("binance-risk-manager")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# SAFE ENV HELPERS
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
# CONFIG
# ============================================================

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")
USE_TESTNET = env_str("BINANCE_TESTNET", "false").lower() == "true"

POLL_INTERVAL_SECONDS = env_int("POLL_INTERVAL_SECONDS", 5)

INITIAL_STOP_LOSS_ROI_PCT = env_float(
    "INITIAL_STOP_LOSS_ROI_PCT",
    20
)

BREAKEVEN_TRIGGER_ROI_PCT = env_float(
    "BREAKEVEN_TRIGGER_ROI_PCT",
    10
)

PARTIAL_TP_ROI_PCT = env_float(
    "PARTIAL_TP_ROI_PCT",
    20
)

PARTIAL_CLOSE_PCT = env_float(
    "PARTIAL_CLOSE_PCT",
    50
)

FINAL_TP_ROI_PCT = env_float(
    "FINAL_TP_ROI_PCT",
    60
)

PORTFOLIO_PROFIT_USD = env_float(
    "PORTFOLIO_PROFIT_USD",
    0
)


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    testnet=USE_TESTNET
)

BINANCE_FUTURES_BASE = (
    "https://testnet.binancefuture.com"
    if USE_TESTNET
    else "https://fapi.binance.com"
)


# ============================================================
# RUNTIME STATE
# ============================================================

position_states = {}

last_snapshot = {
    "last_run": None,
    "positions": [],
    "total_unrealized_profit": 0.0
}

last_errors = {}

BOT_ALGO_PREFIX = "MAQSL"


# ============================================================
# SIGNED BINANCE ALGO REQUEST
# ============================================================

def binance_algo_request(method, path, params=None):

    if params is None:
        params = {}

    params = dict(params)

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query = urllib.parse.urlencode(params)

    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    signed_query = query + "&signature=" + signature

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded"
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
        url = BINANCE_FUTURES_BASE + path + "?" + signed_query

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
        )

    except Exception as exc:

        raise RuntimeError(
            f"Binance Algo request failed: {exc}"
        )


# ============================================================
# POSITION HELPERS
# ============================================================

def position_key(p):

    return (
        f"{p['symbol']}:"
        f"{p.get('positionSide', 'BOTH')}"
    )


def get_open_positions():

    positions = client.futures_position_information()

    return [
        p
        for p in positions
        if abs(float(p["positionAmt"])) > 0
    ]


def position_margin(p):

    try:
        value = float(
            p.get("positionInitialMargin", 0) or 0
        )

        if value > 0:
            return value

    except Exception:
        pass

    try:
        isolated = float(
            p.get("isolatedMargin", 0) or 0
        )

        if isolated > 0:
            return isolated

    except Exception:
        pass

    try:
        notional = abs(
            float(p.get("notional", 0) or 0)
        )

        leverage = float(
            p.get("leverage", 1) or 1
        )

        if leverage > 0:
            return notional / leverage

    except Exception:
        pass

    return 0.0


def calculate_roi_pct(p):

    margin = position_margin(p)

    if margin <= 0:
        return 0.0

    unrealized = float(
        p.get("unRealizedProfit", 0)
        or p.get("unrealizedProfit", 0)
        or 0
    )

    return (unrealized / margin) * 100.0


def get_current_position(
    symbol,
    position_side="BOTH"
):

    positions = client.futures_position_information(
        symbol=symbol
    )

    for p in positions:

        if (
            p["symbol"] == symbol
            and p.get("positionSide", "BOTH")
            == position_side
            and abs(float(p["positionAmt"])) > 0
        ):
            return p

    return None


# ============================================================
# EXCHANGE FILTERS
# ============================================================

def get_symbol_info(symbol):

    exchange_info = client.futures_exchange_info()

    for item in exchange_info["symbols"]:
        if item["symbol"] == symbol:
            return item

    return None


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

    qty_decimal = Decimal(
        str(abs(qty))
    )

    rounded = (
        qty_decimal / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step

    return float(rounded)


def round_price(symbol, price):

    tick = get_tick_size(symbol)

    value = Decimal(
        str(price)
    )

    rounded = (
        value / tick
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * tick

    return format(
        rounded,
        "f"
    )


# ============================================================
# MARKET CLOSE
# ============================================================

def close_position_quantity(
    p,
    quantity
):

    symbol = p["symbol"]

    position_amt = float(
        p["positionAmt"]
    )

    if position_amt == 0:
        return False

    quantity = round_quantity_down(
        symbol,
        quantity
    )

    if quantity <= 0:
        return False

    close_side = (
        SIDE_SELL
        if position_amt > 0
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
        params[
            "positionSide"
        ] = position_side

    try:

        result = client.futures_create_order(
            **params
        )

        log.info(
            "Market close %s qty=%s",
            symbol,
            quantity
        )

        return result

    except Exception as exc:

        last_errors[
            position_key(p)
        ] = str(exc)

        log.exception(
            "Failed market close %s",
            symbol
        )

        return False


def close_position(p):

    return close_position_quantity(
        p,
        abs(float(p["positionAmt"]))
    )


# ============================================================
# ALGO ORDER MANAGEMENT
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

    if isinstance(result, list):
        return result

    return []


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

            algo_position_side = order.get(
                "positionSide",
                "BOTH"
            )

            if (
                position_side != "BOTH"
                and algo_position_side
                != position_side
            ):
                continue

            algo_id = order.get(
                "algoId"
            )

            if algo_id:

                cancel_algo_order(
                    symbol,
                    algo_id
                )

                log.info(
                    "Cancelled bot stop %s algoId=%s",
                    symbol,
                    algo_id
                )

        return True

    except Exception as exc:

        last_errors[
            f"{symbol}:{position_side}"
        ] = str(exc)

        log.exception(
            "Failed cancelling Algo stop %s",
            symbol
        )

        return False


# ============================================================
# CREATE PROTECTIVE STOP
# ============================================================

def create_protective_stop(
    p,
    stop_price,
    reason
):

    symbol = p["symbol"]

    position_amt = float(
        p["positionAmt"]
    )

    if position_amt == 0:
        return False

    position_side = p.get(
        "positionSide",
        "BOTH"
    )

    close_side = (
        "SELL"
        if position_amt > 0
        else "BUY"
    )

    stop_price = round_price(
        symbol,
        stop_price
    )

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
        "triggerPrice": stop_price,
        "workingType": "MARK_PRICE",
        "closePosition": "true",
        "clientAlgoId": client_algo_id
    }

    if position_side != "BOTH":

        params[
            "positionSide"
        ] = position_side

    try:

        response = binance_algo_request(
            "POST",
            "/fapi/v1/algoOrder",
            params
        )

        last_errors.pop(
            position_key(p),
            None
        )

        log.info(
            "%s stop armed %s at %s",
            reason,
            symbol,
            stop_price
        )

        return response

    except Exception as exc:

        last_errors[
            position_key(p)
        ] = str(exc)

        log.exception(
            "Failed creating protective stop %s",
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

    position_amt = float(
        p["positionAmt"]
    )

    if leverage <= 0:
        leverage = 1

    adverse_fraction = (
        INITIAL_STOP_LOSS_ROI_PCT
        / 100.0
        / leverage
    )

    if position_amt > 0:

        return entry * (
            1 - adverse_fraction
        )

    return entry * (
        1 + adverse_fraction
    )


def arm_initial_stop_loss(p):

    stop_price = (
        calculate_initial_stop_price(p)
    )

    return create_protective_stop(
        p,
        stop_price,
        "INITIAL"
    )


# ============================================================
# BREAKEVEN
# ============================================================

def get_breakeven_price(p):

    try:

        be = float(
            p.get(
                "breakEvenPrice",
                0
            )
            or 0
        )

        if be > 0:
            return be

    except Exception:
        pass

    return float(
        p["entryPrice"]
    )


def move_stop_to_breakeven(p):

    return create_protective_stop(
        p,
        get_breakeven_price(p),
        "BREAKEVEN"
    )


# ============================================================
# PARTIAL TP
# ============================================================

def execute_partial_take_profit(p):

    current_qty = abs(
        float(p["positionAmt"])
    )

    qty_to_close = (
        current_qty
        * PARTIAL_CLOSE_PCT
        / 100.0
    )

    result = close_position_quantity(
        p,
        qty_to_close
    )

    if result:

        log.info(
            "%s partial TP %.2f%%",
            p["symbol"],
            PARTIAL_CLOSE_PCT
        )

        return True

    return False


# ============================================================
# STATE
# ============================================================

def get_or_create_state(p):

    key = position_key(p)

    state = position_states.get(
        key
    )

    if state is None:

        state = {
            "entry_price":
                float(p["entryPrice"]),

            "initial_stop_armed":
                False,

            "breakeven_armed":
                False,

            "partial_tp_done":
                False
        }

        position_states[
            key
        ] = state

    return state


# ============================================================
# MAIN MONITOR
# ============================================================

def check_positions():

    global last_snapshot

    open_positions = get_open_positions()

    current_keys = {
        position_key(p)
        for p in open_positions
    }

    for key in list(
        position_states.keys()
    ):

        if key not in current_keys:

            position_states.pop(
                key,
                None
            )

    snapshot_positions = []

    total_unrealized = 0.0

    for p in open_positions:

        symbol = p["symbol"]

        position_side = p.get(
            "positionSide",
            "BOTH"
        )

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

        total_unrealized += unrealized

        log.info(
            "%s ROI %.2f%%",
            symbol,
            roi
        )

        # ====================================================
        # HARD SOFTWARE STOP
        # ROI <= -20% → MARKET CLOSE IMMEDIATELY
        # ====================================================

        if roi <= -INITIAL_STOP_LOSS_ROI_PCT:

            log.warning(
                "%s ROI %.2f%% <= -%.2f%% HARD STOP",
                symbol,
                roi,
                INITIAL_STOP_LOSS_ROI_PCT
            )

            if close_position(p):

                cancel_bot_stops(
                    symbol,
                    position_side
                )

                position_states.pop(
                    key,
                    None
                )

            continue

        # ====================================================
        # FINAL TP
        # ROI >= +60% → CLOSE ALL REMAINING
        # ====================================================

        if roi >= FINAL_TP_ROI_PCT:

            log.info(
                "%s ROI %.2f%% FINAL TP",
                symbol,
                roi
            )

            if close_position(p):

                cancel_bot_stops(
                    symbol,
                    position_side
                )

                position_states.pop(
                    key,
                    None
                )

            continue

        # ====================================================
        # BREAKEVEN
        # ROI >= +10%
        # ====================================================

        if (
            roi >= BREAKEVEN_TRIGGER_ROI_PCT
            and not state[
                "breakeven_armed"
            ]
        ):

            result = move_stop_to_breakeven(
                p
            )

            if result:

                state[
                    "breakeven_armed"
                ] = True

                state[
                    "initial_stop_armed"
                ] = False

        # ====================================================
        # INITIAL PROTECTIVE STOP
        # ====================================================

        elif (
            not state[
                "initial_stop_armed"
            ]
            and not state[
                "breakeven_armed"
            ]
        ):

            result = arm_initial_stop_loss(
                p
            )

            if result:

                state[
                    "initial_stop_armed"
                ] = True

        # ====================================================
        # PARTIAL TAKE PROFIT
        # ROI >= +20% → CLOSE 50%
        # ====================================================

        if (
            roi >= PARTIAL_TP_ROI_PCT
            and not state[
                "partial_tp_done"
            ]
        ):

            if execute_partial_take_profit(
                p
            ):

                state[
                    "partial_tp_done"
                ] = True

                time.sleep(0.5)

                remaining = get_current_position(
                    symbol,
                    position_side
                )

                if remaining:

                    result = move_stop_to_breakeven(
                        remaining
                    )

                    if result:

                        state[
                            "breakeven_armed"
                        ] = True

                        state[
                            "initial_stop_armed"
                        ] = False

        snapshot_positions.append(
            {
                "symbol":
                    symbol,

                "positionSide":
                    position_side,

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

                "unrealizedProfit":
                    unrealized,

                "roiPct":
                    round(
                        roi,
                        2
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

                "lastError":
                    last_errors.get(
                        key
                    )
            }
        )

    # ========================================================
    # OPTIONAL PORTFOLIO EXIT
    # 0 = DISABLED
    # ========================================================

    if (
        PORTFOLIO_PROFIT_USD > 0
        and total_unrealized
        >= PORTFOLIO_PROFIT_USD
    ):

        log.warning(
            "Portfolio target reached %.2f >= %.2f",
            total_unrealized,
            PORTFOLIO_PROFIT_USD
        )

        fresh_positions = get_open_positions()

        for p in fresh_positions:

            close_position(p)

            cancel_bot_stops(
                p["symbol"],
                p.get(
                    "positionSide",
                    "BOTH"
                )
            )

        position_states.clear()

    last_snapshot = {
        "last_run":
            time.time(),

        "positions":
            snapshot_positions,

        "total_unrealized_profit":
            round(
                total_unrealized,
                8
            )
    }


# ============================================================
# LOOP
# ============================================================

def monitor_loop():

    log.info(
        "BINANCE-ONLY manager started"
    )

    log.info(
        "ALL open Futures positions will be monitored"
    )

    log.info(
        "SL -%.2f%% | BE +%.2f%% | "
        "Partial +%.2f%% close %.2f%% | "
        "Final +%.2f%%",
        INITIAL_STOP_LOSS_ROI_PCT,
        BREAKEVEN_TRIGGER_ROI_PCT,
        PARTIAL_TP_ROI_PCT,
        PARTIAL_CLOSE_PCT,
        FINAL_TP_ROI_PCT
    )

    while True:

        try:

            check_positions()

        except Exception:

            log.exception(
                "Monitor iteration failed"
            )

        time.sleep(
            POLL_INTERVAL_SECONDS
        )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def health():

    return jsonify(
        {
            "status": "ok",
            "service": "binance-risk-manager",
            "mode": "BINANCE_ONLY"
        }
    )


@app.route("/status")
def status():

    return jsonify(
        {
            "mode":
                "BINANCE_ONLY",

            "config": {
                "poll_interval_seconds":
                    POLL_INTERVAL_SECONDS,

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
                    PORTFOLIO_PROFIT_USD
            },

            "position_count":
                len(
                    last_snapshot.get(
                        "positions",
                        []
                    )
                ),

            "position_states":
                position_states,

            "snapshot":
                last_snapshot,

            "errors":
                last_errors
        }
    )


# ============================================================
# START
# ============================================================

def start_monitor():

    thread = threading.Thread(
        target=monitor_loop,
        daemon=True
    )

    thread.start()


start_monitor()


if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            8080
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
