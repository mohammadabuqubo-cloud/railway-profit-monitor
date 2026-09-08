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
# CONFIG - Railway Environment Variables
# =========================================================

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")

USE_TESTNET = (
    os.environ.get("BINANCE_TESTNET", "false").lower() == "true"
)

POLL_INTERVAL_SECONDS = int(
    os.environ.get("POLL_INTERVAL_SECONDS", "15")
)

# Close individual position when ROI reaches this level
PER_POSITION_PROFIT_PCT = float(
    os.environ.get("PER_POSITION_PROFIT_PCT", "20")
)

# Close all positions when combined unrealized profit
# reaches this amount
PORTFOLIO_PROFIT_USD = float(
    os.environ.get("PORTFOLIO_PROFIT_USD", "15")
)

# When position ROI reaches this %, move SL to entry price
BREAKEVEN_TRIGGER_ROI_PCT = float(
    os.environ.get("BREAKEVEN_TRIGGER_ROI_PCT", "10")
)

# =========================================================
# WUNDERTRADING CONFIG
# =========================================================

WUNDER_API_KEY = os.environ.get("WUNDER_API_KEY")
WUNDER_API_SECRET = os.environ.get("WUNDER_API_SECRET")

WUNDER_BASE_URL = "https://wundertrading.com"
WUNDER_RECV_WINDOW = "60000"

# These are already in Railway.
# They are NOT used to place trades yet.
WT_MASTER_AMOUNT = float(
    os.environ.get("WT_MASTER_AMOUNT", "50")
)

WT_MASTER_LEVERAGE = int(
    os.environ.get("WT_MASTER_LEVERAGE", "16")
)

# =========================================================
# CONFIG WARNINGS
# =========================================================

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    log.warning(
        "BINANCE_API_KEY / BINANCE_API_SECRET not set."
    )

if not WUNDER_API_KEY or not WUNDER_API_SECRET:
    log.warning(
        "WUNDER_API_KEY / WUNDER_API_SECRET not configured."
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

breakeven_armed = {}

wunder_status = {
    "configured": bool(
        WUNDER_API_KEY and WUNDER_API_SECRET
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
        request_data = body_string.encode("utf-8")
        headers["Content-Type"] = "application/json"

    url = WUNDER_BASE_URL + path

    request = urllib.request.Request(
        url=url,
        data=request_data,
        headers=headers,
        method=method,
    )

    try:

        with urllib.request.urlopen(
            request,
            timeout=15,
        ) as response:

            raw = response.read().decode("utf-8")
            status_code = response.status

            if raw:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = {
                        "raw": raw
                    }
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
# WUNDERTRADING CONNECTION TEST
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

        if isinstance(data, list):

            result_info = {
                "response_type": "list",
                "items_returned": len(data),
            }

        elif isinstance(data, dict):

            result_info = {
                "response_type": "object",
                "top_level_keys": list(
                    data.keys()
                )[:20],
            }

        else:

            result_info = {
                "response_type": type(
                    data
                ).__name__,
            }

        log.info(
            "WunderTrading connection successful | HTTP %s",
            status_code,
        )

        return {
            "success": True,
            "http_status": status_code,
            "message": (
                "WunderTrading API authentication successful"
            ),
            "data_info": result_info,
        }

    except Exception as e:

        wunder_status["configured"] = bool(
            WUNDER_API_KEY
            and WUNDER_API_SECRET
        )

        wunder_status["connected"] = False
        wunder_status["last_test"] = time.time()
        wunder_status["http_status"] = None
        wunder_status["message"] = str(e)

        log.exception(
            "WunderTrading connection test failed"
        )

        return {
            "success": False,
            "message": str(e),
        }

# =========================================================
# READ ONE WUNDERTRADING STRATEGY
# =========================================================

def get_wunder_strategy(
    strategy_id
):

    path = (
        f"/open_api/strategies/"
        f"{strategy_id}"
    )

    status_code, data = wunder_request(
        "GET",
        path,
    )

    return {
        "success": True,
        "http_status": status_code,
        "strategy_id_requested": strategy_id,
        "data": data,
    }

# =========================================================
# READ WUNDERTRADING API PROFILES
# =========================================================

def get_wunder_profiles():

    path = "/open_api/api-profiles"

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
# HELPERS
# =========================================================

def position_key(p):

    symbol = p["symbol"]

    position_side = p.get(
        "positionSide",
        "BOTH",
    )

    return f"{symbol}:{position_side}"


def get_tick_size(symbol):

    try:

        info = client.futures_exchange_info()

        for s in info["symbols"]:

            if s["symbol"] == symbol:

                for f in s["filters"]:

                    if (
                        f["filterType"]
                        == "PRICE_FILTER"
                    ):

                        return float(
                            f["tickSize"]
                        )

    except Exception:

        log.exception(
            "Failed to get tick size for %s",
            symbol,
        )

    return None


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

    rounded_price = (
        rounded_ticks
        * tick_decimal
    )

    return float(
        rounded_price
    )

# =========================================================
# CLOSE POSITION
# =========================================================

def close_position(
    symbol,
    position_amt,
    reason,
    position_side="BOTH",
):

    close_side = (
        SIDE_SELL
        if position_amt > 0
        else SIDE_BUY
    )

    try:

        params = {
            "symbol": symbol,
            "side": close_side,
            "type": ORDER_TYPE_MARKET,
            "quantity": abs(position_amt),
        }

        # One-way mode
        if position_side == "BOTH":

            params[
                "reduceOnly"
            ] = True

        # Hedge mode
        else:

            params[
                "positionSide"
            ] = position_side

        order = client.futures_create_order(
            **params
        )

        log.info(
            "Closed %s (%s): %s",
            symbol,
            reason,
            order,
        )

        return order

    except Exception:

        log.exception(
            "Failed to close %s",
            symbol,
        )

        return None

# =========================================================
# POSITION MARGIN
# =========================================================

def position_margin(p):

    isolated = (
        p.get(
            "isolated",
            False,
        )
        if "isolated" in p
        else
        p.get(
            "marginType"
        ) == "isolated"
    )

    leverage = float(
        p.get(
            "leverage",
            1,
        )
        or 1
    )

    notional = abs(
        float(
            p.get(
                "notional",
                0,
            )
        )
        or (
            float(
                p["positionAmt"]
            )
            *
            float(
                p["markPrice"]
            )
        )
    )

    if (
        isolated
        and
        float(
            p.get(
                "isolatedMargin",
                0,
            )
        ) > 0
    ):

        return float(
            p["isolatedMargin"]
        )

    if leverage <= 0:
        leverage = 1

    return (
        abs(notional)
        / leverage
    )

# =========================================================
# CANCEL EXISTING STOP LOSSES
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

            is_stop = order_type in [
                "STOP",
                "STOP_MARKET",
            ]

            correct_side = (
                order_side
                == close_side
            )

            correct_position = (
                position_side == "BOTH"
                or
                order_position_side
                == position_side
            )

            is_protective = (
                reduce_only
                or
                close_position_flag
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

                order_id = (
                    order["orderId"]
                )

                client.futures_cancel_order(
                    symbol=symbol,
                    orderId=order_id,
                )

                log.info(
                    "Cancelled previous stop-loss "
                    "%s orderId=%s",
                    symbol,
                    order_id,
                )

    except Exception:

        log.exception(
            "Failed checking/cancelling existing "
            "stop loss for %s",
            symbol,
        )

# =========================================================
# MOVE STOP TO BREAKEVEN
# =========================================================

def move_stop_to_breakeven(p):

    symbol = p["symbol"]

    position_amt = float(
        p["positionAmt"]
    )

    entry_price = float(
        p["entryPrice"]
    )

    mark_price = float(
        p["markPrice"]
    )

    position_side = p.get(
        "positionSide",
        "BOTH",
    )

    if position_amt == 0:
        return False

    if entry_price <= 0:

        log.warning(
            "%s has invalid entry price: %s",
            symbol,
            entry_price,
        )

        return False

    # LONG
    if position_amt > 0:

        close_side = SIDE_SELL

        if mark_price <= entry_price:

            log.warning(
                "%s returned to/below breakeven "
                "before SL could be armed.",
                symbol,
            )

            return False

    # SHORT
    else:

        close_side = SIDE_BUY

        if mark_price >= entry_price:

            log.warning(
                "%s returned to/above breakeven "
                "before SL could be armed.",
                symbol,
            )

            return False

    tick_size = get_tick_size(
        symbol
    )

    stop_price = round_price_to_tick(
        entry_price,
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

            params[
                "reduceOnly"
            ] = True

        else:

            params[
                "positionSide"
            ] = position_side

        client.futures_create_order(
            **params
        )

        log.info(
            "BREAKEVEN ARMED %s | "
            "Entry %.8f | "
            "Stop %.8f | "
            "Mark %.8f",
            symbol,
            entry_price,
            stop_price,
            mark_price,
        )

        return True

    except Exception:

        log.exception(
            "Failed moving %s stop to breakeven",
            symbol,
        )

        return False

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

    for key in list(
        breakeven_armed.keys()
    ):

        if key not in active_keys:

            breakeven_armed.pop(
                key,
                None,
            )

    total_unrealized = 0.0
    snapshot = []

    for p in open_positions:

        symbol = p["symbol"]

        position_amt = float(
            p["positionAmt"]
        )

        unrealized = float(
            p["unRealizedProfit"]
        )

        total_unrealized += unrealized

        margin = position_margin(p)

        roi_pct = (
            unrealized
            / margin
            * 100.0
        ) if margin > 0 else 0.0

        key = position_key(p)

        # =================================================
        # RULE 1:
        # BREAKEVEN
        # =================================================

        if (
            roi_pct
            >= BREAKEVEN_TRIGGER_ROI_PCT
            and
            not breakeven_armed.get(
                key,
                False,
            )
        ):

            log.info(
                "%s reached %.2f%% ROI. "
                "Breakeven trigger is %.2f%%.",
                symbol,
                roi_pct,
                BREAKEVEN_TRIGGER_ROI_PCT,
            )

            success = move_stop_to_breakeven(
                p
            )

            if success:

                breakeven_armed[
                    key
                ] = True

        # =================================================
        # SNAPSHOT
        # =================================================

        snapshot.append({
            "symbol": symbol,
            "positionAmt": position_amt,
            "entryPrice": float(
                p["entryPrice"]
            ),
            "markPrice": float(
                p["markPrice"]
            ),
            "unrealizedProfit": round(
                unrealized,
                4,
            ),
            "roiPct": round(
                roi_pct,
                2,
            ),
            "breakevenArmed": (
                breakeven_armed.get(
                    key,
                    False,
                )
            ),
        })

        # =================================================
        # RULE 2:
        # PER POSITION PROFIT
        # =================================================

        if (
            roi_pct
            >= PER_POSITION_PROFIT_PCT
        ):

            log.info(
                "%s hit %.2f%% ROI "
                "(threshold %.2f%%) — closing",
                symbol,
                roi_pct,
                PER_POSITION_PROFIT_PCT,
            )

            close_position(
                symbol,
                position_amt,
                f"+{roi_pct:.2f}% margin ROI",
                p.get(
                    "positionSide",
                    "BOTH",
                ),
            )

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
    # RULE 3:
    # PORTFOLIO COMBINED PROFIT
    # =====================================================

    if (
        total_unrealized
        >= PORTFOLIO_PROFIT_USD
        and open_positions
    ):

        log.info(
            "Portfolio combined profit "
            "$%.2f >= $%.2f — closing all positions",
            total_unrealized,
            PORTFOLIO_PROFIT_USD,
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
                        f"portfolio "
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
        "Starting monitor | "
        "Breakeven %.1f%% ROI | "
        "Position TP %.1f%% ROI | "
        "Portfolio TP $%.2f | "
        "Poll every %ss",
        BREAKEVEN_TRIGGER_ROI_PCT,
        PER_POSITION_PROFIT_PCT,
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
# WEB ENDPOINT - HEALTH
# =========================================================

@app.route(
    "/",
    methods=["GET"],
)
def health():

    return jsonify({
        "status": "ok",
        "service": "binance-profit-monitor",
        "wundertrading_configured": bool(
            WUNDER_API_KEY
            and WUNDER_API_SECRET
        ),
    }), 200

# =========================================================
# WEB ENDPOINT - STATUS
# =========================================================

@app.route(
    "/status",
    methods=["GET"],
)
def status():

    return jsonify({

        "config": {

            "breakeven_trigger_roi_pct":
                BREAKEVEN_TRIGGER_ROI_PCT,

            "per_position_profit_pct":
                PER_POSITION_PROFIT_PCT,

            "portfolio_profit_usd":
                PORTFOLIO_PROFIT_USD,

            "poll_interval_seconds":
                POLL_INTERVAL_SECONDS,

            "wt_master_amount":
                WT_MASTER_AMOUNT,

            "wt_master_leverage":
                WT_MASTER_LEVERAGE,
        },

        "snapshot":
            latest_snapshot,

        "wundertrading":
            wunder_status,

    }), 200

# =========================================================
# WEB ENDPOINT - WUNDERTRADING TEST
# =========================================================

@app.route(
    "/wunder-test",
    methods=["GET"],
)
def wunder_test():

    result = test_wunder_connection()

    if result.get("success"):

        return jsonify(
            result
        ), 200

    return jsonify(
        result
    ), 500

# =========================================================
# WEB ENDPOINT - READ STRATEGY
# =========================================================

@app.route(
    "/wunder-strategy/<strategy_id>",
    methods=["GET"],
)
def wunder_strategy(
    strategy_id
):

    try:

        result = get_wunder_strategy(
            strategy_id
        )

        return jsonify(
            result
        ), 200

    except Exception as e:

        log.exception(
            "Failed reading WunderTrading "
            "strategy %s",
            strategy_id,
        )

        return jsonify({
            "success": False,
            "strategy_id_requested":
                strategy_id,
            "message":
                str(e),
        }), 500

# =========================================================
# WEB ENDPOINT - READ API PROFILES
# =========================================================

@app.route(
    "/wunder-profiles",
    methods=["GET"],
)
def wunder_profiles():

    try:

        result = get_wunder_profiles()

        return jsonify(
            result
        ), 200

    except Exception as e:

        log.exception(
            "Failed reading WunderTrading "
            "API profiles"
        )

        return jsonify({
            "success": False,
            "message": str(e),
        }), 500

# =========================================================
# START BACKGROUND THREAD
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
