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
    os.environ.get("POLL_INTERVAL_SECONDS", "15")
)

PER_POSITION_PROFIT_PCT = float(
    os.environ.get("PER_POSITION_PROFIT_PCT", "20")
)

PORTFOLIO_PROFIT_USD = float(
    os.environ.get("PORTFOLIO_PROFIT_USD", "15")
)

BREAKEVEN_TRIGGER_ROI_PCT = float(
    os.environ.get("BREAKEVEN_TRIGGER_ROI_PCT", "10")
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

breakeven_armed = {}

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

        headers[
            "Content-Type"
        ] = "application/json"

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

    # Set actual Binance Futures leverage
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
# BINANCE HELPERS
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

    return float(
        rounded_ticks
        * tick_decimal
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
            "quantity": abs(
                position_amt
            ),
        }

        if position_side == "BOTH":

            params[
                "reduceOnly"
            ] = True

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
# CANCEL STOP LOSS
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
                position_side
                == "BOTH"
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

                client.futures_cancel_order(
                    symbol=symbol,
                    orderId=order[
                        "orderId"
                    ],
                )

    except Exception:

        log.exception(
            "Failed cancelling stop loss for %s",
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
        return False

    if position_amt > 0:

        close_side = SIDE_SELL

        if mark_price <= entry_price:
            return False

    else:

        close_side = SIDE_BUY

        if mark_price >= entry_price:
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
            "Stop %.8f",
            symbol,
            entry_price,
            stop_price,
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

        if (
            roi_pct
            >= BREAKEVEN_TRIGGER_ROI_PCT
            and
            not breakeven_armed.get(
                key,
                False,
            )
        ):

            success = move_stop_to_breakeven(
                p
            )

            if success:

                breakeven_armed[
                    key
                ] = True

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

        if (
            roi_pct
            >= PER_POSITION_PROFIT_PCT
        ):

            close_position(
                symbol,
                position_amt,
                (
                    f"+{roi_pct:.2f}% "
                    f"margin ROI"
                ),
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

    if (
        total_unrealized
        >= PORTFOLIO_PROFIT_USD
        and
        open_positions
    ):

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
# HEALTH
# =========================================================

@app.route(
    "/",
    methods=["GET"],
)
def health():

    return jsonify({
        "status": "ok",
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

            "wt_pairs":
                WT_PAIRS,
        },

        "snapshot":
            latest_snapshot,

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
            "success": False,
            "message": str(e),
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
            "success": False,
            "message": str(e),
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
