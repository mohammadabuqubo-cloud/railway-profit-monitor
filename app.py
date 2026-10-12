
import os
import re
import time
import json
import hmac
import hashlib
import logging
import sqlite3
import threading
import urllib.parse
import urllib.request
import urllib.error

from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from flask import Flask, request, jsonify
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL


# ============================================================
# MAQ DH RAILWAY BOT V4
# FAST WEBHOOK / PERSISTENT QUEUE / BINANCE FUTURES
#
# - Original DH TradingView alert messages
# - Existing BUY|SYMBOL / SELL|SYMBOL messages
# - Fast webhook acknowledgment
# - Persistent SQLite queue
# - Duplicate alert suppression
# - One-way mode only (no Binance Hedge Mode)
# - Opposite-signal reversals
# - Railway-controlled stop loss and take profit
# - Empty initial stop loss = disabled
# - Dry-run by default
#
# IMPORTANT:
# A persistent Railway volume is required.
# One Gunicorn worker and one Railway replica only.
# ============================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ-DH-V4")
app = Flask(__name__)


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

def env(name, default=""):
    return str(os.getenv(name, default) or "").strip()


def number(name, default=0):
    raw = env(name)
    return float(raw) if raw else float(default)


def integer(name, default=0):
    raw = env(name)
    return int(raw) if raw else int(default)


def percentage(name):
    raw = env(name)
    if not raw:
        return 0.0
    value = float(raw)
    if value < 0 or value > 100:
        raise RuntimeError(
            f"{name} must be between 0 and 100"
        )
    return value


API_KEY = env("BINANCE_API_KEY")
API_SECRET = env("BINANCE_API_SECRET")

TESTNET = env(
    "BINANCE_TESTNET", "true"
).lower() == "true"

LIVE = env(
    "ENABLE_LIVE_ORDERS", "false"
).lower() == "true"

WEBHOOK_SECRET = env("WEBHOOK_SECRET")

ORDER_MARGIN_USDT = number(
    "ORDER_MARGIN_USDT", 50
)

ENTRY_LEVERAGE = integer(
    "ENTRY_LEVERAGE", 16
)

INITIAL_SL = percentage(
    "INITIAL_STOP_LOSS_ROI_PCT"
)

BREAKEVEN = percentage(
    "BREAKEVEN_TRIGGER_ROI_PCT"
)

PARTIAL_TP = percentage(
    "PARTIAL_TP_ROI_PCT"
)

PARTIAL_CLOSE = percentage(
    "PARTIAL_CLOSE_PCT"
)

FINAL_TP = percentage(
    "FINAL_TP_ROI_PCT"
)

PORTFOLIO_TARGET = number(
    "PORTFOLIO_PROFIT_USD", 0
)

POLL_SECONDS = max(
    5, integer("POLL_INTERVAL_SECONDS", 5)
)

# Must be a Railway persistent volume path.
DB_PATH = env(
    "BOT_DB_PATH", "/data/maq_bot.sqlite3"
)

# Prevent an old queued alert from opening a
# new trade after a prolonged outage.
MAX_ALERT_AGE_SECONDS = max(
    60, integer("MAX_ALERT_AGE_SECONDS", 180)
)

# Poll queue frequently; the webhook does not
# wait for Binance.
QUEUE_POLL_SECONDS = 0.5

BOT_PREFIX = "MAQSL"

if ORDER_MARGIN_USDT <= 0:
    raise RuntimeError("ORDER_MARGIN_USDT must be positive")

if ENTRY_LEVERAGE < 1 or ENTRY_LEVERAGE > 125:
    raise RuntimeError("Invalid ENTRY_LEVERAGE")

if PARTIAL_TP > 0 and PARTIAL_CLOSE <= 0:
    raise RuntimeError(
        "PARTIAL_CLOSE_PCT required for partial TP"
    )

if LIVE and (not API_KEY or not API_SECRET):
    raise RuntimeError(
        "Binance credentials required for live orders"
    )

if LIVE and not WEBHOOK_SECRET:
    raise RuntimeError(
        "WEBHOOK_SECRET is required for live orders"
    )

if not DB_PATH.startswith("/data/"):
    raise RuntimeError(
        "BOT_DB_PATH must be inside /data/"
    )

if not os.path.isdir("/data"):
    raise RuntimeError(
        "Mount a persistent Railway volume at /data"
    )


# ============================================================
# DATABASE
# ============================================================

def db():
    connection = sqlite3.connect(
        DB_PATH,
        timeout=10,
        isolation_level=None
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def init_database():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE NOT NULL,
                symbol TEXT NOT NULL,
                signal TEXT NOT NULL,
                raw_message TEXT NOT NULL,
                received_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                updated_at REAL NOT NULL
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_alert_status
            ON alerts(status, id)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_state (
                symbol TEXT PRIMARY KEY,
                entry_price REAL NOT NULL,
                entry_qty REAL NOT NULL,
                partial_done INTEGER NOT NULL DEFAULT 0,
                breakeven_done INTEGER NOT NULL DEFAULT 0,
                initial_stop_done INTEGER NOT NULL DEFAULT 0
            )
        """)


init_database()


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    API_KEY,
    API_SECRET,
    testnet=TESTNET
)

BASE_URL = (
    "https://testnet.binancefuture.com"
    if TESTNET
    else "https://fapi.binance.com"
)

symbol_cache = {}
symbol_cache_time = 0
symbol_lock = threading.Lock()
trading_lock = threading.RLock()


# ============================================================
# SYMBOL HANDLING
# ============================================================

def normalize_symbol(raw):
    symbol = str(raw).upper().strip()

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
        raise ValueError("Invalid symbol")

    return symbol


def get_symbol_info(symbol):
    global symbol_cache, symbol_cache_time

    with symbol_lock:
        if (
            not symbol_cache
            or time.time() - symbol_cache_time > 21600
        ):
            response = client.futures_exchange_info()
            symbol_cache = {
                item["symbol"]: item
                for item in response["symbols"]
            }
            symbol_cache_time = time.time()

    info = symbol_cache.get(symbol)

    if not info or info.get("status") != "TRADING":
        raise ValueError(
            f"{symbol} is not an active Binance Futures symbol"
        )

    return info


def get_filter(symbol, filter_type):
    info = get_symbol_info(symbol)

    for item in info.get("filters", []):
        if item["filterType"] == filter_type:
            return item

    raise ValueError(
        f"Missing {filter_type} for {symbol}"
    )


def step_round(value, step, rounding=ROUND_DOWN):
    value = Decimal(str(value))
    step = Decimal(str(step))

    return (
        (value / step).to_integral_value(
            rounding=rounding
        ) * step
    )


def quantity_step(symbol):
    info = get_symbol_info(symbol)

    for item in info["filters"]:
        if item["filterType"] == "MARKET_LOT_SIZE":
            step = float(item.get("stepSize", 0))
            if step > 0:
                return step

    return float(
        get_filter(symbol, "LOT_SIZE")["stepSize"]
    )


def rounded_quantity(symbol, qty):
    return float(
        step_round(qty, quantity_step(symbol))
    )


def rounded_stop_price(symbol, price, side):
    tick = get_filter(
        symbol, "PRICE_FILTER"
    )["tickSize"]

    rounding = (
        ROUND_DOWN if side == "SELL"
        else ROUND_UP
    )

    return format(
        step_round(price, tick, rounding),
        "f"
    )


# ============================================================
# ORIGINAL DH ALERT PARSER
# ============================================================

def parse_message(raw):
    raw = str(raw or "").strip()

    if not raw:
        raise ValueError("Empty alert")

    # MAQ format: BUY|BTCUSDT
    match = re.fullmatch(
        r"(BUY|SELL)\s*\|\s*([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?",
        raw,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    # Original DH strategy:
    # order sell @ 12345 filled on VETUSDT.P.
    match = re.search(
        r"\border\s+(buy|sell)\s+@\s+"
        r"[^\r\n]*?\bfilled\s+on\s+"
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
        "Unsupported TradingView message"
    )


def extract_alert(req):
    data = req.get_json(silent=True)

    if isinstance(data, dict):
        action = data.get("signal") or data.get("action")
        symbol = data.get("symbol") or data.get("ticker")

        if action and symbol:
            action = str(action).upper().strip()

            if action not in ("BUY", "SELL"):
                raise ValueError("Invalid action")

            return (
                action,
                normalize_symbol(symbol),
                json.dumps(data, sort_keys=True)
            )

        for field in ("message", "text", "alert"):
            if isinstance(data.get(field), str):
                action, symbol = parse_message(data[field])
                return action, symbol, data[field]

    if isinstance(data, str):
        action, symbol = parse_message(data)
        return action, symbol, data

    raw = req.get_data(as_text=True)
    action, symbol = parse_message(raw)
    return action, symbol, raw


# ============================================================
# WEBHOOK AUTHENTICATION
# ============================================================

def authenticated(req):
    if not WEBHOOK_SECRET:
        return not LIVE

    supplied = (
        req.args.get("secret")
        or req.headers.get("X-Webhook-Secret", "")
    )

    return hmac.compare_digest(
        supplied,
        WEBHOOK_SECRET
    )


# ============================================================
# FAST PERSISTENT WEBHOOK
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook():

    if not authenticated(request):
        return jsonify({
            "status": "rejected",
            "error": "Invalid webhook secret"
        }), 401

    try:
        signal, symbol, raw = extract_alert(request)

        # Identical messages within the same 15-minute
        # bucket are treated as duplicates.
        #
        # If TradingView emits multiple legitimate
        # identical fills in one bucket, a unique alert
        # identifier should be added to the payload.
        bucket = int(time.time() // 900)

        fingerprint = hashlib.sha256(
            f"{bucket}|{signal}|{symbol}|{raw}".encode()
        ).hexdigest()

        now = time.time()

        with db() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO alerts (
                    fingerprint,
                    symbol,
                    signal,
                    raw_message,
                    received_at,
                    status,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """, (
                fingerprint,
                symbol,
                signal,
                raw,
                now,
                now
            ))

            inserted = cursor.rowcount == 1

        log.info(
            "WEBHOOK %s %s status=%s",
            signal,
            symbol,
            "queued" if inserted else "duplicate"
        )

        return jsonify({
            "status": (
                "queued" if inserted else "duplicate"
            ),
            "signal": signal,
            "symbol": symbol
        }), 200

    except ValueError as exc:
        return jsonify({
            "status": "rejected",
            "error": str(exc)
        }), 400

    except Exception:
        log.exception("Webhook queue failure")

        return jsonify({
            "status": "error",
            "error": "Alert could not be saved"
        }), 503


# ============================================================
# POSITION HELPERS
# ============================================================

def positions_for_symbol(symbol):
    positions = client.futures_position_information(
        symbol=symbol
    )

    return [
        p for p in positions
        if abs(float(p["positionAmt"])) > 0
    ]


def current_position(symbol):
    positions = positions_for_symbol(symbol)

    if len(positions) > 1:
        raise RuntimeError(
            "Multiple position sides detected"
        )

    if positions:
        p = positions[0]

        if p.get("positionSide", "BOTH") != "BOTH":
            raise RuntimeError(
                "Hedge Mode is not supported"
            )

        return p

    return None


def assert_one_way_mode():
    result = client.futures_get_position_mode()

    if result.get("dualSidePosition"):
        raise RuntimeError(
            "Disable Binance Hedge Mode before using this bot"
        )


def get_price(symbol):
    return float(
        client.futures_symbol_ticker(
            symbol=symbol
        )["price"]
    )


def roi_pct(position):
    amount = float(position["positionAmt"])
    entry = float(position["entryPrice"])
    mark = float(position.get("markPrice", 0) or 0)
    leverage = float(position.get("leverage", 1) or 1)

    if entry <= 0 or mark <= 0:
        return 0.0

    change = (mark - entry) / entry

    if amount < 0:
        change = -change

    return change * leverage * 100


def state_for(position):
    symbol = position["symbol"]
    entry = float(position["entryPrice"])
    qty = abs(float(position["positionAmt"]))

    with db() as conn:
        row = conn.execute(
            "SELECT * FROM position_state WHERE symbol=?",
            (symbol,)
        ).fetchone()

        if row is None or abs(
            float(row["entry_price"]) - entry
        ) > max(entry * 0.000001, 1e-12):

            conn.execute("""
                INSERT OR REPLACE INTO position_state
                (symbol, entry_price, entry_qty,
                 partial_done, breakeven_done,
                 initial_stop_done)
                VALUES (?, ?, ?, 0, 0, 0)
            """, (symbol, entry, qty))

            row = conn.execute(
                "SELECT * FROM position_state WHERE symbol=?",
                (symbol,)
            ).fetchone()

        return dict(row)


def update_state(symbol, **fields):
    allowed = {
        "partial_done",
        "breakeven_done",
        "initial_stop_done"
    }

    with db() as conn:
        for field, value in fields.items():
            if field not in allowed:
                raise ValueError("Invalid state field")

            conn.execute(
                f"UPDATE position_state SET {field}=? "
                "WHERE symbol=?",
                (int(bool(value)), symbol)
            )


# ============================================================
# BINANCE MARKET ORDERS
# ============================================================

def market_order(symbol, side, quantity, reduce_only=False):
    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": rounded_quantity(symbol, quantity)
    }

    if params["quantity"] <= 0:
        raise ValueError("Quantity rounds to zero")

    if reduce_only:
        params["reduceOnly"] = True

    return client.futures_create_order(**params)


def close_position(position, fraction=1.0):
    amount = float(position["positionAmt"])

    if amount == 0:
        return None

    side = SIDE_SELL if amount > 0 else SIDE_BUY

    return market_order(
        position["symbol"],
        side,
        abs(amount) * fraction,
        reduce_only=True
    )


def wait_for_position(symbol, expected_direction=None):
    for _ in range(20):
        p = current_position(symbol)

        if expected_direction is None:
            if p is None:
                return None
        elif p is not None:
            amount = float(p["positionAmt"])

            if (
                expected_direction == "BUY" and amount > 0
            ) or (
                expected_direction == "SELL" and amount < 0
            ):
                return p

        time.sleep(0.5)

    raise RuntimeError(
        f"Position confirmation timed out: {symbol}"
    )


# ============================================================
# BINANCE CONDITIONAL STOP API
# ============================================================

def algo_request(method, path, params=None):
    params = dict(params or {})

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query = urllib.parse.urlencode(params)

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    payload = query + "&signature=" + signature

    headers = {
        "X-MBX-APIKEY": API_KEY,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    method = method.upper()

    if method == "POST":
        req = urllib.request.Request(
            BASE_URL + path,
            data=payload.encode(),
            headers=headers,
            method=method
        )
    else:
        req = urllib.request.Request(
            BASE_URL + path + "?" + payload,
            headers=headers,
            method=method
        )

    try:
        with urllib.request.urlopen(
            req, timeout=15
        ) as response:
            body = response.read().decode()
            return json.loads(body) if body else {}

    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(
            f"Binance Algo API {exc.code}: {body}"
        ) from exc


def open_bot_stops(symbol):
    response = algo_request(
        "GET",
        "/fapi/v1/openAlgoOrders",
        {
            "symbol": symbol,
            "algoType": "CONDITIONAL"
        }
    )

    return [
        o for o in response
        if str(
            o.get("clientAlgoId", "")
        ).startswith(BOT_PREFIX)
    ]


def cancel_bot_stops(symbol):
    for order in open_bot_stops(symbol):
        algo_request(
            "DELETE",
            "/fapi/v1/algoOrder",
            {
                "symbol": symbol,
                "algoId": order["algoId"]
            }
        )


def place_stop(position, stop_price, reason):
    symbol = position["symbol"]
    amount = float(position["positionAmt"])

    side = "SELL" if amount > 0 else "BUY"

    price = rounded_stop_price(
        symbol, stop_price, side
    )

    client_id = (
        f"{BOT_PREFIX}{reason[:2]}"
        f"{int(time.time() * 1000)}"
    )

    result = algo_request(
        "POST",
        "/fapi/v1/algoOrder",
        {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "triggerPrice": price,
            "workingType": "MARK_PRICE",
            "closePosition": "true",
            "clientAlgoId": client_id
        }
    )

    # Only cancel older stops after the new
    # protective order was accepted.
    for order in open_bot_stops(symbol):
        if order.get("clientAlgoId") != client_id:
            try:
                algo_request(
                    "DELETE",
                    "/fapi/v1/algoOrder",
                    {
                        "symbol": symbol,
                        "algoId": order["algoId"]
                    }
                )
            except Exception:
                log.exception(
                    "Could not cancel older stop for %s",
                    symbol
                )

    return result


def initial_stop_price(position):
    entry = float(position["entryPrice"])
    leverage = float(position.get("leverage", 1) or 1)
    amount = float(position["positionAmt"])

    fraction = INITIAL_SL / 100 / leverage

    return (
        entry * (1 - fraction)
        if amount > 0
        else entry * (1 + fraction)
    )


# ============================================================
# ENTRY / REVERSAL
# ============================================================

def execute_signal(signal, symbol):
    with trading_lock:

        assert_one_way_mode()
        get_symbol_info(symbol)

        current = current_position(symbol)

        if current:
            amount = float(current["positionAmt"])
            direction = "BUY" if amount > 0 else "SELL"

            if direction == signal:
                return "already_in_position"

            # Stop orders are canceled only after
            # confirming that the position is closed.
            close_position(current)
            wait_for_position(symbol, None)
            cancel_bot_stops(symbol)

            with db() as conn:
                conn.execute(
                    "DELETE FROM position_state WHERE symbol=?",
                    (symbol,)
                )

        client.futures_change_leverage(
            symbol=symbol,
            leverage=ENTRY_LEVERAGE
        )

        price = get_price(symbol)
        qty = (
            ORDER_MARGIN_USDT * ENTRY_LEVERAGE
        ) / price

        side = (
            SIDE_BUY if signal == "BUY"
            else SIDE_SELL
        )

        order = market_order(symbol, side, qty)

        log.warning(
            "ENTRY ORDER %s %s ID=%s",
            signal,
            symbol,
            order.get("orderId")
        )

        position = wait_for_position(symbol, signal)
        state_for(position)

        if INITIAL_SL > 0:
            place_stop(
                position,
                initial_stop_price(position),
                "INITIAL"
            )

            update_state(
                symbol, initial_stop_done=True
            )

        else:
            log.warning(
                "%s initial stop loss DISABLED",
                symbol
            )

        return "executed"


# ============================================================
# QUEUE PROCESSOR
# ============================================================

def claim_alert():
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute("""
            SELECT * FROM alerts
            WHERE status='pending'
            ORDER BY id
            LIMIT 1
        """).fetchone()

        if row:
            conn.execute("""
                UPDATE alerts
                SET status='processing',
                    updated_at=?
                WHERE id=? AND status='pending'
            """, (time.time(), row["id"]))

        conn.commit()

        return dict(row) if row else None


def set_alert_status(alert_id, status, error=None):
    with db() as conn:
        conn.execute("""
            UPDATE alerts
            SET status=?, error=?, updated_at=?
            WHERE id=?
        """, (
            status,
            error,
            time.time(),
            alert_id
        ))


def queue_worker():
    log.info("Persistent queue worker started")

    while True:
        try:
            alert = claim_alert()

            if not alert:
                time.sleep(QUEUE_POLL_SECONDS)
                continue

            alert_id = alert["id"]
            age = time.time() - alert["received_at"]

            if age > MAX_ALERT_AGE_SECONDS:
                set_alert_status(
                    alert_id,
                    "expired",
                    "Alert too old to execute"
                )
                continue

            if not LIVE:
                log.info(
                    "DRY RUN %s %s",
                    alert["signal"],
                    alert["symbol"]
                )

                set_alert_status(
                    alert_id, "dry_run"
                )
                continue

            try:
                result = execute_signal(
                    alert["signal"],
                    alert["symbol"]
                )

                set_alert_status(
                    alert_id, result
                )

            except Exception as exc:
                # Binance may have accepted an order
                # before a network timeout occurred.
                # Do NOT blindly retry.
                log.exception(
                    "Alert %s requires review",
                    alert_id
                )

                set_alert_status(
                    alert_id,
                    "needs_review",
                    str(exc)[:500]
                )

        except Exception:
            log.exception("Queue worker error")
            time.sleep(2)


# ============================================================
# RISK MONITOR
# ============================================================

def monitor_positions():
    if not LIVE:
        return

    with trading_lock:
        assert_one_way_mode()

        positions = client.futures_position_information()
        active = [
            p for p in positions
            if abs(float(p["positionAmt"])) > 0
        ]

        total_profit = sum(
            float(p.get("unRealizedProfit", 0) or 0)
            for p in active
        )

        if (
            PORTFOLIO_TARGET > 0
            and total_profit >= PORTFOLIO_TARGET
        ):
            log.warning(
                "PORTFOLIO TARGET REACHED %.2f",
                total_profit
            )

            for p in active:
                close_position(p)
                wait_for_position(p["symbol"], None)
                cancel_bot_stops(p["symbol"])

            return

        for p in active:
            symbol = p["symbol"]
            roi = roi_pct(p)
            state = state_for(p)

            # Initial stop disabled when empty.
            if INITIAL_SL > 0 and roi <= -INITIAL_SL:
                close_position(p)
                wait_for_position(symbol, None)
                cancel_bot_stops(symbol)
                continue

            # Final take profit.
            if FINAL_TP > 0 and roi >= FINAL_TP:
                close_position(p)
                wait_for_position(symbol, None)
                cancel_bot_stops(symbol)
                continue

            # Breakeven stop.
            if (
                BREAKEVEN > 0
                and roi >= BREAKEVEN
                and not state["breakeven_done"]
            ):
                place_stop(
                    p,
                    float(p["entryPrice"]),
                    "BREAKEVEN"
                )

                update_state(
                    symbol, breakeven_done=True
                )

            # Partial take profit.
            if (
                PARTIAL_TP > 0
                and PARTIAL_CLOSE > 0
                and roi >= PARTIAL_TP
                and not state["partial_done"]
            ):
                result = close_position(
                    p, PARTIAL_CLOSE / 100
                )

                if result:
                    update_state(
                        symbol, partial_done=True
                    )

                    # Breakeven after partial TP
                    # only when the BE variable is on.
                    if BREAKEVEN > 0:
                        remaining = current_position(
                            symbol
                        )

                        if remaining:
                            place_stop(
                                remaining,
                                float(remaining["entryPrice"]),
                                "BREAKEVEN"
                            )

                            update_state(
                                symbol,
                                breakeven_done=True
                            )

            # Initial stop rearm only when enabled.
            if (
                INITIAL_SL > 0
                and not state["initial_stop_done"]
                and not state["breakeven_done"]
            ):
                place_stop(
                    p,
                    initial_stop_price(p),
                    "INITIAL"
                )

                update_state(
                    symbol,
                    initial_stop_done=True
                )


def risk_worker():
    log.info("Risk monitor started")

    while True:
        try:
            monitor_positions()
        except Exception:
            log.exception("Risk monitor error")

        time.sleep(POLL_SECONDS)


# ============================================================
# HEALTH / STATUS
# ============================================================

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "MAQ DH V4",
        "testnet": TESTNET,
        "live_orders": LIVE
    })


@app.route("/status", methods=["GET"])
def status():
    with db() as conn:
        rows = conn.execute("""
            SELECT status, COUNT(*) AS count
            FROM alerts
            GROUP BY status
        """).fetchall()

        recent = conn.execute("""
            SELECT id, symbol, signal, status,
                   error, received_at
            FROM alerts
            ORDER BY id DESC
            LIMIT 20
        """).fetchall()

    return jsonify({
        "service": "MAQ DH V4",
        "testnet": TESTNET,
        "live_orders": LIVE,
        "queue": {
            row["status"]: row["count"]
            for row in rows
        },
        "recent_alerts": [
            dict(row) for row in recent
        ],
        "settings": {
            "margin_usdt": ORDER_MARGIN_USDT,
            "leverage": ENTRY_LEVERAGE,
            "initial_stop_loss_roi_pct": INITIAL_SL,
            "breakeven_roi_pct": BREAKEVEN,
            "partial_tp_roi_pct": PARTIAL_TP,
            "partial_close_pct": PARTIAL_CLOSE,
            "final_tp_roi_pct": FINAL_TP,
            "portfolio_profit_usd": PORTFOLIO_TARGET
        }
    })


# ============================================================
# START BACKGROUND WORKERS
# ============================================================

def start_workers():
    # Processing alerts from a previous deployment is
    # intentionally NOT resumed automatically if they
    # were already marked 'processing'.
    #
    # Review these through /status before taking action.

    threading.Thread(
        target=queue_worker,
        daemon=True,
        name="MAQ-Queue"
    ).start()

    threading.Thread(
        target=risk_worker,
        daemon=True,
        name="MAQ-Risk"
    ).start()


start_workers()


if __name__ == "__main__":
    port = int(env("PORT", "8080"))

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
