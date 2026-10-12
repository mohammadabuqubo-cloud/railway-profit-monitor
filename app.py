
import os
import re
import json
import time
import hmac
import hashlib
import logging
import sqlite3
import threading
from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify
from binance.client import Client


# ============================================================
# MAQ DH BINANCE FUTURES BOT V6
#
# Persistent SQLite queue: /data
# Fast TradingView webhook response
# Original DH strategy message support
# BUY|SYMBOL and SELL|SYMBOL support
# One-way Binance Futures positions
# Opposite-signal reversal
# Railway-variable risk settings
#
# EMPTY INITIAL_STOP_LOSS_ROI_PCT = DISABLED
#
# Single Railway replica / single Gunicorn worker.
# Dry-run by default.
# ============================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ-DH-V6")
app = Flask(__name__)


# ============================================================
# ENVIRONMENT
# ============================================================

def env(name, default=""):
    value = os.getenv(name)
    return str(default if value is None else value).strip()


def env_float(name, default=0):
    value = env(name)
    return float(value) if value else float(default)


def env_int(name, default=0):
    value = env(name)
    return int(value) if value else int(default)


def env_bool(name, default=False):
    value = env(
        name,
        "true" if default else "false"
    ).lower()

    return value in ("true", "1", "yes", "on")


def env_pct(name):
    value = env(name)

    if not value:
        return 0.0

    result = float(value)

    if not 0 <= result <= 100:
        raise RuntimeError(
            f"{name} must be between 0 and 100"
        )

    return result


API_KEY = env("BINANCE_API_KEY")
API_SECRET = env("BINANCE_API_SECRET")

TESTNET = env_bool("BINANCE_TESTNET", True)
LIVE = env_bool("ENABLE_LIVE_ORDERS", False)

WEBHOOK_SECRET = env("WEBHOOK_SECRET")
STATUS_SECRET = env("STATUS_SECRET", WEBHOOK_SECRET)

ORDER_MARGIN_USDT = env_float(
    "ORDER_MARGIN_USDT", 50
)

ENTRY_LEVERAGE = env_int(
    "ENTRY_LEVERAGE", 16
)

INITIAL_SL = env_pct(
    "INITIAL_STOP_LOSS_ROI_PCT"
)

BREAKEVEN = env_pct(
    "BREAKEVEN_TRIGGER_ROI_PCT"
)

PARTIAL_TP = env_pct(
    "PARTIAL_TP_ROI_PCT"
)

PARTIAL_CLOSE = env_pct(
    "PARTIAL_CLOSE_PCT"
)

FINAL_TP = env_pct(
    "FINAL_TP_ROI_PCT"
)

PORTFOLIO_TARGET = env_float(
    "PORTFOLIO_PROFIT_USD", 0
)

POLL_SECONDS = max(
    5, env_int("POLL_INTERVAL_SECONDS", 5)
)

MAX_ALERT_AGE = max(
    60, env_int("MAX_ALERT_AGE_SECONDS", 180)
)

DB_PATH = env(
    "BOT_DB_PATH", "/data/maq_bot.sqlite3"
)

BOT_PREFIX = "MAQSL"

if not DB_PATH.startswith("/data/"):
    raise RuntimeError(
        "BOT_DB_PATH must be under /data/"
    )

if not os.path.isdir("/data"):
    raise RuntimeError(
        "Mount a persistent Railway volume at /data"
    )

if ORDER_MARGIN_USDT <= 0:
    raise RuntimeError("ORDER_MARGIN_USDT must be positive")

if not 1 <= ENTRY_LEVERAGE <= 125:
    raise RuntimeError("Invalid ENTRY_LEVERAGE")

if LIVE and (not API_KEY or not API_SECRET):
    raise RuntimeError(
        "Binance credentials required for live orders"
    )

if LIVE and not WEBHOOK_SECRET:
    raise RuntimeError(
        "WEBHOOK_SECRET required for live orders"
    )

if LIVE and not STATUS_SECRET:
    raise RuntimeError(
        "STATUS_SECRET required for live orders"
    )

if PARTIAL_TP > 0 and PARTIAL_CLOSE <= 0:
    raise RuntimeError(
        "PARTIAL_CLOSE_PCT must be positive"
    )


# ============================================================
# SQLITE
# ============================================================

def db():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=10,
        isolation_level=None
    )

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")

    return conn


def init_db():
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


init_db()


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    API_KEY,
    API_SECRET,
    testnet=TESTNET
)

trading_lock = threading.RLock()
symbol_lock = threading.Lock()
worker_lock = threading.Lock()

workers_started = False
symbol_cache = {}
symbol_cache_time = 0.0


# ============================================================
# TRADINGVIEW MESSAGE PARSER
# ============================================================

def normalize_symbol(raw):
    symbol = str(raw).strip().upper()

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


def parse_message(raw):
    raw = str(raw or "").strip()

    if not raw:
        raise ValueError("Empty alert")

    # MAQ alerts:
    # BUY|BTCUSDT
    # SELL|BTCUSDT

    match = re.fullmatch(
        r"(BUY|SELL)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?",
        raw,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    # Original DH strategy order-fill alert:
    # order sell @ 1000 filled on BINANCE:BTCUSDT.P

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
        signal = data.get("signal") or data.get("action")
        symbol = data.get("symbol") or data.get("ticker")

        if signal and symbol:
            signal = str(signal).upper().strip()

            if signal not in ("BUY", "SELL"):
                raise ValueError("Invalid signal")

            return (
                signal,
                normalize_symbol(symbol),
                json.dumps(data, sort_keys=True)
            )

        for field in ("message", "text", "alert"):
            if isinstance(data.get(field), str):
                signal, symbol = parse_message(data[field])
                return signal, symbol, data[field]

    if isinstance(data, str):
        signal, symbol = parse_message(data)
        return signal, symbol, data

    raw = req.get_data(as_text=True)
    signal, symbol = parse_message(raw)
    return signal, symbol, raw


# ============================================================
# AUTHENTICATION
# ============================================================

def secret_matches(req, secret):
    if not secret:
        return not LIVE

    supplied = (
        req.args.get("secret")
        or req.headers.get("X-Webhook-Secret", "")
    )

    return hmac.compare_digest(supplied, secret)


# ============================================================
# FAST WEBHOOK
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    if not secret_matches(request, WEBHOOK_SECRET):
        return jsonify({
            "status": "rejected",
            "error": "Invalid webhook secret"
        }), 401

    try:
        signal, symbol, raw = extract_alert(request)

        now = time.time()
        bucket = int(now // 900)

        fingerprint = hashlib.sha256(
            f"{bucket}|{signal}|{symbol}|{raw}".encode()
        ).hexdigest()

        with db() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO alerts (
                    fingerprint, symbol, signal,
                    raw_message, received_at,
                    status, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """, (
                fingerprint, symbol, signal,
                raw, now, now
            ))

            inserted = cursor.rowcount == 1

        log.info(
            "Alert %s %s: %s",
            signal,
            symbol,
            "queued" if inserted else "duplicate"
        )

        return jsonify({
            "status": "queued" if inserted else "duplicate",
            "signal": signal,
            "symbol": symbol
        }), 200

    except ValueError as exc:
        return jsonify({
            "status": "rejected",
            "error": str(exc)
        }), 400

    except Exception:
        log.exception("Webhook storage failure")
        return jsonify({
            "status": "error",
            "error": "Could not save alert"
        }), 503


# ============================================================
# EXCHANGE INFORMATION
# ============================================================

def get_symbol_info(symbol):
    global symbol_cache, symbol_cache_time

    with symbol_lock:
        if (
            not symbol_cache
            or time.time() - symbol_cache_time > 21600
        ):
            info = client.futures_exchange_info()

            symbol_cache = {
                item["symbol"]: item
                for item in info["symbols"]
            }

            symbol_cache_time = time.time()

    result = symbol_cache.get(symbol)

    if not result or result.get("status") != "TRADING":
        raise ValueError(
            f"Unavailable Futures symbol: {symbol}"
        )

    return result


def symbol_filter(symbol, filter_type):
    info = get_symbol_info(symbol)

    for item in info["filters"]:
        if item["filterType"] == filter_type:
            return item

    raise ValueError(f"Missing {filter_type}")


def round_step(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))

    return (
        (value / step).to_integral_value(
            rounding=ROUND_DOWN
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
        symbol_filter(symbol, "LOT_SIZE")["stepSize"]
    )


def round_qty(symbol, quantity):
    return float(
        round_step(quantity, quantity_step(symbol))
    )


# ============================================================
# POSITIONS
# ============================================================

def assert_one_way():
    mode = client.futures_get_position_mode()

    if mode.get("dualSidePosition"):
        raise RuntimeError(
            "Binance Hedge Mode is unsupported"
        )


def current_position(symbol):
    positions = client.futures_position_information(
        symbol=symbol
    )

    active = [
        p for p in positions
        if abs(float(p["positionAmt"])) > 0
    ]

    if len(active) > 1:
        raise RuntimeError(
            "Multiple position sides detected"
        )

    if active:
        if active[0].get("positionSide", "BOTH") != "BOTH":
            raise RuntimeError("Hedge Mode unsupported")

        return active[0]

    return None


def all_positions():
    positions = client.futures_position_information()

    return [
        p for p in positions
        if abs(float(p["positionAmt"])) > 0
    ]


def position_roi(position):
    entry = float(position["entryPrice"])
    mark = float(position.get("markPrice", 0) or 0)
    amount = float(position["positionAmt"])
    leverage = float(position.get("leverage", 1) or 1)

    if entry <= 0 or mark <= 0:
        return 0.0

    change = (mark - entry) / entry

    if amount < 0:
        change = -change

    return change * leverage * 100


# ============================================================
# PERSISTENT POSITION STATE
# ============================================================

def position_state(position):
    symbol = position["symbol"]
    entry = float(position["entryPrice"])
    qty = abs(float(position["positionAmt"]))

    with db() as conn:
        row = conn.execute(
            "SELECT * FROM position_state WHERE symbol=?",
            (symbol,)
        ).fetchone()

        if (
            row is None
            or abs(float(row["entry_price"]) - entry)
            > max(entry * 0.000001, 1e-12)
        ):
            conn.execute("""
                INSERT OR REPLACE INTO position_state
                (
                    symbol, entry_price, entry_qty,
                    partial_done, breakeven_done,
                    initial_stop_done
                )
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


def delete_state(symbol):
    with db() as conn:
        conn.execute(
            "DELETE FROM position_state WHERE symbol=?",
            (symbol,)
        )


# ============================================================
# MARKET ORDERS
# ============================================================

def market_order(symbol, side, quantity, reduce_only=False):
    quantity = round_qty(symbol, quantity)

    if quantity <= 0:
        raise RuntimeError(
            f"Quantity too small for {symbol}"
        )

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": quantity
    }

    if reduce_only:
        params["reduceOnly"] = True

    return client.futures_create_order(**params)


def close_position(position, fraction=1.0):
    amount = float(position["positionAmt"])

    if amount == 0:
        return None

    side = "SELL" if amount > 0 else "BUY"

    return market_order(
        position["symbol"],
        side,
        abs(amount) * fraction,
        reduce_only=True
    )


def wait_for_position(symbol, direction=None, timeout=10):
    start = time.time()

    while time.time() - start < timeout:
        p = current_position(symbol)

        if direction is None:
            if p is None:
                return None

        elif p:
            amount = float(p["positionAmt"])

            if (
                direction == "BUY" and amount > 0
            ) or (
                direction == "SELL" and amount < 0
            ):
                return p

        time.sleep(0.5)

    raise RuntimeError(
        f"Position confirmation timeout: {symbol}"
    )


# ============================================================
# STOP ORDERS
# ============================================================

def initial_stop_price(position):
    entry = float(position["entryPrice"])
    leverage = float(position.get("leverage", 1) or 1)
    amount = float(position["positionAmt"])

    fraction = INITIAL_SL / 100 / leverage

    if amount > 0:
        return entry * (1 - fraction)

    return entry * (1 + fraction)


# ============================================================
# ENTRY AND REVERSAL
# ============================================================

def execute_signal(signal, symbol):
    with trading_lock:
        assert_one_way()
        get_symbol_info(symbol)

        current = current_position(symbol)

        if current:
            amount = float(current["positionAmt"])
            direction = "BUY" if amount > 0 else "SELL"

            if direction == signal:
                return "already_in_position"

            close_position(current)
            wait_for_position(symbol, None)
            delete_state(symbol)

        client.futures_change_leverage(
            symbol=symbol,
            leverage=ENTRY_LEVERAGE
        )

        price = float(
            client.futures_symbol_ticker(
                symbol=symbol
            )["price"]
        )

        quantity = (
            ORDER_MARGIN_USDT * ENTRY_LEVERAGE
        ) / price

        order = market_order(
            symbol,
            signal,
            quantity
        )

        log.warning(
            "Entry %s %s order=%s",
            signal,
            symbol,
            order.get("orderId")
        )

        position = wait_for_position(symbol, signal)
        position_state(position)

        if INITIAL_SL > 0:
            # Software-managed initial stop.
            # Checked by the risk monitor.
            update_state(
                symbol,
                initial_stop_done=True
            )
        else:
            log.info(
                "%s initial stop disabled",
                symbol
            )

        return "executed"


# ============================================================
# QUEUE
# ============================================================

def claim_alert():
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute("""
            SELECT * FROM alerts
            WHERE status='pending'
            ORDER BY id LIMIT 1
        """).fetchone()

        if row:
            conn.execute("""
                UPDATE alerts
                SET status='processing',
                    updated_at=?
                WHERE id=?
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
            status, error, time.time(), alert_id
        ))


def queue_worker():
    log.info("MAQ queue worker started")

    while True:
        try:
            alert = claim_alert()

            if not alert:
                time.sleep(0.5)
                continue

            alert_id = alert["id"]

            if time.time() - alert["received_at"] > MAX_ALERT_AGE:
                set_alert_status(
                    alert_id,
                    "expired",
                    "Alert too old"
                )
                continue

            if not LIVE:
                log.info(
                    "DRY RUN %s %s",
                    alert["signal"],
                    alert["symbol"]
                )

                set_alert_status(alert_id, "dry_run")
                continue

            try:
                result = execute_signal(
                    alert["signal"],
                    alert["symbol"]
                )

                set_alert_status(alert_id, result)

            except Exception as exc:
                log.exception(
                    "Alert %s needs review",
                    alert_id
                )

                set_alert_status(
                    alert_id,
                    "needs_review",
                    str(exc)[:500]
                )

        except Exception:
            log.exception("Queue worker failure")
            time.sleep(2)


# ============================================================
# RISK MONITOR
# ============================================================

def monitor_positions():
    if not LIVE:
        return

    with trading_lock:
        assert_one_way()

        positions = all_positions()

        total_profit = sum(
            float(p.get("unRealizedProfit", 0) or 0)
            for p in positions
        )

        if (
            PORTFOLIO_TARGET > 0
            and total_profit >= PORTFOLIO_TARGET
        ):
            for p in positions:
                close_position(p)
                wait_for_position(p["symbol"], None)
                delete_state(p["symbol"])

            return

        for p in positions:
            symbol = p["symbol"]
            roi = position_roi(p)
            state = position_state(p)

            # Initial stop-loss, disabled when empty.
            if INITIAL_SL > 0 and roi <= -INITIAL_SL:
                close_position(p)
                wait_for_position(symbol, None)
                delete_state(symbol)
                continue

            # Final take-profit.
            if FINAL_TP > 0 and roi >= FINAL_TP:
                close_position(p)
                wait_for_position(symbol, None)
                delete_state(symbol)
                continue

            # Breakeven:
            # Software-managed exit at ROI <= 0
            # after the trigger has been reached.
            if (
                BREAKEVEN > 0
                and roi >= BREAKEVEN
                and not state["breakeven_done"]
            ):
                update_state(
                    symbol,
                    breakeven_done=True
                )
                state["breakeven_done"] = 1

            if (
                state["breakeven_done"]
                and roi <= 0
            ):
                close_position(p)
                wait_for_position(symbol, None)
                delete_state(symbol)
                continue

            # Partial take-profit.
            if (
                PARTIAL_TP > 0
                and PARTIAL_CLOSE > 0
                and roi >= PARTIAL_TP
                and not state["partial_done"]
            ):
                result = close_position(
                    p,
                    PARTIAL_CLOSE / 100
                )

                if result:
                    update_state(
                        symbol,
                        partial_done=True
                    )

                    if BREAKEVEN > 0:
                        update_state(
                            symbol,
                            breakeven_done=True
                        )


def risk_worker():
    log.info("MAQ risk monitor started")

    while True:
        try:
            monitor_positions()
        except Exception:
            log.exception("Risk monitor failure")

        time.sleep(POLL_SECONDS)


# ============================================================
# ROUTES
# ============================================================

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "MAQ DH V6",
        "testnet": TESTNET,
        "live_orders": LIVE
    })


@app.route("/status", methods=["GET"])
def status():
    if not secret_matches(request, STATUS_SECRET):
        return jsonify({
            "error": "Unauthorized"
        }), 401

    with db() as conn:
        counts = conn.execute("""
            SELECT status, COUNT(*) AS count
            FROM alerts GROUP BY status
        """).fetchall()

        recent = conn.execute("""
            SELECT id, symbol, signal, status,
                   error, received_at
            FROM alerts
            ORDER BY id DESC LIMIT 20
        """).fetchall()

    return jsonify({
        "service": "MAQ DH V6",
        "testnet": TESTNET,
        "live_orders": LIVE,
        "queue": {
            row["status"]: row["count"]
            for row in counts
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
# START WORKERS
# ============================================================

def start_workers():
    global workers_started

    with worker_lock:
        if workers_started:
            return

        workers_started = True

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
    port = env_int("PORT", 8080)

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
