
import os
import re
import json
import time
import hmac
import hashlib
import logging
import sqlite3
import threading
import ipaddress

from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify
from binance.client import Client


# ============================================================
# MAQ DH RAILWAY BOT V7
#
# Persistent SQLite at /data
# Original DH TradingView alerts unchanged
# TradingView IP allowlist
# Optional webhook secret
# Fast webhook queue
# Binance USD-M Futures
# One-way positions only
# Software-managed ROI exits
#
# LIVE TRADING DISABLED BY DEFAULT
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ-V7")
app = Flask(__name__)


# ============================================================
# SETTINGS
# ============================================================

def setting(name, default=""):
    value = os.getenv(name)
    return str(default if value is None else value).strip()


def number(name, default=0):
    value = setting(name)
    return float(value) if value else float(default)


def boolean(name, default=False):
    return setting(
        name,
        "true" if default else "false"
    ).lower() in ("true", "1", "yes", "on")


API_KEY = setting("BINANCE_API_KEY")
API_SECRET = setting("BINANCE_API_SECRET")

TESTNET = boolean("BINANCE_TESTNET", True)
LIVE = boolean("ENABLE_LIVE_ORDERS", False)

WEBHOOK_SECRET = setting("WEBHOOK_SECRET")
STATUS_SECRET = setting("STATUS_SECRET")

# Explicitly acknowledge IP-only authentication risk.
ALLOW_IP_ONLY_LIVE = boolean(
    "ALLOW_TV_IP_ONLY_LIVE", False
)

MARGIN = number("ORDER_MARGIN_USDT", 50)
LEVERAGE = int(number("ENTRY_LEVERAGE", 16))

INITIAL_SL = number("INITIAL_STOP_LOSS_ROI_PCT")
BREAKEVEN = number("BREAKEVEN_TRIGGER_ROI_PCT")
PARTIAL_TP = number("PARTIAL_TP_ROI_PCT")
PARTIAL_CLOSE = number("PARTIAL_CLOSE_PCT")
FINAL_TP = number("FINAL_TP_ROI_PCT")

PORTFOLIO_TARGET = number(
    "PORTFOLIO_PROFIT_USD"
)

POLL = max(
    5,
    int(number("POLL_INTERVAL_SECONDS", 5))
)

MAX_AGE = max(
    60,
    int(number("MAX_ALERT_AGE_SECONDS", 180))
)

DB_PATH = setting(
    "BOT_DB_PATH",
    "/data/maq_bot.sqlite3"
)

TRADINGVIEW_IPS = {
    "52.89.214.238",
    "34.212.75.30",
    "54.218.53.128",
    "52.32.178.7"
}


# ============================================================
# STARTUP VALIDATION
# ============================================================

if not os.path.isdir("/data"):
    raise RuntimeError(
        "Railway persistent volume missing at /data"
    )

if not DB_PATH.startswith("/data/"):
    raise RuntimeError(
        "BOT_DB_PATH must be inside /data"
    )

if MARGIN <= 0:
    raise RuntimeError("Invalid margin")

if not 1 <= LEVERAGE <= 125:
    raise RuntimeError("Invalid leverage")

for value in (
    INITIAL_SL,
    BREAKEVEN,
    PARTIAL_TP,
    PARTIAL_CLOSE,
    FINAL_TP
):
    if not 0 <= value <= 100:
        raise RuntimeError(
            "Risk percentages must be 0 to 100"
        )

if LIVE:
    if not API_KEY or not API_SECRET:
        raise RuntimeError(
            "Binance credentials required"
        )

    if not STATUS_SECRET:
        raise RuntimeError(
            "STATUS_SECRET required for live trading"
        )

    if not WEBHOOK_SECRET and not ALLOW_IP_ONLY_LIVE:
        raise RuntimeError(
            "Set WEBHOOK_SECRET or explicitly set "
            "ALLOW_TV_IP_ONLY_LIVE=true"
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


def initialize():
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
            CREATE TABLE IF NOT EXISTS position_state (
                symbol TEXT PRIMARY KEY,
                entry_price REAL NOT NULL,
                entry_qty REAL NOT NULL,
                partial_done INTEGER NOT NULL DEFAULT 0,
                breakeven_done INTEGER NOT NULL DEFAULT 0,
                initial_stop_done INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_positions (
                symbol TEXT PRIMARY KEY,
                direction TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)


initialize()


# ============================================================
# CLIENT
# ============================================================

_client = None
client_lock = threading.Lock()
trade_lock = threading.RLock()


def get_client():
    global _client

    with client_lock:
        if _client is None:
            _client = Client(
                API_KEY,
                API_SECRET,
                testnet=TESTNET
            )

        return _client


# ============================================================
# SECURITY
# ============================================================

def tradingview_source(req):
    """
    Railway public HTTP edge supplies X-Real-IP.

    This must not be exposed over untrusted private
    networking or an alternate proxy that permits
    callers to forge X-Real-IP.
    """

    raw = req.headers.get("X-Real-IP", "").strip()

    try:
        address = str(ipaddress.ip_address(raw))
    except ValueError:
        return False

    return address in TRADINGVIEW_IPS


def secret_valid(req, expected):
    supplied = (
        req.args.get("secret", "")
        or req.headers.get("X-Webhook-Secret", "")
    )

    return bool(expected) and hmac.compare_digest(
        supplied,
        expected
    )


def webhook_allowed(req):
    if WEBHOOK_SECRET and secret_valid(
        req,
        WEBHOOK_SECRET
    ):
        return True

    return tradingview_source(req)


# ============================================================
# PARSER
# ============================================================

def normalize_symbol(raw):
    symbol = str(raw).strip().upper().rstrip(".")

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


def parse_alert(text):
    text = str(text or "").strip()

    match = re.fullmatch(
        r"(BUY|SELL)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?",
        text,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    match = re.search(
        r"\border\s+(buy|sell)\s+@\s+"
        r"[^\r\n]*?\bfilled\s+on\s+"
        r"([A-Za-z0-9:._/\-]+)",
        text,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    raise ValueError("Unsupported alert message")


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

        for key in ("message", "text", "alert"):
            if isinstance(data.get(key), str):
                signal, symbol = parse_alert(data[key])
                return signal, symbol, data[key]

    raw = (
        data
        if isinstance(data, str)
        else req.get_data(as_text=True)
    )

    signal, symbol = parse_alert(raw)

    return signal, symbol, raw


# ============================================================
# WEBHOOK
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    if not webhook_allowed(request):
        return jsonify({
            "status": "rejected",
            "reason": "Source not authenticated"
        }), 403

    try:
        signal, symbol, raw = extract_alert(request)

        now = time.time()

        # Duplicate suppression is approximate.
        # Identical genuine signals inside this window
        # may also be suppressed.
        bucket = int(now // 900)

        fingerprint = hashlib.sha256(
            f"{bucket}|{signal}|{symbol}|{raw}".encode()
        ).hexdigest()

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
            "Alert %s %s: %s",
            signal,
            symbol,
            "queued" if inserted else "duplicate"
        )

        return jsonify({
            "status": (
                "queued" if inserted else "duplicate"
            ),
            "symbol": symbol,
            "signal": signal
        }), 200

    except ValueError as exc:
        return jsonify({
            "error": str(exc)
        }), 400

    except Exception:
        log.exception("Webhook error")
        return jsonify({
            "error": "Queue unavailable"
        }), 503


# ============================================================
# BINANCE HELPERS
# ============================================================

def assert_one_way():
    result = get_client().futures_get_position_mode()

    if result.get("dualSidePosition"):
        raise RuntimeError(
            "Binance must use One-way Position Mode"
        )


def symbol_info(symbol):
    info = get_client().futures_exchange_info()

    for item in info["symbols"]:
        if item["symbol"] == symbol:
            if item["status"] != "TRADING":
                raise RuntimeError("Symbol not trading")

            return item

    raise RuntimeError(
        f"Unknown Binance Futures symbol {symbol}"
    )


def quantity_step(symbol):
    info = symbol_info(symbol)

    for filter_type in (
        "MARKET_LOT_SIZE",
        "LOT_SIZE"
    ):
        for item in info["filters"]:
            if item["filterType"] == filter_type:
                step = Decimal(item["stepSize"])

                if step > 0:
                    return step

    raise RuntimeError("Quantity step unavailable")


def rounded_quantity(symbol, quantity):
    step = quantity_step(symbol)

    quantity = Decimal(str(quantity))

    return float(
        (quantity / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step
    )


def current_position(symbol):
    rows = get_client().futures_position_information(
        symbol=symbol
    )

    active = [
        row for row in rows
        if abs(float(row["positionAmt"])) > 0
    ]

    if len(active) > 1:
        raise RuntimeError(
            "Multiple position sides unsupported"
        )

    if active:
        if active[0].get(
            "positionSide", "BOTH"
        ) != "BOTH":
            raise RuntimeError("Hedge Mode unsupported")

        return active[0]

    return None


def all_positions():
    return [
        row
        for row in get_client().futures_position_information()
        if abs(float(row["positionAmt"])) > 0
    ]


def roi(position):
    entry = float(position["entryPrice"])
    mark = float(position.get("markPrice", 0) or 0)
    amount = float(position["positionAmt"])
    leverage = float(position.get("leverage", 1) or 1)

    if entry <= 0 or mark <= 0:
        raise RuntimeError("Invalid position pricing")

    result = (mark - entry) / entry

    if amount < 0:
        result = -result

    return result * leverage * 100


def market_order(symbol, side, qty, reduce_only=False):
    qty = rounded_quantity(symbol, qty)

    if qty <= 0:
        raise RuntimeError("Quantity too small")

    args = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty
    }

    if reduce_only:
        args["reduceOnly"] = True

    return get_client().futures_create_order(**args)


def close_position(position, fraction=1):
    amount = float(position["positionAmt"])

    if not amount:
        return None

    side = "SELL" if amount > 0 else "BUY"

    return market_order(
        position["symbol"],
        side,
        abs(amount) * fraction,
        reduce_only=True
    )


def wait_position(symbol, direction=None):
    for _ in range(20):
        position = current_position(symbol)

        if direction is None:
            if position is None:
                return None

        elif position:
            amount = float(position["positionAmt"])

            if (
                direction == "BUY" and amount > 0
            ) or (
                direction == "SELL" and amount < 0
            ):
                return position

        time.sleep(0.5)

    raise RuntimeError(
        f"Position confirmation timeout: {symbol}"
    )


# ============================================================
# BOT OWNERSHIP
# ============================================================

def is_bot_position(symbol):
    with db() as conn:
        return conn.execute(
            "SELECT 1 FROM bot_positions WHERE symbol=?",
            (symbol,)
        ).fetchone() is not None


def save_bot_position(symbol, direction):
    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO bot_positions
            (symbol, direction, created_at)
            VALUES (?, ?, ?)
        """, (symbol, direction, time.time()))


def remove_bot_position(symbol):
    with db() as conn:
        conn.execute(
            "DELETE FROM bot_positions WHERE symbol=?",
            (symbol,)
        )

        conn.execute(
            "DELETE FROM position_state WHERE symbol=?",
            (symbol,)
        )


def get_state(position):
    symbol = position["symbol"]
    entry = float(position["entryPrice"])
    qty = abs(float(position["positionAmt"]))

    with db() as conn:
        row = conn.execute(
            "SELECT * FROM position_state WHERE symbol=?",
            (symbol,)
        ).fetchone()

        if row is None:
            conn.execute("""
                INSERT INTO position_state (
                    symbol,
                    entry_price,
                    entry_qty
                )
                VALUES (?, ?, ?)
            """, (symbol, entry, qty))

            row = conn.execute(
                "SELECT * FROM position_state WHERE symbol=?",
                (symbol,)
            ).fetchone()

        elif abs(
            float(row["entry_price"]) - entry
        ) > max(entry * 0.000001, 1e-12):
            raise RuntimeError(
                "Position entry changed unexpectedly; "
                "manual reconciliation required"
            )

        return dict(row)


def update_state(symbol, field):
    if field not in (
        "partial_done",
        "breakeven_done",
        "initial_stop_done"
    ):
        raise ValueError("Invalid state field")

    with db() as conn:
        conn.execute(
            f"UPDATE position_state SET {field}=1 "
            "WHERE symbol=?",
            (symbol,)
        )


# ============================================================
# ENTRY
# ============================================================

def execute_signal(signal, symbol):
    with trade_lock:
        assert_one_way()

        current = current_position(symbol)

        if current:
            if not is_bot_position(symbol):
                raise RuntimeError(
                    "Existing non-bot position detected; "
                    "refusing to modify it"
                )

            direction = (
                "BUY"
                if float(current["positionAmt"]) > 0
                else "SELL"
            )

            if direction == signal:
                return "already_in_position"

            # Close first; never open opposite side until
            # exchange confirms the position is flat.
            close_position(current)
            wait_position(symbol, None)
            remove_bot_position(symbol)

        get_client().futures_change_leverage(
            symbol=symbol,
            leverage=LEVERAGE
        )

        price = float(
            get_client().futures_symbol_ticker(
                symbol=symbol
            )["price"]
        )

        qty = MARGIN * LEVERAGE / price

        order = market_order(
            symbol,
            signal,
            qty
        )

        log.warning(
            "Entry submitted %s %s order=%s",
            signal,
            symbol,
            order.get("orderId")
        )

        position = wait_position(symbol, signal)

        save_bot_position(symbol, signal)
        get_state(position)

        return "executed"


# ============================================================
# QUEUE
# ============================================================

def claim_alert():
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute("""
            SELECT *
            FROM alerts
            WHERE status='pending'
            ORDER BY id
            LIMIT 1
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


def finish_alert(alert_id, status, error=None):
    with db() as conn:
        conn.execute("""
            UPDATE alerts
            SET status=?,
                error=?,
                updated_at=?
            WHERE id=?
        """, (
            status,
            error,
            time.time(),
            alert_id
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

            if time.time() - alert["received_at"] > MAX_AGE:
                finish_alert(alert_id, "expired")
                continue

            if not LIVE:
                log.info(
                    "DRY RUN %s %s",
                    alert["signal"],
                    alert["symbol"]
                )

                finish_alert(alert_id, "dry_run")
                continue

            try:
                result = execute_signal(
                    alert["signal"],
                    alert["symbol"]
                )

                finish_alert(alert_id, result)

            except Exception as exc:
                log.exception(
                    "Execution requires review"
                )

                finish_alert(
                    alert_id,
                    "needs_review",
                    str(exc)[:500]
                )

        except Exception:
            log.exception("Queue failure")
            time.sleep(2)


# ============================================================
# RISK MONITOR
# ============================================================

def monitor():
    if not LIVE:
        return

    with trade_lock:
        assert_one_way()

        positions = [
            p for p in all_positions()
            if is_bot_position(p["symbol"])
        ]

        total_profit = sum(
            float(p.get("unRealizedProfit", 0) or 0)
            for p in positions
        )

        if (
            PORTFOLIO_TARGET > 0
            and total_profit >= PORTFOLIO_TARGET
        ):
            for p in positions:
                symbol = p["symbol"]
                close_position(p)
                wait_position(symbol, None)
                remove_bot_position(symbol)

            return

        for p in positions:
            symbol = p["symbol"]
            value = roi(p)
            state = get_state(p)

            if INITIAL_SL > 0 and value <= -INITIAL_SL:
                close_position(p)
                wait_position(symbol, None)
                remove_bot_position(symbol)
                continue

            if FINAL_TP > 0 and value >= FINAL_TP:
                close_position(p)
                wait_position(symbol, None)
                remove_bot_position(symbol)
                continue

            if (
                BREAKEVEN > 0
                and value >= BREAKEVEN
                and not state["breakeven_done"]
            ):
                update_state(symbol, "breakeven_done")
                state["breakeven_done"] = 1

            if (
                state["breakeven_done"]
                and value <= 0
            ):
                close_position(p)
                wait_position(symbol, None)
                remove_bot_position(symbol)
                continue

            if (
                PARTIAL_TP > 0
                and PARTIAL_CLOSE > 0
                and value >= PARTIAL_TP
                and not state["partial_done"]
            ):
                result = close_position(
                    p,
                    PARTIAL_CLOSE / 100
                )

                if result:
                    update_state(symbol, "partial_done")

                    if BREAKEVEN > 0:
                        update_state(
                            symbol,
                            "breakeven_done"
                        )


def risk_worker():
    log.info("MAQ risk monitor started")

    while True:
        try:
            monitor()
        except Exception:
            log.exception("Risk monitor failure")

        time.sleep(POLL)


# ============================================================
# HEALTH AND STATUS
# ============================================================

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "MAQ DH V7",
        "live_orders": LIVE,
        "testnet": TESTNET
    })


@app.route("/status", methods=["GET"])
def status():
    if not STATUS_SECRET or not secret_valid(
        request,
        STATUS_SECRET
    ):
        return jsonify({
            "error": "Unauthorized"
        }), 401

    with db() as conn:
        counts = conn.execute("""
            SELECT status, COUNT(*) AS count
            FROM alerts
            GROUP BY status
        """).fetchall()

        recent = conn.execute("""
            SELECT id, symbol, signal,
                   status, error, received_at
            FROM alerts
            ORDER BY id DESC
            LIMIT 20
        """).fetchall()

    return jsonify({
        "service": "MAQ DH V7",
        "live_orders": LIVE,
        "testnet": TESTNET,
        "queue": {
            row["status"]: row["count"]
            for row in counts
        },
        "recent_alerts": [
            dict(row) for row in recent
        ],
        "settings": {
            "margin": MARGIN,
            "leverage": LEVERAGE,
            "initial_stop_loss_roi_pct": INITIAL_SL,
            "breakeven_roi_pct": BREAKEVEN,
            "partial_tp_roi_pct": PARTIAL_TP,
            "partial_close_pct": PARTIAL_CLOSE,
            "final_tp_roi_pct": FINAL_TP,
            "portfolio_profit_usd": PORTFOLIO_TARGET
        }
    })


# ============================================================
# START
# ============================================================

def start_workers():
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
    app.run(
        host="0.0.0.0",
        port=int(setting("PORT", 8080)),
        threaded=True
    )
