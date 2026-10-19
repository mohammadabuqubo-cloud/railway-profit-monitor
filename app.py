
import os
import re
import time
import json
import hmac
import hashlib
import sqlite3
import logging
import threading

from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify
from binance.client import Client


# ============================================================
# MAQ RAILWAY BOT
# TradingView Entries + Five Partial TPs
# Railway Stop Loss + Breakeven
#
# TESTING VERSION - LIVE ORDERS DISABLED BY DEFAULT
# Binance USD-M Futures, One-Way Mode
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ")
app = Flask(__name__)


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

def env(name, default=""):
    value = os.getenv(name)
    return str(default if value is None else value).strip()


def flag(name, default=False):
    return env(
        name,
        "true" if default else "false"
    ).lower() in ("true", "1", "yes", "on")


def number(name, default=0):
    value = env(name)
    return float(value) if value else float(default)


API_KEY = env("BINANCE_API_KEY")
API_SECRET = env("BINANCE_API_SECRET")

LIVE = flag("ENABLE_LIVE_ORDERS", False)
TESTNET = flag("BINANCE_TESTNET", False)

WEBHOOK_SECRET = env("WEBHOOK_SECRET")

MARGIN = number("ORDER_MARGIN_USDT", 50)
LEVERAGE = int(number("ENTRY_LEVERAGE", 16))

INITIAL_SL = number(
    "INITIAL_STOP_LOSS_ROI_PCT", 20
)

BREAKEVEN_TRIGGER = number(
    "BREAKEVEN_TRIGGER_ROI_PCT", 10
)

BREAKEVEN_STOP = number(
    "BREAKEVEN_STOP_ROI_PCT", 0
)

POLL = max(
    5,
    int(number("POLL_INTERVAL_SECONDS", 5))
)

MAX_ALERT_AGE = max(
    30,
    int(number("MAX_ALERT_AGE_SECONDS", 180))
)

PARTIAL_DEDUP_SECONDS = max(
    5,
    int(number("PARTIAL_DEDUP_SECONDS", 15))
)

DB_PATH = env(
    "BOT_DB_PATH",
    "/data/maq_bot.sqlite3"
)

# Old Railway take-profit rules are intentionally
# not used by this TradingView-controlled bot.

if LIVE and (not API_KEY or not API_SECRET):
    raise RuntimeError(
        "Missing Binance API credentials"
    )

if MARGIN <= 0 or LEVERAGE <= 0:
    raise RuntimeError(
        "Invalid margin or leverage"
    )

if INITIAL_SL <= 0:
    raise RuntimeError(
        "Initial stop-loss must be positive"
    )

if BREAKEVEN_TRIGGER < 0:
    raise RuntimeError(
        "Invalid breakeven trigger"
    )


# ============================================================
# DATABASE
# ============================================================

os.makedirs(
    os.path.dirname(DB_PATH) or ".",
    exist_ok=True
)

DB_LOCK = threading.RLock()
TRADE_LOCK = threading.RLock()
CLIENT_LOCK = threading.Lock()

_client = None


def database():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=20
    )

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    conn.execute("PRAGMA journal_mode=WAL")

    return conn


def initialize_database():
    with DB_LOCK, database() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE,
                symbol TEXT,
                signal TEXT,
                raw_message TEXT,
                received_at REAL,
                status TEXT DEFAULT 'pending',
                error TEXT,
                updated_at REAL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_positions (
                symbol TEXT PRIMARY KEY,
                direction TEXT,
                created_at REAL,
                strategy TEXT DEFAULT 'VWAP'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_state (
                symbol TEXT PRIMARY KEY,
                entry_price REAL,
                entry_qty REAL,
                partial_done INTEGER DEFAULT 0,
                breakeven_done INTEGER DEFAULT 0,
                tp_count INTEGER DEFAULT 0
            )
        """)

        # Preserve existing V8 databases.
        for table, column, definition in (
            ("bot_positions", "strategy", "TEXT DEFAULT 'DH'"),
            ("position_state", "tp_count", "INTEGER DEFAULT 0")
        ):
            columns = {
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info({table})"
                )
            }

            if column not in columns:
                conn.execute(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN {column} {definition}"
                )

        # Never replay interrupted orders automatically.
        conn.execute("""
            UPDATE alerts
            SET status='needs_review',
                error='Interrupted execution; verify Binance',
                updated_at=?
            WHERE status='processing'
        """, (time.time(),))


initialize_database()


# ============================================================
# BINANCE CLIENT
# ============================================================

def client():
    global _client

    with CLIENT_LOCK:
        if _client is None:
            _client = Client(
                API_KEY,
                API_SECRET,
                testnet=TESTNET
            )

        return _client


# ============================================================
# TRADINGVIEW MESSAGE PARSER
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

    if not re.fullmatch(
        r"[A-Z0-9]{5,30}",
        symbol
    ):
        raise ValueError("Invalid symbol")

    return symbol


def parse_message(message):
    message = str(message or "").strip()

    # PARTIAL|BTCUSDT|20
    partial = re.fullmatch(
        r"PARTIAL\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)\s*\|\s*"
        r"([0-9]+(?:\.[0-9]+)?)",
        message,
        re.IGNORECASE
    )

    if partial:
        percentage = float(partial.group(2))

        if percentage != 20:
            raise ValueError(
                "Only 20% partial exits supported"
            )

        return (
            "PARTIAL",
            normalize_symbol(partial.group(1))
        )

    # CLOSE|BTCUSDT
    close = re.fullmatch(
        r"CLOSE\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if close:
        return (
            "CLOSE",
            normalize_symbol(close.group(1))
        )

    # BUY|BTCUSDT / SELL|BTCUSDT
    entry = re.fullmatch(
        r"(BUY|SELL)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?",
        message,
        re.IGNORECASE
    )

    if entry:
        return (
            entry.group(1).upper(),
            normalize_symbol(entry.group(2))
        )

    # Legacy DH strategy order-fill messages
    dh = re.search(
        r"\border\s+(buy|sell)\s+@\s+"
        r"[^\r\n]*?\bfilled\s+on\s+"
        r"([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if dh:
        return (
            dh.group(1).upper(),
            normalize_symbol(dh.group(2))
        )

    raise ValueError(
        "Unsupported TradingView alert"
    )


def extract_alert():
    data = request.get_json(silent=True)

    if isinstance(data, dict):
        signal = data.get("signal") or data.get("action")
        symbol = data.get("symbol") or data.get("ticker")

        if signal and symbol:
            signal = str(signal).upper().strip()

            if signal not in (
                "BUY", "SELL", "PARTIAL", "CLOSE"
            ):
                raise ValueError("Invalid signal")

            if signal == "PARTIAL":
                percentage = float(
                    data.get("percentage", 20)
                )

                if percentage != 20:
                    raise ValueError(
                        "Only 20% partial exits supported"
                    )

            return (
                signal,
                normalize_symbol(symbol),
                json.dumps(data, sort_keys=True)
            )

        for key in ("message", "text", "alert"):
            if isinstance(data.get(key), str):
                signal, symbol = parse_message(data[key])
                return signal, symbol, data[key]

    raw = (
        data if isinstance(data, str)
        else request.get_data(as_text=True)
    )

    signal, symbol = parse_message(raw)
    return signal, symbol, str(raw)


# ============================================================
# WEBHOOK
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook():

    if WEBHOOK_SECRET:
        supplied = (
            request.args.get("secret", "")
            or request.headers.get(
                "X-Webhook-Secret", ""
            )
        )

        if not hmac.compare_digest(
            supplied,
            WEBHOOK_SECRET
        ):
            return jsonify(
                error="Unauthorized"
            ), 403

    try:
        signal, symbol, raw = extract_alert()
        now = time.time()

        # Partial alerts need shorter duplicate windows
        # because multiple TP levels may fire.
        #
        # IMPORTANT: This is only best-effort deduplication.
        # Identical messages cannot guarantee exactly-once
        # execution without a unique TradingView event ID.

        window = (
            PARTIAL_DEDUP_SECONDS
            if signal == "PARTIAL"
            else 900
        )

        bucket = int(now // window)

        fingerprint = hashlib.sha256(
            f"{bucket}|{signal}|{symbol}|{raw}".encode()
        ).hexdigest()

        with DB_LOCK, database() as conn:
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

        status = (
            "queued" if inserted else "duplicate"
        )

        log.info(
            "TradingView %s %s: %s",
            signal,
            symbol,
            status
        )

        return jsonify(
            status=status,
            symbol=symbol,
            signal=signal
        )

    except (ValueError, TypeError) as exc:
        log.warning("Invalid alert: %s", exc)
        return jsonify(error=str(exc)), 400

    except Exception:
        log.exception("Webhook failure")
        return jsonify(
            error="Queue unavailable"
        ), 503


# ============================================================
# POSITION OWNERSHIP
# ============================================================

def owned(symbol):
    with DB_LOCK, database() as conn:
        row = conn.execute(
            "SELECT 1 FROM bot_positions "
            "WHERE symbol=?",
            (symbol,)
        ).fetchone()

        return row is not None


def save_owned(symbol, direction):
    with DB_LOCK, database() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO bot_positions
            (symbol, direction, created_at, strategy)
            VALUES (?, ?, ?, 'VWAP')
        """, (
            symbol,
            direction,
            time.time()
        ))


def remove_owned(symbol):
    with DB_LOCK, database() as conn:
        conn.execute(
            "DELETE FROM bot_positions "
            "WHERE symbol=?",
            (symbol,)
        )

        conn.execute(
            "DELETE FROM position_state "
            "WHERE symbol=?",
            (symbol,)
        )


# ============================================================
# BINANCE POSITION HELPERS
# ============================================================

def positions(symbol=None):
    args = (
        {"symbol": symbol}
        if symbol else {}
    )

    result = client().futures_position_information(
        **args
    )

    return [
        p for p in result
        if abs(float(p["positionAmt"])) > 0
    ]


def current_position(symbol):
    result = positions(symbol)

    if len(result) > 1:
        raise RuntimeError(
            "Hedge Mode unsupported"
        )

    if result:
        if result[0].get(
            "positionSide", "BOTH"
        ) != "BOTH":
            raise RuntimeError(
                "One-way Position Mode required"
            )

        return result[0]

    return None


def quantity_step(symbol):
    exchange = client().futures_exchange_info()

    for item in exchange["symbols"]:
        if item["symbol"] != symbol:
            continue

        if item["status"] != "TRADING":
            raise RuntimeError(
                "Symbol not trading"
            )

        for filter_type in (
            "MARKET_LOT_SIZE",
            "LOT_SIZE"
        ):
            for item_filter in item["filters"]:
                if item_filter["filterType"] == filter_type:
                    step = Decimal(
                        item_filter["stepSize"]
                    )

                    if step > 0:
                        return step

    raise RuntimeError(
        "Symbol quantity step unavailable"
    )


def rounded_quantity(symbol, quantity):
    step = quantity_step(symbol)
    quantity = Decimal(str(quantity))

    return float(
        (quantity / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step
    )


def market_order(
    symbol,
    side,
    quantity,
    reduce_only=False
):
    quantity = rounded_quantity(
        symbol,
        quantity
    )

    if quantity <= 0:
        raise RuntimeError(
            "Order quantity too small"
        )

    args = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": quantity
    }

    if reduce_only:
        args["reduceOnly"] = True

    return client().futures_create_order(
        **args
    )


def close_position(position):
    amount = float(position["positionAmt"])

    if amount == 0:
        return None

    side = (
        "SELL" if amount > 0 else "BUY"
    )

    return market_order(
        position["symbol"],
        side,
        abs(amount),
        reduce_only=True
    )


def wait_position(symbol, direction=None):
    for _ in range(20):
        position = current_position(symbol)

        if direction is None and position is None:
            return None

        if direction and position:
            amount = float(
                position["positionAmt"]
            )

            if (
                direction == "BUY" and amount > 0
            ) or (
                direction == "SELL" and amount < 0
            ):
                return position

        time.sleep(0.5)

    raise RuntimeError(
        "Position confirmation timeout; "
        "manual review required"
    )


# ============================================================
# POSITION STATE
# ============================================================

def position_state(position):
    symbol = position["symbol"]
    entry = float(position["entryPrice"])
    quantity = abs(
        float(position["positionAmt"])
    )

    with DB_LOCK, database() as conn:
        row = conn.execute(
            "SELECT * FROM position_state "
            "WHERE symbol=?",
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
            """, (
                symbol,
                entry,
                quantity
            ))

            row = conn.execute(
                "SELECT * FROM position_state "
                "WHERE symbol=?",
                (symbol,)
            ).fetchone()

        elif abs(
            float(row["entry_price"]) - entry
        ) > max(
            entry * 0.000001,
            1e-12
        ):
            raise RuntimeError(
                f"{symbol}: entry price mismatch. "
                f"Saved={row['entry_price']}, "
                f"Binance={entry}. "
                "Manual reconciliation required."
            )

        return dict(row)


def mark_breakeven(symbol):
    with DB_LOCK, database() as conn:
        conn.execute("""
            UPDATE position_state
            SET breakeven_done=1
            WHERE symbol=?
        """, (symbol,))


def calculate_roi(position):
    entry = float(position["entryPrice"])
    mark = float(
        position.get("markPrice") or 0
    )

    if entry <= 0 or mark <= 0:
        raise RuntimeError(
            "Invalid mark or entry price"
        )

    direction = (
        1 if float(
            position["positionAmt"]
        ) > 0 else -1
    )

    leverage = float(
        position.get("leverage") or 1
    )

    return (
        (mark - entry) / entry
        * direction
        * leverage
        * 100
    )


# ============================================================
# ENTRY ENGINE
# ============================================================

def execute_entry(signal, symbol):
    with TRADE_LOCK:

        mode = client().futures_get_position_mode()

        if mode.get("dualSidePosition"):
            raise RuntimeError(
                "One-way Position Mode required"
            )

        existing = current_position(symbol)

        if existing:
            if not owned(symbol):
                raise RuntimeError(
                    "Existing non-bot position; "
                    "refusing to modify"
                )

            current_direction = (
                "BUY"
                if float(
                    existing["positionAmt"]
                ) > 0
                else "SELL"
            )

            if current_direction == signal:
                return "already_in_position"

            # Reverse only after confirming
            # the existing position is closed.
            close_position(existing)
            wait_position(symbol)
            remove_owned(symbol)

        client().futures_change_leverage(
            symbol=symbol,
            leverage=LEVERAGE
        )

        ticker = client().futures_symbol_ticker(
            symbol=symbol
        )

        price = float(ticker["price"])

        quantity = (
            MARGIN * LEVERAGE / price
        )

        result = market_order(
            symbol,
            signal,
            quantity
        )

        log.warning(
            "ENTRY SUBMITTED %s %s ID=%s",
            signal,
            symbol,
            result.get("orderId")
        )

        position = wait_position(
            symbol,
            signal
        )

        save_owned(symbol, signal)
        position_state(position)

        return "entry_executed"


# ============================================================
# TRADINGVIEW PARTIAL TP ENGINE
# ============================================================

def execute_partial(symbol):
    with TRADE_LOCK:

        if not owned(symbol):
            raise RuntimeError(
                f"{symbol}: position not owned "
                "by Railway"
            )

        position = current_position(symbol)

        if not position:
            remove_owned(symbol)
            return "already_flat"

        state = position_state(position)

        tp_count = int(
            state["tp_count"] or 0
        )

        if tp_count >= 4:
            return "four_partial_tps_completed"

        original_qty = float(
            state["entry_qty"]
        )

        remaining_qty = abs(
            float(position["positionAmt"])
        )

        # 20% of ORIGINAL position,
        # not 20% of remaining position.
        target_qty = rounded_quantity(
            symbol,
            original_qty * 0.20
        )

        if target_qty <= 0:
            raise RuntimeError(
                "Partial quantity below "
                "Binance minimum step"
            )

        # Avoid accidentally attempting
        # to close more than remains.
        quantity = min(
            target_qty,
            remaining_qty
        )

        side = (
            "SELL"
            if float(
                position["positionAmt"]
            ) > 0
            else "BUY"
        )

        result = market_order(
            symbol,
            side,
            quantity,
            reduce_only=True
        )

        log.warning(
            "PARTIAL TP SUBMITTED "
            "%s TP=%s QTY=%s ORDER=%s",
            symbol,
            tp_count + 1,
            quantity,
            result.get("orderId")
        )

        # Confirm Binance quantity changed
        # before marking the TP completed.
        updated = current_position(symbol)

        if updated:
            new_qty = abs(
                float(updated["positionAmt"])
            )

            if new_qty >= remaining_qty:
                raise RuntimeError(
                    "Partial order not confirmed; "
                    "manual review required"
                )

        with DB_LOCK, database() as conn:
            conn.execute("""
                UPDATE position_state
                SET tp_count=?
                WHERE symbol=?
            """, (
                tp_count + 1,
                symbol
            ))

        if not updated:
            remove_owned(symbol)

        return (
            f"partial_tp_{tp_count + 1}_executed"
        )


# ============================================================
# FULL CLOSE ENGINE
# ============================================================

def execute_close(symbol):
    with TRADE_LOCK:

        if not owned(symbol):
            raise RuntimeError(
                "No Railway-owned position"
            )

        position = current_position(symbol)

        if not position:
            remove_owned(symbol)
            return "already_flat"

        close_position(position)
        wait_position(symbol)
        remove_owned(symbol)

        return "closed"


# ============================================================
# SIGNAL DISPATCH
# ============================================================

def execute_signal(signal, symbol):
    if signal in ("BUY", "SELL"):
        return execute_entry(
            signal,
            symbol
        )

    if signal == "PARTIAL":
        return execute_partial(symbol)

    if signal == "CLOSE":
        return execute_close(symbol)

    raise ValueError(
        "Unsupported execution signal"
    )


# ============================================================
# ALERT QUEUE
# ============================================================

def claim_alert():
    with DB_LOCK, database() as conn:

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
            """, (
                time.time(),
                row["id"]
            ))

        conn.commit()

        return (
            dict(row) if row else None
        )


def finish_alert(
    alert_id,
    status,
    error=None
):
    with DB_LOCK, database() as conn:
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
    log.info(
        "MAQ queue worker started "
        "LIVE=%s TESTNET=%s",
        LIVE,
        TESTNET
    )

    while True:
        try:
            alert = claim_alert()

            if not alert:
                time.sleep(0.5)
                continue

            if (
                time.time()
                - alert["received_at"]
                > MAX_ALERT_AGE
            ):
                finish_alert(
                    alert["id"],
                    "expired"
                )
                continue

            if not LIVE:
                log.info(
                    "DRY RUN %s %s",
                    alert["signal"],
                    alert["symbol"]
                )

                finish_alert(
                    alert["id"],
                    "dry_run"
                )
                continue

            try:
                result = execute_signal(
                    alert["signal"],
                    alert["symbol"]
                )

                finish_alert(
                    alert["id"],
                    result
                )

            except Exception as exc:
                log.exception(
                    "Trade requires manual review"
                )

                finish_alert(
                    alert["id"],
                    "needs_review",
                    str(exc)[:500]
                )

        except Exception:
            log.exception(
                "Queue worker error"
            )

            time.sleep(2)


# ============================================================
# RAILWAY STOP LOSS AND BREAKEVEN
# ============================================================

def monitor_risk():
    if not LIVE:
        return

    with TRADE_LOCK:

        active = [
            p for p in positions()
            if owned(p["symbol"])
        ]

        for position in active:
            symbol = position["symbol"]

            try:
                roi = calculate_roi(position)
                state = position_state(position)

                # Initial stop-loss
                if roi <= -INITIAL_SL:
                    log.warning(
                        "INITIAL STOP %s ROI=%.3f",
                        symbol,
                        roi
                    )

                    close_position(position)
                    wait_position(symbol)
                    remove_owned(symbol)
                    continue

                # Arm breakeven at trigger
                if (
                    BREAKEVEN_TRIGGER > 0
                    and roi >= BREAKEVEN_TRIGGER
                    and not state["breakeven_done"]
                ):
                    mark_breakeven(symbol)
                    state["breakeven_done"] = 1

                    log.warning(
                        "BREAKEVEN ARMED "
                        "%s ROI=%.3f",
                        symbol,
                        roi
                    )

                # Close at configured BE stop
                if (
                    state["breakeven_done"]
                    and roi <= BREAKEVEN_STOP
                ):
                    log.warning(
                        "BREAKEVEN EXIT "
                        "%s ROI=%.3f",
                        symbol,
                        roi
                    )

                    close_position(position)
                    wait_position(symbol)
                    remove_owned(symbol)
                    continue

                # No Railway take-profit rules.
                # TradingView controls TP1-TP5.

            except Exception:
                log.exception(
                    "RISK ERROR symbol=%s; "
                    "manual review required",
                    symbol
                )


def risk_worker():
    log.info(
        "MAQ risk monitor started"
    )

    while True:
        try:
            monitor_risk()

        except Exception:
            log.exception(
                "Risk monitor error"
            )

        time.sleep(POLL)


# ============================================================
# HEALTH
# ============================================================

@app.route("/", methods=["GET"])
def health():
    return jsonify(
        status="ok",
        bot="MAQ TradingView TP Ladder",
        live=LIVE,
        testnet=TESTNET
    )


# ============================================================
# STATUS
# ============================================================

@app.route("/status", methods=["GET"])
def status():

    with DB_LOCK, database() as conn:
        alerts = conn.execute("""
            SELECT status, COUNT(*) AS count
            FROM alerts
            GROUP BY status
        """).fetchall()

        states = conn.execute("""
            SELECT symbol, entry_price,
                   entry_qty, tp_count,
                   breakeven_done
            FROM position_state
        """).fetchall()

    return jsonify(
        bot="MAQ TradingView TP Ladder",
        live=LIVE,
        testnet=TESTNET,
        margin_usdt=MARGIN,
        leverage=LEVERAGE,
        initial_stop_loss_roi=INITIAL_SL,
        breakeven_trigger_roi=BREAKEVEN_TRIGGER,
        breakeven_stop_roi=BREAKEVEN_STOP,
        stop_type="software_polling",
        queue={
            row["status"]: row["count"]
            for row in alerts
        },
        position_states=[
            dict(row) for row in states
        ]
    )


# ============================================================
# START WORKERS
# ============================================================

# Railway: one replica, one Gunicorn worker.
# A persistent volume is required for SQLite state.

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


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(env("PORT", 8080)),
        threaded=True
    )
