
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
# MAQ BINANCE FUTURES BOT
#
# TradingView:
# BUY|BTCUSDT
# SELL|BTCUSDT
# PARTIAL|BTCUSDT|20
# CLOSE|BTCUSDT
#
# Also supports:
# VWAP|BUY|BTCUSDT
# VWAP|SELL|BTCUSDT
# VWAP|TP1|BTCUSDT
# VWAP|TP2|BTCUSDT
# VWAP|TP3|BTCUSDT
# VWAP|TP4|BTCUSDT
# VWAP|TP5|BTCUSDT
# VWAP|CLOSE|BTCUSDT
#
# Railway:
# Initial SL and Breakeven based on Binance entry price.
#
# IMPORTANT:
# - One-way Futures Mode only
# - Software stops, not exchange-native stop orders
# - Single Railway replica / single Gunicorn worker
# - Use a persistent Railway volume for SQLite
# - Live execution disabled by default
# ============================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ")

app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

def env(name, default=""):
    value = os.getenv(name)
    return str(default if value is None else value).strip()


def boolean(name, default=False):
    return env(
        name,
        "true" if default else "false"
    ).lower() in ("true", "1", "yes", "on")


def number(name, default):
    value = env(name)
    return float(value) if value else float(default)


API_KEY = env("BINANCE_API_KEY")
API_SECRET = env("BINANCE_API_SECRET")

ENABLE_LIVE_ORDERS = boolean(
    "ENABLE_LIVE_ORDERS", False
)

BINANCE_TESTNET = boolean(
    "BINANCE_TESTNET", False
)

WEBHOOK_SECRET = env("WEBHOOK_SECRET")

ORDER_MARGIN_USDT = number(
    "ORDER_MARGIN_USDT", 50
)

ENTRY_LEVERAGE = int(
    number("ENTRY_LEVERAGE", 16)
)

INITIAL_STOP_LOSS_ROI_PCT = number(
    "INITIAL_STOP_LOSS_ROI_PCT", 20
)

BREAKEVEN_TRIGGER_ROI_PCT = number(
    "BREAKEVEN_TRIGGER_ROI_PCT", 10
)

BREAKEVEN_STOP_ROI_PCT = number(
    "BREAKEVEN_STOP_ROI_PCT", 0
)

POLL_INTERVAL_SECONDS = max(
    5,
    int(number("POLL_INTERVAL_SECONDS", 5))
)

PARTIAL_DEDUP_SECONDS = max(
    1,
    int(number("PARTIAL_DEDUP_SECONDS", 15))
)

MAX_ALERT_AGE_SECONDS = max(
    30,
    int(number("MAX_ALERT_AGE_SECONDS", 180))
)

BOT_DB_PATH = env(
    "BOT_DB_PATH",
    "/data/maq_bot.sqlite3"
)

# When True, mismatched entry prices are logged.
# Binance actual entry price is used for risk calculation.
LOG_ENTRY_MISMATCH = boolean(
    "LOG_ENTRY_MISMATCH", True
)

# Optional: refuse new positions when unresolved
# existing bot positions are detected.
BLOCK_NEW_ENTRIES_ON_MISMATCH = boolean(
    "BLOCK_NEW_ENTRIES_ON_MISMATCH", True
)


if ENABLE_LIVE_ORDERS:
    if not API_KEY or not API_SECRET:
        raise RuntimeError(
            "Binance API credentials are missing"
        )

if ORDER_MARGIN_USDT <= 0:
    raise RuntimeError("Invalid margin")

if ENTRY_LEVERAGE < 1:
    raise RuntimeError("Invalid leverage")

if INITIAL_STOP_LOSS_ROI_PCT <= 0:
    raise RuntimeError("Invalid initial SL")


# ============================================================
# LOCKS AND CLIENT
# ============================================================

DB_LOCK = threading.RLock()
TRADE_LOCK = threading.RLock()
CLIENT_LOCK = threading.Lock()

_client = None


def client():
    global _client

    with CLIENT_LOCK:
        if _client is None:
            _client = Client(
                API_KEY,
                API_SECRET,
                testnet=BINANCE_TESTNET
            )

        return _client


# ============================================================
# DATABASE
# ============================================================

os.makedirs(
    os.path.dirname(BOT_DB_PATH) or ".",
    exist_ok=True
)


def database():
    conn = sqlite3.connect(
        BOT_DB_PATH,
        timeout=20
    )

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    conn.execute("PRAGMA journal_mode=WAL")

    return conn


def init_db():
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

        migrations = (
            (
                "bot_positions",
                "strategy",
                "TEXT DEFAULT 'DH'"
            ),
            (
                "position_state",
                "tp_count",
                "INTEGER DEFAULT 0"
            ),
            (
                "position_state",
                "breakeven_done",
                "INTEGER DEFAULT 0"
            ),
            (
                "position_state",
                "partial_done",
                "INTEGER DEFAULT 0"
            )
        )

        for table, column, definition in migrations:
            existing = {
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info({table})"
                )
            }

            if column not in existing:
                conn.execute(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN {column} {definition}"
                )

        # Do not automatically replay orders that were
        # interrupted by a deployment or process restart.
        conn.execute("""
            UPDATE alerts
            SET status='needs_review',
                error='Interrupted order; verify Binance',
                updated_at=?
            WHERE status='processing'
        """, (time.time(),))


init_db()


# ============================================================
# SYMBOLS AND ALERTS
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

    if not re.fullmatch(
        r"[A-Z0-9]{5,30}", symbol
    ):
        raise ValueError(
            f"Invalid symbol: {raw}"
        )

    return symbol


def parse_message(message):
    message = str(message or "").strip()

    # Plain TradingView alerts:
    # BUY|BTCUSDT
    # SELL|BTCUSDT
    # CLOSE|BTCUSDT
    # PARTIAL|BTCUSDT|20

    match = re.fullmatch(
        r"(BUY|SELL|CLOSE)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    match = re.fullmatch(
        r"PARTIAL\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)\s*\|\s*"
        r"([0-9]+(?:\.[0-9]+)?)",
        message,
        re.IGNORECASE
    )

    if match:
        percent = float(match.group(2))

        if percent != 20:
            raise ValueError(
                "This bot supports 20% partial TP only"
            )

        return (
            "PARTIAL",
            normalize_symbol(match.group(1))
        )

    # Unique VWAP messages:
    # VWAP|TP1|BTCUSDT, etc.

    match = re.fullmatch(
        r"VWAP\s*\|\s*"
        r"(BUY|SELL|TP[1-5]|CLOSE)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    # Legacy DH order-fill text

    match = re.search(
        r"\border\s+(buy|sell)\s+@\s+"
        r"[^\r\n]*?\bfilled\s+on\s+"
        r"([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(match.group(2))
        )

    raise ValueError(
        f"Unsupported alert: {message[:150]}"
    )


def extract_alert():
    payload = request.get_json(silent=True)

    if isinstance(payload, dict):

        if payload.get("message"):
            raw = str(payload["message"])
            signal, symbol = parse_message(raw)
            return signal, symbol, raw

        if payload.get("signal") and payload.get("symbol"):
            signal = str(
                payload["signal"]
            ).upper().strip()

            symbol = normalize_symbol(
                payload["symbol"]
            )

            if signal not in (
                "BUY", "SELL", "PARTIAL",
                "CLOSE", "TP1", "TP2",
                "TP3", "TP4", "TP5"
            ):
                raise ValueError("Invalid signal")

            if signal == "PARTIAL":
                if float(
                    payload.get("percentage", 20)
                ) != 20:
                    raise ValueError(
                        "Only 20% partial TP supported"
                    )

            return (
                signal,
                symbol,
                json.dumps(payload, sort_keys=True)
            )

    raw = request.get_data(as_text=True)

    signal, symbol = parse_message(raw)

    return signal, symbol, raw


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
                status="unauthorized"
            ), 403

    try:
        signal, symbol, raw = extract_alert()
        now = time.time()

        # For unique TP1-TP5 alerts, prevent repeated
        # delivery of the same numbered TP.
        #
        # For plain PARTIAL alerts, use a short time
        # window. This cannot distinguish every real
        # TP from a retry.

        unique_tp = bool(
            re.fullmatch(r"TP[1-5]", signal)
        )

        if unique_tp:
            dedup_key = (
                f"{signal}|{symbol}|{raw}"
            )
            fingerprint = hashlib.sha256(
                dedup_key.encode()
            ).hexdigest()
        else:
            fingerprint = hashlib.sha256(
                f"{signal}|{symbol}|{raw}|{now}".encode()
            ).hexdigest()

        with DB_LOCK, database() as conn:

            if signal == "PARTIAL":
                previous = conn.execute("""
                    SELECT id
                    FROM alerts
                    WHERE symbol=?
                      AND signal='PARTIAL'
                      AND raw_message=?
                      AND received_at>=?
                    ORDER BY id DESC
                    LIMIT 1
                """, (
                    symbol,
                    raw,
                    now - PARTIAL_DEDUP_SECONDS
                )).fetchone()

                if previous:
                    log.warning(
                        "Duplicate PARTIAL suppressed %s",
                        symbol
                    )

                    return jsonify(
                        status="duplicate_suppressed",
                        symbol=symbol
                    )

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

        return jsonify(
            status="queued" if inserted else "duplicate",
            symbol=symbol,
            signal=signal
        )

    except (ValueError, TypeError) as exc:
        log.warning(
            "Invalid webhook: %s", exc
        )

        return jsonify(
            error=str(exc)
        ), 400

    except Exception:
        log.exception("Webhook error")

        return jsonify(
            error="Webhook processing failed"
        ), 503


# ============================================================
# BINANCE POSITION HELPERS
# ============================================================

def get_positions(symbol=None):
    kwargs = (
        {"symbol": symbol}
        if symbol else {}
    )

    result = (
        client().futures_position_information(
            **kwargs
        )
    )

    return [
        p for p in result
        if abs(float(p["positionAmt"])) > 0
    ]


def get_position(symbol):
    result = get_positions(symbol)

    if len(result) > 1:
        raise RuntimeError(
            "Hedge Mode not supported"
        )

    if result:
        if result[0].get(
            "positionSide", "BOTH"
        ) != "BOTH":
            raise RuntimeError(
                "One-way Futures Mode required"
            )

        return result[0]

    return None


def get_step_size(symbol):
    info = client().futures_exchange_info()

    for item in info["symbols"]:
        if item["symbol"] != symbol:
            continue

        if item["status"] != "TRADING":
            raise RuntimeError(
                "Symbol not trading"
            )

        filters = item["filters"]

        for filter_name in (
            "MARKET_LOT_SIZE",
            "LOT_SIZE"
        ):
            for f in filters:
                if f["filterType"] == filter_name:
                    step = Decimal(
                        f["stepSize"]
                    )

                    if step > 0:
                        return step

    raise RuntimeError(
        f"No quantity step for {symbol}"
    )


def round_quantity(symbol, quantity):
    step = get_step_size(symbol)

    value = Decimal(str(quantity))

    return float(
        (value / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step
    )


def place_market_order(
    symbol,
    side,
    quantity,
    reduce_only=False
):
    quantity = round_quantity(
        symbol,
        quantity
    )

    if quantity <= 0:
        raise RuntimeError(
            "Quantity below minimum step"
        )

    kwargs = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": quantity
    }

    if reduce_only:
        kwargs["reduceOnly"] = True

    result = client().futures_create_order(
        **kwargs
    )

    log.warning(
        "BINANCE ORDER SUBMITTED "
        "symbol=%s side=%s qty=%s "
        "reduceOnly=%s orderId=%s",
        symbol,
        side,
        quantity,
        reduce_only,
        result.get("orderId")
    )

    return result


def close_position(position):
    amount = float(
        position["positionAmt"]
    )

    if amount == 0:
        return None

    side = (
        "SELL" if amount > 0 else "BUY"
    )

    return place_market_order(
        position["symbol"],
        side,
        abs(amount),
        reduce_only=True
    )


def wait_for_position(
    symbol,
    direction=None,
    attempts=20
):
    for _ in range(attempts):
        position = get_position(symbol)

        if direction is None:
            if not position:
                return None

        elif position:
            amount = float(
                position["positionAmt"]
            )

            if direction == "BUY" and amount > 0:
                return position

            if direction == "SELL" and amount < 0:
                return position

        time.sleep(0.5)

    raise RuntimeError(
        "Position confirmation timeout. "
        "Check Binance manually."
    )


# ============================================================
# OWNERSHIP AND POSITION STATE
# ============================================================

def is_owned(symbol):
    with DB_LOCK, database() as conn:
        row = conn.execute("""
            SELECT 1
            FROM bot_positions
            WHERE symbol=?
        """, (symbol,)).fetchone()

        return row is not None


def get_saved_state(symbol):
    with DB_LOCK, database() as conn:
        row = conn.execute("""
            SELECT *
            FROM position_state
            WHERE symbol=?
        """, (symbol,)).fetchone()

        return dict(row) if row else None


def save_position(symbol, direction, position):
    entry = float(
        position["entryPrice"]
    )

    quantity = abs(
        float(position["positionAmt"])
    )

    with DB_LOCK, database() as conn:

        conn.execute("""
            INSERT OR REPLACE INTO bot_positions (
                symbol, direction, created_at, strategy
            )
            VALUES (?, ?, ?, 'VWAP')
        """, (
            symbol,
            direction,
            time.time()
        ))

        conn.execute("""
            INSERT OR REPLACE INTO position_state (
                symbol,
                entry_price,
                entry_qty,
                partial_done,
                breakeven_done,
                tp_count
            )
            VALUES (?, ?, ?, 0, 0, 0)
        """, (
            symbol,
            entry,
            quantity
        ))


def remove_position(symbol):
    with DB_LOCK, database() as conn:

        conn.execute(
            "DELETE FROM bot_positions WHERE symbol=?",
            (symbol,)
        )

        conn.execute(
            "DELETE FROM position_state WHERE symbol=?",
            (symbol,)
        )


def reconcile_flat_positions():
    """
    Remove old bot state only when Binance confirms
    that the position is completely flat.

    Does not change or overwrite the entry price
    of any open position.
    """

    with TRADE_LOCK:

        with DB_LOCK, database() as conn:
            rows = conn.execute("""
                SELECT symbol
                FROM bot_positions
            """).fetchall()

        for row in rows:
            symbol = row["symbol"]

            try:
                position = get_position(symbol)

                if position is None:
                    log.info(
                        "FLAT POSITION CLEANUP %s",
                        symbol
                    )
                    remove_position(symbol)

            except Exception:
                log.exception(
                    "Flat cleanup failed %s",
                    symbol
                )


def entry_mismatch(position, state):
    if not state:
        return False

    saved = float(
        state["entry_price"]
    )

    actual = float(
        position["entryPrice"]
    )

    return abs(saved - actual) > max(
        abs(actual) * 0.000001,
        1e-12
    )


def log_mismatch(position, state):
    if (
        state
        and LOG_ENTRY_MISMATCH
        and entry_mismatch(position, state)
    ):
        log.warning(
            "ENTRY PRICE MISMATCH %s "
            "saved=%s Binance=%s; "
            "using Binance entry for risk, "
            "preserving original TP state",
            position["symbol"],
            state["entry_price"],
            position["entryPrice"]
        )


def update_breakeven(symbol):
    with DB_LOCK, database() as conn:
        conn.execute("""
            UPDATE position_state
            SET breakeven_done=1
            WHERE symbol=?
        """, (symbol,))


def update_tp_count(symbol, count):
    with DB_LOCK, database() as conn:
        conn.execute("""
            UPDATE position_state
            SET tp_count=?,
                partial_done=?
            WHERE symbol=?
        """, (
            count,
            1 if count > 0 else 0,
            symbol
        ))


# ============================================================
# RISK CALCULATION
# ============================================================

def calculate_roi(position):
    entry = float(
        position["entryPrice"]
    )

    mark = float(
        position["markPrice"]
    )

    leverage = float(
        position.get("leverage") or 1
    )

    amount = float(
        position["positionAmt"]
    )

    if entry <= 0 or mark <= 0:
        raise RuntimeError(
            "Invalid Binance price"
        )

    direction = (
        1 if amount > 0 else -1
    )

    return (
        ((mark - entry) / entry)
        * direction
        * leverage
        * 100
    )


# ============================================================
# BLOCK UNRESOLVED ENTRIES
# ============================================================

def has_unresolved_mismatches():
    if not BLOCK_NEW_ENTRIES_ON_MISMATCH:
        return False

    with DB_LOCK, database() as conn:
        rows = conn.execute("""
            SELECT symbol
            FROM bot_positions
        """).fetchall()

    for row in rows:
        symbol = row["symbol"]
        position = get_position(symbol)

        if not position:
            continue

        state = get_saved_state(symbol)

        if state and entry_mismatch(position, state):
            return True

    return False


# ============================================================
# ENTRY EXECUTION
# ============================================================

def execute_entry(signal, symbol):
    with TRADE_LOCK:

        position_mode = (
            client().futures_get_position_mode()
        )

        if position_mode.get("dualSidePosition"):
            raise RuntimeError(
                "Switch Binance to One-way Mode"
            )

        existing = get_position(symbol)

        if existing:

            if not is_owned(symbol):
                raise RuntimeError(
                    "Existing position not owned "
                    "by Railway"
                )

            current_direction = (
                "BUY"
                if float(
                    existing["positionAmt"]
                ) > 0
                else "SELL"
            )

            if current_direction == signal:
                log.info(
                    "Already in %s %s",
                    signal,
                    symbol
                )
                return "already_in_position"

        if has_unresolved_mismatches():
            raise RuntimeError(
                "Unresolved entry-price mismatch "
                "exists. New entries blocked."
            )

        # Reverse existing position if required.
        if existing:
            close_position(existing)
            wait_for_position(symbol)
            remove_position(symbol)

        client().futures_change_leverage(
            symbol=symbol,
            leverage=ENTRY_LEVERAGE
        )

        ticker = (
            client().futures_symbol_ticker(
                symbol=symbol
            )
        )

        price = float(
            ticker["price"]
        )

        quantity = (
            ORDER_MARGIN_USDT
            * ENTRY_LEVERAGE
            / price
        )

        place_market_order(
            symbol,
            signal,
            quantity
        )

        position = wait_for_position(
            symbol,
            signal
        )

        save_position(
            symbol,
            signal,
            position
        )

        log.info(
            "ENTRY CONFIRMED %s %s "
            "entry=%s qty=%s",
            signal,
            symbol,
            position["entryPrice"],
            position["positionAmt"]
        )

        return "entry_executed"


# ============================================================
# PARTIAL TAKE PROFIT
# ============================================================

def execute_partial(symbol, requested_tp=None):
    with TRADE_LOCK:

        if not is_owned(symbol):
            raise RuntimeError(
                "Position is not owned by Railway"
            )

        position = get_position(symbol)

        if not position:
            remove_position(symbol)
            return "already_flat"

        state = get_saved_state(symbol)

        if not state:
            raise RuntimeError(
                "Missing original position state. "
                "Manual reconciliation required."
            )

        # IMPORTANT:
        # Entry-price mismatch does NOT automatically
        # prevent partial TP execution.
        #
        # We verify ownership, quantity, direction,
        # and TP progress instead.
        log_mismatch(position, state)

        count = int(
            state["tp_count"] or 0
        )

        original_qty = float(
            state["entry_qty"]
        )

        current_qty = abs(
            float(position["positionAmt"])
        )

        if original_qty <= 0:
            raise RuntimeError(
                "Invalid original position quantity"
            )

        if current_qty > original_qty * 1.000001:
            raise RuntimeError(
                "Binance position larger than "
                "original saved position. "
                "Manual reconciliation required."
            )

        if count >= 5:
            return "all_tps_completed"

        if requested_tp is not None:

            # Reject old and out-of-sequence TP alerts.
            if requested_tp <= count:
                return "tp_already_processed"

            if requested_tp != count + 1:
                raise RuntimeError(
                    "TP received out of sequence"
                )

        next_tp = count + 1

        side = (
            "SELL"
            if float(
                position["positionAmt"]
            ) > 0
            else "BUY"
        )

        if next_tp == 5:
            quantity = current_qty
        else:
            quantity = round_quantity(
                symbol,
                original_qty * 0.20
            )

            if quantity <= 0:
                raise RuntimeError(
                    "20% TP below minimum quantity"
                )

            if quantity >= current_qty:
                # Closing all remaining quantity at
                # an early TP would be unexpected.
                raise RuntimeError(
                    "Partial TP exceeds remaining "
                    "position. Manual review required."
                )

        log.info(
            "TP REQUEST %s TP%s "
            "original=%s remaining=%s "
            "close_qty=%s",
            symbol,
            next_tp,
            original_qty,
            current_qty,
            quantity
        )

        place_market_order(
            symbol,
            side,
            quantity,
            reduce_only=True
        )

        # Confirm quantity reduction.
        confirmed = False
        updated = None

        for _ in range(20):
            updated = get_position(symbol)

            new_qty = (
                abs(float(updated["positionAmt"]))
                if updated else 0
            )

            if new_qty < current_qty:
                confirmed = True
                break

            time.sleep(0.5)

        if not confirmed:
            raise RuntimeError(
                "Partial TP order submitted but "
                "quantity reduction not confirmed. "
                "Check Binance before retrying."
            )

        if updated is None:
            remove_position(symbol)
            return "position_fully_closed"

        update_tp_count(
            symbol,
            next_tp
        )

        log.info(
            "TP CONFIRMED %s TP%s "
            "remaining=%s",
            symbol,
            next_tp,
            updated["positionAmt"]
        )

        return f"tp_{next_tp}_executed"


# ============================================================
# FULL CLOSE
# ============================================================

def execute_close(symbol):
    with TRADE_LOCK:

        if not is_owned(symbol):
            raise RuntimeError(
                "Position not owned by Railway"
            )

        position = get_position(symbol)

        if not position:
            remove_position(symbol)
            return "already_flat"

        close_position(position)
        wait_for_position(symbol)

        remove_position(symbol)

        return "position_closed"


# ============================================================
# SIGNAL EXECUTION
# ============================================================

def execute_signal(signal, symbol):

    if signal in ("BUY", "SELL"):
        return execute_entry(
            signal,
            symbol
        )

    if signal == "PARTIAL":
        return execute_partial(symbol)

    if re.fullmatch(r"TP[1-5]", signal):
        tp_number = int(signal[2:])

        return execute_partial(
            symbol,
            requested_tp=tp_number
        )

    if signal == "CLOSE":
        return execute_close(symbol)

    raise ValueError(
        f"Unsupported signal: {signal}"
    )


# ============================================================
# QUEUE WORKER
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

        return dict(row) if row else None


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
        "QUEUE STARTED live=%s testnet=%s",
        ENABLE_LIVE_ORDERS,
        BINANCE_TESTNET
    )

    while True:

        try:
            alert = claim_alert()

            if not alert:
                time.sleep(0.5)
                continue

            signal = alert["signal"]
            symbol = alert["symbol"]

            age = (
                time.time()
                - alert["received_at"]
            )

            if age > MAX_ALERT_AGE_SECONDS:
                finish_alert(
                    alert["id"],
                    "expired"
                )
                continue

            if not ENABLE_LIVE_ORDERS:
                log.info(
                    "DRY RUN %s %s",
                    signal,
                    symbol
                )

                finish_alert(
                    alert["id"],
                    "dry_run"
                )
                continue

            try:
                result = execute_signal(
                    signal,
                    symbol
                )

                finish_alert(
                    alert["id"],
                    result
                )

                log.info(
                    "ALERT COMPLETED %s %s %s",
                    signal,
                    symbol,
                    result
                )

            except Exception as exc:
                log.exception(
                    "ALERT FAILED %s %s",
                    signal,
                    symbol
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
# RAILWAY STOP LOSS + BREAKEVEN
# ============================================================

def monitor_risk():

    if not ENABLE_LIVE_ORDERS:
        return

    with TRADE_LOCK:

        active_positions = get_positions()

        for position in active_positions:

            symbol = position["symbol"]

            if not is_owned(symbol):
                continue

            try:
                state = get_saved_state(symbol)

                if not state:
                    log.error(
                        "RISK NOT MANAGED %s: "
                        "missing position state",
                        symbol
                    )
                    continue

                # Binance actual entry price is used.
                # Saved price is not overwritten.
                log_mismatch(position, state)

                roi = calculate_roi(position)

                # Initial SL
                if roi <= -INITIAL_STOP_LOSS_ROI_PCT:

                    log.warning(
                        "STOP LOSS TRIGGERED "
                        "%s ROI=%.2f",
                        symbol,
                        roi
                    )

                    close_position(position)
                    wait_for_position(symbol)
                    remove_position(symbol)

                    continue

                # Breakeven activation
                breakeven_active = bool(
                    state["breakeven_done"]
                )

                if (
                    not breakeven_active
                    and BREAKEVEN_TRIGGER_ROI_PCT > 0
                    and roi >= BREAKEVEN_TRIGGER_ROI_PCT
                ):
                    update_breakeven(symbol)

                    breakeven_active = True

                    log.warning(
                        "BREAKEVEN ARMED "
                        "%s ROI=%.2f",
                        symbol,
                        roi
                    )

                # Breakeven exit
                if (
                    breakeven_active
                    and roi <= BREAKEVEN_STOP_ROI_PCT
                ):

                    log.warning(
                        "BREAKEVEN EXIT "
                        "%s ROI=%.2f",
                        symbol,
                        roi
                    )

                    close_position(position)
                    wait_for_position(symbol)
                    remove_position(symbol)

                    continue

                # No automatic Railway take profits.
                # TradingView controls TP1-TP5.

            except Exception:
                log.exception(
                    "RISK ERROR %s",
                    symbol
                )


def risk_worker():

    log.info(
        "RISK MONITOR STARTED"
    )

    while True:
        try:
            monitor_risk()

        except Exception:
            log.exception(
                "Risk monitor cycle failed"
            )

        time.sleep(
            POLL_INTERVAL_SECONDS
        )


# ============================================================
# CLEANUP WORKER
# ============================================================

def cleanup_worker():

    while True:
        try:
            if ENABLE_LIVE_ORDERS:
                reconcile_flat_positions()

        except Exception:
            log.exception(
                "Position cleanup error"
            )

        time.sleep(60)


# ============================================================
# STATUS
# ============================================================

@app.route("/", methods=["GET"])
def health():
    return jsonify(
        status="ok",
        bot="MAQ Futures TP Ladder",
        live=ENABLE_LIVE_ORDERS,
        testnet=BINANCE_TESTNET
    )


@app.route("/status", methods=["GET"])
def status():

    with DB_LOCK, database() as conn:

        alerts = conn.execute("""
            SELECT status, COUNT(*) AS count
            FROM alerts
            GROUP BY status
        """).fetchall()

        positions = conn.execute("""
            SELECT
                b.symbol,
                b.direction,
                b.strategy,
                s.entry_price,
                s.entry_qty,
                s.tp_count,
                s.breakeven_done
            FROM bot_positions b
            LEFT JOIN position_state s
                ON b.symbol=s.symbol
        """).fetchall()

    return jsonify(
        bot="MAQ Futures TP Ladder",
        live=ENABLE_LIVE_ORDERS,
        testnet=BINANCE_TESTNET,
        order_margin_usdt=ORDER_MARGIN_USDT,
        leverage=ENTRY_LEVERAGE,
        initial_sl_roi=INITIAL_STOP_LOSS_ROI_PCT,
        breakeven_trigger_roi=BREAKEVEN_TRIGGER_ROI_PCT,
        breakeven_stop_roi=BREAKEVEN_STOP_ROI_PCT,
        partial_dedup_seconds=PARTIAL_DEDUP_SECONDS,
        risk_type="software_polling",
        alert_status={
            row["status"]: row["count"]
            for row in alerts
        },
        positions=[
            dict(row)
            for row in positions
        ]
    )


# ============================================================
# START
# ============================================================

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

threading.Thread(
    target=cleanup_worker,
    daemon=True,
    name="MAQ-Cleanup"
).start()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(env("PORT", 8080)),
        threaded=True
    )
