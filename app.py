
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
from binance.exceptions import BinanceAPIException


# ============================================================
# MAQ FUTURES BOT
# TradingView controls entries and take profits.
# Railway controls optional SL and breakeven.
#
# Blank INITIAL_STOP_LOSS_ROI_PCT = SL disabled.
# One-way Binance USD-M Futures mode only.
# Run one Gunicorn worker and one Railway replica.
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

    if value is None:
        return str(default).strip()

    return str(value).strip()


def env_bool(name, default=False):
    return env(
        name,
        "true" if default else "false"
    ).lower() in ("true", "1", "yes", "on")


def env_float(name, default):
    """
    Blank numeric Railway variables return zero.

    This intentionally allows:
    INITIAL_STOP_LOSS_ROI_PCT=""
    """
    value = os.getenv(name)

    if value is None:
        return float(default)

    value = value.strip()

    if value == "":
        return 0.0

    return float(value)


API_KEY = env("BINANCE_API_KEY")
API_SECRET = env("BINANCE_API_SECRET")

LIVE = env_bool("ENABLE_LIVE_ORDERS", False)
TESTNET = env_bool("BINANCE_TESTNET", False)

WEBHOOK_SECRET = env("WEBHOOK_SECRET")

ORDER_MARGIN = env_float(
    "ORDER_MARGIN_USDT", 50
)

LEVERAGE = int(
    env_float("ENTRY_LEVERAGE", 16)
)

INITIAL_SL = env_float(
    "INITIAL_STOP_LOSS_ROI_PCT", 20
)

BE_TRIGGER = env_float(
    "BREAKEVEN_TRIGGER_ROI_PCT", 10
)

BE_STOP = env_float(
    "BREAKEVEN_STOP_ROI_PCT", 0
)

POLL_SECONDS = max(
    15,
    int(env_float("POLL_INTERVAL_SECONDS", 15))
)

PARTIAL_DEDUP_SECONDS = max(
    1,
    int(env_float("PARTIAL_DEDUP_SECONDS", 15))
)

MAX_ALERT_AGE = max(
    30,
    int(env_float("MAX_ALERT_AGE_SECONDS", 180))
)

DB_PATH = env(
    "BOT_DB_PATH",
    "/data/maq_bot.sqlite3"
)

BLOCK_NEW_ON_MISMATCH = env_bool(
    "BLOCK_NEW_ENTRIES_ON_MISMATCH",
    True
)

if LIVE and (not API_KEY or not API_SECRET):
    raise RuntimeError(
        "Missing Binance API credentials"
    )

if ORDER_MARGIN <= 0 or LEVERAGE <= 0:
    raise RuntimeError(
        "ORDER_MARGIN_USDT and ENTRY_LEVERAGE "
        "must be greater than zero"
    )

if INITIAL_SL < 0:
    raise RuntimeError(
        "Initial stop loss cannot be negative"
    )

if BE_TRIGGER < 0:
    raise RuntimeError(
        "Breakeven trigger cannot be negative"
    )


# ============================================================
# SHARED LOCKS
# ============================================================

DB_LOCK = threading.RLock()
TRADE_LOCK = threading.RLock()
API_LOCK = threading.RLock()

_client = None

API_PAUSE_UNTIL = 0.0

EXCHANGE_CACHE = None
EXCHANGE_CACHE_TIME = 0.0

MISMATCH_LOG_TIME = {}


# ============================================================
# DATABASE
# ============================================================

os.makedirs(
    os.path.dirname(DB_PATH) or ".",
    exist_ok=True
)


def db():
    connection = sqlite3.connect(
        DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA busy_timeout=30000"
    )

    connection.execute(
        "PRAGMA journal_mode=WAL"
    )

    return connection


def init_db():
    with DB_LOCK, db() as conn:

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
                "partial_done",
                "INTEGER DEFAULT 0"
            ),
            (
                "position_state",
                "breakeven_done",
                "INTEGER DEFAULT 0"
            )
        )

        for table, column, definition in migrations:

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

        conn.execute("""
            UPDATE alerts
            SET status='needs_review',
                error='Execution interrupted; check Binance',
                updated_at=?
            WHERE status='processing'
        """, (time.time(),))


init_db()


# ============================================================
# BINANCE API AND RATE LIMIT HANDLING
# ============================================================

def client():
    global _client

    with API_LOCK:

        if _client is None:
            _client = Client(
                API_KEY,
                API_SECRET,
                testnet=TESTNET
            )

        return _client


def api_call(method, *args, **kwargs):
    """
    Never automatically retry order submissions.

    Binance may execute an order even when the
    response is lost, so uncertain executions
    require reconciliation.
    """
    global API_PAUSE_UNTIL

    with API_LOCK:

        remaining = (
            API_PAUSE_UNTIL - time.time()
        )

        if remaining > 0:
            raise RuntimeError(
                f"Binance API cooldown active: "
                f"{remaining:.0f}s remaining"
            )

        try:
            function = getattr(
                client(),
                method
            )

            return function(
                *args,
                **kwargs
            )

        except BinanceAPIException as exc:

            if exc.code == -1003:

                API_PAUSE_UNTIL = max(
                    API_PAUSE_UNTIL,
                    time.time() + 120
                )

                log.error(
                    "BINANCE RATE LIMIT -1003. "
                    "Cooldown activated for 120s."
                )

            elif exc.code == -1015:

                API_PAUSE_UNTIL = max(
                    API_PAUSE_UNTIL,
                    time.time() + 60
                )

                log.error(
                    "BINANCE ORDER RATE LIMIT -1015. "
                    "Cooldown activated for 60s."
                )

            raise


# ============================================================
# CACHED SYMBOL RULES
# ============================================================

def exchange_info():
    global EXCHANGE_CACHE
    global EXCHANGE_CACHE_TIME

    with API_LOCK:

        if (
            EXCHANGE_CACHE is not None
            and time.time() - EXCHANGE_CACHE_TIME
            < 21600
        ):
            return EXCHANGE_CACHE

        info = api_call(
            "futures_exchange_info"
        )

        EXCHANGE_CACHE = {
            item["symbol"]: item
            for item in info["symbols"]
        }

        EXCHANGE_CACHE_TIME = time.time()

        log.info(
            "Binance exchange rules cached"
        )

        return EXCHANGE_CACHE


def symbol_rules(symbol):
    info = exchange_info().get(symbol)

    if not info:
        raise RuntimeError(
            f"Unknown symbol: {symbol}"
        )

    if info["status"] != "TRADING":
        raise RuntimeError(
            f"Symbol not trading: {symbol}"
        )

    filters = {
        item["filterType"]: item
        for item in info["filters"]
    }

    market = filters.get(
        "MARKET_LOT_SIZE", {}
    )

    lot = filters.get(
        "LOT_SIZE", {}
    )

    step = Decimal(
        market.get("stepSize", "0")
    )

    if step <= 0:
        step = Decimal(
            lot.get("stepSize", "0")
        )

    if step <= 0:
        raise RuntimeError(
            f"Invalid quantity step: {symbol}"
        )

    minimum = Decimal(
        market.get("minQty", "0")
    )

    if minimum <= 0:
        minimum = Decimal(
            lot.get("minQty", "0")
        )

    return step, minimum


def round_qty(symbol, quantity):
    step, minimum = symbol_rules(symbol)

    quantity = Decimal(
        str(quantity)
    )

    rounded = (
        quantity / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step

    if rounded < minimum:
        raise RuntimeError(
            f"{symbol}: quantity below minimum"
        )

    return str(rounded)


# ============================================================
# BINANCE POSITIONS
# ============================================================

def all_positions():
    data = api_call(
        "futures_position_information"
    )

    return {
        p["symbol"]: p
        for p in data
        if abs(
            float(p["positionAmt"])
        ) > 0
    }


def get_position(symbol):
    data = api_call(
        "futures_position_information",
        symbol=symbol
    )

    active = [
        p for p in data
        if abs(
            float(p["positionAmt"])
        ) > 0
    ]

    if len(active) > 1:
        raise RuntimeError(
            "Hedge Mode not supported"
        )

    if active:

        if active[0].get(
            "positionSide", "BOTH"
        ) != "BOTH":
            raise RuntimeError(
                "One-way Position Mode required"
            )

        return active[0]

    return None


def order_market(
    symbol,
    side,
    quantity,
    reduce_only=False
):
    qty = round_qty(
        symbol,
        quantity
    )

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty
    }

    if reduce_only:
        params["reduceOnly"] = True

    result = api_call(
        "futures_create_order",
        **params
    )

    log.warning(
        "ORDER SUBMITTED %s %s qty=%s "
        "reduceOnly=%s orderId=%s",
        symbol,
        side,
        qty,
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

    return order_market(
        position["symbol"],
        side,
        abs(amount),
        reduce_only=True
    )


def wait_for_position(
    symbol,
    direction=None
):
    for _ in range(6):

        time.sleep(1)

        position = get_position(symbol)

        if direction is None:

            if not position:
                return None

        elif position:

            amount = float(
                position["positionAmt"]
            )

            if (
                direction == "BUY"
                and amount > 0
            ):
                return position

            if (
                direction == "SELL"
                and amount < 0
            ):
                return position

    raise RuntimeError(
        "Position confirmation failed. "
        "Check Binance manually."
    )


# ============================================================
# DATABASE POSITION STATE
# ============================================================

def owned(symbol):
    with DB_LOCK, db() as conn:

        row = conn.execute("""
            SELECT 1
            FROM bot_positions
            WHERE symbol=?
        """, (symbol,)).fetchone()

        return row is not None


def get_state(symbol):
    with DB_LOCK, db() as conn:

        row = conn.execute("""
            SELECT *
            FROM position_state
            WHERE symbol=?
        """, (symbol,)).fetchone()

        return (
            dict(row)
            if row
            else None
        )


def save_position(
    symbol,
    direction,
    position
):
    entry = float(
        position["entryPrice"]
    )

    quantity = abs(
        float(position["positionAmt"])
    )

    with DB_LOCK, db() as conn:

        conn.execute("""
            INSERT OR REPLACE INTO bot_positions
            (
                symbol,
                direction,
                created_at,
                strategy
            )
            VALUES (?, ?, ?, 'VWAP')
        """, (
            symbol,
            direction,
            time.time()
        ))

        conn.execute("""
            INSERT OR REPLACE INTO position_state
            (
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
    with DB_LOCK, db() as conn:

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


def log_entry_mismatch(
    position,
    state
):
    symbol = position["symbol"]

    saved = float(
        state["entry_price"]
    )

    actual = float(
        position["entryPrice"]
    )

    if abs(saved - actual) <= max(
        abs(actual) * 0.000001,
        1e-12
    ):
        return False

    now = time.time()

    if (
        now - MISMATCH_LOG_TIME.get(
            symbol, 0
        ) >= 300
    ):
        log.warning(
            "ENTRY PRICE MISMATCH %s "
            "saved=%s Binance=%s",
            symbol,
            saved,
            actual
        )

        MISMATCH_LOG_TIME[symbol] = now

    return True


# ============================================================
# TRADINGVIEW PARSER
# ============================================================

def normalize_symbol(value):
    value = str(
        value
    ).strip().upper()

    if ":" in value:
        value = value.split(":")[-1]

    if value.endswith(".P"):
        value = value[:-2]

    value = (
        value.replace("/", "")
        .replace("-", "")
        .replace("_", "")
    )

    if not re.fullmatch(
        r"[A-Z0-9]{5,30}",
        value
    ):
        raise ValueError(
            "Invalid symbol"
        )

    return value


def parse_message(message):
    message = str(
        message or ""
    ).strip()

    match = re.fullmatch(
        r"VWAP\|(BUY|SELL|TP[1-5]|CLOSE)"
        r"\|([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(
                match.group(2)
            )
        )

    match = re.fullmatch(
        r"PARTIAL\|([A-Za-z0-9:._/\-]+)"
        r"\|([0-9]+(?:\.[0-9]+)?)",
        message,
        re.IGNORECASE
    )

    if match:

        if float(
            match.group(2)
        ) != 20:
            raise ValueError(
                "Only 20% partial supported"
            )

        return (
            "PARTIAL",
            normalize_symbol(
                match.group(1)
            )
        )

    match = re.fullmatch(
        r"(BUY|SELL|CLOSE)"
        r"\|([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if match:
        return (
            match.group(1).upper(),
            normalize_symbol(
                match.group(2)
            )
        )

    raise ValueError(
        "Unsupported TradingView message"
    )


def extract_alert():
    payload = request.get_json(
        silent=True
    )

    if isinstance(payload, dict):

        if payload.get("message"):

            raw = str(
                payload["message"]
            )

            signal, symbol = parse_message(
                raw
            )

            return (
                signal,
                symbol,
                raw
            )

        if (
            payload.get("signal")
            and payload.get("symbol")
        ):
            signal = str(
                payload["signal"]
            ).upper()

            symbol = normalize_symbol(
                payload["symbol"]
            )

            allowed = (
                "BUY",
                "SELL",
                "PARTIAL",
                "CLOSE",
                "TP1",
                "TP2",
                "TP3",
                "TP4",
                "TP5"
            )

            if signal not in allowed:
                raise ValueError(
                    "Invalid signal"
                )

            if signal == "PARTIAL":

                if float(
                    payload.get(
                        "percentage", 20
                    )
                ) != 20:
                    raise ValueError(
                        "Only 20% supported"
                    )

            return (
                signal,
                symbol,
                json.dumps(
                    payload,
                    sort_keys=True
                )
            )

    raw = request.get_data(
        as_text=True
    )

    signal, symbol = parse_message(
        raw
    )

    return (
        signal,
        symbol,
        raw
    )


# ============================================================
# WEBHOOK
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    if WEBHOOK_SECRET:

        supplied = (
            request.args.get(
                "secret", ""
            )
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

        with DB_LOCK, db() as conn:

            if signal == "PARTIAL":

                duplicate = conn.execute("""
                    SELECT id
                    FROM alerts
                    WHERE symbol=?
                      AND signal='PARTIAL'
                      AND raw_message=?
                      AND received_at>=?
                    LIMIT 1
                """, (
                    symbol,
                    raw,
                    now - PARTIAL_DEDUP_SECONDS
                )).fetchone()

                if duplicate:
                    return jsonify(
                        status="duplicate_suppressed",
                        symbol=symbol
                    )

            fingerprint = hashlib.sha256(
                (
                    f"{signal}|{symbol}|"
                    f"{raw}|{now}"
                ).encode()
            ).hexdigest()

            conn.execute("""
                INSERT INTO alerts
                (
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

        log.info(
            "WEBHOOK QUEUED %s %s",
            signal,
            symbol
        )

        return jsonify(
            status="queued",
            signal=signal,
            symbol=symbol
        )

    except (
        ValueError,
        TypeError
    ) as exc:

        return jsonify(
            error=str(exc)
        ), 400

    except Exception:
        log.exception(
            "Webhook error"
        )

        return jsonify(
            error="Webhook failure"
        ), 503


# ============================================================
# ENTRY ENGINE
# ============================================================

def execute_entry(
    signal,
    symbol
):
    with TRADE_LOCK:

        mode = api_call(
            "futures_get_position_mode"
        )

        if mode.get(
            "dualSidePosition"
        ):
            raise RuntimeError(
                "One-way Mode required"
            )

        current = get_position(
            symbol
        )

        if current:

            if not owned(symbol):
                raise RuntimeError(
                    "Existing position not bot-owned"
                )

            direction = (
                "BUY"
                if float(
                    current["positionAmt"]
                ) > 0
                else "SELL"
            )

            if direction == signal:
                return "already_in_position"

        if BLOCK_NEW_ON_MISMATCH:

            with DB_LOCK, db() as conn:

                rows = conn.execute("""
                    SELECT symbol
                    FROM bot_positions
                """).fetchall()

            snapshot = all_positions()

            for row in rows:

                sym = row["symbol"]
                pos = snapshot.get(sym)

                if not pos:
                    continue

                state = get_state(sym)

                if (
                    state
                    and log_entry_mismatch(
                        pos,
                        state
                    )
                ):
                    raise RuntimeError(
                        "Unreconciled position. "
                        "New entry blocked."
                    )

        if current:

            close_position(
                current
            )

            wait_for_position(
                symbol
            )

            remove_position(
                symbol
            )

        api_call(
            "futures_change_leverage",
            symbol=symbol,
            leverage=LEVERAGE
        )

        ticker = api_call(
            "futures_symbol_ticker",
            symbol=symbol
        )

        price = float(
            ticker["price"]
        )

        quantity = (
            ORDER_MARGIN
            * LEVERAGE
            / price
        )

        order_market(
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

        return "entry_executed"


# ============================================================
# PARTIAL TAKE PROFIT
# ============================================================

def execute_partial(
    symbol,
    requested_tp=None
):
    with TRADE_LOCK:

        if not owned(symbol):
            raise RuntimeError(
                "Position not bot-owned"
            )

        position = get_position(
            symbol
        )

        if not position:

            remove_position(
                symbol
            )

            return "already_flat"

        state = get_state(
            symbol
        )

        if not state:
            raise RuntimeError(
                "Missing original position state"
            )

        log_entry_mismatch(
            position,
            state
        )

        count = int(
            state["tp_count"] or 0
        )

        original_qty = float(
            state["entry_qty"]
        )

        current_qty = abs(
            float(
                position["positionAmt"]
            )
        )

        if original_qty <= 0:
            raise RuntimeError(
                "Invalid original quantity"
            )

        if (
            current_qty
            > original_qty * 1.000001
        ):
            raise RuntimeError(
                "Position quantity increased. "
                "Manual reconciliation required."
            )

        if count >= 5:
            return "all_tps_completed"

        if requested_tp is not None:

            if requested_tp <= count:
                return "tp_already_completed"

            if requested_tp != count + 1:
                raise RuntimeError(
                    "TP out of sequence"
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

            quantity = float(
                round_qty(
                    symbol,
                    original_qty * 0.20
                )
            )

            if quantity >= current_qty:
                raise RuntimeError(
                    "Partial TP exceeds "
                    "remaining quantity"
                )

        log.warning(
            "TP EXECUTION %s TP%s "
            "qty=%s remaining_before=%s",
            symbol,
            next_tp,
            quantity,
            current_qty
        )

        order_market(
            symbol,
            side,
            quantity,
            reduce_only=True
        )

        confirmed = False
        updated_position = None

        for _ in range(5):

            time.sleep(1)

            updated_position = get_position(
                symbol
            )

            remaining = (
                abs(
                    float(
                        updated_position[
                            "positionAmt"
                        ]
                    )
                )
                if updated_position
                else 0
            )

            if remaining < current_qty:
                confirmed = True
                break

        if not confirmed:
            raise RuntimeError(
                "TP confirmation uncertain. "
                "Check Binance before retrying."
            )

        if updated_position is None:

            remove_position(
                symbol
            )

            log.warning(
                "TP FINAL CLOSE CONFIRMED %s",
                symbol
            )

            return "position_fully_closed"

        with DB_LOCK, db() as conn:

            conn.execute("""
                UPDATE position_state
                SET tp_count=?,
                    partial_done=1
                WHERE symbol=?
            """, (
                next_tp,
                symbol
            ))

        log.warning(
            "TP CONFIRMED %s TP%s "
            "remaining=%s",
            symbol,
            next_tp,
            updated_position["positionAmt"]
        )

        return (
            f"tp_{next_tp}_executed"
        )


# ============================================================
# FULL CLOSE
# ============================================================

def execute_close(symbol):
    with TRADE_LOCK:

        if not owned(symbol):
            raise RuntimeError(
                "Position not bot-owned"
            )

        position = get_position(
            symbol
        )

        if not position:

            remove_position(
                symbol
            )

            return "already_flat"

        close_position(
            position
        )

        wait_for_position(
            symbol
        )

        remove_position(
            symbol
        )

        return "position_closed"


# ============================================================
# SIGNAL ROUTING
# ============================================================

def execute_signal(
    signal,
    symbol
):
    if signal in (
        "BUY",
        "SELL"
    ):
        return execute_entry(
            signal,
            symbol
        )

    if signal == "PARTIAL":
        return execute_partial(
            symbol
        )

    if re.fullmatch(
        r"TP[1-5]",
        signal
    ):
        return execute_partial(
            symbol,
            requested_tp=int(
                signal[2:]
            )
        )

    if signal == "CLOSE":
        return execute_close(
            symbol
        )

    raise ValueError(
        f"Unsupported signal: {signal}"
    )


# ============================================================
# ALERT QUEUE
# ============================================================

def claim_alert():
    with DB_LOCK, db() as conn:

        conn.execute(
            "BEGIN IMMEDIATE"
        )

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
            dict(row)
            if row
            else None
        )


def finish_alert(
    alert_id,
    status,
    error=None
):
    with DB_LOCK, db() as conn:

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
        LIVE,
        TESTNET
    )

    while True:

        try:
            alert = claim_alert()

            if not alert:
                time.sleep(0.5)
                continue

            age = (
                time.time()
                - alert["received_at"]
            )

            if age > MAX_ALERT_AGE:

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

                log.info(
                    "ALERT COMPLETED %s %s %s",
                    alert["signal"],
                    alert["symbol"],
                    result
                )

            except Exception as exc:

                log.exception(
                    "ALERT FAILED %s %s",
                    alert["signal"],
                    alert["symbol"]
                )

                finish_alert(
                    alert["id"],
                    "needs_review",
                    str(exc)[:500]
                )

        except Exception:

            log.exception(
                "Queue worker failure"
            )

            time.sleep(2)


# ============================================================
# RISK MONITOR
# ============================================================

def calculate_roi(position):

    entry = float(
        position["entryPrice"]
    )

    mark = float(
        position["markPrice"]
    )

    amount = float(
        position["positionAmt"]
    )

    leverage = float(
        position.get(
            "leverage"
        ) or 1
    )

    if entry <= 0 or mark <= 0:
        raise RuntimeError(
            "Invalid Binance price"
        )

    direction = (
        1 if amount > 0 else -1
    )

    return (
        (mark - entry)
        / entry
        * direction
        * leverage
        * 100
    )


def monitor_risk():

    if not LIVE:
        return

    with TRADE_LOCK:

        snapshot = all_positions()

        with DB_LOCK, db() as conn:

            rows = conn.execute("""
                SELECT symbol
                FROM bot_positions
            """).fetchall()

        for row in rows:

            symbol = row["symbol"]

            try:

                position = snapshot.get(
                    symbol
                )

                if not position:

                    remove_position(
                        symbol
                    )

                    continue

                state = get_state(
                    symbol
                )

                if not state:

                    log.error(
                        "UNMANAGED POSITION %s: "
                        "missing state",
                        symbol
                    )

                    continue

                log_entry_mismatch(
                    position,
                    state
                )

                roi = calculate_roi(
                    position
                )

                # --------------------------------
                # INITIAL STOP LOSS
                #
                # Blank variable -> INITIAL_SL=0
                # -> Stop-loss disabled.
                # --------------------------------

                if (
                    INITIAL_SL > 0
                    and roi <= -INITIAL_SL
                ):

                    log.warning(
                        "STOP LOSS %s ROI=%.2f",
                        symbol,
                        roi
                    )

                    close_position(
                        position
                    )

                    wait_for_position(
                        symbol
                    )

                    remove_position(
                        symbol
                    )

                    continue

                # --------------------------------
                # BREAKEVEN TRIGGER
                # --------------------------------

                armed = bool(
                    state["breakeven_done"]
                )

                if (
                    not armed
                    and BE_TRIGGER > 0
                    and roi >= BE_TRIGGER
                ):

                    with DB_LOCK, db() as conn:

                        conn.execute("""
                            UPDATE position_state
                            SET breakeven_done=1
                            WHERE symbol=?
                        """, (
                            symbol,
                        ))

                    armed = True

                    log.warning(
                        "BREAKEVEN ARMED %s",
                        symbol
                    )

                # --------------------------------
                # BREAKEVEN EXIT
                # --------------------------------

                if (
                    armed
                    and roi <= BE_STOP
                ):

                    log.warning(
                        "BREAKEVEN EXIT %s ROI=%.2f",
                        symbol,
                        roi
                    )

                    close_position(
                        position
                    )

                    wait_for_position(
                        symbol
                    )

                    remove_position(
                        symbol
                    )

            except Exception:

                log.exception(
                    "RISK ERROR %s",
                    symbol
                )


def risk_worker():

    log.info(
        "RISK MONITOR STARTED "
        "interval=%ss initialSL=%s",
        POLL_SECONDS,
        (
            "DISABLED"
            if INITIAL_SL == 0
            else INITIAL_SL
        )
    )

    while True:

        started = time.monotonic()

        try:

            monitor_risk()

        except Exception:

            log.exception(
                "Risk monitor cycle failed"
            )

        elapsed = (
            time.monotonic()
            - started
        )

        time.sleep(
            max(
                1,
                POLL_SECONDS - elapsed
            )
        )


# ============================================================
# HEALTH
# ============================================================

@app.route(
    "/",
    methods=["GET"]
)
def health():

    return jsonify(
        status="ok",
        bot="MAQ Optimized Futures Bot",
        live=LIVE,
        testnet=TESTNET,
        poll_seconds=POLL_SECONDS,
        initial_stop_loss=(
            "disabled"
            if INITIAL_SL == 0
            else INITIAL_SL
        )
    )


# ============================================================
# STATUS
# ============================================================

@app.route(
    "/status",
    methods=["GET"]
)
def status():

    with DB_LOCK, db() as conn:

        alerts = conn.execute("""
            SELECT status, COUNT(*) AS count
            FROM alerts
            GROUP BY status
        """).fetchall()

        positions = conn.execute("""
            SELECT
                b.symbol,
                b.direction,
                s.entry_price,
                s.entry_qty,
                s.tp_count,
                s.breakeven_done
            FROM bot_positions b
            LEFT JOIN position_state s
            ON b.symbol=s.symbol
        """).fetchall()

    return jsonify(
        live=LIVE,
        testnet=TESTNET,
        poll_seconds=POLL_SECONDS,
        initial_stop_loss_roi=INITIAL_SL,
        initial_stop_loss_enabled=(
            INITIAL_SL > 0
        ),
        breakeven_trigger_roi=BE_TRIGGER,
        breakeven_stop_roi=BE_STOP,
        stop_type="software",
        api_cooldown_remaining=max(
            0,
            int(
                API_PAUSE_UNTIL
                - time.time()
            )
        ),
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
# START WORKERS
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


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            env("PORT", 8080)
        ),
        threaded=True
    )
