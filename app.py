
import os
import re
import time
import hmac
import json
import sqlite3
import hashlib
import logging
import threading
from decimal import Decimal, ROUND_DOWN

from flask import Flask, request, jsonify
from binance.client import Client
from binance.exceptions import (
    BinanceAPIException,
    BinanceRequestException
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("MAQ")
app = Flask(__name__)


# ==========================================================
# CONFIGURATION
# ==========================================================

def setting(name, default=""):
    value = os.getenv(name)
    return str(default) if value is None else value.strip()


def number(name, default, blank_zero=False):
    value = setting(name, default)
    if value == "":
        return 0.0 if blank_zero else float(default)
    return float(value)


def boolean(name, default=False):
    return setting(name, str(default)).lower() in (
        "true", "1", "yes", "on"
    )


LIVE = boolean("ENABLE_LIVE_ORDERS", False)
TESTNET = boolean("BINANCE_TESTNET", False)

KEY = setting("BINANCE_API_KEY")
SECRET = setting("BINANCE_API_SECRET")
WEBHOOK_SECRET = setting("WEBHOOK_SECRET")

# Allows existing TradingView URLs without a secret.
# WARNING: This permits unauthenticated trade alerts.
ALLOW_LEGACY_WEBHOOK = boolean(
    "ALLOW_LEGACY_WEBHOOK", False
)

MARGIN = number("ORDER_MARGIN_USDT", 50)
LEVERAGE = int(number("ENTRY_LEVERAGE", 16))

# Blank or zero = no initial stop loss.
INITIAL_SL = number(
    "INITIAL_STOP_LOSS_ROI_PCT",
    0,
    blank_zero=True
)

BE_TRIGGER = number(
    "BREAKEVEN_TRIGGER_ROI_PCT", 10
)

BE_STOP = number(
    "BREAKEVEN_STOP_ROI_PCT", 0
)

POLL = max(
    30,
    int(number("POLL_INTERVAL_SECONDS", 60))
)

DEDUP = max(
    1,
    int(number("PARTIAL_DEDUP_SECONDS", 15))
)

MAX_AGE = max(
    30,
    int(number("MAX_ALERT_AGE_SECONDS", 180))
)

DB_PATH = setting(
    "BOT_DB_PATH",
    "/data/maq_bot.sqlite3"
)

if LIVE and not (KEY and SECRET):
    raise RuntimeError(
        "Live trading requires Binance API credentials"
    )

if LIVE and not (
    WEBHOOK_SECRET or ALLOW_LEGACY_WEBHOOK
):
    raise RuntimeError(
        "Configure WEBHOOK_SECRET or explicitly enable "
        "ALLOW_LEGACY_WEBHOOK"
    )

if ALLOW_LEGACY_WEBHOOK:
    log.warning(
        "SECURITY WARNING: Legacy webhook enabled. "
        "Unauthenticated alerts can submit trades."
    )

if (
    MARGIN <= 0
    or LEVERAGE < 1
    or LEVERAGE > 125
    or INITIAL_SL < 0
    or BE_TRIGGER < 0
):
    raise RuntimeError("Invalid trading settings")


# ==========================================================
# LOCKS AND DATABASE
# ==========================================================

DB_LOCK = threading.RLock()
TRADE_LOCK = threading.RLock()
API_LOCK = threading.RLock()

_client = None
cooldown_until = 0.0

exchange_cache = {}
exchange_cache_at = 0.0

os.makedirs(
    os.path.dirname(DB_PATH) or ".",
    exist_ok=True
)


def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("PRAGMA journal_mode=WAL")
    return c


def init_db():
    with DB_LOCK, db() as c:

        c.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE,
                symbol TEXT,
                signal TEXT,
                raw_message TEXT,
                received_at REAL,
                status TEXT DEFAULT 'pending',
                error TEXT,
                updated_at REAL,
                retry_count INTEGER DEFAULT 0,
                next_attempt REAL DEFAULT 0
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS bot_positions (
                symbol TEXT PRIMARY KEY,
                direction TEXT,
                created_at REAL,
                strategy TEXT DEFAULT 'VWAP'
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS position_state (
                symbol TEXT PRIMARY KEY,
                entry_price REAL,
                entry_qty REAL,
                partial_done INTEGER DEFAULT 0,
                breakeven_done INTEGER DEFAULT 0,
                tp_count INTEGER DEFAULT 0
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS trade_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                alert_id INTEGER,
                client_id TEXT UNIQUE,
                status TEXT NOT NULL,
                before_qty REAL,
                requested_qty REAL,
                order_id TEXT,
                created_at REAL,
                updated_at REAL,
                note TEXT
            )
        """)

        migrations = (
            ("alerts", "retry_count", "INTEGER DEFAULT 0"),
            ("alerts", "next_attempt", "REAL DEFAULT 0"),
            ("bot_positions", "strategy", "TEXT DEFAULT 'VWAP'"),
            ("position_state", "tp_count", "INTEGER DEFAULT 0"),
            ("position_state", "partial_done", "INTEGER DEFAULT 0"),
            ("position_state", "breakeven_done", "INTEGER DEFAULT 0"),
            ("position_state", "entry_price", "REAL"),
            ("position_state", "entry_qty", "REAL")
        )

        for table, col, definition in migrations:
            existing = {
                x[1]
                for x in c.execute(
                    f"PRAGMA table_info({table})"
                )
            }

            if col not in existing:
                c.execute(
                    f"ALTER TABLE {table} ADD COLUMN "
                    f"{col} {definition}"
                )

        c.execute("""
            CREATE INDEX IF NOT EXISTS journal_symbol_status
            ON trade_journal(symbol, status)
        """)

        now = time.time()

        c.execute("""
            UPDATE alerts
            SET status='needs_review',
                error='Interrupted by restart',
                updated_at=?
            WHERE status='processing'
        """, (now,))

        c.execute("""
            UPDATE trade_journal
            SET status='uncertain',
                note='Interrupted by restart',
                updated_at=?
            WHERE status IN ('submitting', 'submitted')
        """, (now,))


init_db()


# ==========================================================
# BINANCE API
# ==========================================================

def api(method, **kwargs):
    global _client, cooldown_until

    with API_LOCK:

        remaining = cooldown_until - time.time()

        if remaining > 0:
            raise RuntimeError(
                f"Binance cooldown active: {remaining:.0f}s"
            )

        try:
            if _client is None:
                _client = Client(
                    KEY,
                    SECRET,
                    testnet=TESTNET
                )

            return getattr(_client, method)(**kwargs)

        except BinanceAPIException as exc:

            if exc.code == -1003:
                cooldown_until = time.time() + 300

            elif exc.code == -1015:
                cooldown_until = time.time() + 120

            log.error(
                "Binance API %s error %s: %s",
                method, exc.code, exc
            )
            raise

        except Exception as exc:
            log.error(
                "Binance request %s failed: %s",
                method, exc
            )
            raise


def positions():
    raw = api("futures_position_information")
    result = {}

    for p in raw:
        qty = float(p.get("positionAmt", 0))

        if qty == 0:
            continue

        symbol = p["symbol"]

        if symbol in result:
            raise RuntimeError(
                "Hedge mode not supported. "
                "Use Binance one-way position mode."
            )

        result[symbol] = {
            "qty": qty,
            "entry": float(p.get("entryPrice", 0)),
            "mark": float(p.get("markPrice", 0)),
            "side": "LONG" if qty > 0 else "SHORT"
        }

    return result


def get_position(symbol):
    return positions().get(symbol)


# ==========================================================
# EXCHANGE RULES
# ==========================================================

def rules(symbol):
    global exchange_cache, exchange_cache_at

    with API_LOCK:

        if (
            not exchange_cache
            or time.time() - exchange_cache_at > 21600
        ):
            info = api("futures_exchange_info")

            exchange_cache = {
                x["symbol"]: x
                for x in info["symbols"]
            }

            exchange_cache_at = time.time()

        info = exchange_cache.get(symbol)

        if not info or info.get("status") != "TRADING":
            raise ValueError(
                f"{symbol} is not an active futures symbol"
            )

        filters = {
            f["filterType"]: f
            for f in info["filters"]
        }

        market = filters.get("MARKET_LOT_SIZE")
        lot = filters.get("LOT_SIZE")

        f = (
            market
            if market
            and float(market.get("stepSize", 0)) > 0
            else lot
        )

        if not f:
            raise RuntimeError(
                f"No quantity rules for {symbol}"
            )

        return (
            Decimal(f["stepSize"]),
            Decimal(f["minQty"]),
            Decimal(f.get("maxQty", "999999999"))
        )


def format_qty(symbol, value):
    step, minimum, maximum = rules(symbol)

    value = Decimal(str(value))

    qty = (
        (value / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step
    )

    if qty < minimum or qty > maximum:
        raise ValueError(
            f"Invalid {symbol} quantity: {qty}"
        )

    return format(qty, "f")


# ==========================================================
# BOT OWNERSHIP
# ==========================================================

def owned(symbol):
    with DB_LOCK, db() as c:

        owner = c.execute(
            "SELECT * FROM bot_positions WHERE symbol=?",
            (symbol,)
        ).fetchone()

        state = c.execute(
            "SELECT * FROM position_state WHERE symbol=?",
            (symbol,)
        ).fetchone()

        return (
            dict(owner) if owner else None,
            dict(state) if state else None
        )


def save_owned(symbol, direction, p, strategy):
    with DB_LOCK, db() as c:

        c.execute("""
            INSERT OR REPLACE INTO bot_positions
            (symbol, direction, created_at, strategy)
            VALUES (?, ?, ?, ?)
        """, (
            symbol,
            direction,
            time.time(),
            strategy
        ))

        c.execute("""
            INSERT OR REPLACE INTO position_state
            (
                symbol, entry_price, entry_qty,
                partial_done, breakeven_done, tp_count
            )
            VALUES (?, ?, ?, 0, 0, 0)
        """, (
            symbol,
            p["entry"],
            abs(p["qty"])
        ))


def forget_owned(symbol):
    with DB_LOCK, db() as c:

        c.execute(
            "DELETE FROM bot_positions WHERE symbol=?",
            (symbol,)
        )

        c.execute(
            "DELETE FROM position_state WHERE symbol=?",
            (symbol,)
        )


# ==========================================================
# ORDER JOURNAL
# ==========================================================

def unresolved(symbol):
    with DB_LOCK, db() as c:

        row = c.execute("""
            SELECT *
            FROM trade_journal
            WHERE symbol=?
              AND status IN (
                'submitting', 'submitted', 'uncertain'
              )
            ORDER BY id DESC
            LIMIT 1
        """, (symbol,)).fetchone()

        return dict(row) if row else None


def journal_create(
    symbol, action, alert_id, before_qty, qty
):
    with DB_LOCK, db() as c:

        cur = c.execute("""
            INSERT INTO trade_journal
            (
                symbol, action, alert_id, status,
                before_qty, requested_qty,
                created_at, updated_at
            )
            VALUES (?, ?, ?, 'prepared', ?, ?, ?, ?)
        """, (
            symbol,
            action,
            alert_id,
            before_qty,
            float(qty),
            time.time(),
            time.time()
        ))

        jid = cur.lastrowid
        cid = f"MAQ{jid}_{int(time.time())}"

        c.execute("""
            UPDATE trade_journal
            SET client_id=?
            WHERE id=?
        """, (cid, jid))

        return jid, cid


def journal_update(
    jid, status, order_id=None, note=None
):
    with DB_LOCK, db() as c:

        c.execute("""
            UPDATE trade_journal
            SET status=?,
                order_id=COALESCE(?, order_id),
                note=COALESCE(?, note),
                updated_at=?
            WHERE id=?
        """, (
            status,
            str(order_id) if order_id is not None else None,
            note,
            time.time(),
            jid
        ))


# ==========================================================
# MARKET ORDERS
# ==========================================================

def submit_order(
    symbol, side, qty, reduce_only,
    action, alert_id, before_qty
):
    if not LIVE:
        raise RuntimeError("Live orders disabled")

    if unresolved(symbol):
        raise RuntimeError(
            f"{symbol}: unresolved order requires review"
        )

    jid, cid = journal_create(
        symbol,
        action,
        alert_id,
        before_qty,
        qty
    )

    journal_update(jid, "submitting")

    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
        "newClientOrderId": cid
    }

    if reduce_only:
        params["reduceOnly"] = "true"

    try:
        order = api(
            "futures_create_order",
            **params
        )

        journal_update(
            jid,
            "submitted",
            order.get("orderId")
        )

        return jid

    except BinanceAPIException as exc:

        # Only known definite rejections are marked rejected.
        # Ambiguous outcomes must not be blindly retried.
        if exc.code in (
            -1013, -1100, -1102, -1111,
            -2019, -2021, -2022, -4164
        ):
            journal_update(
                jid, "rejected", note=str(exc)
            )
        else:
            journal_update(
                jid, "uncertain", note=str(exc)
            )

        raise

    except Exception as exc:
        journal_update(
            jid, "uncertain", note=str(exc)
        )
        raise


def confirm_position(jid, symbol, predicate):
    last_error = None

    for attempt in range(4):
        if attempt:
            time.sleep(1)

        try:
            p = get_position(symbol)

            if predicate(p):
                journal_update(jid, "confirmed")
                return p

        except Exception as exc:
            last_error = exc

    journal_update(
        jid,
        "uncertain",
        note=(
            str(last_error)
            if last_error
            else "Position confirmation failed"
        )
    )

    raise RuntimeError(
        f"{symbol}: exchange state not confirmed; "
        "check Binance before another order"
    )


# ==========================================================
# CLOSE POSITION
# ==========================================================

def close_position(
    symbol, alert_id=None, reason="CLOSE"
):
    with TRADE_LOCK:

        p = get_position(symbol)
        owner, _ = owned(symbol)

        if not p:
            if owner:
                forget_owned(symbol)
            return "already_flat"

        if (
            not owner
            or owner["direction"] != p["side"]
        ):
            raise RuntimeError(
                f"{symbol}: refusing to close "
                "unverified or manually owned position"
            )

        if unresolved(symbol):
            raise RuntimeError(
                f"{symbol}: unresolved previous order"
            )

        side = "SELL" if p["qty"] > 0 else "BUY"

        qty = format_qty(
            symbol,
            abs(p["qty"])
        )

        jid = submit_order(
            symbol,
            side,
            qty,
            True,
            reason,
            alert_id,
            p["qty"]
        )

        confirm_position(
            jid,
            symbol,
            lambda x: x is None
        )

        forget_owned(symbol)

        log.info(
            "%s position closed: %s",
            symbol, reason
        )

        return "closed"


# ==========================================================
# BUY, SELL AND AUTOMATIC REVERSAL
# ==========================================================

def enter(
    symbol, direction,
    strategy="STANDARD", alert_id=None
):
    with TRADE_LOCK:

        if unresolved(symbol):
            raise RuntimeError(
                f"{symbol}: unresolved journal"
            )

        p = get_position(symbol)
        owner, _ = owned(symbol)

        if p:

            if (
                not owner
                or owner["direction"] != p["side"]
            ):
                raise RuntimeError(
                    f"{symbol}: existing position "
                    "is not verified as bot-owned"
                )

            if p["side"] == direction:
                return "already_in_position"

            # Reverse only bot-owned positions.
            close_position(
                symbol,
                alert_id,
                "REVERSAL_CLOSE"
            )

            if get_position(symbol):
                raise RuntimeError(
                    f"{symbol}: reversal close not verified"
                )

        elif owner:
            forget_owned(symbol)

        api(
            "futures_change_leverage",
            symbol=symbol,
            leverage=LEVERAGE
        )

        ticker = api(
            "futures_symbol_ticker",
            symbol=symbol
        )

        price = float(ticker["price"])

        if price <= 0:
            raise RuntimeError(
                "Invalid market price"
            )

        qty = format_qty(
            symbol,
            MARGIN * LEVERAGE / price
        )

        side = (
            "BUY" if direction == "LONG"
            else "SELL"
        )

        jid = submit_order(
            symbol,
            side,
            qty,
            False,
            "ENTRY_" + direction,
            alert_id,
            0.0
        )

        p = confirm_position(
            jid,
            symbol,
            lambda x: (
                x is not None
                and x["side"] == direction
            )
        )

        save_owned(
            symbol,
            direction,
            p,
            strategy
        )

        log.info(
            "Entered %s %s",
            direction, symbol
        )

        return "entered_" + direction.lower()


# ==========================================================
# FIVE TAKE-PROFIT STAGES
# ==========================================================

def partial(
    symbol, pct=20,
    stage=None, alert_id=None
):
    with TRADE_LOCK:

        if not 0 < pct <= 100:
            raise ValueError(
                "Partial percentage must be 0-100"
            )

        p = get_position(symbol)
        owner, state = owned(symbol)

        if not p:
            if owner:
                forget_owned(symbol)
            return "already_flat"

        if (
            not owner
            or not state
            or owner["direction"] != p["side"]
        ):
            raise RuntimeError(
                f"{symbol}: not a verified bot position"
            )

        if unresolved(symbol):
            raise RuntimeError(
                f"{symbol}: unresolved previous order"
            )

        count = int(state["tp_count"] or 0)

        if count >= 5:
            return "all_take_profits_completed"

        if stage is not None:
            if stage <= count:
                return "stage_already_completed"

            if stage != count + 1:
                raise RuntimeError(
                    f"{symbol}: expected TP{count + 1}"
                )

        if stage is None:
            with DB_LOCK, db() as c:
                recent = c.execute("""
                    SELECT updated_at
                    FROM trade_journal
                    WHERE symbol=?
                      AND action='PARTIAL'
                      AND status='confirmed'
                    ORDER BY id DESC
                    LIMIT 1
                """, (symbol,)).fetchone()

            if recent and (
                time.time() - recent["updated_at"] < DEDUP
            ):
                return "partial_deduplicated"

        original = float(
            state["entry_qty"] or 0
        )

        target = original * pct / 100

        if (
            count == 4
            or stage == 5
            or target >= abs(p["qty"])
        ):
            amount = abs(p["qty"])
        else:
            amount = min(
                target,
                abs(p["qty"])
            )

        qty = format_qty(symbol, amount)

        side = (
            "SELL" if p["qty"] > 0
            else "BUY"
        )

        before = abs(p["qty"])

        jid = submit_order(
            symbol,
            side,
            qty,
            True,
            "PARTIAL",
            alert_id,
            p["qty"]
        )

        after = confirm_position(
            jid,
            symbol,
            lambda x: (
                x is None
                or abs(x["qty"]) < before - 1e-12
            )
        )

        if after is None:
            forget_owned(symbol)
            return "partial_closed_position"

        with DB_LOCK, db() as c:
            c.execute("""
                UPDATE position_state
                SET tp_count=?,
                    partial_done=1
                WHERE symbol=?
            """, (
                count + 1,
                symbol
            ))

        return f"partial_tp_{count + 1}"


# ==========================================================
# ROI AND BREAKEVEN
# ==========================================================

def calculate_roi(p):
    entry = float(p["entry"])

    if entry <= 0:
        return None

    mark = float(p["mark"])
    sign = 1 if p["qty"] > 0 else -1

    return (
        sign
        * (mark - entry)
        / entry
        * LEVERAGE
        * 100
    )


def monitor_once():
    if not LIVE:
        return

    with TRADE_LOCK:

        ps = positions()

        with DB_LOCK, db() as c:
            owners = [
                dict(row)
                for row in c.execute(
                    "SELECT * FROM bot_positions"
                )
            ]

        for owner in owners:
            symbol = owner["symbol"]
            p = ps.get(symbol)

            if not p:
                forget_owned(symbol)
                continue

            if p["side"] != owner["direction"]:
                log.error(
                    "Position direction mismatch: %s",
                    symbol
                )
                continue

            if unresolved(symbol):
                log.error(
                    "Unresolved order; monitor skipped %s",
                    symbol
                )
                continue

            _, state = owned(symbol)

            if not state:
                log.error(
                    "Missing position state: %s",
                    symbol
                )
                continue

            value = calculate_roi(p)

            if value is None:
                continue

            # Disabled when INITIAL_SL is zero.
            if (
                INITIAL_SL > 0
                and value <= -INITIAL_SL
            ):
                close_position(
                    symbol,
                    reason="INITIAL_SL"
                )
                continue

            armed = bool(
                state["breakeven_done"]
            )

            if (
                not armed
                and value >= BE_TRIGGER
            ):
                with DB_LOCK, db() as c:
                    c.execute("""
                        UPDATE position_state
                        SET breakeven_done=1
                        WHERE symbol=?
                    """, (symbol,))

                armed = True

                log.info(
                    "Breakeven armed %s ROI %.2f",
                    symbol, value
                )

            if armed and value <= BE_STOP:
                log.warning(
                    "Breakeven triggered %s ROI %.2f",
                    symbol, value
                )

                close_position(
                    symbol,
                    reason="BREAKEVEN"
                )


# ==========================================================
# TRADINGVIEW ALERT PARSER
# ==========================================================

def parse(message):
    if isinstance(message, dict):
        message = (
            message.get("message")
            or message.get("text")
            or message.get("alert")
            or ""
        )

    raw = (
        str(message)
        .strip()
        .upper()
        .replace(" ", "")
    )

    parts = raw.split("|")
    strategy = "STANDARD"

    if parts and parts[0] == "VWAP":
        parts = parts[1:]
        strategy = "VWAP"

    if len(parts) < 2:
        raise ValueError(
            "Expected BUY|SYMBOL or SELL|SYMBOL"
        )

    signal = parts[0]
    symbol = parts[1]

    if not re.fullmatch(
        r"[A-Z0-9]{4,25}",
        symbol
    ):
        raise ValueError("Invalid symbol")

    stage = None
    pct = 20.0

    if re.fullmatch(r"TP[1-5]", signal):
        stage = int(signal[2:])
        signal = "PARTIAL"

    elif signal == "PARTIAL" and len(parts) > 2:
        pct = float(parts[2])

    elif signal not in (
        "BUY", "SELL", "PARTIAL", "CLOSE"
    ):
        raise ValueError(
            f"Unsupported signal: {signal}"
        )

    if signal == "PARTIAL" and not 0 < pct <= 100:
        raise ValueError(
            "Invalid partial percentage"
        )

    return {
        "signal": signal,
        "symbol": symbol,
        "pct": pct,
        "stage": stage,
        "strategy": strategy,
        "raw": raw
    }


def dispatch(alert, alert_id):
    signal = alert["signal"]
    symbol = alert["symbol"]

    if signal == "BUY":
        return enter(
            symbol,
            "LONG",
            alert["strategy"],
            alert_id
        )

    if signal == "SELL":
        return enter(
            symbol,
            "SHORT",
            alert["strategy"],
            alert_id
        )

    if signal == "CLOSE":
        return close_position(
            symbol,
            alert_id
        )

    if signal == "PARTIAL":
        return partial(
            symbol,
            alert["pct"],
            alert["stage"],
            alert_id
        )

    raise ValueError("Unknown signal")


# ==========================================================
# ALERT QUEUE
# ==========================================================

def queue_once():
    with DB_LOCK, db() as c:

        row = c.execute("""
            SELECT *
            FROM alerts
            WHERE status='pending'
            ORDER BY id
            LIMIT 1
        """).fetchone()

        if not row:
            return False

        c.execute("""
            UPDATE alerts
            SET status='processing',
                updated_at=?
            WHERE id=?
              AND status='pending'
        """, (
            time.time(),
            row["id"]
        ))

    alert_id = row["id"]

    if time.time() - row["received_at"] > MAX_AGE:
        status = "expired"
        result = "Alert expired"

    elif not LIVE:
        status = "dry_run"
        result = "Live orders disabled"

    else:
        try:
            alert = parse(row["raw_message"])
            result = dispatch(alert, alert_id)
            status = "completed"

        except Exception as exc:
            log.exception(
                "Alert %s failed",
                alert_id
            )

            result = str(exc)
            status = "needs_review"

    with DB_LOCK, db() as c:
        c.execute("""
            UPDATE alerts
            SET status=?,
                error=?,
                updated_at=?
            WHERE id=?
        """, (
            status,
            result,
            time.time(),
            alert_id
        ))

    log.info(
        "Alert %s: %s - %s",
        alert_id,
        status,
        result
    )

    return True


def queue_worker():
    while True:
        try:
            if not queue_once():
                time.sleep(1)

        except Exception:
            log.exception("Queue worker error")
            time.sleep(5)


def risk_worker():
    while True:
        try:
            monitor_once()

        except Exception:
            log.exception("Risk monitor error")

        time.sleep(POLL)


# ==========================================================
# WEBHOOK AUTHENTICATION
# ==========================================================

def secret_ok():
    supplied = (
        request.args.get("secret", "")
        or request.headers.get(
            "X-Webhook-Secret", ""
        )
    )

    # Authenticated requests always require a valid secret.
    if supplied:
        return bool(WEBHOOK_SECRET) and hmac.compare_digest(
            supplied,
            WEBHOOK_SECRET
        )

    # Compatibility with existing TradingView alert URLs.
    return ALLOW_LEGACY_WEBHOOK


# ==========================================================
# FLASK ROUTES
# ==========================================================

@app.route("/", methods=["GET"])
def home():
    return jsonify(
        service="MAQ Binance Futures Bot",
        status="running",
        live=LIVE,
        webhook="/webhook",
        legacy_webhook=ALLOW_LEGACY_WEBHOOK
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        ok=True,
        live=LIVE
    )


@app.route("/webhook", methods=["POST"])
def webhook():

    if not secret_ok():
        return jsonify(
            error="Unauthorized webhook"
        ), 401

    raw = request.get_data(
        as_text=True
    ).strip()

    try:
        payload = request.get_json(silent=True)

        content = (
            payload
            if isinstance(payload, dict)
            else raw
        )

        alert = parse(content)

    except (ValueError, TypeError) as exc:
        return jsonify(
            error=str(exc)
        ), 400

    now = time.time()
    window = int(now // DEDUP)

    fingerprint = hashlib.sha256(
        (
            alert["raw"]
            + "|"
            + str(window)
        ).encode()
    ).hexdigest()

    with DB_LOCK, db() as c:
        try:
            cursor = c.execute("""
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
                alert["symbol"],
                alert["signal"],
                alert["raw"],
                now,
                now
            ))

            alert_id = cursor.lastrowid

        except sqlite3.IntegrityError:
            return jsonify(
                ok=True,
                duplicate=True
            ), 200

    return jsonify(
        ok=True,
        queued=True,
        alert_id=alert_id,
        live=LIVE
    ), 202


@app.route("/status", methods=["GET"])
def status():

    if not secret_ok():
        return jsonify(
            error="Unauthorized"
        ), 401

    with DB_LOCK, db() as c:

        counts = {
            r["status"]: r["n"]
            for r in c.execute("""
                SELECT status, COUNT(*) AS n
                FROM alerts
                GROUP BY status
            """)
        }

        tracked = [
            dict(r)
            for r in c.execute(
                "SELECT * FROM bot_positions"
            )
        ]

        recent = [
            dict(r)
            for r in c.execute("""
                SELECT
                    id, symbol, signal,
                    status, error, received_at
                FROM alerts
                ORDER BY id DESC
                LIMIT 20
            """)
        ]

        uncertain = [
            dict(r)
            for r in c.execute("""
                SELECT
                    id, symbol, action,
                    status, client_id
                FROM trade_journal
                WHERE status IN (
                    'uncertain',
                    'submitting',
                    'submitted'
                )
                ORDER BY id DESC
                LIMIT 20
            """)
        ]

    return jsonify(
        ok=True,
        live=LIVE,
        counts=counts,
        tracked_positions=tracked,
        recent_alerts=recent,
        unresolved_orders=uncertain,
        cooldown_remaining=max(
            0,
            round(cooldown_until - time.time())
        )
    )


# ==========================================================
# START BACKGROUND WORKERS
# ==========================================================

# IMPORTANT:
# Railway replicas = 1
# Gunicorn workers = 1
# Do not use --preload.

threading.Thread(
    target=queue_worker,
    daemon=True,
    name="maq-queue"
).start()

threading.Thread(
    target=risk_worker,
    daemon=True,
    name="maq-risk"
).start()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(setting("PORT", 8080))
    )
