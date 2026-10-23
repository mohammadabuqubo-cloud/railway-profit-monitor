
import os
import re
import time
import json
import hmac
import sqlite3
import hashlib
import logging
import threading
from decimal import Decimal, ROUND_DOWN

from flask import Flask, request, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger('MAQ')
app = Flask(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

def setting(name, default=''):
    value = os.getenv(name)
    return str(default) if value is None else value.strip()


def number(name, default, blank_zero=False):
    value = setting(name, default)
    if not value:
        return 0.0 if blank_zero else float(default)
    return float(value)


def boolean(name, default=False):
    return setting(name, str(default)).lower() in (
        'true', '1', 'yes', 'on'
    )


LIVE = boolean('ENABLE_LIVE_ORDERS', False)
TESTNET = boolean('BINANCE_TESTNET', False)

KEY = setting('BINANCE_API_KEY')
SECRET = setting('BINANCE_API_SECRET')
WEBHOOK_SECRET = setting('WEBHOOK_SECRET')

MARGIN = number('ORDER_MARGIN_USDT', 50)
LEVERAGE = int(number('ENTRY_LEVERAGE', 16))

# Blank means initial stop loss DISABLED.
INITIAL_SL = number(
    'INITIAL_STOP_LOSS_ROI_PCT', 0, blank_zero=True
)

BE_TRIGGER = number('BREAKEVEN_TRIGGER_ROI_PCT', 10)
BE_STOP = number('BREAKEVEN_STOP_ROI_PCT', 0)

POLL = max(
    30,
    int(number('POLL_INTERVAL_SECONDS', 60))
)

DEDUP = max(
    1,
    int(number('PARTIAL_DEDUP_SECONDS', 15))
)

MAX_AGE = max(
    30,
    int(number('MAX_ALERT_AGE_SECONDS', 180))
)

DB_PATH = setting(
    'BOT_DB_PATH', '/data/maq_bot.sqlite3'
)

POSITION_CACHE_TTL = 5.0

if LIVE and not (KEY and SECRET and WEBHOOK_SECRET):
    raise RuntimeError(
        'Live trading requires Binance credentials '
        'and WEBHOOK_SECRET'
    )

if (
    MARGIN <= 0
    or not 1 <= LEVERAGE <= 125
    or INITIAL_SL < 0
    or BE_TRIGGER < 0
):
    raise RuntimeError('Invalid trading configuration')

if not WEBHOOK_SECRET:
    log.warning(
        'WEBHOOK_SECRET unset: webhook refuses requests '
        'until configured'
    )


# ============================================================
# LOCKS AND CACHES
# ============================================================

DB_LOCK = threading.RLock()
TRADE_LOCK = threading.RLock()
API_LOCK = threading.RLock()

_client = None
cooldown_until = 0.0

exchange_cache = {}
exchange_cache_at = 0.0

snapshot_cache = None
snapshot_at = 0.0

os.makedirs(
    os.path.dirname(DB_PATH) or '.',
    exist_ok=True
)


# ============================================================
# SQLITE DATABASE
# ============================================================

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=30000')
    c.execute('PRAGMA journal_mode=WAL')
    return c


def init_db():
    with DB_LOCK, db() as c:

        c.execute('''
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
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_positions (
                symbol TEXT PRIMARY KEY,
                direction TEXT,
                created_at REAL,
                strategy TEXT DEFAULT 'VWAP'
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS position_state (
                symbol TEXT PRIMARY KEY,
                entry_price REAL,
                entry_qty REAL,
                partial_done INTEGER DEFAULT 0,
                breakeven_done INTEGER DEFAULT 0,
                tp_count INTEGER DEFAULT 0
            )
        ''')

        c.execute('''
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
        ''')

        columns = (
            (
                'alerts',
                'retry_count',
                'INTEGER DEFAULT 0'
            ),
            (
                'alerts',
                'next_attempt',
                'REAL DEFAULT 0'
            ),
            (
                'bot_positions',
                'strategy',
                "TEXT DEFAULT 'VWAP'"
            ),
            (
                'position_state',
                'tp_count',
                'INTEGER DEFAULT 0'
            ),
            (
                'position_state',
                'partial_done',
                'INTEGER DEFAULT 0'
            ),
            (
                'position_state',
                'breakeven_done',
                'INTEGER DEFAULT 0'
            ),
            (
                'position_state',
                'entry_price',
                'REAL'
            ),
            (
                'position_state',
                'entry_qty',
                'REAL'
            )
        )

        for table, col, definition in columns:
            names = {
                r[1]
                for r in c.execute(
                    f'PRAGMA table_info({table})'
                )
            }

            if col not in names:
                c.execute(
                    f'ALTER TABLE {table} '
                    f'ADD COLUMN {col} {definition}'
                )

        c.execute('''
            CREATE INDEX IF NOT EXISTS journal_symbol_status
            ON trade_journal(symbol, status)
        ''')

        c.execute(
            """
            UPDATE alerts
            SET status='needs_review',
                error='Interrupted on restart',
                updated_at=?
            WHERE status='processing'
            """,
            (time.time(),)
        )

        c.execute(
            """
            UPDATE trade_journal
            SET status='uncertain',
                note='Restart during submission',
                updated_at=?
            WHERE status IN ('submitting', 'submitted')
            """,
            (time.time(),)
        )


init_db()


# ============================================================
# BINANCE API
# ============================================================

def api(method, **kwargs):
    global _client, cooldown_until

    with API_LOCK:

        remaining = cooldown_until - time.time()

        if remaining > 0:
            raise RuntimeError(
                f'Binance API cooldown {remaining:.0f}s'
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
                'Binance %s error %s: %s',
                method,
                exc.code,
                exc
            )

            raise

        except (
            BinanceRequestException,
            TimeoutError,
            ConnectionError
        ) as exc:

            log.error(
                'Binance %s network error: %s',
                method,
                exc
            )

            raise


# ============================================================
# POSITION SNAPSHOTS
# ============================================================

def positions(force=False):
    global snapshot_cache, snapshot_at

    with API_LOCK:

        if (
            not force
            and snapshot_cache is not None
            and time.time() - snapshot_at < POSITION_CACHE_TTL
        ):
            return snapshot_cache

        raw = api('futures_position_information')
        data = {}

        for p in raw:

            qty = float(p.get('positionAmt', 0))

            if abs(qty) > 0:

                symbol = p['symbol']

                if symbol in data:
                    raise RuntimeError(
                        'Hedge mode detected; '
                        'this bot requires one-way position mode'
                    )

                data[symbol] = {
                    'qty': qty,
                    'entry': float(
                        p.get('entryPrice', 0)
                    ),
                    'mark': float(
                        p.get('markPrice', 0)
                    ),
                    'side': (
                        'LONG' if qty > 0 else 'SHORT'
                    )
                }

        snapshot_cache = data
        snapshot_at = time.time()

        return data


def invalidate_positions():
    global snapshot_cache, snapshot_at

    with API_LOCK:
        snapshot_cache = None
        snapshot_at = 0.0


# ============================================================
# EXCHANGE QUANTITY RULES
# ============================================================

def rules(symbol):
    global exchange_cache, exchange_cache_at

    with API_LOCK:

        if (
            not exchange_cache
            or time.time() - exchange_cache_at > 21600
        ):

            info = api('futures_exchange_info')

            exchange_cache = {
                x['symbol']: x
                for x in info['symbols']
            }

            exchange_cache_at = time.time()

        info = exchange_cache.get(symbol)

        if not info or info.get('status') != 'TRADING':
            raise ValueError(
                f'{symbol} not a trading futures symbol'
            )

        lot = next(
            (
                x for x in info['filters']
                if x['filterType'] == 'LOT_SIZE'
            ),
            None
        )

        market = next(
            (
                x for x in info['filters']
                if x['filterType'] == 'MARKET_LOT_SIZE'
            ),
            None
        )

        f = (
            market
            if (
                market
                and float(
                    market.get('stepSize', 0)
                ) > 0
            )
            else lot
        )

        if not f:
            raise RuntimeError(
                f'Missing quantity filter for {symbol}'
            )

        return (
            Decimal(f['stepSize']),
            Decimal(f['minQty']),
            Decimal(
                f.get('maxQty', '999999999')
            )
        )


def floor_qty(value, step):

    value = Decimal(str(value))

    return (
        (value / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step
    )


def quantity(symbol, value):

    step, minimum, maximum = rules(symbol)

    qty = floor_qty(value, step)

    if qty < minimum or qty > maximum:
        raise ValueError(
            f'Quantity {qty} outside '
            f'[{minimum}, {maximum}] for {symbol}'
        )

    return format(qty, 'f')


# ============================================================
# BOT POSITION OWNERSHIP
# ============================================================

def owned(symbol):

    with DB_LOCK, db() as c:

        row = c.execute(
            'SELECT * FROM bot_positions WHERE symbol=?',
            (symbol,)
        ).fetchone()

        state = c.execute(
            'SELECT * FROM position_state WHERE symbol=?',
            (symbol,)
        ).fetchone()

        return (
            dict(row) if row else None,
            dict(state) if state else None
        )


def save_owned(
    symbol,
    side,
    entry,
    qty,
    strategy
):

    with DB_LOCK, db() as c:

        c.execute(
            '''
            INSERT OR REPLACE INTO bot_positions
            (symbol, direction, created_at, strategy)
            VALUES (?, ?, ?, ?)
            ''',
            (
                symbol,
                side,
                time.time(),
                strategy
            )
        )

        c.execute(
            '''
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
            ''',
            (
                symbol,
                entry,
                abs(qty)
            )
        )


def forget_owned(symbol):

    with DB_LOCK, db() as c:

        c.execute(
            'DELETE FROM bot_positions WHERE symbol=?',
            (symbol,)
        )

        c.execute(
            'DELETE FROM position_state WHERE symbol=?',
            (symbol,)
        )


# ============================================================
# ORDER JOURNAL
# ============================================================

def unresolved(symbol):

    with DB_LOCK, db() as c:

        return c.execute(
            """
            SELECT *
            FROM trade_journal
            WHERE symbol=?
              AND status IN (
                'submitting',
                'submitted',
                'uncertain'
              )
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,)
        ).fetchone()


def journal_new(
    symbol,
    action,
    alert_id,
    before,
    requested
):

    with DB_LOCK, db() as c:

        cursor = c.execute(
            '''
            INSERT INTO trade_journal
            (
                symbol,
                action,
                alert_id,
                status,
                before_qty,
                requested_qty,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                symbol,
                action,
                alert_id,
                'prepared',
                before,
                requested,
                time.time(),
                time.time()
            )
        )

        jid = cursor.lastrowid

        client_id = (
            f'MAQ{jid}_{int(time.time())}'
        )

        c.execute(
            '''
            UPDATE trade_journal
            SET client_id=?
            WHERE id=?
            ''',
            (client_id, jid)
        )

        return jid, client_id


def journal_update(
    jid,
    status,
    order_id=None,
    note=None
):

    with DB_LOCK, db() as c:

        c.execute(
            '''
            UPDATE trade_journal
            SET status=?,
                order_id=COALESCE(?, order_id),
                note=COALESCE(?, note),
                updated_at=?
            WHERE id=?
            ''',
            (
                status,
                (
                    str(order_id)
                    if order_id is not None
                    else None
                ),
                note,
                time.time(),
                jid
            )
        )


# ============================================================
# ORDER SUBMISSION
# ============================================================

def submit(
    symbol,
    side,
    qty,
    reduce_only,
    action,
    alert_id,
    before
):

    if not LIVE:
        raise RuntimeError(
            'Live trading disabled'
        )

    if unresolved(symbol):
        raise RuntimeError(
            f'{symbol}: unresolved order journal; '
            'manual reconciliation required'
        )

    jid, cid = journal_new(
        symbol,
        action,
        alert_id,
        before,
        float(qty)
    )

    journal_update(jid, 'submitting')

    try:

        params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET',
            'quantity': qty,
            'newClientOrderId': cid
        }

        if reduce_only:
            params['reduceOnly'] = 'true'

        order = api(
            'futures_create_order',
            **params
        )

        journal_update(
            jid,
            'submitted',
            order.get('orderId')
        )

        invalidate_positions()

        return jid, order

    except BinanceAPIException as exc:

        if exc.code in (
            -1013,
            -1100,
            -1102,
            -1111,
            -2019,
            -2021,
            -2022,
            -4164
        ):

            journal_update(
                jid,
                'rejected',
                note=str(exc)
            )

        else:

            journal_update(
                jid,
                'uncertain',
                note=str(exc)
            )

        raise

    except Exception as exc:

        journal_update(
            jid,
            'uncertain',
            note=str(exc)
        )

        raise


# ============================================================
# VERIFY EXCHANGE POSITION
# ============================================================

def verified(
    symbol,
    predicate,
    attempts=4
):

    for attempt in range(attempts):

        if attempt:
            time.sleep(1.0)

        p = positions(
            force=True
        ).get(symbol)

        if predicate(p):
            return p

    raise RuntimeError(
        f'{symbol}: exchange state not verified; '
        'inspect orders before retrying'
    )


def confirm_order(
    jid,
    symbol,
    predicate
):

    try:

        p = verified(
            symbol,
            predicate
        )

    except Exception as exc:

        journal_update(
            jid,
            'uncertain',
            note=str(exc)
        )

        raise

    journal_update(
        jid,
        'confirmed'
    )

    return p


# ============================================================
# CLOSE POSITION
# ============================================================

def close_position(
    symbol,
    alert_id=None,
    reason='CLOSE'
):

    with TRADE_LOCK:

        owner, state = owned(symbol)

        p = positions(
            force=True
        ).get(symbol)

        if not p:

            if owner:
                forget_owned(symbol)

            return 'already_flat'

        if (
            not owner
            or owner['direction'] != p['side']
        ):

            raise RuntimeError(
                f'{symbol}: position not verified '
                'as bot-owned; will not close'
            )

        if unresolved(symbol):

            raise RuntimeError(
                f'{symbol}: unresolved journal'
            )

        side = (
            'SELL'
            if p['qty'] > 0
            else 'BUY'
        )

        qty = quantity(
            symbol,
            abs(p['qty'])
        )

        jid, _ = submit(
            symbol,
            side,
            qty,
            True,
            reason,
            alert_id,
            p['qty']
        )

        confirm_order(
            jid,
            symbol,
            lambda x: x is None
        )

        forget_owned(symbol)

        return 'closed'


# ============================================================
# BUY / SELL / AUTOMATIC REVERSAL
# ============================================================

def enter(
    symbol,
    direction,
    strategy='STANDARD',
    alert_id=None
):

    with TRADE_LOCK:

        if unresolved(symbol):

            raise RuntimeError(
                f'{symbol}: unresolved journal'
            )

        p = positions(
            force=True
        ).get(symbol)

        owner, _ = owned(symbol)

        if p:

            if (
                not owner
                or owner['direction'] != p['side']
            ):

                raise RuntimeError(
                    f'{symbol}: existing position '
                    'not bot-owned; refusing entry'
                )

            if p['side'] == direction:

                return 'already_in_position'

            # Opposite signal:
            # Close old bot-owned position first.
            close_position(
                symbol,
                alert_id,
                'REVERSAL_CLOSE'
            )

            if positions(
                force=True
            ).get(symbol):

                raise RuntimeError(
                    f'{symbol}: reversal close '
                    'not confirmed'
                )

        elif owner:

            forget_owned(symbol)

        api(
            'futures_change_leverage',
            symbol=symbol,
            leverage=LEVERAGE
        )

        ticker = api(
            'futures_symbol_ticker',
            symbol=symbol
        )

        price = float(
            ticker['price']
        )

        if price <= 0:

            raise RuntimeError(
                'Invalid market price'
            )

        qty = quantity(
            symbol,
            MARGIN * LEVERAGE / price
        )

        side = (
            'BUY'
            if direction == 'LONG'
            else 'SELL'
        )

        jid, _ = submit(
            symbol,
            side,
            qty,
            False,
            'ENTRY_' + direction,
            alert_id,
            0.0
        )

        p = confirm_order(
            jid,
            symbol,
            lambda x: (
                x is not None
                and x['side'] == direction
            )
        )

        save_owned(
            symbol,
            direction,
            p['entry'],
            abs(p['qty']),
            strategy
        )

        return (
            'entered_' + direction.lower()
        )


# ============================================================
# PARTIAL TAKE PROFIT
# FIVE STAGES OF 20% OF ORIGINAL POSITION
# ============================================================

def partial(
    symbol,
    pct=20,
    stage=None,
    alert_id=None
):

    with TRADE_LOCK:

        if not 0 < pct <= 100:

            raise ValueError(
                'Partial percent must be '
                'between 0 and 100'
            )

        p = positions(
            force=True
        ).get(symbol)

        owner, state = owned(symbol)

        if not p:

            if owner:
                forget_owned(symbol)

            return 'already_flat'

        if (
            not owner
            or not state
            or owner['direction'] != p['side']
        ):

            raise RuntimeError(
                f'{symbol}: position not bot-owned'
            )

        if unresolved(symbol):

            raise RuntimeError(
                f'{symbol}: unresolved journal'
            )

        count = int(
            state['tp_count'] or 0
        )

        if stage is not None:

            if stage <= count:

                return 'stage_already_completed'

            if stage != count + 1:

                raise RuntimeError(
                    f'{symbol}: expected TP{count + 1}, '
                    f'got TP{stage}'
                )

        if count >= 5:

            return 'all_stages_completed'

        with DB_LOCK, db() as c:

            recent = c.execute(
                """
                SELECT updated_at
                FROM trade_journal
                WHERE symbol=?
                  AND action='PARTIAL'
                  AND status='confirmed'
                ORDER BY id DESC
                LIMIT 1
                """,
                (symbol,)
            ).fetchone()

        if (
            stage is None
            and recent
            and time.time()
            - float(recent['updated_at']) < DEDUP
        ):

            return 'partial_deduplicated'

        initial = float(
            state['entry_qty'] or 0
        )

        target = (
            initial * pct / 100
        )

        if (
            stage == 5
            or count == 4
            or target >= abs(p['qty'])
        ):

            qty = quantity(
                symbol,
                abs(p['qty'])
            )

        else:

            try:

                qty = quantity(
                    symbol,
                    min(
                        target,
                        abs(p['qty'])
                    )
                )

            except ValueError:

                return (
                    'quantity_below_exchange_minimum'
                )

        side = (
            'SELL'
            if p['qty'] > 0
            else 'BUY'
        )

        jid, _ = submit(
            symbol,
            side,
            qty,
            True,
            'PARTIAL',
            alert_id,
            p['qty']
        )

        before = abs(
            p['qty']
        )

        after = confirm_order(
            jid,
            symbol,
            lambda x: (
                x is None
                or abs(x['qty']) < before - 1e-12
            )
        )

        if after is None:

            forget_owned(symbol)

            return 'partial_closed_position'

        with DB_LOCK, db() as c:

            c.execute(
                '''
                UPDATE position_state
                SET tp_count=?,
                    partial_done=1
                WHERE symbol=?
                ''',
                (
                    count + 1,
                    symbol
                )
            )

        return (
            f'partial_tp_{count + 1}'
        )


# ============================================================
# ROI AND BREAKEVEN MONITOR
# ============================================================

def roi(p):

    entry = float(
        p['entry']
    )

    if entry <= 0:
        return None

    mark = float(
        p['mark']
    )

    sign = (
        1
        if p['qty'] > 0
        else -1
    )

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

        ps = positions(
            force=True
        )

        with DB_LOCK, db() as c:

            owners = [
                dict(r)
                for r in c.execute(
                    'SELECT * FROM bot_positions'
                )
            ]

        for owner in owners:

            symbol = owner['symbol']

            p = ps.get(symbol)

            if not p:

                forget_owned(symbol)
                continue

            if (
                p['side'] != owner['direction']
                or unresolved(symbol)
            ):

                log.error(
                    'Risk monitor skipped %s: '
                    'ownership mismatch or unresolved order',
                    symbol
                )

                continue

            _, state = owned(symbol)

            if not state:

                log.error(
                    'Risk monitor skipped %s: '
                    'missing position state',
                    symbol
                )

                continue

            value = roi(p)

            if value is None:
                continue

            # Initial stop is disabled when INITIAL_SL = 0.
            if (
                INITIAL_SL > 0
                and value <= -INITIAL_SL
            ):

                log.warning(
                    'Initial SL triggered %s ROI %.2f',
                    symbol,
                    value
                )

                close_position(
                    symbol,
                    reason='INITIAL_SL'
                )

                continue

            armed = bool(
                state['breakeven_done']
            )

            if (
                not armed
                and value >= BE_TRIGGER
            ):

                with DB_LOCK, db() as c:

                    c.execute(
                        '''
                        UPDATE position_state
                        SET breakeven_done=1
                        WHERE symbol=?
                        ''',
                        (symbol,)
                    )

                armed = True

                log.info(
                    'Breakeven armed %s ROI %.2f',
                    symbol,
                    value
                )

            if (
                armed
                and value <= BE_STOP
            ):

                log.warning(
                    'Breakeven exit %s ROI %.2f',
                    symbol,
                    value
                )

                close_position(
                    symbol,
                    reason='BREAKEVEN'
                )


# ============================================================
# TRADINGVIEW SIGNAL PARSER
# ============================================================

def parse(message):

    if isinstance(message, dict):

        message = (
            message.get('message')
            or message.get('text')
            or message.get('alert')
            or ''
        )

    raw = (
        str(message)
        .strip()
        .upper()
        .replace(' ', '')
    )

    pieces = raw.split('|')

    if (
        pieces
        and pieces[0] == 'VWAP'
    ):

        pieces = pieces[1:]
        strategy = 'VWAP'

    else:

        strategy = 'STANDARD'

    if len(pieces) < 2:

        raise ValueError(
            'Expected BUY|SYMBOL, SELL|SYMBOL, '
            'CLOSE|SYMBOL, PARTIAL|SYMBOL|20, '
            'or VWAP|TP1|SYMBOL'
        )

    signal, symbol = pieces[:2]

    if not re.fullmatch(
        r'[A-Z0-9]{4,25}',
        symbol
    ):

        raise ValueError(
            'Invalid symbol'
        )

    stage = None
    pct = 20.0

    if re.fullmatch(
        r'TP[1-5]',
        signal
    ):

        stage = int(
            signal[2:]
        )

        signal = 'PARTIAL'

    elif (
        signal == 'PARTIAL'
        and len(pieces) > 2
    ):

        pct = float(
            pieces[2]
        )

    elif signal not in (
        'BUY',
        'SELL',
        'CLOSE',
        'PARTIAL'
    ):

        raise ValueError(
            'Unknown signal'
        )

    if (
        signal == 'PARTIAL'
        and not 0 < pct <= 100
    ):

        raise ValueError(
            'Invalid partial percent'
        )

    return {
        'signal': signal,
        'symbol': symbol,
        'pct': pct,
        'stage': stage,
        'strategy': strategy,
        'raw': raw
    }


# ============================================================
# SIGNAL DISPATCH
# ============================================================

def dispatch(alert, alert_id):

    signal = alert['signal']
    symbol = alert['symbol']

    if signal == 'BUY':

        return enter(
            symbol,
            'LONG',
            alert['strategy'],
            alert_id
        )

    if signal == 'SELL':

        return enter(
            symbol,
            'SHORT',
            alert['strategy'],
            alert_id
        )

    if signal == 'CLOSE':

        return close_position(
            symbol,
            alert_id
        )

    if signal == 'PARTIAL':

        return partial(
            symbol,
            alert['pct'],
            alert['stage'],
            alert_id
        )

    raise ValueError(
        'Unknown signal'
    )


# ============================================================
# ALERT QUEUE
# ============================================================

def queue_once():

    with DB_LOCK, db() as c:

        row = c.execute(
            """
            SELECT *
            FROM alerts
            WHERE status='pending'
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()

        if not row:
            return False

        c.execute(
            """
            UPDATE alerts
            SET status='processing',
                updated_at=?
            WHERE id=?
              AND status='pending'
            """,
            (
                time.time(),
                row['id']
            )
        )

    alert_id = row['id']

    age = (
        time.time()
        - float(row['received_at'])
    )

    if age > MAX_AGE:

        result = 'Expired before processing'
        status = 'expired'

    elif not LIVE:

        result = (
            'Dry run: no Binance order placed'
        )

        status = 'dry_run'

    else:

        try:

            alert = parse(
                row['raw_message']
            )

            result = dispatch(
                alert,
                alert_id
            )

            status = 'completed'

        except Exception as exc:

            log.exception(
                'Alert %s failed',
                alert_id
            )

            result = str(exc)
            status = 'needs_review'

    with DB_LOCK, db() as c:

        c.execute(
            '''
            UPDATE alerts
            SET status=?,
                error=?,
                updated_at=?
            WHERE id=?
            ''',
            (
                status,
                result,
                time.time(),
                alert_id
            )
        )

    log.info(
        'Alert %s: %s %s',
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

            log.exception(
                'Queue worker error'
            )

            time.sleep(5)


# ============================================================
# BACKGROUND RISK MONITOR
# ============================================================

def risk_worker():

    while True:

        try:

            monitor_once()

        except Exception:

            log.exception(
                'Risk monitor error'
            )

        time.sleep(POLL)


# ============================================================
# WEBHOOK AUTHENTICATION
# ============================================================

def secret_ok():

    if not WEBHOOK_SECRET:
        return False

    supplied = (
        request.args.get('secret', '')
        or request.headers.get(
            'X-Webhook-Secret', ''
        )
    )

    return hmac.compare_digest(
        str(supplied),
        WEBHOOK_SECRET
    )


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/', methods=['GET'])
def home():

    return jsonify(
        service='MAQ Binance Futures Bot',
        status='running',
        live=LIVE,
        webhook='/webhook',
        monitoring_seconds=POLL
    )


@app.route('/health', methods=['GET'])
def health():

    return jsonify(
        ok=True,
        live=LIVE
    )


@app.route('/webhook', methods=['POST'])
def webhook():

    if not secret_ok():

        return jsonify(
            error='Unauthorized webhook'
        ), 401

    raw = request.get_data(
        as_text=True
    ).strip()

    try:

        payload = request.get_json(
            silent=True
        )

        content = (
            payload
            if isinstance(payload, dict)
            else raw
        )

        alert = parse(
            content
        )

    except (
        ValueError,
        TypeError
    ) as exc:

        return jsonify(
            error=str(exc)
        ), 400

    now = time.time()

    # Deduplicate identical alerts in a short window.
    window = int(
        now // DEDUP
    )

    fingerprint = hashlib.sha256(
        f'{alert["raw"]}|{window}'.encode()
    ).hexdigest()

    with DB_LOCK, db() as c:

        try:

            cursor = c.execute(
                '''
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
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    fingerprint,
                    alert['symbol'],
                    alert['signal'],
                    alert['raw'],
                    now,
                    'pending',
                    now
                )
            )

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


@app.route('/status', methods=['GET'])
def status():

    if not secret_ok():

        return jsonify(
            error='Unauthorized'
        ), 401

    with DB_LOCK, db() as c:

        counts = {
            r['status']: r['n']
            for r in c.execute(
                '''
                SELECT status, COUNT(*) AS n
                FROM alerts
                GROUP BY status
                '''
            )
        }

        tracked = [
            dict(r)
            for r in c.execute(
                'SELECT * FROM bot_positions'
            )
        ]

        recent = [
            dict(r)
            for r in c.execute(
                '''
                SELECT
                    id,
                    symbol,
                    signal,
                    status,
                    error,
                    received_at
                FROM alerts
                ORDER BY id DESC
                LIMIT 15
                '''
            )
        ]

        uncertain = [
            dict(r)
            for r in c.execute(
                """
                SELECT
                    id,
                    symbol,
                    action,
                    status,
                    client_id
                FROM trade_journal
                WHERE status IN (
                    'uncertain',
                    'submitted',
                    'submitting'
                )
                ORDER BY id DESC
                LIMIT 20
                """
            )
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
            round(
                cooldown_until - time.time()
            )
        )
    )


# ============================================================
# START BACKGROUND WORKERS
# ============================================================

# Use exactly one Gunicorn worker and one Railway replica.
# Do not use Gunicorn --preload.

threading.Thread(
    target=queue_worker,
    daemon=True,
    name='maq-queue'
).start()

threading.Thread(
    target=risk_worker,
    daemon=True,
    name='maq-risk'
).start()


if __name__ == '__main__':

    app.run(
        host='0.0.0.0',
        port=int(
            setting('PORT', 8080)
        )
    )
