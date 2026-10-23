import os, re, time, json, hmac, sqlite3, hashlib, logging, threading
from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('MAQ')
app = Flask(__name__)

def setting(name, default):
    value = os.getenv(name)
    return str(default) if value is None else value.strip()

def number(name, default, blank_zero=False):
    value = setting(name, default)
    if not value and blank_zero:
        return 0.0
    return float(value)

def boolean(name, default=False):
    return setting(name, str(default)).lower() in ('true', '1', 'yes', 'on')

LIVE = boolean('ENABLE_LIVE_ORDERS', False)
TESTNET = boolean('BINANCE_TESTNET', False)
KEY = setting('BINANCE_API_KEY', '')
SECRET = setting('BINANCE_API_SECRET', '')
WEBHOOK_SECRET = setting('WEBHOOK_SECRET', '')
MARGIN = number('ORDER_MARGIN_USDT', 50)
LEVERAGE = int(number('ENTRY_LEVERAGE', 16))
INITIAL_SL = number('INITIAL_STOP_LOSS_ROI_PCT', 0, blank_zero=True)
BE_TRIGGER = number('BREAKEVEN_TRIGGER_ROI_PCT', 10)
BE_STOP = number('BREAKEVEN_STOP_ROI_PCT', 0)
POLL = max(15, int(number('POLL_INTERVAL_SECONDS', 15)))
DEDUP = max(1, int(number('PARTIAL_DEDUP_SECONDS', 15)))
MAX_AGE = max(30, int(number('MAX_ALERT_AGE_SECONDS', 180)))
DB_PATH = setting('BOT_DB_PATH', '/data/maq_bot.sqlite3')
# Retained for Railway compatibility. Global mismatch blocking is deliberately removed.
BLOCK_NEW_ON_MISMATCH = boolean('BLOCK_NEW_ENTRIES_ON_MISMATCH', True)
if LIVE and not (KEY and SECRET):
    raise RuntimeError('Missing Binance API credentials')
if MARGIN <= 0 or not 1 <= LEVERAGE <= 125 or INITIAL_SL < 0 or BE_TRIGGER < 0:
    raise RuntimeError('Invalid margin, leverage or risk configuration')
if LIVE and not WEBHOOK_SECRET:
    log.warning('SECURITY: WEBHOOK_SECRET unset. Anyone with webhook URL can send trade alerts.')

DB_LOCK = threading.RLock()
TRADE_LOCK = threading.RLock()
API_LOCK = threading.RLock()
_client = None
cooldown_until = 0.0
exchange_cache = {}
exchange_cache_at = 0.0
snapshot_cache = None
snapshot_at = 0.0
POSITION_CACHE_TTL = 4.0

os.makedirs(os.path.dirname(DB_PATH) or '.', exist_ok=True)

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=30000')
    c.execute('PRAGMA journal_mode=WAL')
    return c

def init_db():
    with DB_LOCK, db() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT UNIQUE,
          symbol TEXT, signal TEXT, raw_message TEXT, received_at REAL,
          status TEXT DEFAULT 'pending', error TEXT, updated_at REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS bot_positions (
          symbol TEXT PRIMARY KEY, direction TEXT, created_at REAL,
          strategy TEXT DEFAULT 'VWAP')''')
        c.execute('''CREATE TABLE IF NOT EXISTS position_state (
          symbol TEXT PRIMARY KEY, entry_price REAL, entry_qty REAL,
          partial_done INTEGER DEFAULT 0, breakeven_done INTEGER DEFAULT 0,
          tp_count INTEGER DEFAULT 0)''')
        for table, col, definition in (
          ('bot_positions','strategy',"TEXT DEFAULT 'VWAP'"),
          ('position_state','tp_count','INTEGER DEFAULT 0'),
          ('position_state','partial_done','INTEGER DEFAULT 0'),
          ('position_state','breakeven_done','INTEGER DEFAULT 0'),
          ('position_state','entry_price','REAL'),
          ('position_state','entry_qty','REAL')):
            names = {x[1] for x in c.execute(f'PRAGMA table_info({table})')}
            if col not in names:
                c.execute(f'ALTER TABLE {table} ADD COLUMN {col} {definition}')
        c.execute('''CREATE TABLE IF NOT EXISTS trade_journal (
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
          action TEXT NOT NULL, alert_id INTEGER, client_id TEXT UNIQUE,
          status TEXT NOT NULL, before_qty REAL, requested_qty REAL,
          order_id TEXT, created_at REAL, updated_at REAL, note TEXT)''')
        c.execute('''CREATE INDEX IF NOT EXISTS journal_symbol_status
                     ON trade_journal(symbol,status)''')
