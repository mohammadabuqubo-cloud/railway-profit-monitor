MARGIN = number('ORDER_MARGIN_USDT', 50)
LEVERAGE = int(number('ENTRY_LEVERAGE', 16))
INITIAL_SL = number('INITIAL_STOP_LOSS_ROI_PCT', 0, blank_zero=True)
BE_TRIGGER = number('BREAKEVEN_TRIGGER_ROI_PCT', 10)
BE_STOP = number('BREAKEVEN_STOP_ROI_PCT', 0)
POLL = max(60, int(number('POLL_INTERVAL_SECONDS', 60)))
DEDUP = max(1, int(number('PARTIAL_DEDUP_SECONDS', 15)))
MAX_AGE = max(30, int(number('MAX_ALERT_AGE_SECONDS', 180)))
DB_PATH = setting('BOT_DB_PATH', '/data/maq_bot.sqlite3')

# Retained for Railway compatibility.
# Global mismatch blocking is deliberately removed.
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
POSITION_CACHE_TTL = 10.0

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
        )''')

        existing_alert_cols = {x[1] for x in c.execute('PRAGMA table_info(alerts)')}
        if 'retry_count' not in existing_alert_cols:
            c.execute('ALTER TABLE alerts ADD COLUMN retry_count INTEGER DEFAULT 0')
        if 'next_attempt' not in existing_alert_cols:
            c.execute('ALTER TABLE alerts ADD COLUMN next_attempt REAL DEFAULT 0')

        c.execute('''CREATE TABLE IF NOT EXISTS bot_positions (
          symbol TEXT PRIMARY KEY,
          direction TEXT,
          created_at REAL,
          strategy TEXT DEFAULT 'VWAP'
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS position_state (
          symbol TEXT PRIMARY KEY,
          entry_price REAL,
          entry_qty REAL,
          partial_done INTEGER DEFAULT 0,
          breakeven_done INTEGER DEFAULT 0,
          tp_count INTEGER DEFAULT 0
        )''')

        for table, col, definition in (
            ('bot_positions', 'strategy', "TEXT DEFAULT 'VWAP'"),
            ('position_state', 'tp_count', 'INTEGER DEFAULT 0'),
            ('position_state', 'partial_done', 'INTEGER DEFAULT 0'),
            ('position_state', 'breakeven_done', 'INTEGER DEFAULT 0'),
            ('position_state', 'entry_price', 'REAL'),
            ('position_state', 'entry_qty', 'REAL')
        ):
            names = {
                x[1] for x in c.execute(f'PRAGMA table_info({table})')
            }
            if col not in names:
                c.execute(
                    f'ALTER TABLE {table} ADD COLUMN {col} {definition}'
                )

        c.execute('''CREATE TABLE IF NOT EXISTS trade_journal (
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
        )''')

        c.execute('''CREATE INDEX IF NOT EXISTS journal_symbol_status
                     ON trade_journal(symbol,status)''')

        c.execute(
            """UPDATE alerts
               SET status='needs_review',
                   error='Interrupted; inspect Binance and journal',
                   updated_at=?
               WHERE status='processing'""",
            (time.time(),)
        )

        c.execute(
            """UPDATE trade_journal
               SET status='uncertain',
                   note='Process restarted during order',
                   updated_at=?
               WHERE status IN ('submitting','submitted')""",
            (time.time(),)
        )


init_db()


def api(method, **kwargs):
    global _client, cooldown_until

    with API_LOCK:
        remaining = cooldown_until - time.time()

        if remaining > 0:
            raise RuntimeError(
                f'Binance cooldown active ({remaining:.0f}s)'
            )

        try:
            if _client is None:
                # Compatible with installed python-binance versions.
                # Do not pass ping=False.
                _client = Client(
                    KEY,
                    SECRET,
                    testnet=TESTNET
                )
