
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
        c.execute("UPDATE alerts SET status='needs_review', error='Interrupted; inspect Binance and journal', updated_at=? WHERE status='processing'", (time.time(),))
        c.execute("UPDATE trade_journal SET status='uncertain', note='Process restarted during order', updated_at=? WHERE status IN ('submitting','submitted')", (time.time(),))
init_db()

def api(method, **kwargs):
    global _client, cooldown_until
    with API_LOCK:
        remaining = cooldown_until - time.time()
        if remaining > 0:
            raise RuntimeError(f'Binance cooldown active ({remaining:.0f}s)')
        try:
            if _client is None:
                _client = Client(KEY, SECRET, testnet=TESTNET, ping=False)
            return getattr(_client, method)(**kwargs)
        except BinanceAPIException as e:
            if e.code in (-1003, -1015):
                # Avoid hammering a shared Railway egress IP after rate limits.
                cooldown_until = time.time() + (300 if e.code == -1003 else 120)
                log.error('Binance rate limit %s; API paused', e.code)
            raise

def positions(force=False):
    global snapshot_cache, snapshot_at
    with API_LOCK:
        if not force and snapshot_cache is not None and time.time() - snapshot_at < POSITION_CACHE_TTL:
            return snapshot_cache
        raw = api('futures_position_information')
        result = {}
        for p in raw:
            if abs(float(p['positionAmt'])) > 0:
                if p.get('positionSide', 'BOTH') != 'BOTH' or p['symbol'] in result:
                    raise RuntimeError('Hedge Mode unsupported; use One-way Mode')
                result[p['symbol']] = p
        snapshot_cache, snapshot_at = result, time.time()
        return result

def invalidate_positions():
    global snapshot_cache, snapshot_at
    with API_LOCK:
        snapshot_cache, snapshot_at = None, 0.0

def rules(symbol):
    global exchange_cache, exchange_cache_at
    if time.time() - exchange_cache_at > 21600 or not exchange_cache:
        info = api('futures_exchange_info')
        exchange_cache = {s['symbol']: s for s in info['symbols']}
        exchange_cache_at = time.time()
    info = exchange_cache.get(symbol)
    if not info or info.get('status') != 'TRADING':
        raise RuntimeError(f'{symbol}: symbol not trading')
    filters = {f['filterType']: f for f in info['filters']}
    market = filters.get('MARKET_LOT_SIZE', {})
    lot = filters.get('LOT_SIZE', {})
    step = Decimal(str(market.get('stepSize', '0')))
    if step <= 0:
        step = Decimal(str(lot.get('stepSize', '0')))
    minimum = Decimal(str(market.get('minQty', '0')))
    if minimum <= 0:
        minimum = Decimal(str(lot.get('minQty', '0')))
    if step <= 0:
        raise RuntimeError(f'{symbol}: invalid market step size')
    return step, minimum

def quantity(symbol, amount):
    step, minimum = rules(symbol)
    q = (Decimal(str(amount)) / step).to_integral_value(rounding=ROUND_DOWN) * step
    if q <= 0 or q < minimum:
        raise RuntimeError(f'{symbol}: order quantity below Binance minimum')
    return format(q, 'f')

def owned_state(symbol):
    with DB_LOCK, db() as c:
        b = c.execute('SELECT * FROM bot_positions WHERE symbol=?', (symbol,)).fetchone()
        s = c.execute('SELECT * FROM position_state WHERE symbol=?', (symbol,)).fetchone()
    return (dict(b) if b else None, dict(s) if s else None)

def save_position(symbol, direction, p):
    with DB_LOCK, db() as c:
        c.execute('INSERT OR REPLACE INTO bot_positions(symbol,direction,created_at,strategy) VALUES(?,?,?,?)',
                  (symbol, direction, time.time(), 'VWAP'))
        c.execute('''INSERT OR REPLACE INTO position_state
          (symbol,entry_price,entry_qty,partial_done,breakeven_done,tp_count)
          VALUES(?,?,?,0,0,0)''',
          (symbol, float(p['entryPrice']), abs(float(p['positionAmt']))))

def remove_position(symbol):
    with DB_LOCK, db() as c:
        c.execute('DELETE FROM bot_positions WHERE symbol=?', (symbol,))
        c.execute('DELETE FROM position_state WHERE symbol=?', (symbol,))

def unresolved(symbol):
    with DB_LOCK, db() as c:
        row = c.execute("SELECT id,action,status FROM trade_journal WHERE symbol=? AND status IN ('submitting','submitted','uncertain') ORDER BY id DESC LIMIT 1", (symbol,)).fetchone()
        return dict(row) if row else None

def journal_start(symbol, action, alert_id, before, qty):
    with DB_LOCK, db() as c:
        c.execute('''INSERT INTO trade_journal(symbol,action,alert_id,client_id,status,before_qty,requested_qty,created_at,updated_at)
                     VALUES(?,?,?,?,?,?,?,?,?)''',
                  (symbol,action,alert_id,None,'submitting',before,qty,time.time(),time.time()))
        jid = c.execute('SELECT last_insert_rowid()').fetchone()[0]
        client_id = f'MAQ{jid}_{int(time.time())}'
        c.execute('UPDATE trade_journal SET client_id=? WHERE id=?', (client_id,jid))
    return jid, client_id

def journal_update(jid, status, order_id=None, note=None):
    with DB_LOCK, db() as c:
        c.execute('UPDATE trade_journal SET status=?,order_id=COALESCE(?,order_id),note=?,updated_at=? WHERE id=?',
                  (status,str(order_id) if order_id is not None else None,note,time.time(),jid))

def place(symbol, side, amount, reduce_only, action, alert_id, before_qty):
    qty = quantity(symbol, amount)
    jid, cid = journal_start(symbol, action, alert_id, before_qty, float(qty))
    params = dict(symbol=symbol,side=side,type='MARKET',quantity=qty,newClientOrderId=cid)
    if reduce_only:
        params['reduceOnly'] = 'true'
    try:
        result = api('futures_create_order', **params)
        journal_update(jid,'submitted',result.get('orderId'),'Awaiting position verification')
        log.warning('ORDER SUBMITTED %s %s qty=%s reduceOnly=%s id=%s',symbol,side,qty,reduce_only,result.get('orderId'))
        return jid, result
    except BinanceAPIException as e:
        # Exchange-side deterministic validation rejections cannot have filled.
        if e.code in (-1121,-1111,-1013,-2019,-2022,-4061,-1102):
            journal_update(jid,'rejected',note=str(e)[:400])
        else:
            journal_update(jid,'uncertain',note=str(e)[:400])
        raise
    except Exception as e:
        # Never retry a request when the response may have been lost.
        journal_update(jid,'uncertain',note=str(e)[:400])
        raise

def confirmed_position(symbol, check, tries=3):
    for i in range(tries):
        if i:
            time.sleep(1)
        invalidate_positions()
        p = positions(force=True).get(symbol)
        if check(p):
            return p
    raise RuntimeError('Order outcome not confirmed. Do not resend; inspect Binance and trade_journal')

def ensure_safe(symbol):
    item = unresolved(symbol)
    if item:
        raise RuntimeError(f'{symbol}: uncertain order journal #{item["id"]}; manual review required')

def direction_of(p):
    return 'BUY' if float(p['positionAmt']) > 0 else 'SELL'

def entry(signal, symbol, alert_id):
    with TRADE_LOCK:
        ensure_safe(symbol)
        current = positions(force=True).get(symbol)
        b, state = owned_state(symbol)
        if current:
            if not b or not state:
                raise RuntimeError('Existing Binance position not verified bot-owned; entry blocked for this symbol')
            if b['direction'] != direction_of(current):
                raise RuntimeError('Saved position direction differs from Binance; review required')
            if direction_of(current) == signal:
                return 'already_in_position'
            # Do not auto-reverse an existing position; CLOSE must be explicit.
            raise RuntimeError('Opposite position already open; send CLOSE before reversing')
        if b:
            # Only clear a stale record for THIS flat symbol, not other symbols.
            remove_position(symbol)
        # Global historical entry-price mismatches must not block a new flat symbol.
        mode = api('futures_get_position_mode')
        if mode.get('dualSidePosition'):
            raise RuntimeError('One-way Position Mode required')
        api('futures_change_leverage',symbol=symbol,leverage=LEVERAGE)
        ticker = api('futures_symbol_ticker',symbol=symbol)
        price = float(ticker['price'])
        if price <= 0:
            raise RuntimeError('Invalid ticker price')
        desired = MARGIN * LEVERAGE / price
        jid, _ = place(symbol,signal,desired,False,'ENTRY',alert_id,0)
        try:
            p = confirmed_position(symbol, lambda x: x is not None and direction_of(x) == signal)
            save_position(symbol,signal,p)
            journal_update(jid,'confirmed',note='Entry position verified and saved')
            return 'entry_executed'
        except Exception as e:
            journal_update(jid,'uncertain',note=str(e)[:400])
            raise

def partial(symbol, alert_id, requested=None):
    with TRADE_LOCK:
        ensure_safe(symbol)
        b, state = owned_state(symbol)
        if not b or not state:
            raise RuntimeError('Position not bot-owned or missing state')
        p = positions(force=True).get(symbol)
        if not p:
            remove_position(symbol)
            return 'already_flat'
        if b['direction'] != direction_of(p):
            raise RuntimeError('Direction mismatch; manual review required')
        count = int(state['tp_count'] or 0)
        if count >= 5 or (requested is not None and requested <= count):
            return 'tp_already_completed'
        if requested is not None and requested != count + 1:
            raise RuntimeError('TP out of sequence')
        original = float(state['entry_qty'] or 0)
        current = abs(float(p['positionAmt']))
        if original <= 0 or current > original * 1.000001:
            raise RuntimeError('Original quantity inconsistent with Binance; manual reconciliation required')
        # Detect old manually closed/reduced quantities. Do not reset TP history.
        if count < 4 and current < original * (1 - 0.2 * count) - float(rules(symbol)[0]) * 2:
            raise RuntimeError('Position quantity below expected TP schedule; manual reconciliation required')
        next_tp = count + 1
        amount = current if next_tp == 5 else float(quantity(symbol,original * 0.20))
        if next_tp < 5 and amount >= current:
            raise RuntimeError('Partial quantity exceeds remaining position')
        side = 'SELL' if b['direction'] == 'BUY' else 'BUY'
        jid, _ = place(symbol,side,amount,True,f'TP{next_tp}',alert_id,current)
        try:
            after = confirmed_position(symbol,lambda x: (abs(float(x['positionAmt'])) if x else 0) < current - 1e-12)
            remain = abs(float(after['positionAmt'])) if after else 0
            if after and direction_of(after) != b['direction']:
                raise RuntimeError('Unexpected position direction after TP')
            # Check approximate actual quantity reduction; partial fills need review.
            step = float(rules(symbol)[0])
            if abs((current - remain) - float(quantity(symbol,amount))) > max(step * 2, float(amount)*0.01):
                raise RuntimeError('TP filled quantity differs from requested; manual review')
            if after:
                with DB_LOCK, db() as c:
                    c.execute('UPDATE position_state SET tp_count=?,partial_done=1 WHERE symbol=?',(next_tp,symbol))
            else:
                remove_position(symbol)
            journal_update(jid,'confirmed',note=f'TP{next_tp} verified; remaining={remain}')
            return f'tp_{next_tp}_executed' if after else 'position_fully_closed'
        except Exception as e:
            journal_update(jid,'uncertain',note=str(e)[:400])
            raise

def close(symbol, alert_id=None, reason='CLOSE'):
    with TRADE_LOCK:
        ensure_safe(symbol)
        b, state = owned_state(symbol)
        if not b or not state:
            raise RuntimeError('Position not bot-owned or missing state')
        p = positions(force=True).get(symbol)
        if not p:
            remove_position(symbol)
            return 'already_flat'
        if direction_of(p) != b['direction']:
            raise RuntimeError('Direction mismatch; manual review required')
        current = abs(float(p['positionAmt']))
        original = float(state['entry_qty'] or 0)
        if original <= 0 or current > original * 1.000001:
            raise RuntimeError('Close blocked: Binance quantity exceeds saved bot quantity; review ownership')
        side = 'SELL' if b['direction']=='BUY' else 'BUY'
        jid, _ = place(symbol,side,current,True,reason,alert_id,current)
        try:
            confirmed_position(symbol,lambda x: x is None)
            remove_position(symbol)
            journal_update(jid,'confirmed',note='Flat confirmed')
            return 'position_closed'
        except Exception as e:
            journal_update(jid,'uncertain',note=str(e)[:400])
            raise

def normalize(value):
    s = str(value).strip().upper().split(':')[-1]
    if s.endswith('.P'):
        s=s[:-2]
    s=s.replace('/','').replace('-','').replace('_','')
    if not re.fullmatch(r'[A-Z0-9]{5,30}',s):
        raise ValueError('Invalid symbol')
    return s

def parse(message):
    s = str(message or '').strip()
    m = re.fullmatch(r'(?:VWAP\|)?(BUY|SELL|TP[1-5]|CLOSE)\|([A-Za-z0-9:._/\-]+)',s,re.I)
    if m:
        return m.group(1).upper(),normalize(m.group(2))
    m = re.fullmatch(r'PARTIAL\|([A-Za-z0-9:._/\-]+)\|([0-9]+(?:\.[0-9]+)?)',s,re.I)
    if m and float(m.group(2)) == 20:
        return 'PARTIAL',normalize(m.group(1))
    raise ValueError('Unsupported TradingView message')

def extract():
    obj = request.get_json(silent=True)
    if isinstance(obj,dict):
        if obj.get('message'):
            raw=str(obj['message'])
            return (*parse(raw),raw)
        if obj.get('signal') and obj.get('symbol'):
            sig=str(obj['signal']).upper()
            if sig not in ('BUY','SELL','PARTIAL','CLOSE','TP1','TP2','TP3','TP4','TP5'):
                raise ValueError('Unsupported signal')
            if sig=='PARTIAL' and float(obj.get('percentage',20))!=20:
                raise ValueError('Only 20% partial supported')
            return sig,normalize(obj['symbol']),json.dumps(obj,sort_keys=True)
    raw=request.get_data(as_text=True)
    return (*parse(raw),raw)

@app.route('/webhook',methods=['POST'])
def webhook():
    if WEBHOOK_SECRET:
        supplied=request.args.get('secret','') or request.headers.get('X-Webhook-Secret','')
        if not hmac.compare_digest(supplied,WEBHOOK_SECRET):
            return jsonify(error='Unauthorized'),403
    try:
        sig,sym,raw=extract()
        now=time.time()
        with DB_LOCK,db() as c:
            # Explicit TP1..TP5 are idempotent by TP state; raw PARTIAL has no unique TP identity.
            # Suppress only rapid identical repeats, not distinct signals.
            if sig=='PARTIAL':
                row=c.execute('''SELECT id FROM alerts WHERE symbol=? AND signal='PARTIAL'
                                 AND raw_message=? AND received_at>=? LIMIT 1''',(sym,raw,now-DEDUP)).fetchone()
                if row:
                    return jsonify(status='duplicate_suppressed',symbol=sym)
            fingerprint=hashlib.sha256(f'{sig}|{sym}|{raw}|{time.time_ns()}'.encode()).hexdigest()
            c.execute('''INSERT INTO alerts(fingerprint,symbol,signal,raw_message,received_at,status,updated_at)
                         VALUES(?,?,?,?,?,'pending',?)''',(fingerprint,sym,sig,raw,now,now))
        log.info('WEBHOOK QUEUED %s %s',sig,sym)
        return jsonify(status='queued',symbol=sym,signal=sig)
    except (ValueError,TypeError) as e:
        return jsonify(error=str(e)),400
    except Exception:
        log.exception('Webhook failed')
        return jsonify(error='Webhook failure'),503

def claim():
    with DB_LOCK,db() as c:
        c.execute('BEGIN IMMEDIATE')
        row=c.execute("SELECT * FROM alerts WHERE status='pending' ORDER BY id LIMIT 1").fetchone()
        if row:
            c.execute("UPDATE alerts SET status='processing',updated_at=? WHERE id=?",(time.time(),row['id']))
        c.commit()
        return dict(row) if row else None

def finish(alert_id,status,error=None):
    with DB_LOCK,db() as c:
        c.execute('UPDATE alerts SET status=?,error=?,updated_at=? WHERE id=?',
                  (status,error,time.time(),alert_id))

def dispatch(a):
    sig,sym=a['signal'],a['symbol']
    if sig in ('BUY','SELL'):
        return entry(sig,sym,a['id'])
    if sig=='PARTIAL':
        return partial(sym,a['id'])
    if re.fullmatch(r'TP[1-5]',sig):
        return partial(sym,a['id'],int(sig[2:]))
    if sig=='CLOSE':
        return close(sym,a['id'])
    raise ValueError('Unknown signal')

def queue_worker():
    log.info('QUEUE STARTED live=%s testnet=%s',LIVE,TESTNET)
    while True:
        try:
            a=claim()
            if not a:
                time.sleep(.5)
                continue
            if time.time()-a['received_at']>MAX_AGE:
                finish(a['id'],'expired')
                continue
            if not LIVE:
                finish(a['id'],'dry_run')
                continue
            try:
                result=dispatch(a)
                finish(a['id'],result)
                log.info('ALERT COMPLETED %s %s %s',a['signal'],a['symbol'],result)
            except Exception as e:
                log.exception('ALERT FAILED %s %s',a['signal'],a['symbol'])
                finish(a['id'],'needs_review',str(e)[:500])
        except Exception:
            log.exception('Queue worker error')
            time.sleep(2)

def roi(p):
    entry_price=float(p['entryPrice'])
    mark=float(p['markPrice'])
    if entry_price<=0 or mark<=0:
        raise RuntimeError('Invalid Binance mark or entry price')
    sign=1 if float(p['positionAmt'])>0 else -1
    return (mark-entry_price)/entry_price*sign*float(p.get('leverage') or 1)*100

def monitor():
    if not LIVE:
        return
    # Exactly one account position snapshot per monitoring cycle; no per-coin calls.
    with TRADE_LOCK:
        snapshot=positions(force=True)
        with DB_LOCK,db() as c:
            tracked=[dict(r) for r in c.execute('''SELECT b.symbol,b.direction,s.entry_price,s.entry_qty,
                s.tp_count,s.breakeven_done FROM bot_positions b
                LEFT JOIN position_state s ON b.symbol=s.symbol''')]
        for row in tracked:
            sym=row['symbol']
            try:
                p=snapshot.get(sym)
                if p is None:
                    # Do not delete while an uncertain order is outstanding.
                    if not unresolved(sym):
                        remove_position(sym)
                    continue
                if row['entry_qty'] is None or direction_of(p)!=row['direction']:
                    log.error('RISK SKIPPED %s: missing state or direction mismatch',sym)
                    continue
                if unresolved(sym):
                    log.error('RISK SKIPPED %s: uncertain order needs review',sym)
                    continue
                result=roi(p)
                if INITIAL_SL>0 and result<=-INITIAL_SL:
                    log.warning('SOFTWARE STOP %s ROI=%.2f',sym,result)
                    close(sym,reason='SOFTWARE_SL')
                    continue
                armed=bool(row['breakeven_done'])
                if not armed and BE_TRIGGER>0 and result>=BE_TRIGGER:
                    with DB_LOCK,db() as c:
                        c.execute('UPDATE position_state SET breakeven_done=1 WHERE symbol=?',(sym,))
                    armed=True
                    log.warning('BREAKEVEN ARMED %s ROI=%.2f',sym,result)
                if armed and result<=BE_STOP:
                    log.warning('BREAKEVEN EXIT %s ROI=%.2f',sym,result)
                    close(sym,reason='BREAKEVEN')
            except Exception:
                log.exception('RISK ERROR %s',sym)

def risk_worker():
    log.info('RISK MONITOR STARTED interval=%ss initialSL=%s',POLL,INITIAL_SL or 'DISABLED')
    while True:
        start=time.monotonic()
        try:
            monitor()
        except Exception:
            log.exception('Risk monitor cycle failed')
        time.sleep(max(1,POLL-(time.monotonic()-start)))

@app.route('/',methods=['GET'])
def health():
    return jsonify(status='ok',bot='MAQ Futures Reconciled',live=LIVE,testnet=TESTNET,
                   poll_seconds=POLL,initial_stop_loss='disabled' if INITIAL_SL==0 else INITIAL_SL)

@app.route('/status',methods=['GET'])
def status():
    with DB_LOCK,db() as c:
        counts={r['status']:r['n'] for r in c.execute('SELECT status,COUNT(*) n FROM alerts GROUP BY status')}
        tracked=[dict(r) for r in c.execute('''SELECT b.symbol,b.direction,s.entry_price,s.entry_qty,s.tp_count,
                      s.breakeven_done FROM bot_positions b LEFT JOIN position_state s ON b.symbol=s.symbol''')]
        uncertain=[dict(r) for r in c.execute('''SELECT id,symbol,action,status,note,created_at FROM trade_journal
                       WHERE status IN ('uncertain','submitting','submitted') ORDER BY id DESC LIMIT 30''')]
    return jsonify(live=LIVE,testnet=TESTNET,poll_seconds=POLL,initial_stop_loss_roi=INITIAL_SL,
                   breakeven_trigger_roi=BE_TRIGGER,breakeven_stop_roi=BE_STOP,
                   api_cooldown_remaining=max(0,int(cooldown_until-time.time())),
                   global_mismatch_block_enabled=False,alert_status=counts,
                   positions=tracked,uncertain_orders=uncertain,stop_type='software')

# Exactly one process / one Railway replica. Gunicorn: --workers 1 --threads 4
threading.Thread(target=queue_worker,daemon=True,name='MAQ-Queue').start()
threading.Thread(target=risk_worker,daemon=True,name='MAQ-Risk').start()

if __name__=='__main__':
    app.run(host='0.0.0.0',port=int(setting('PORT',8080)),threaded=True)
