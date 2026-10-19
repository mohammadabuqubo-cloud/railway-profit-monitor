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
