"""Local historical simulation. All fills use the exact TX tick selected by replay.
No broker connection. Other products are hypothetical TX-price scenarios.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from uuid import UUID
from dashboard import replay_service as replay

D = Decimal
PRODUCTS = {'TX': 200, 'MTX': 50, 'TMF': 10}
TAX = D('0.00002')
DEFAULT_FEES = {'TX': D(50), 'MTX': D(30), 'TMF': D(15)}


def dec(value):
    return D(str(value))


def money(value):
    return dec(value).quantize(D('0.01'), rounding=ROUND_HALF_UP)


def costs(entry, exit, direction, qty, fees, tax_rate=TAX):
    """Round estimated tax per contract/side to NTD; fees are per contract/side."""
    entry, exit = dec(entry), dec(exit)
    points = (exit-entry)*(1 if direction == 'LONG' else -1)
    result = {}
    for product, multiplier in PRODUCTS.items():
        fee = money(dec(fees[product])*2*qty)
        tax = sum((p*multiplier*dec(tax_rate)).quantize(D('1'), rounding=ROUND_HALF_UP) for p in (entry,exit))*qty
        gross = money(points*multiplier*qty)
        result[product] = dict(points=points, gross=gross, fee=fee, tax=tax, net=gross-fee-tax)
    return result


def slipped(price, direction, slippage, closing=False):
    sign = (1 if direction=='LONG' else -1)*(-1 if closing else 1)
    return dec(price)+sign*slippage


def triggered(kind, direction, target, price):
    price, target = dec(price), dec(target)
    if kind == 'LIMIT': return price<=target if direction=='LONG' else price>=target
    if kind == 'STOP': return price>=target if direction=='LONG' else price<=target
    return True


def exit_trigger(order, price):
    p=dec(price); long=order['direction']=='LONG'
    stop, take=order['stop_loss'],order['take_profit']
    if stop is not None and (p<=stop if long else p>=stop): return 'STOP_LOSS'
    if take is not None and (p>=take if long else p<=take): return 'TAKE_PROFIT'
    return None


def bracket_valid(direction, reference, stop, take):
    ref=dec(reference); long=direction=='LONG'
    if stop is not None and not (dec(stop)<ref if long else dec(stop)>ref): raise ValueError('停損須在預計進場價的虧損方向')
    if take is not None and not (dec(take)>ref if long else dec(take)<ref): raise ValueError('停利須在預計進場價的獲利方向')


def connection():
    from common import db
    return db.get_connection()


def cursor(conn):
    from psycopg.rows import dict_row
    return conn.cursor(row_factory=dict_row)


def one(cur, sql, args=()):
    cur.execute(sql,args); return cur.fetchone()


def many(cur, sql, args=()):
    cur.execute(sql,args); return cur.fetchall()


def require(row, label):
    if not row: raise ValueError(label+'不存在')
    return row


def client_time(value):
    try: t=datetime.fromisoformat(value.replace('Z','+00:00'))
    except (ValueError,AttributeError): raise ValueError('無效的電腦時間')
    if t.tzinfo is None: raise ValueError('電腦時間必須包含時區')
    return t


def account_values(data):
    name=data['name'].strip()
    if not 1<=len(name)<=80: raise ValueError('帳號名稱須為 1–80 字')
    return (name,data['initial_equity'],data['fee_tx'],data['fee_mtx'],data['fee_tmf'],data['slippage'])


def accounts():
    with connection() as conn, cursor(conn) as cur:
        return many(cur,'SELECT * FROM paper_trade.accounts ORDER BY id')


def save_account(data, account_id=None):
    args=account_values(data)
    with connection() as conn, cursor(conn) as cur:
        if account_id is None:
            row=one(cur,'''INSERT INTO paper_trade.accounts(name,initial_equity,fee_tx,fee_mtx,fee_tmf,slippage)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(name) DO NOTHING RETURNING *''',args)
        else:
            row=one(cur,'''UPDATE paper_trade.accounts SET name=%s,initial_equity=%s,fee_tx=%s,fee_mtx=%s,fee_tmf=%s,slippage=%s
                WHERE id=%s RETURNING *''',(*args,account_id))
        if not row: raise ValueError('帳號已存在或不存在')
        conn.commit(); return row


def run_list(account_id, market=None, contract=None, start=None, end=None):
    with connection() as conn, cursor(conn) as cur:
        if market is None: return many(cur,'SELECT * FROM paper_trade.runs WHERE account_id=%s ORDER BY id DESC',(account_id,))
        a,b=replay.parse_time(start),replay.parse_time(end)
        return many(cur,'''SELECT * FROM paper_trade.runs WHERE account_id=%s AND market=%s AND contract=%s AND start_at=%s AND end_at=%s ORDER BY id DESC''',(account_id,market,contract,a,b))


def create_run(data):
    market,contract=data['market'],data['contract']; a,b=replay.parse_time(data['start']),replay.parse_time(data['end'])
    if market not in ('tx_day','tx_night'): raise ValueError('只支援 TX 逐筆回放')
    replay.validate(market,'TAIEX',contract,5); replay.validate_range(market,a,b)
    with connection() as conn, cursor(conn) as cur:
        require(one(cur,'SELECT id FROM paper_trade.accounts WHERE id=%s',(data['account_id'],)),'帳號')
        run=one(cur,'''INSERT INTO paper_trade.runs(account_id,market,contract,start_at,end_at) VALUES(%s,%s,%s,%s,%s) RETURNING *''',(data['account_id'],market,contract,a,b))
        conn.commit(); return run


def tick_key(t): return (t['trade_datetime'],t['source_row_no'],t['id'])

def run_key(r): return None if r['cursor_id'] is None else (r['cursor_time'],r['cursor_row'],r['cursor_id'])


def get_tick(cur, run, tick_id):
    # Exact row, not the last tick of a second. IDs are strings in JSON to avoid JS precision loss.
    try: tid=int(tick_id)
    except (ValueError,TypeError): raise ValueError('沒有可成交的逐筆行情')
    t=one(cur,'''SELECT id,trade_datetime,source_row_no,price FROM raw_finmind.fm_taiwan_futures_tick
        WHERE id=%s AND futures_id='TX' AND contract_date=%s AND trade_datetime >= %s AND trade_datetime <= %s''',
        (tid,run['contract'],run['start_at'],run['end_at']))
    if not t: raise ValueError('此回放區間的成交資料不存在（原始資料可能已重新匯入，請建立新場次）')
    if not dec(t['price']).is_finite() or dec(t['price'])<=0: raise ValueError('原始成交價無效')
    return t


def lock_run(cur, run_id): return require(one(cur,'SELECT * FROM paper_trade.runs WHERE id=%s FOR UPDATE',(run_id,)),'回放交易場次')


def fill(cur, order, tick, wall, limit=False):
    price=dec(tick['price']) if limit else slipped(tick['price'],order['direction'],order['slippage'])
    if price<=0: raise ValueError('模擬成交價格無效')
    cur.execute('''INSERT INTO paper_trade.trades(order_id,entry_time,entry_tick_id,entry_price,entry_raw_price,entry_client_at)
        VALUES(%s,%s,%s,%s,%s,%s)''',(order['id'],tick['trade_datetime'],tick['id'],price,dec(tick['price']),wall))
    cur.execute("UPDATE paper_trade.orders SET status='FILLED' WHERE id=%s",(order['id'],))


def close_trade(cur, trade, tick, wall, reason):
    price=slipped(tick['price'],trade['direction'],trade['slippage'],True)
    if price<=0: raise ValueError('模擬平倉價格無效')
    cur.execute('''UPDATE paper_trade.trades SET exit_time=%s,exit_tick_id=%s,exit_price=%s,exit_raw_price=%s,
        exit_client_at=%s,exit_server_at=now(),exit_reason=%s WHERE id=%s AND exit_time IS NULL''',
        (tick['trade_datetime'],tick['id'],price,dec(tick['price']),wall,reason,trade['trade_id']))


TRADE_SELECT='''SELECT t.*,t.id AS trade_id,o.run_id,o.product,o.direction,o.quantity,o.stop_loss,o.take_profit,
 o.fee_tx,o.fee_mtx,o.fee_tmf,o.slippage,o.tax_rate,o.order_type,o.target,o.submitted_at,o.submitted_clock_at,o.submitted_client_at,o.submitted_server_at,
 r.account_id,r.market,r.contract,r.start_at,r.end_at FROM paper_trade.trades t JOIN paper_trade.orders o ON o.id=t.order_id JOIN paper_trade.runs r ON r.id=o.run_id'''


def advance(cur, run, tick, wall):
    old=run_key(run); new=tick_key(tick)
    if old is not None and new<old: raise ValueError('目前已倒回已交易時間之前。請回到原進度，或建立新場次重新練習。')
    if old==new: return
    if old is not None:
        # A source-date reimport can replace tick IDs; do not silently resume a changed tape.
        prior=get_tick(cur,run,old[2])
        if tick_key(prior)!=old: raise ValueError('原始行情已變更，請建立新場次。')
    pending=many(cur,"SELECT * FROM paper_trade.orders WHERE run_id=%s AND status='PENDING' ORDER BY id",(run['id'],))
    positions=many(cur,TRADE_SELECT+' WHERE o.run_id=%s AND t.exit_time IS NULL ORDER BY t.id',(run['id'],))
    protective=[p for p in positions if p['stop_loss'] is not None or p['take_profit'] is not None]
    if old is not None and (pending or protective):
        # Process every intermediate trade, even if the chart skipped frames at 300x.
        # Server-side cursor is monotonic and serialized by row lock.
        rows=many(cur,'''SELECT id,trade_datetime,source_row_no,price FROM raw_finmind.fm_taiwan_futures_tick
           WHERE futures_id='TX' AND contract_date=%s AND (trade_datetime,source_row_no,id)>(%s,%s,%s)
           AND (trade_datetime,source_row_no,id)<=(%s,%s,%s) ORDER BY trade_datetime,source_row_no,id''',
           (run['contract'],*old,*new))
        for t in rows:
            if not pending and not protective: break
            for p in protective[:]:
                why=exit_trigger(p,t['price'])
                if why:
                    close_trade(cur,p,t,wall,why); protective.remove(p)
            for o in pending[:]:
                if triggered(o['order_type'],o['direction'],o['target'],t['price']):
                    fill(cur,o,t,wall,o['order_type']=='LIMIT'); pending.remove(o)
                    if o['stop_loss'] is not None or o['take_profit'] is not None:
                        p=one(cur,TRADE_SELECT+' WHERE t.order_id=%s',(o['id'],)); protective.append(p)
                        # A gap may already be beyond a protective level at entry; trigger on the next tick, not before entry.
    cur.execute('UPDATE paper_trade.runs SET cursor_time=%s,cursor_row=%s,cursor_id=%s WHERE id=%s',(*new,run['id']))
    run.update(cursor_time=new[0],cursor_row=new[1],cursor_id=new[2])


def fee_map(row): return {p:row['fee_'+p.lower()] for p in PRODUCTS}


def enrich(row, mark=None):
    row=dict(row); price=row['exit_price'] if row['exit_time'] is not None else (slipped(mark,row['direction'],row['slippage'],True) if mark is not None else None)
    if price is not None:
        row['comparison']=costs(row['entry_price'],price,row['direction'],row['quantity'],fee_map(row),row['tax_rate'])
        row['actual']=row['comparison'][row['product']]
    else: row['comparison']=None; row['actual']=None
    row['tick_points']=None if price is None else (dec(price)-row['entry_price'])*(1 if row['direction']=='LONG' else -1)
    return row


def snapshot(cur, run, mark=None):
    trades=many(cur,TRADE_SELECT+' WHERE o.run_id=%s ORDER BY t.id DESC',(run['id'],))
    orders=many(cur,'SELECT * FROM paper_trade.orders WHERE run_id=%s ORDER BY id DESC',(run['id'],))
    # Never expose a later trade's details in an earlier cursor. Caller disallows rewound snapshots.
    return {'run':{**run,'cursor_id':str(run['cursor_id']) if run['cursor_id'] is not None else None},'mark':mark,'trades':[enrich(t,mark) for t in trades], 'orders':orders}


def execute_action(run_id, data):
    wall=client_time(data['client_at']); action=data['action']
    with connection() as conn, cursor(conn) as cur:
        run=lock_run(cur,run_id); tick=get_tick(cur,run,data['tick_id'])
        try: clock=datetime.fromtimestamp(data['clock_ms']/1000,replay.TZ).replace(tzinfo=None)
        except (ValueError,OverflowError,OSError,TypeError): raise ValueError('回放時鐘無效')
        if not run['start_at']<=clock<=run['end_at'] or clock+timedelta(microseconds=1)<tick['trade_datetime']: raise ValueError('回放時鐘與成交時間不一致')
        clock=max(clock,tick['trade_datetime'])
        # A repeat of the same request must not create another order.
        if action=='order':
            request_id=UUID(data['request_id'])
            existing=one(cur,'SELECT id,run_id FROM paper_trade.orders WHERE request_id=%s',(request_id,))
            if existing and existing['run_id']!=run_id: raise ValueError('委託識別碼已被使用')
        advance(cur,run,tick,wall)
        if action=='order' and not existing:
            a=require(one(cur,'SELECT * FROM paper_trade.accounts WHERE id=%s',(run['account_id'],)),'帳號')
            kind=data['order_type']; direction=data['direction']; target=data.get('target'); stop=data.get('stop_loss'); take=data.get('take_profit')
            ref=slipped(tick['price'],direction,a['slippage']) if kind=='MARKET' else target
            if kind!='MARKET' and target is None: raise ValueError('請輸入委託點位')
            bracket_valid(direction,ref,stop,take)
            if kind=='MARKET': target=None
            order=one(cur,'''INSERT INTO paper_trade.orders(run_id,request_id,product,direction,quantity,order_type,target,stop_loss,take_profit,status,
              submitted_at,submitted_tick_id,submitted_clock_at,submitted_client_at,fee_tx,fee_mtx,fee_tmf,slippage,tax_rate)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
              (run_id,request_id,data['product'],direction,data['quantity'],kind,target,stop,take,tick['trade_datetime'],tick['id'],clock,wall,a['fee_tx'],a['fee_mtx'],a['fee_tmf'],a['slippage'],TAX))
            # Market: immediate known tick. Marketable limit/stop: immediate known tick, no queue model.
            if kind=='MARKET' or triggered(kind,direction,target,tick['price']): fill(cur,order,tick,wall,kind=='LIMIT')
        elif action=='cancel':
            cur.execute("UPDATE paper_trade.orders SET status='CANCELLED',cancelled_at=%s,cancelled_client_at=%s WHERE id=%s AND run_id=%s AND status='PENDING'",(clock,wall,data['item_id'],run_id))
            if not cur.rowcount: raise ValueError('委託已成交、已取消或不存在')
        elif action in ('close','close_all'):
            args=(run_id,) if action=='close_all' else (run_id,data['item_id'])
            trades=many(cur,TRADE_SELECT+' WHERE o.run_id=%s AND t.exit_time IS NULL'+('' if action=='close_all' else ' AND t.id=%s'),args)
            if not trades: raise ValueError('沒有可平倉部位')
            for p in trades: close_trade(cur,p,tick,wall,'MANUAL')
        result=snapshot(cur,run,dec(tick['price'])); conn.commit(); return result


def note_trade(trade_id, note):
    with connection() as conn,cursor(conn) as cur:
        row=one(cur,'UPDATE paper_trade.trades SET note=%s,note_updated_at=now() WHERE id=%s RETURNING id,note,note_updated_at',(note,trade_id))
        conn.commit(); return require(row,'交易')


def delete_trade(trade_id):
    with connection() as conn,cursor(conn) as cur:
        row=require(one(cur,TRADE_SELECT+' WHERE t.id=%s',(trade_id,)),'交易')
        lock_run(cur,row['run_id'])
        # Delete entire record including original filled order. No point editing.
        cur.execute('DELETE FROM paper_trade.orders WHERE id=%s',(row['order_id'],)); conn.commit(); return {'deleted':trade_id}


def statistics(trades, equity, product=None):
    seq=sorted((t for t in trades if t['exit_time'] is not None),key=lambda t:(t['exit_server_at'],t['id']))
    vals=[t['comparison'][product] if product else t['actual'] for t in seq]
    nets=[v['net'] for v in vals]; wins=[x for x in nets if x>0]; losses=[x for x in nets if x<0]
    total=sum(nets,D(0)); running=dec(equity); peak=running; dd=D(0); curve=[]
    for t,n in zip(seq,nets):
        running+=n; peak=max(peak,running); dd=max(dd,peak-running)
        curve.append({'trade_id':t['id'],'time':t['exit_server_at'],'historical_time':t['exit_time'],'equity':running})
    return {'count':len(vals),'wins':len(wins),'losses':len(losses),'breakeven':len(vals)-len(wins)-len(losses),
        'win_rate':len(wins)/len(vals)*100 if vals else None,'gross':sum((v['gross'] for v in vals),D(0)),
        'fee':sum((v['fee'] for v in vals),D(0)),'tax':sum((v['tax'] for v in vals),D(0)), 'net':total,
        'avg':total/len(vals) if vals else None,'profit_factor':sum(wins)/abs(sum(losses)) if losses else None,
        'max_drawdown':dd,'equity':running,'return_pct':total/dec(equity)*100,'curve':curve}


def performance(account_id, run_id=None):
    with connection() as conn,cursor(conn) as cur:
        a=require(one(cur,'SELECT * FROM paper_trade.accounts WHERE id=%s',(account_id,)),'帳號')
        sql=TRADE_SELECT+' WHERE r.account_id=%s'; args=(account_id,)
        if run_id is not None: sql+=' AND r.id=%s'; args+=(run_id,)
        rows=many(cur,sql+' ORDER BY t.id DESC',args)
        trades=[enrich(t) for t in rows]; equity=a['initial_equity']
        return {'account':a,'trades':trades,'actual':statistics(trades,equity),
                'comparison':{p:statistics(trades,equity,p) for p in PRODUCTS},'open_count':sum(t['exit_time'] is None for t in trades)}
