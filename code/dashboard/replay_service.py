"""Historical replay, read-only. All input timestamps are Asia/Taipei wall time."""
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import re

TZ = ZoneInfo('Asia/Taipei')
PERIODS = (1, 5, 15, 30, 60)
MARKETS = ('tx_day', 'tx_night', 'stock')
MAX_CHUNK = 250000


def parse_time(value):
    try:
        t = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError('請輸入有效的台北日期時間') from exc
    if t.tzinfo is not None:
        raise ValueError('請使用台北本地時間，不含時區')
    return t


def ms(t):
    return t.replace(tzinfo=TZ).timestamp() * 1000


def validate(market, symbol, contract, period):
    if market not in MARKETS or period not in PERIODS:
        raise ValueError('不支援的市場或週期')
    if market == 'stock':
        if not re.fullmatch(r'[A-Za-z0-9]{1,16}', symbol):
            raise ValueError('請輸入有效股票代號或 TAIEX')
    elif not re.fullmatch(r'\d{6}', contract or ''):
        raise ValueError('請指定實際期貨契約月份（YYYYMM）')


def session_bounds(market, anchor):
    day = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    if market == 'tx_night':
        if anchor.hour < 6:
            day -= timedelta(days=1)
        return day + timedelta(hours=15), day + timedelta(days=1, hours=5, minutes=1)
    if market == 'tx_day':
        return day + timedelta(hours=8, minutes=45), day + timedelta(hours=13, minutes=46)
    return day + timedelta(hours=9), day + timedelta(hours=13, minutes=31)


def bucket(t, market, period):
    origin, _ = session_bounds(market, t)
    return origin + timedelta(minutes=(int((t-origin).total_seconds()//60)//period)*period)


def validate_range(market, start, end):
    left, right = session_bounds(market, start)
    if not left <= start < end <= right:
        raise ValueError('起訖時間必須位於同一交易時段；夜盤請使用實際日曆日期（可跨午夜）')


def _query(sql, args):
    from common import db
    return db.query_to_df(sql, args)


def contracts(market, start, end):
    if market not in ('tx_day', 'tx_night'):
        raise ValueError('僅台指期需要選擇契約')
    a, b = parse_time(start), parse_time(end)
    validate_range(market, a, b)
    # Only list months, never disclose future prices or volume-based rankings.
    df = _query('''SELECT DISTINCT contract_date FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id='TX' AND source_date BETWEEN ? AND ?
          AND trade_datetime >= ? AND trade_datetime < ? AND contract_date ~ '^[0-9]{6}$'
        ORDER BY contract_date''', (a.date()-timedelta(days=1), b.date()+timedelta(days=1), a, b))
    return {'contracts': df['contract_date'].tolist()}


def aggregate_history(rows, market, period):
    result = []
    for t, o, h, l, c, v in rows:
        k = int(ms(bucket(t, market, period)) / 1000)
        if not result or result[-1]['time'] != k:
            result.append(dict(time=k, open=float(o), high=float(h), low=float(l), close=float(c), volume=float(v)))
        else:
            bar = result[-1]
            bar['high'] = max(bar['high'], float(h)); bar['low'] = min(bar['low'], float(l))
            bar['close'] = float(c); bar['volume'] += float(v)
    return result


def initialize(market, symbol, contract, period, start, end):
    validate(market, symbol, contract, period)
    a, b = parse_time(start), parse_time(end)
    validate_range(market, a, b)
    first = bucket(a, market, period)
    # Use completed minutes strictly before the current bucket. A partial bucket
    # is reconstructed from events, never from its final historical OHLC.
    earliest = first - timedelta(days=90)
    if market == 'stock':
        df = _query('''SELECT date+minute, open, high, low, close, volume
            FROM raw_finmind.fm_taiwan_stock_kbar_1m
            WHERE stock_id=? AND date BETWEEN ? AND ? AND date+minute < ?
            AND minute >= TIME '09:00' AND minute <= TIME '13:30'
            ORDER BY date, minute''', (symbol, earliest.date(), first.date(), first))
    else:
        session = 'position' if market == 'tx_day' else 'after_market'
        df = _query('''SELECT bar_datetime, open, high, low, close, volume
            FROM raw_finmind.fm_taiwan_futures_kbar_1m
            WHERE futures_id='TX' AND contract_date=? AND trading_session=?
            AND bar_datetime >= ? AND bar_datetime < ? ORDER BY bar_datetime''',
            (contract, session, earliest, first))
    history = aggregate_history(df.itertuples(index=False, name=None), market, period)[-6000:]
    return {'history': history, 'event_start': first.isoformat(), 'start_ms': ms(a), 'end_ms': ms(b),
            'resolution': 'minute' if market == 'stock' else 'tick',
            'volume_available': not (market == 'stock' and symbol == 'TAIEX')}


def events(market, symbol, contract, period, start, end, final=False):
    validate(market, symbol, contract, period)
    a, b = parse_time(start), parse_time(end)
    validate_range(market, a, b)
    if b-a > timedelta(minutes=15):
        raise ValueError('單次資料請求不可超過 15 分鐘')
    if market == 'stock':
        # Conservative availability convention: stored minute is bar start;
        # expose its OHLC only one minute later, never invent an intraminute path.
        x, y = a-timedelta(minutes=1), b-timedelta(minutes=1)
        if final: y += timedelta(microseconds=1)
        df = _query('''SELECT date+minute, open, high, low, close, volume
            FROM raw_finmind.fm_taiwan_stock_kbar_1m
            WHERE stock_id=? AND date BETWEEN ? AND ? AND date+minute >= ? AND date+minute < ?
            AND minute >= TIME '09:00' AND minute <= TIME '13:30'
            ORDER BY date, minute''', (symbol, x.date(), y.date(), x, y))
        data = [[ms(t+timedelta(minutes=1)), int(ms(bucket(t, market, period))/1000),
                 float(o),float(h),float(l),float(c),float(v)] for t,o,h,l,c,v in df.itertuples(index=False,name=None)]
    else:
        upper = b + timedelta(microseconds=1) if final else b
        df = _query('''SELECT trade_datetime, price, volume, id
            FROM raw_finmind.fm_taiwan_futures_tick
            WHERE futures_id='TX' AND contract_date=? AND trade_datetime >= ? AND trade_datetime < ?
            ORDER BY trade_datetime, source_row_no, id LIMIT ?''', (contract,a,upper,MAX_CHUNK+1))
        if len(df)>MAX_CHUNK:
            raise ValueError('逐筆資料過多，請縮短查詢區間')
        data = [[ms(t),int(ms(bucket(t,market,period))/1000),float(p),float(p),float(p),float(p),float(v)/2,str(tid)]
                for t,p,v,tid in df.itertuples(index=False,name=None)]
    return {'events':data}
