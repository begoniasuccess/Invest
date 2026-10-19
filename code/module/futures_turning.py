"""TX five-minute session turning levels. No candles after the session are used."""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import math

COLUMNS = ('日盤轉折支撐', '日盤轉折壓力', '夜盤轉折支撐', '夜盤轉折壓力')


def calculate_bars(rows, seed=None):
    """SMA-seeded EMA12/26, continuous across sessions, with resumable state."""
    state = dict(seed or {'n': 0, 'fast_sum': 0., 'slow_sum': 0.,
                          'fast': None, 'slow': None, 'dif': None})
    result = []
    for source in rows:
        row = dict(source)
        close = float(row['close'])
        state['n'] += 1
        n = state['n']
        if n <= 12: state['fast_sum'] += close
        if n <= 26: state['slow_sum'] += close
        if n == 12: state['fast'] = state['fast_sum'] / 12
        elif n > 12: state['fast'] += (close - state['fast']) * 2 / 13
        if n == 26: state['slow'] = state['slow_sum'] / 26
        elif n > 26: state['slow'] += (close - state['slow']) * 2 / 27
        previous = state['dif']
        dif = None if state['slow'] is None else state['fast'] - state['slow']
        direction = None if dif is None or previous is None else (1 if dif > previous else -1 if dif < previous else 0)
        state['dif'] = dif
        row.update(dif=dif, direction=direction, ema_state=dict(state))
        result.append(row)
    return result


def find_turning_match(bars, support=True, previous_direction=None, previous_close=None):
    """Return provenance using the same ranking/search used for report values."""
    key = 'low' if support else 'high'
    wanted = 1 if support else -1
    candidates = sorted(range(len(bars)), key=lambda i: (float(bars[i][key]) * (1 if support else -1), bars[i]['time']))
    # Only the session's first candle at its absolute extreme may bypass reversal.
    # The previous candle is historical; no candle after this session is consulted.
    if candidates and candidates[0] == 0 and previous_close is not None:
        first = bars[0]
        if (first['direction'] == wanted and previous_direction == wanted
                and (float(first['close']) - float(previous_close)) * wanted > 0):
            return dict(value=float(first['close']), candidate_index=0,
                        selected_index=0, candidate_rank=1,
                        rule='優先例外：首根極值K、前後DIF同向、收盤同向')
    for rank, index in enumerate(candidates, 1):
        direction = bars[index]['direction']
        selected = None
        rule = ''
        if direction == -wanted:
            selected = next((i for i in range(index + 1, len(bars)) if bars[i]['direction'] == wanted), None)
            rule = '規則1：右側第一根DIF反向K'
        elif direction == wanted:
            left = bars[index - 1]['direction'] if index else previous_direction
            if left == -wanted:
                selected = index
                rule = '規則2：左側立即前一根DIF反向'
        if selected is not None:
            return dict(value=float(bars[selected]['close']), candidate_index=index,
                        selected_index=selected, candidate_rank=rank, rule=rule)
    return None


def find_turning(bars, support=True, previous_direction=None, previous_close=None):
    match = find_turning_match(bars, support, previous_direction, previous_close)
    return match['value'] if match else None


def session_results(bars, now=None, previous_direction=None, previous_close=None):
    now = now or datetime.now(ZoneInfo('Asia/Taipei')).replace(tzinfo=None)
    groups = {}
    previous = previous_direction
    prior_close = previous_close
    for bar in bars:
        key = (bar['session_date'], bar['session'])
        if key not in groups: groups[key] = ([], previous, prior_close)
        groups[key][0].append(bar)
        previous = bar['direction']
        prior_close = bar['close']
    results = []
    for (date, session), (items, left, left_close) in groups.items():
        start = datetime.combine(date, datetime.min.time()) + timedelta(hours=8, minutes=45) if session == 'position' else datetime.combine(date, datetime.min.time()) + timedelta(hours=15)
        end = start + (timedelta(hours=5) if session == 'position' else timedelta(hours=14))
        # Incomplete/in-progress sessions cannot represent the whole interval.
        complete = now >= end and items[0]['time'] <= start + timedelta(minutes=5) and items[-1]['last_time'] >= end - timedelta(minutes=5)
        results.append((date, session, find_turning(items, True, left, left_close) if complete else None,
                        find_turning(items, False, left, left_close) if complete else None))
    return results


DDL = '''
CREATE TABLE IF NOT EXISTS report.futures_turning_5m_bars (
 bar_time timestamp PRIMARY KEY, session_date date NOT NULL, session text NOT NULL,
 high numeric NOT NULL, low numeric NOT NULL, close numeric NOT NULL,
 last_time timestamp NOT NULL, dif double precision, direction smallint, ema_state jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS report.futures_turning_levels (
 futures_id text NOT NULL DEFAULT 'TX', "日期" date NOT NULL,
 "日盤轉折支撐" numeric, "日盤轉折壓力" numeric,
 "夜盤轉折支撐" numeric, "夜盤轉折壓力" numeric,
 PRIMARY KEY (futures_id, "日期")
);
'''


def ensure_turning_views():
    """Wrap the public report, keeping old dependent views/cache layouts intact."""
    from common import db
    if not db.execute_sql(DDL): raise RuntimeError('建立轉折結果資料表失敗')
    # Renaming preserves existing dependent objects; the new public name exposes
    # columns at the required ordinal positions without DROP ... CASCADE.
    statement = '''
DO $turning$
DECLARE fields text;
DECLARE additions text :=
 'CASE WHEN b."日盤高" IS NULL OR b."日盤低" IS NULL OR b."日盤收" IS NULL THEN NULL '
 'WHEN abs(b."日盤高"-b."日盤收") < abs(b."日盤收"-b."日盤低") THEN ''漲勢盤'' '
 'ELSE ''跌勢盤'' END AS "日盤走勢", '
 't."日盤轉折支撐", t."日盤轉折壓力", '
 'CASE WHEN b."夜盤高" IS NULL OR b."夜盤低" IS NULL OR b."夜盤收" IS NULL THEN NULL '
 'WHEN abs(b."夜盤高"-b."夜盤收") < abs(b."夜盤收"-b."夜盤低") THEN ''漲勢盤'' '
 'ELSE ''跌勢盤'' END AS "夜盤走勢", '
 't."夜盤轉折支撐", t."夜盤轉折壓力"';
BEGIN
 PERFORM pg_advisory_xact_lock(26100451);
 IF to_regclass('report.v_futures_report_daily_without_turning') IS NULL THEN
   ALTER VIEW report.v_futures_report_daily RENAME TO v_futures_report_daily_without_turning;
 END IF;
 -- PostgreSQL cannot insert columns in the middle with CREATE OR REPLACE VIEW.
 -- Rename the old wrappers instead of dropping them: database objects depending
 -- on their OIDs remain valid, while the public names can expose the new order.
 IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='report' AND c.relname='v_futures_report_daily')
    AND NOT EXISTS (SELECT 1 FROM pg_attribute
                    WHERE attrelid=to_regclass('report.v_futures_report_daily')
                      AND attname='日盤走勢' AND attnum>0 AND NOT attisdropped) THEN
   IF to_regclass('report.v_futures_report_daily_without_session_trend') IS NULL THEN
     ALTER VIEW report.v_futures_report_daily RENAME TO v_futures_report_daily_without_session_trend;
   ELSE
     RAISE EXCEPTION '舊版相容 View 已存在，但公開 View 尚無日盤走勢，請檢查資料庫遷移狀態';
   END IF;
 END IF;
 IF to_regclass('report.v_futures_report_daily_corrected_turning') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM pg_attribute
                    WHERE attrelid=to_regclass('report.v_futures_report_daily_corrected_turning')
                      AND attname='日盤走勢' AND attnum>0 AND NOT attisdropped) THEN
   IF to_regclass('report.v_futures_report_daily_corrected_turning_without_session_trend') IS NULL THEN
     ALTER VIEW report.v_futures_report_daily_corrected_turning
       RENAME TO v_futures_report_daily_corrected_turning_without_session_trend;
   ELSE
     RAISE EXCEPTION '舊版修正版相容 View 已存在，但公開 View 尚無日盤走勢';
   END IF;
 END IF;
 IF to_regclass('report.v_futures_dashboard_turning') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM pg_attribute
                    WHERE attrelid=to_regclass('report.v_futures_dashboard_turning')
                      AND attname='日盤走勢' AND attnum>0 AND NOT attisdropped) THEN
   IF to_regclass('report.v_futures_dashboard_turning_without_session_trend') IS NULL THEN
     ALTER VIEW report.v_futures_dashboard_turning
       RENAME TO v_futures_dashboard_turning_without_session_trend;
   ELSE
     RAISE EXCEPTION '舊版 Dashboard 相容 View 已存在，但公開 View 尚無日盤走勢';
   END IF;
 END IF;
 SELECT string_agg(format('b.%I', attname) || CASE WHEN attname='買賣權比_週五' THEN ', '||additions ELSE '' END, ', ' ORDER BY attnum)
 INTO fields FROM pg_attribute WHERE attrelid='report.v_futures_report_daily_without_turning'::regclass AND attnum>0 AND NOT attisdropped;
 EXECUTE 'CREATE OR REPLACE VIEW report.v_futures_report_daily AS SELECT '||fields||
 ' FROM report.v_futures_report_daily_without_turning b LEFT JOIN report.futures_turning_levels t ON t.futures_id=b.futures_id AND t."日期"=b."日期"';
 SELECT string_agg(format('b.%I', attname) || CASE WHEN attname='買賣權比_週五' THEN ', '||additions ELSE '' END, ', ' ORDER BY attnum)
 INTO fields FROM pg_attribute WHERE attrelid='report.v_futures_report_daily_corrected'::regclass AND attnum>0 AND NOT attisdropped;
 EXECUTE 'CREATE OR REPLACE VIEW report.v_futures_report_daily_corrected_turning AS SELECT '||fields||
 ' FROM report.v_futures_report_daily_corrected b LEFT JOIN report.futures_turning_levels t ON t.futures_id=b.futures_id AND t."日期"=b."日期"';
 IF to_regclass('report.futures_report_daily_dashboard_cache') IS NOT NULL THEN
   SELECT string_agg(format('b.%I', attname) || CASE WHEN attname='買賣權比_週五' THEN ', '||additions ELSE '' END, ', ' ORDER BY attnum)
   INTO fields FROM pg_attribute WHERE attrelid='report.futures_report_daily_dashboard_cache'::regclass AND attnum>0 AND NOT attisdropped;
   EXECUTE 'CREATE OR REPLACE VIEW report.v_futures_dashboard_turning AS SELECT '||fields||
   ' FROM report.futures_report_daily_dashboard_cache b LEFT JOIN report.futures_turning_levels t ON t.futures_id=b.futures_id AND t."日期"=b."日期"';
 END IF;
END $turning$;
'''
    if not db.execute_sql(statement): raise RuntimeError('安裝轉折報表 View 失敗')


SOURCE_SQL = '''
WITH ranked AS (
 SELECT *, row_number() OVER (PARTITION BY bar_datetime,trading_session ORDER BY volume DESC,contract_date ASC) AS rk
 FROM fm_taiwan_futures_kbar_1m
 WHERE futures_id='TX' AND trading_session IN ('position','after_market') AND bar_datetime >= ?
), sessions AS (
 SELECT *, CASE WHEN trading_session='position' THEN bar_datetime::date
   WHEN bar_datetime::time >= TIME '15:00' THEN bar_datetime::date ELSE bar_datetime::date-1 END AS session_date
 FROM ranked WHERE rk=1
 AND ((trading_session='position' AND bar_datetime::time BETWEEN TIME '08:45' AND TIME '13:45')
 OR (trading_session='after_market' AND (bar_datetime::time>=TIME '15:00' OR bar_datetime::time<=TIME '05:00')))
), buckets AS (
 SELECT *, session_date + CASE WHEN trading_session='position' THEN TIME '08:45' ELSE TIME '15:00' END AS origin
 FROM sessions
), grouped AS (
 SELECT *, origin + floor(extract(epoch FROM (bar_datetime-origin))/300)*INTERVAL '5 minutes' AS bar_time
 FROM buckets
)
SELECT min(bar_datetime) AS time, session_date, trading_session AS session,
 max(high) AS high, min(low) AS low,
 (array_agg(close ORDER BY bar_datetime DESC))[1] AS close, max(bar_datetime) AS last_time
FROM grouped GROUP BY session_date,trading_session,bar_time ORDER BY time
'''


AUDIT_FIELDS = ['日期', '時段', '欄位', '轉折值', '狀態', '規則', '候選順位',
    '候選K時間', '候選高', '候選低', '候選收', '候選DIF', '候選前DIF', '候選DIF方向',
    '取價K時間', '取價K最後1分時間', '取價K收', '取價DIF', '取價前DIF', '取價DIF方向',
    '左側K時間', '左側DIF', '左側DIF方向', '區間開始', '區間結束', '實際首K時間', '實際末1分時間', '是否首根極值例外', '左側K收盤']


def turning_audit_rows(bars, now=None):
    now = now or datetime.now(ZoneInfo('Asia/Taipei')).replace(tzinfo=None)
    groups = {}
    previous = None
    for source in bars:
        bar = dict(source)
        bar['previous_dif'] = previous.get('dif') if previous else None
        key = (bar['session_date'], bar['session'])
        if key not in groups: groups[key] = ([], previous)
        groups[key][0].append(bar)
        previous = bar
    output = []
    direction_name = lambda value: {1: '上', -1: '下', 0: '平'}.get(value, '不足')
    for (day, session), (items, prior) in groups.items():
        start = datetime.combine(day, datetime.min.time()) + (timedelta(hours=8, minutes=45) if session == 'position' else timedelta(hours=15))
        end = start + (timedelta(hours=5) if session == 'position' else timedelta(hours=14))
        status = '尚未收盤' if now < end else ('區間資料不完整' if items[0]['time'] > start + timedelta(minutes=5) or items[-1]['last_time'] < end - timedelta(minutes=5) else '完整')
        for support, field in zip((True, False), COLUMNS[:2] if session == 'position' else COLUMNS[2:]):
            row = dict.fromkeys(AUDIT_FIELDS, '')
            row.update({'日期': day, '時段': '日盤' if session == 'position' else '夜盤', '欄位': field,
                '狀態': status, '區間開始': start, '區間結束': end,
                '實際首K時間': items[0]['time'], '實際末1分時間': items[-1]['last_time']})
            match = find_turning_match(items, support, prior.get('direction') if prior else None, prior.get('close') if prior else None) if status == '完整' else None
            if match:
                candidate = items[match['candidate_index']]
                selected = items[match['selected_index']]
                left = items[match['candidate_index'] - 1] if match['candidate_index'] else prior
                row.update({'轉折值': match['value'], '狀態': '命中',
                    '是否首根極值例外': '是' if match['rule'].startswith('優先例外') else '否',
                    '左側K收盤': left['close'] if left else '', '規則': match['rule'], '候選順位': match['candidate_rank'],
                    '候選K時間': candidate['time'], '候選高': candidate['high'], '候選低': candidate['low'], '候選收': candidate['close'],
                    '候選DIF': candidate.get('dif'), '候選前DIF': candidate.get('previous_dif'), '候選DIF方向': direction_name(candidate['direction']),
                    '取價K時間': selected['time'], '取價K最後1分時間': selected['last_time'], '取價K收': selected['close'],
                    '取價DIF': selected.get('dif'), '取價前DIF': selected.get('previous_dif'), '取價DIF方向': direction_name(selected['direction']),
                    '左側K時間': left['time'] if left else '', '左側DIF': left.get('dif') if left else '',
                    '左側DIF方向': direction_name(left['direction']) if left else '不足'})
            elif status == '完整': row['狀態'] = '未找到符合規則的轉折'
            output.append(row)
    return output


def export_turning_audit(output_path=None):
    """Overwrite one UTF-8 BOM CSV from the full cached bar history, atomically."""
    import csv
    import os
    import tempfile
    from pathlib import Path
    from common import db
    path = Path(output_path) if output_path else Path(__file__).resolve().parents[2] / 'output' / 'futures_turning_debug.csv'
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = db.query_to_df('SELECT bar_time AS time, session_date, session, high, low, close, last_time, dif, direction FROM report.futures_turning_5m_bars ORDER BY bar_time')
    bars = frame.to_dict('records')
    for bar in bars:
        for key in ('dif', 'direction'):
            if bar[key] is not None and not math.isfinite(float(bar[key])): bar[key] = None
        if hasattr(bar['session_date'], 'date'): bar['session_date'] = bar['session_date'].date()
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8-sig', newline='', dir=path.parent, delete=False) as stream:
            temporary = stream.name
            writer = csv.DictWriter(stream, fieldnames=AUDIT_FIELDS)
            writer.writeheader()
            writer.writerows(turning_audit_rows(bars))
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary): os.unlink(temporary)
    print(f'[轉折除錯CSV] {path}')
    return path


def _refresh_turning_levels(*, full=False, start_date=None):
    """Initial history build, then exact EMA continuation over a refreshed tail.

    For revisions earlier than the tail, use full=True or the earliest changed
    start_date; all later EMA/session results are rebuilt from that point.
    """
    from common import db
    ensure_turning_views()
    latest = db.query_single_value('SELECT max(bar_time) FROM report.futures_turning_5m_bars')
    start = datetime(1900,1,1) if full or latest is None else datetime.combine((latest - timedelta(days=14)).date(), datetime.min.time()) + timedelta(hours=8, minutes=45)
    if start_date is not None and not full:
        start = min(start, datetime.combine(start_date, datetime.min.time()) + timedelta(hours=8, minutes=45))
    checkpoint = db.query_to_df('SELECT direction,close,ema_state FROM report.futures_turning_5m_bars WHERE bar_time < ? ORDER BY bar_time DESC LIMIT 1', (start,))
    seed = None if checkpoint.empty else checkpoint.iloc[0]['ema_state']
    if isinstance(seed, str): seed = json.loads(seed)
    prior_value = None if checkpoint.empty else checkpoint.iloc[0]['direction']
    prior = int(prior_value) if prior_value is not None and math.isfinite(float(prior_value)) else None
    frame = db.query_to_df(SOURCE_SQL, (start,))
    rows = frame.to_dict('records')
    for row in rows:
        row['time'] = row['time'].to_pydatetime() if hasattr(row['time'],'to_pydatetime') else row['time']
        row['last_time'] = row['last_time'].to_pydatetime() if hasattr(row['last_time'],'to_pydatetime') else row['last_time']
        if hasattr(row['session_date'],'date'): row['session_date'] = row['session_date'].date()
    bars = calculate_bars(rows, seed)
    results = session_results(bars, previous_direction=prior,
                              previous_close=None if checkpoint.empty else checkpoint.iloc[0].get('close'))
    # Replace the refreshed tail and its levels in one transaction.
    with db.get_connection() as conn:
        conn.execute('DELETE FROM report.futures_turning_5m_bars WHERE bar_time >= %s', (start,))
        conn.execute('DELETE FROM report.futures_turning_levels WHERE "日期" >= %s', (start.date(),))
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO report.futures_turning_5m_bars VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)',
                [(b['time'],b['session_date'],b['session'],float(b['high']),float(b['low']),float(b['close']),b['last_time'],b['dif'],b['direction'],json.dumps(b['ema_state'])) for b in bars])
            for date, session, support, resistance in results:
                names = COLUMNS[:2] if session=='position' else COLUMNS[2:]
                cur.execute(f'INSERT INTO report.futures_turning_levels (futures_id,"日期","{names[0]}","{names[1]}") VALUES (\'TX\',%s,%s,%s) ON CONFLICT (futures_id,"日期") DO UPDATE SET "{names[0]}"=EXCLUDED."{names[0]}","{names[1]}"=EXCLUDED."{names[1]}"', (date,support,resistance))
        conn.commit()
    try:
        export_turning_audit()
    except OSError as exc:
        print(f'[轉折除錯CSV] 寫入失敗（資料庫結果已提交）：{exc}')
    return len(results)


def refresh_turning_levels(*, full=False, start_date=None):
    from common import db
    # Serialize report/minute-bar refreshes to protect the exact EMA checkpoint.
    with db.get_connection() as lock_connection:
        lock_connection.execute('SELECT pg_advisory_lock(26100450)')
        try:
            return _refresh_turning_levels(full=full, start_date=start_date)
        finally:
            lock_connection.execute('SELECT pg_advisory_unlock(26100450)')


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--full',action='store_true');parser.add_argument('--audit-only',action='store_true')
    args=parser.parse_args()
    if args.audit_only: export_turning_audit()
    else: print(f'轉折支撐壓力更新完成：{refresh_turning_levels(full=args.full)} 個時段')
