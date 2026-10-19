"""Audit and repair missing next-calendar-day TX night ticks.

Run from code/: python -m main.repair_tx_night_gaps --start 2026-05-01 --end 2026-05-29 --apply
Without --apply this only reads the database. Dates refer to night session start dates.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def target_dates(session_dates, tick_rows, now=None):
    now = now or datetime.now(ZoneInfo('Asia/Taipei')).replace(tzinfo=None)
    latest = {r['calendar_date']: r['last_tick'] for r in tick_rows}
    result = []
    for session_date in session_dates:
        following = session_date + timedelta(days=1)
        deadline = datetime.combine(following, time(5))
        if now < deadline + timedelta(hours=2):
            continue
        last = latest.get(following)
        if last is None or last < deadline - timedelta(minutes=5):
            result.append((session_date, following))
    return result


def audit(start, end):
    from common import db
    calendar = db.query_to_df('''
        SELECT td.date FROM fm_taiwan_stock_trading_date td
        WHERE td.date >= ? AND td.date <= ?
          AND EXISTS (SELECT 1 FROM fm_taiwan_futures_tick t
            WHERE t.futures_id = 'TX'
              AND t.trade_datetime >= td.date + TIME '08:45'
              AND t.trade_datetime < td.date + TIME '13:46')
        ORDER BY td.date
    ''', (start, end))
    ticks = db.query_to_df('''
        SELECT trade_datetime::date AS calendar_date, MAX(trade_datetime) AS last_tick
        FROM fm_taiwan_futures_tick WHERE futures_id = 'TX'
          AND trade_datetime >= ? AND trade_datetime < ?
          AND trade_datetime::time <= TIME '05:00'
        GROUP BY trade_datetime::date
    ''', (start + timedelta(days=1), end + timedelta(days=2)))
    dates = [v.date() if isinstance(v, datetime) else v for v in calendar['date']]
    rows = ticks.to_dict('records')
    for row in rows:
        if isinstance(row['calendar_date'], datetime):
            row['calendar_date'] = row['calendar_date'].date()
    return target_dates(dates, rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', required=True, type=date.fromisoformat)
    parser.add_argument('--end', required=True, type=date.fromisoformat)
    parser.add_argument('--apply', action='store_true', help='Download missing dates, rebuild bars, recompute turning levels')
    args = parser.parse_args(argv)
    if args.start > args.end:
        parser.error('--start must not exceed --end')
    targets = audit(args.start, args.end)
    for session, following in targets:
        print(f'[缺漏] {session} 夜盤：{following} 凌晨未涵蓋至 04:55')
    print(f'需要檢查／補抓：{len(targets)} 個日期')
    if not args.apply or not targets:
        return 0
    from module import finMind as fm
    from module.futures_turning import refresh_turning_levels
    repaired = []
    failed = []
    for session, following in targets:
        try:
            # Force bypasses a partial source-date cache; the API date remains a calendar date.
            summary = fm.get_tw_futures_tick('TX', following, following, force_refresh=True)
            print(summary.to_string(index=False))
            if summary.empty or not (summary['status'] == 'stored').any():
                failed.append(following)
                print(f'[未補齊] {following}：API 未回傳可入庫資料；不重建這一天 K 棒')
                continue
            fm.build_tw_futures_kbar_1m('TX', following, following)
            repaired.append(following)
        except Exception as exc:
            failed.append(following)
            print(f'[失敗] {following}: {type(exc).__name__}: {exc}')
    if repaired:
        # Historical additions change subsequent continuous EMAs, so recompute all levels.
        count = refresh_turning_levels(full=True)
        print(f'[重算] {count} 個時段；完成後請在 Dashboard 按重新整理')
    unresolved = audit(args.start, args.end)
    for session, following in unresolved:
        print(f'[仍缺漏] {session} 夜盤／{following} 凌晨')
    print(f'下載並重建 {len(repaired)} 日；請求失敗 {len(failed)} 日；仍缺凌晨資料 {len(unresolved)} 日')
    return 1 if unresolved or failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
