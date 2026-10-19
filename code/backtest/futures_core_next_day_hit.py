from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd


SOURCE_VIEW = 'report.v_futures_report_daily_corrected'
STRATEGY_VERSION = '2.0'
SIGNAL_COLUMN = '策略_核心_五日價量'


@dataclass(frozen=True)
class BacktestSettings:
    start: date
    end: date
    shadow_threshold: float = 125.0


def load_source(settings: BacktestSettings) -> pd.DataFrame:
    from common import db

    # 先用 LEAD 找下一個「實際存在的交易日」，再篩訊號日期；不可先 WHERE 日期，
    # 否則區間最後一天永遠找不到下一交易日。
    sql = f'''
        WITH ordered AS (
            SELECT
                "日期" AS signal_date,
                TRIM("策略_核心_五日價量"::text) AS signal_direction,
                LEAD("日期") OVER (ORDER BY "日期") AS evaluation_date,
                LEAD("日盤開") OVER (ORDER BY "日期") AS day_open,
                LEAD("日盤高") OVER (ORDER BY "日期") AS day_high,
                LEAD("日盤低") OVER (ORDER BY "日期") AS day_low,
                LEAD("日盤收") OVER (ORDER BY "日期") AS day_close
            FROM {SOURCE_VIEW}
        )
        SELECT *
        FROM ordered
        WHERE signal_date BETWEEN ? AND ?
          AND signal_direction IN ('多', '空')
        ORDER BY signal_date
    '''
    return db.sql_to_df(sql, params=(settings.start, settings.end),
                        parse_dates=['signal_date', 'evaluation_date'])


def calculate_detail(source: pd.DataFrame, threshold: float = 125.0) -> pd.DataFrame:
    df = source.copy()
    numeric = ['day_open', 'day_high', 'day_low', 'day_close']
    for column in numeric:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    df = df.dropna(subset=['evaluation_date', *numeric]).copy()

    body_high = df[['day_open', 'day_close']].max(axis=1)
    body_low = df[['day_open', 'day_close']].min(axis=1)
    df['upper_shadow_points'] = df['day_high'] - body_high
    df['lower_shadow_points'] = body_low - df['day_low']
    df['candle'] = '十字'
    df.loc[df['day_close'] > df['day_open'], 'candle'] = '紅K'
    df.loc[df['day_close'] < df['day_open'], 'candle'] = '黑K'

    df['direction_hit'] = (
        ((df['signal_direction'] == '多') & (df['day_close'] > df['day_open'])) |
        ((df['signal_direction'] == '空') & (df['day_close'] < df['day_open']))
    )
    df['upper_shadow_hit'] = df['upper_shadow_points'] > threshold
    df['lower_shadow_hit'] = df['lower_shadow_points'] > threshold
    df['any_shadow_hit'] = df['upper_shadow_hit'] | df['lower_shadow_hit']
    df['overall_hit'] = df['direction_hit'] | df['any_shadow_hit']
    df['shadow_rescue'] = ~df['direction_hit'] & df['any_shadow_hit']
    df['open_to_close_points'] = (df['day_close'] - df['day_open']).where(
        df['signal_direction'] == '多', df['day_open'] - df['day_close']
    )

    labels = {
        'signal_date': '訊號日', 'evaluation_date': '判定日',
        'signal_direction': '策略核心', 'day_open': '隔日日盤開',
        'day_high': '隔日日盤高', 'day_low': '隔日日盤低',
        'day_close': '隔日日盤收', 'candle': '隔日K線',
        'upper_shadow_points': '上影線點數', 'lower_shadow_points': '下影線點數',
        'direction_hit': '方向命中', 'upper_shadow_hit': '長上影命中',
        'lower_shadow_hit': '長下影命中', 'any_shadow_hit': '任一長影線命中',
        'overall_hit': '綜合命中', 'shadow_rescue': '影線追加命中',
        'open_to_close_points': '模擬開收盤點數',
    }
    return df.rename(columns=labels)[list(labels.values())]


def _summary_row(name: str, frame: pd.DataFrame) -> dict:
    count = len(frame)
    rate = lambda column: round(float(frame[column].mean() * 100), 2) if count else 0.0
    return {
        '分類': name, '有效訊號數': count,
        '方向命中數': int(frame['方向命中'].sum()), '方向命中率_pct': rate('方向命中'),
        '長上影命中數': int(frame['長上影命中'].sum()), '長上影命中率_pct': rate('長上影命中'),
        '長下影命中數': int(frame['長下影命中'].sum()), '長下影命中率_pct': rate('長下影命中'),
        '任一長影線命中數': int(frame['任一長影線命中'].sum()),
        '任一長影線命中率_pct': rate('任一長影線命中'),
        '綜合命中數': int(frame['綜合命中'].sum()), '綜合命中率_pct': rate('綜合命中'),
        '影線追加命中數': int(frame['影線追加命中'].sum()),
        '模擬開收盤總點數': round(float(frame['模擬開收盤點數'].sum()), 2),
        '模擬開收盤平均點數': round(float(frame['模擬開收盤點數'].mean()), 2) if count else 0.0,
    }


def calculate_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows = [_summary_row('整體', detail)]
    for direction in ('多', '空'):
        rows.append(_summary_row(direction, detail[detail['策略核心'] == direction]))
    return pd.DataFrame(rows)


def _json_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, 'item'):
        value = value.item()
    return value


def save_to_database(detail: pd.DataFrame, summary: pd.DataFrame,
                     settings: BacktestSettings) -> uuid.UUID:
    """儲存一個完整批次；任何明細失敗時整批回滾。"""
    from common import db

    run_id = uuid.uuid4()
    overall = summary.loc[summary['分類'] == '整體'].iloc[0]
    settings_json = json.dumps(
        {'shadow_threshold': settings.shadow_threshold,
         'shadow_operator': 'strictly_greater_than',
         'signal_column': SIGNAL_COLUMN,
         'source_view': SOURCE_VIEW},
        ensure_ascii=False,
    )
    summary_json = json.dumps(
        [{key: _json_value(value) for key, value in row.items()}
         for row in summary.to_dict(orient='records')],
        ensure_ascii=False,
    )

    insert_run = '''
        INSERT INTO backtest.strategy_runs (
            run_id, strategy_code, strategy_version, start_date, end_date,
            settings, valid_signal_count, direction_hit_rate,
            overall_hit_rate, total_points, average_points, summary
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb)
    '''
    insert_detail = '''
        INSERT INTO backtest.futures_core_next_day_details (
            run_id, signal_date, evaluation_date, signal_direction,
            day_open, day_high, day_low, day_close, candle,
            upper_shadow_points, lower_shadow_points, direction_hit,
            upper_shadow_hit, lower_shadow_hit, any_shadow_hit,
            overall_hit, shadow_rescue, open_to_close_points
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_run, (
                run_id, 'futures_core_next_day_hit', STRATEGY_VERSION,
                settings.start, settings.end,
                settings_json, int(overall['有效訊號數']),
                float(overall['方向命中率_pct']), float(overall['綜合命中率_pct']),
                float(overall['模擬開收盤總點數']), float(overall['模擬開收盤平均點數']),
                summary_json,
            ))
            rows = []
            for record in detail.to_dict(orient='records'):
                rows.append((
                    run_id, record['訊號日'].date(), record['判定日'].date(),
                    record['策略核心'], record['隔日日盤開'], record['隔日日盤高'],
                    record['隔日日盤低'], record['隔日日盤收'], record['隔日K線'],
                    record['上影線點數'], record['下影線點數'], record['方向命中'],
                    record['長上影命中'], record['長下影命中'], record['任一長影線命中'],
                    record['綜合命中'], record['影線追加命中'], record['模擬開收盤點數'],
                ))
            if rows:
                cur.executemany(insert_detail, rows)
        conn.commit()
    return run_id


def export_reports(detail: pd.DataFrame, summary: pd.DataFrame, settings: BacktestSettings,
                   output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f'{settings.start:%Y%m%d}_{settings.end:%Y%m%d}'
    detail_path = output_dir / f'futures_core_next_day_detail_corrected_{suffix}.csv'
    summary_path = output_dir / f'futures_core_next_day_summary_corrected_{suffix}.csv'
    detail.to_csv(detail_path, index=False, encoding='utf-8-sig')
    summary.to_csv(summary_path, index=False, encoding='utf-8-sig')
    return detail_path, summary_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='策略_核心_五日價量隔日日盤 K 線命中率回測'
    )
    parser.add_argument('--start', required=True, type=date.fromisoformat)
    parser.add_argument('--end', required=True, type=date.fromisoformat)
    parser.add_argument('--shadow-threshold', type=float, default=125.0)
    parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest'))
    parser.add_argument('--no-save-db', action='store_true', help='只輸出 CSV，不寫入資料庫')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start > args.end:
        raise SystemExit('[錯誤] --start 不可晚於 --end')
    settings = BacktestSettings(args.start, args.end, args.shadow_threshold)
    detail = calculate_detail(load_source(settings), settings.shadow_threshold)
    summary = calculate_summary(detail)
    detail_path, summary_path = export_reports(detail, summary, settings, args.output_dir)
    run_id = None if args.no_save_db else save_to_database(detail, summary, settings)
    print(summary.to_string(index=False))
    print(f'\n資料來源：{SOURCE_VIEW}（策略版本 {STRATEGY_VERSION}）')
    print(f'\n明細報表：{detail_path.resolve()}')
    print(f'摘要報表：{summary_path.resolve()}')
    if run_id:
        print(f'資料庫 run_id：{run_id}')


if __name__ == '__main__':
    main()
