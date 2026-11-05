from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, time, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


RULE_VERSION = "1.0.0"
SOURCE_RELATION = "raw_finmind.fm_taiwan_futures_kbar_1m"
TARGET_TABLE = "research.futures_daily_amplitude_pattern"


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    session_start: time = time(8, 45)
    session_end: time = time(13, 45)
    first_window_minutes: int = 30
    minimum_bars: int = 290
    range_days: int = 30
    low_range_threshold: float = 0.75
    expanded_range_threshold: float = 1.25
    violent_range_threshold: float = 1.50
    extreme_range_threshold: float = 2.00
    biased_share_threshold: float = 0.65
    strong_share_threshold: float = 0.80

    def validate(self) -> None:
        if self.end is not None and self.end < self.start:
            raise ValueError("end 不可早於 start")
        if not 2 <= self.range_days <= 252:
            raise ValueError("range_days 必須介於 2 到 252")
        if not (
            0 < self.low_range_threshold < self.expanded_range_threshold
            < self.violent_range_threshold < self.extreme_range_threshold
        ):
            raise ValueError("振幅門檻必須依序遞增")
        if not 0.5 < self.biased_share_threshold < self.strong_share_threshold < 1:
            raise ValueError("方向占比門檻必須符合 0.5 < biased < strong < 1")
        if self.first_window_minutes <= 0:
            raise ValueError("first_window_minutes 必須大於 0")


def load_bars(settings: Settings) -> pd.DataFrame:
    """讀取全部交易日的日盤一分K，並固定每個交易日使用最前月完整契約。"""
    from common import db

    settings.validate()
    history_start = settings.start - timedelta(days=max(90, settings.range_days * 4))
    end = settings.end or date.today()
    sql = """
        WITH contract_stats AS (
            SELECT
                trading_date,
                contract_date,
                COUNT(*) AS bar_count,
                MIN(bar_datetime) AS first_at,
                MAX(bar_datetime) AS last_at,
                ROW_NUMBER() OVER (
                    PARTITION BY trading_date
                    ORDER BY
                        CASE
                            WHEN MIN(bar_datetime)::time <= (?::time + INTERVAL '1 minute')
                             AND MAX(bar_datetime)::time >= (?::time - INTERVAL '1 minute')
                            THEN 0 ELSE 1
                        END,
                        contract_date
                ) AS contract_rank
            FROM raw_finmind.fm_taiwan_futures_kbar_1m
            WHERE futures_id = ?
              AND trading_session = 'position'
              AND contract_date ~ '^[0-9]{6}$'
              AND trading_date BETWEEN ? AND ?
              AND bar_datetime::time BETWEEN ? AND ?
            GROUP BY trading_date, contract_date
        ), chosen AS (
            SELECT trading_date, contract_date
            FROM contract_stats
            WHERE contract_rank = 1
        )
        SELECT
            k.trading_date,
            k.contract_date,
            k.bar_datetime,
            k.open,
            k.high,
            k.low,
            k.close,
            k.volume
        FROM raw_finmind.fm_taiwan_futures_kbar_1m k
        JOIN chosen c
          ON c.trading_date = k.trading_date
         AND c.contract_date = k.contract_date
        WHERE k.futures_id = ?
          AND k.trading_session = 'position'
          AND k.bar_datetime::time BETWEEN ? AND ?
        ORDER BY k.trading_date, k.bar_datetime
    """
    return db.sql_to_df(
        sql,
        params=(
            settings.session_start, settings.session_end, settings.futures_id,
            history_start, end, settings.session_start, settings.session_end,
            settings.futures_id, settings.session_start, settings.session_end,
        ),
        parse_dates=["trading_date", "bar_datetime"],
    )


def _amplitude_label(ratio: float | None, settings: Settings) -> str | None:
    if ratio is None or pd.isna(ratio):
        return None
    if ratio < settings.low_range_threshold:
        return "低振幅"
    if ratio < settings.expanded_range_threshold:
        return "一般振幅"
    if ratio < settings.violent_range_threshold:
        return "明顯放大"
    if ratio < settings.extreme_range_threshold:
        return "劇烈振幅"
    return "極端振幅"


def _excursion_bias(up_share: float | None, settings: Settings) -> str:
    if up_share is None or pd.isna(up_share):
        return "無法判定"
    if up_share >= settings.strong_share_threshold:
        return "強上行集中"
    if up_share >= settings.biased_share_threshold:
        return "上行集中"
    if up_share <= 1.0 - settings.strong_share_threshold:
        return "強下行集中"
    if up_share <= 1.0 - settings.biased_share_threshold:
        return "下行集中"
    return "上下均衡"


def _path_pattern(
    up_share: float | None,
    close_location: float | None,
    extreme_order: str,
    settings: Settings,
) -> str:
    if up_share is None or close_location is None or pd.isna(up_share) or pd.isna(close_location):
        return "無法判定"
    down_share = 1.0 - up_share
    if up_share >= settings.strong_share_threshold and close_location >= 0.75:
        return "強單邊多"
    if down_share >= settings.strong_share_threshold and close_location <= 0.25:
        return "強單邊空"
    if up_share >= settings.biased_share_threshold:
        if close_location <= 0.45:
            return "先拉後殺"
        return "偏多震盪"
    if down_share >= settings.biased_share_threshold:
        if close_location >= 0.55:
            return "先殺後拉"
        return "偏空震盪"
    if extreme_order == "LOW_FIRST" and close_location >= 0.65:
        return "先殺後拉"
    if extreme_order == "HIGH_FIRST" and close_location <= 0.35:
        return "先拉後殺"
    return "上下雙向震盪"


def _daily_row(group: pd.DataFrame, settings: Settings) -> dict[str, Any]:
    bars = group.sort_values("bar_datetime").reset_index(drop=True)
    first = bars.iloc[0]
    last = bars.iloc[-1]
    day_high = float(bars["high"].max())
    day_low = float(bars["low"].min())
    day_open = float(first["open"])
    day_close = float(last["close"])
    range_points = day_high - day_low
    high_bars = bars[bars["high"].eq(day_high)]
    low_bars = bars[bars["low"].eq(day_low)]
    first_high_at = pd.Timestamp(high_bars.iloc[0]["bar_datetime"])
    last_high_at = pd.Timestamp(high_bars.iloc[-1]["bar_datetime"])
    first_low_at = pd.Timestamp(low_bars.iloc[0]["bar_datetime"])
    last_low_at = pd.Timestamp(low_bars.iloc[-1]["bar_datetime"])
    if last_low_at < last_high_at:
        extreme_order = "LOW_FIRST"
        session_trend = "漲勢盤"
    elif last_high_at < last_low_at:
        extreme_order = "HIGH_FIRST"
        session_trend = "跌勢盤"
    else:
        extreme_order = "SAME_BAR"
        session_trend = "同K極值"

    up_excursion = day_high - day_open
    down_excursion = day_open - day_low
    up_share = up_excursion / range_points if range_points > 0 else np.nan
    down_share = down_excursion / range_points if range_points > 0 else np.nan
    close_location = (day_close - day_low) / range_points if range_points > 0 else np.nan
    window_end = (
        pd.Timestamp.combine(pd.Timestamp(first["trading_date"]).date(), settings.session_start)
        + pd.Timedelta(minutes=settings.first_window_minutes)
    )
    opening = bars[bars["bar_datetime"] < window_end]
    if opening.empty:
        first_30m_high = first_30m_low = first_30m_close = first_30m_range = np.nan
    else:
        first_30m_high = float(opening["high"].max())
        first_30m_low = float(opening["low"].min())
        first_30m_close = float(opening.iloc[-1]["close"])
        first_30m_range = first_30m_high - first_30m_low
    first_30m_share = first_30m_range / range_points if range_points > 0 else np.nan

    first_at = pd.Timestamp(first["bar_datetime"])
    last_at = pd.Timestamp(last["bar_datetime"])
    expected_start = pd.Timestamp.combine(first_at.date(), settings.session_start)
    expected_end = pd.Timestamp.combine(first_at.date(), settings.session_end)
    complete = bool(
        len(bars) >= settings.minimum_bars
        and first_at <= expected_start + pd.Timedelta(minutes=1)
        and last_at >= expected_end - pd.Timedelta(minutes=1)
    )
    return {
        "futures_id": settings.futures_id,
        "trading_date": pd.Timestamp(first["trading_date"]).date(),
        "contract_date": str(first["contract_date"]),
        "rule_version": RULE_VERSION,
        "source_first_at": first_at,
        "source_last_at": last_at,
        "bar_count": int(len(bars)),
        "session_complete": complete,
        "day_open": day_open,
        "day_high": day_high,
        "day_low": day_low,
        "day_close": day_close,
        "day_volume": int(pd.to_numeric(bars["volume"], errors="coerce").fillna(0).sum()),
        "range_points": range_points,
        "up_excursion_points": up_excursion,
        "down_excursion_points": down_excursion,
        "up_share": up_share,
        "down_share": down_share,
        "open_close_points": day_close - day_open,
        "close_location": close_location,
        "first_high_at": first_high_at,
        "last_high_at": last_high_at,
        "first_low_at": first_low_at,
        "last_low_at": last_low_at,
        "extreme_order": extreme_order,
        "session_trend_label": session_trend,
        "first_30m_high": first_30m_high,
        "first_30m_low": first_30m_low,
        "first_30m_close": first_30m_close,
        "first_30m_range_points": first_30m_range,
        "first_30m_range_share": first_30m_share,
    }


def build_daily_patterns(bars: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    settings.validate()
    if bars.empty:
        return pd.DataFrame()
    required = {
        "trading_date", "contract_date", "bar_datetime", "open", "high", "low", "close", "volume"
    }
    missing = sorted(required.difference(bars.columns))
    if missing:
        raise ValueError(f"一分K缺少欄位：{', '.join(missing)}")
    source = bars.copy()
    source["trading_date"] = pd.to_datetime(source["trading_date"])
    source["bar_datetime"] = pd.to_datetime(source["bar_datetime"])
    rows = [_daily_row(group, settings) for _, group in source.groupby("trading_date", sort=True)]
    daily = pd.DataFrame(rows).sort_values("trading_date").reset_index(drop=True)
    complete_range = daily["range_points"].where(daily["session_complete"])
    daily["average_range_previous"] = (
        complete_range.shift(1).rolling(settings.range_days, min_periods=settings.range_days).mean()
    )
    daily["range_ratio"] = daily["range_points"] / daily["average_range_previous"]
    daily["valid_for_research"] = (
        daily["session_complete"]
        & daily["average_range_previous"].notna()
        & daily["range_points"].gt(0)
    )
    daily["amplitude_label"] = daily["range_ratio"].map(lambda value: _amplitude_label(value, settings))
    daily["excursion_bias_label"] = daily["up_share"].map(lambda value: _excursion_bias(value, settings))
    daily["path_pattern_label"] = daily.apply(
        lambda row: _path_pattern(row["up_share"], row["close_location"], row["extreme_order"], settings),
        axis=1,
    )
    daily.loc[~daily["session_complete"], ["amplitude_label", "excursion_bias_label", "path_pattern_label"]] = [
        None, "資料不完整", "資料不完整"
    ]
    daily["auto_label"] = daily.apply(
        lambda row: (
            f"{row['amplitude_label']}｜{row['path_pattern_label']}"
            if row["amplitude_label"] is not None else None
        ),
        axis=1,
    )
    daily["range_days"] = settings.range_days
    daily["low_range_threshold"] = settings.low_range_threshold
    daily["expanded_range_threshold"] = settings.expanded_range_threshold
    daily["violent_range_threshold"] = settings.violent_range_threshold
    daily["extreme_range_threshold"] = settings.extreme_range_threshold
    daily["biased_share_threshold"] = settings.biased_share_threshold
    daily["strong_share_threshold"] = settings.strong_share_threshold
    start = settings.start
    end = settings.end or date.max
    return daily[daily["trading_date"].between(start, end)].reset_index(drop=True)


DB_COLUMNS = [
    "futures_id", "trading_date", "contract_date", "rule_version",
    "source_first_at", "source_last_at", "bar_count", "session_complete", "valid_for_research",
    "day_open", "day_high", "day_low", "day_close", "day_volume", "range_points",
    "average_range_previous", "range_ratio", "up_excursion_points", "down_excursion_points",
    "up_share", "down_share", "open_close_points", "close_location",
    "first_high_at", "last_high_at", "first_low_at", "last_low_at",
    "extreme_order", "session_trend_label",
    "first_30m_high", "first_30m_low", "first_30m_close",
    "first_30m_range_points", "first_30m_range_share",
    "amplitude_label", "excursion_bias_label", "path_pattern_label", "auto_label",
    "range_days", "low_range_threshold", "expanded_range_threshold",
    "violent_range_threshold", "extreme_range_threshold",
    "biased_share_threshold", "strong_share_threshold",
]


def _db_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    try:
        return None if bool(pd.isna(value)) else value
    except (TypeError, ValueError):
        return value


def save_to_database(daily: pd.DataFrame) -> int:
    """Upsert 自動計算欄位；刻意不更新 manual_* 與 manually_reviewed_at。"""
    from common.db import get_connection

    if daily.empty:
        return 0
    missing = [column for column in DB_COLUMNS if column not in daily.columns]
    if missing:
        raise ValueError(f"入庫資料缺少欄位：{', '.join(missing)}")
    update_columns = [column for column in DB_COLUMNS if column not in ("futures_id", "trading_date")]
    sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(DB_COLUMNS)})
        VALUES ({', '.join(['%s'] * len(DB_COLUMNS))})
        ON CONFLICT (futures_id, trading_date) DO UPDATE SET
            {', '.join(f'{column} = EXCLUDED.{column}' for column in update_columns)},
            updated_at = CURRENT_TIMESTAMP
    """
    rows = [
        tuple(_db_value(value) for value in row)
        for row in daily[DB_COLUMNS].itertuples(index=False, name=None)
    ]
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (TARGET_TABLE,))
            if cursor.fetchone()[0] is None:
                raise RuntimeError(
                    "尚未建立逐日振幅型態表；請先執行 "
                    "db_migrate/20261105_add_futures_daily_amplitude_pattern.sql"
                )
            cursor.executemany(sql, rows)
        conn.commit()
    return len(rows)


def export_csv(daily: pd.DataFrame, output_dir: Path, settings: Settings) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_text = settings.end.strftime("%Y%m%d") if settings.end else "latest"
    path = output_dir / f"daily_amplitude_pattern_{settings.start:%Y%m%d}_{end_text}.csv"
    daily.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="建立全部交易日的日盤振幅與盤中路徑標籤")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--range-days", type=int, default=30)
    parser.add_argument("--minimum-bars", type=int, default=290)
    parser.add_argument("--low-range-threshold", type=float, default=0.75)
    parser.add_argument("--expanded-range-threshold", type=float, default=1.25)
    parser.add_argument("--violent-range-threshold", type=float, default=1.50)
    parser.add_argument("--extreme-range-threshold", type=float, default=2.00)
    parser.add_argument("--biased-share-threshold", type=float, default=0.65)
    parser.add_argument("--strong-share-threshold", type=float, default=0.80)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("../output/analysis/futures_daily_amplitude_pattern"),
    )
    parser.add_argument("--save-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start,
        end=args.end,
        futures_id=args.futures_id,
        range_days=args.range_days,
        minimum_bars=args.minimum_bars,
        low_range_threshold=args.low_range_threshold,
        expanded_range_threshold=args.expanded_range_threshold,
        violent_range_threshold=args.violent_range_threshold,
        extreme_range_threshold=args.extreme_range_threshold,
        biased_share_threshold=args.biased_share_threshold,
        strong_share_threshold=args.strong_share_threshold,
    )
    bars = load_bars(settings)
    daily = build_daily_patterns(bars, settings)
    path = export_csv(daily, args.output_dir, settings)
    saved = save_to_database(daily) if args.save_db else 0
    print(f"全部交易日：{len(daily):,} 日")
    print(f"有效研究樣本：{int(daily['valid_for_research'].sum()) if not daily.empty else 0:,} 日")
    print(f"資料庫保存：{saved:,} 日" if args.save_db else "資料庫保存：未啟用（加 --save-db）")
    print(f"CSV：{path.resolve()}")


if __name__ == "__main__":
    main()
