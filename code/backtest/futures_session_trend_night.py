from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

from backtest.futures_session_trend_intraday import (
    SOURCE_VIEW,
    Tick,
    core_direction,
    normalize_trend,
)
from backtest.futures_turning_level_intraday import simulate_limit_trade, summarize

FIVE_DAY_SIGNAL = "策略_核心_五日價量"


@dataclass(frozen=True)
class NightStrategyDefinition:
    code: str
    direction: str
    level_source: str
    description: str


STRATEGY_DEFINITIONS = {
    "2-2.11": NightStrategyDefinition(
        "2-2.11", "LONG", "DAY",
        "五日核心多；當日日盤漲勢掛日盤支撐，當日日盤跌勢掛日盤壓力",
    ),
    "2-2.12": NightStrategyDefinition(
        "2-2.12", "SHORT", "DAY",
        "五日核心空；當日日盤漲勢掛日盤支撐，當日日盤跌勢掛日盤壓力",
    ),
    "2-2.13": NightStrategyDefinition(
        "2-2.13", "LONG", "PREVIOUS_NIGHT",
        "五日核心多；上一個已完成夜盤漲勢掛夜盤支撐，跌勢掛夜盤壓力",
    ),
    "2-2.14": NightStrategyDefinition(
        "2-2.14", "SHORT", "PREVIOUS_NIGHT",
        "五日核心空；上一個已完成夜盤漲勢掛夜盤支撐，跌勢掛夜盤壓力",
    ),
}


@dataclass(frozen=True)
class NightSettings:
    start: date
    end: date
    futures_id: str = "TX"
    entry_time: time = time(17, 0)
    forced_exit_time: time = time(5, 0)
    range_days: int = 30
    stop_loss_ratio: float = 0.15
    take_profit_ratio: float = 0.50
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0
    contracts: int = 1

    def __post_init__(self) -> None:
        if self.start != date(2018, 1, 1):
            raise ValueError("完整回測的 start 固定為 2018-01-01")
        if self.start > self.end:
            raise ValueError("日期範圍錯誤")
        if self.entry_time < time(15, 0):
            raise ValueError("夜盤最早進場時間不可早於15:00")
        if self.forced_exit_time > time(5, 0):
            raise ValueError("夜盤強制出場時間不可晚於05:00")
        if self.range_days < 1 or min(self.stop_loss_ratio, self.take_profit_ratio) <= 0:
            raise ValueError("振幅天數、停損與停利比例必須大於0")
        if self.round_trip_cost_points < 0 or self.point_value_twd <= 0 or self.contracts < 1:
            raise ValueError("成本、每點價值或口數設定錯誤")


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def session_window(session_date: date, settings: NightSettings) -> tuple[datetime, datetime]:
    start_at = datetime.combine(session_date, settings.entry_time)
    end_at = datetime.combine(session_date + timedelta(days=1), settings.forced_exit_time)
    return start_at, end_at


def select_entry_level(row: object, strategy: str) -> tuple[str | None, str | None, float | None, date | None]:
    definition = STRATEGY_DEFINITIONS[strategy]
    if definition.level_source == "DAY":
        prefix = "day"
        level_date_value = getattr(row, "signal_date", None)
    else:
        prefix = "previous_night"
        level_date_value = getattr(row, "previous_night_date", None)
    trend = normalize_trend(getattr(row, f"{prefix}_trend", None))
    level_date = None if level_date_value is None or pd.isna(level_date_value) else pd.Timestamp(level_date_value).date()
    if trend == "漲勢盤":
        return trend, "SUPPORT", _number(getattr(row, f"{prefix}_support", None)), level_date
    if trend == "跌勢盤":
        return trend, "RESISTANCE", _number(getattr(row, f"{prefix}_resistance", None)), level_date
    return None, None, None, level_date


def load_candidates(settings: NightSettings) -> pd.DataFrame:
    from common import db

    sql = f'''
        WITH history AS (
            SELECT futures_id, "日期" AS signal_date,
                TRIM("{FIVE_DAY_SIGNAL}"::text) AS signal_five_day,
                TRIM("日盤走勢"::text) AS day_trend,
                "日盤轉折支撐" AS day_support,
                "日盤轉折壓力" AS day_resistance,
                LAG("日期") OVER (PARTITION BY futures_id ORDER BY "日期") AS previous_night_date,
                LAG(TRIM("夜盤走勢"::text)) OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS previous_night_trend,
                LAG("夜盤轉折支撐") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS previous_night_support,
                LAG("夜盤轉折壓力") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS previous_night_resistance,
                CASE WHEN COUNT("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) = {settings.range_days}
                THEN AVG("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) END AS average_range_30
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        ), candidates AS (
            SELECT * FROM history WHERE signal_date BETWEEN ? AND ?
        ), opening_contract AS (
            SELECT c.signal_date, k.contract_date,
                ROW_NUMBER() OVER (
                    PARTITION BY c.signal_date
                    ORDER BY k.contract_date, k.bar_datetime
                ) AS contract_rank
            FROM candidates c
            JOIN raw_finmind.fm_taiwan_futures_kbar_1m k
              ON k.futures_id = ?
             AND k.trading_session = 'after_market'
             AND k.contract_date ~ '^[0-9]{{6}}$'
             AND k.bar_datetime >= c.signal_date + ?::time
             AND k.bar_datetime < c.signal_date + ?::time + INTERVAL '1 minute'
        )
        SELECT c.*, oc.contract_date
        FROM candidates c
        LEFT JOIN opening_contract oc
          ON oc.signal_date = c.signal_date AND oc.contract_rank = 1
        ORDER BY c.signal_date
    '''
    return db.sql_to_df(
        sql,
        params=(settings.futures_id, settings.start, settings.end,
                settings.futures_id, settings.entry_time, settings.entry_time),
        parse_dates=["signal_date", "previous_night_date"],
    )


def load_night_ticks(settings: NightSettings, session_date: date, contract_date: str) -> list[Tick]:
    from common import db

    start_at, end_at = session_window(session_date, settings)
    frame = db.sql_to_df('''
        SELECT trade_datetime, price, source_row_no
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ? AND contract_date = ?
          AND trade_datetime >= ? AND trade_datetime <= ?
          AND (trade_datetime::time >= TIME '15:00' OR trade_datetime::time <= TIME '05:00')
        ORDER BY trade_datetime, source_row_no
    ''', params=(settings.futures_id, contract_date, start_at, end_at),
        parse_dates=["trade_datetime"])
    return [
        Tick(r.trade_datetime.to_pydatetime(), float(r.price), int(r.source_row_no))
        for r in frame.itertuples(index=False)
    ]


def classify_entry(ticks: list[Tick], result: dict) -> str | None:
    if result.get("entry_status") != "ENTERED" or not ticks:
        return None
    return "WINDOW_START_IMMEDIATE" if result.get("entry_source_row_no") == ticks[0].source_row_no else "INTRADAY_TOUCH"


def run_backtest(settings: NightSettings, candidates: pd.DataFrame | None = None,
                 tick_loader=load_night_ticks) -> pd.DataFrame:
    source = load_candidates(settings) if candidates is None else candidates.copy()
    rows: list[dict] = []
    for item in source.itertuples(index=False):
        session_date = pd.Timestamp(item.signal_date).date()
        signal = None if pd.isna(item.signal_five_day) else str(item.signal_five_day).strip()
        actual_direction = core_direction(signal)
        average_range = _number(item.average_range_30)
        contract = None if pd.isna(item.contract_date) else str(item.contract_date)
        ticks: list[Tick] | None = None

        for strategy, definition in STRATEGY_DEFINITIONS.items():
            trend, selected_level_type, entry_level, level_signal_date = select_entry_level(item, strategy)
            matches = actual_direction == definition.direction
            base = {
                "strategy": strategy,
                "strategy_description": definition.description,
                "signal_column": FIVE_DAY_SIGNAL,
                "signal_date": session_date,
                "session_date": session_date,
                "signal_direction": signal or None,
                "direction": definition.direction if matches else None,
                "strategy_direction": definition.direction,
                "actual_core_direction": actual_direction,
                "level_source": definition.level_source,
                "level_signal_date": level_signal_date,
                "session_trend": trend,
                "selected_level_type": selected_level_type,
                "entry_level": entry_level,
                "average_range_30": average_range,
                "contract_date": contract,
            }
            if not matches:
                rows.append({**base, "entry_status": "CORE_DIRECTION_MISMATCH", "exit_reason": "NO_ENTRY"})
                continue
            if trend is None:
                rows.append({**base, "entry_status": "MISSING_TREND", "exit_reason": "MISSING_DATA"})
                continue
            if entry_level is None or average_range is None or not contract:
                rows.append({**base, "entry_status": "MISSING_SOURCE_DATA", "exit_reason": "MISSING_DATA"})
                continue
            if ticks is None:
                ticks = tick_loader(settings, session_date, contract)
            result = simulate_limit_trade(
                ticks, definition.direction, entry_level, average_range,
                settings.stop_loss_ratio, settings.take_profit_ratio,
                settings.round_trip_cost_points,
            )
            result["entry_type"] = classify_entry(ticks, result)
            rows.append({**base, **result})

    detail = pd.DataFrame(rows)
    if not detail.empty:
        detail["net_twd"] = (
            pd.to_numeric(detail.get("net_points"), errors="coerce")
            * settings.point_value_twd * settings.contracts
        )
    return detail


def export_reports(detail: pd.DataFrame, settings: NightSettings, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = pd.to_datetime(detail["session_date"]).max().date() if not detail.empty else settings.end
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    detail = detail.copy()
    detail["year"] = pd.to_datetime(detail["session_date"]).dt.year

    summary = summarize(detail.rename(columns={"session_date": "trade_date"}), ["strategy"])
    by_trend = summarize(
        detail[detail["session_trend"].notna()].rename(columns={"session_date": "trade_date"}),
        ["strategy", "session_trend"],
    )
    by_entry_type = summarize(
        detail[detail["entry_type"].notna()].rename(columns={"session_date": "trade_date"}),
        ["strategy", "entry_type"],
    )
    yearly = summarize(
        detail.rename(columns={"session_date": "trade_date"}),
        ["strategy", "year"],
    )

    paths = [
        output_dir / f"night_session_trend_detail_{suffix}.csv",
        output_dir / f"night_session_trend_summary_{suffix}.csv",
        output_dir / f"night_session_trend_by_trend_{suffix}.csv",
        output_dir / f"night_session_trend_by_entry_type_{suffix}.csv",
        output_dir / f"night_session_trend_yearly_{suffix}.csv",
        output_dir / f"night_session_trend_manifest_{suffix}.json",
    ]
    detail.to_csv(paths[0], index=False, encoding="utf-8-sig")
    summary.to_csv(paths[1], index=False, encoding="utf-8-sig")
    by_trend.to_csv(paths[2], index=False, encoding="utf-8-sig")
    by_entry_type.to_csv(paths[3], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths[4], index=False, encoding="utf-8-sig")
    paths[5].write_text(json.dumps({
        "source_view": SOURCE_VIEW,
        "actual_data_end": actual_end.isoformat(),
        "settings": {
            **asdict(settings),
            "start": settings.start.isoformat(),
            "end": settings.end.isoformat(),
            "entry_time": settings.entry_time.isoformat(),
            "forced_exit_time": settings.forced_exit_time.isoformat(),
        },
        "strategies": {key: asdict(value) for key, value in STRATEGY_DEFINITIONS.items()},
        "definitions": {
            "availability": "當日核心與日盤資料自設定的17:00起使用；夜盤型策略只使用當日05:00前已完成的上一夜盤",
            "night_window": "訊號日17:00至次日05:00；設定可調但不得早於15:00",
            "trend_level": "漲勢盤選支撐；跌勢盤選壓力；核心方向決定多空",
            "limit_fill": "多單價格小於等於掛單價成交；空單價格大於等於掛單價成交；首Tick已符合則以較優首Tick價成交",
            "range": "訊號日含當日往回30個完整交易日之(日盤高-日盤低)平均",
            "exit": "成交後依Tick時序觸發15%停損或50%停利；否則次日05:00前最後Tick出場",
        },
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def load_config(path: Path) -> tuple[NightSettings, Path]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    settings = NightSettings(
        start=date.fromisoformat(raw.get("start", "2018-01-01")),
        end=date.fromisoformat(raw.get("end", "2099-12-31")),
        futures_id=raw.get("futures_id", "TX"),
        entry_time=time.fromisoformat(raw.get("entry_time", "17:00:00")),
        forced_exit_time=time.fromisoformat(raw.get("forced_exit_time", "05:00:00")),
        range_days=int(raw.get("range_days", 30)),
        stop_loss_ratio=float(raw.get("stop_loss_ratio", 0.15)),
        take_profit_ratio=float(raw.get("take_profit_ratio", 0.50)),
        round_trip_cost_points=float(raw.get("round_trip_cost_points", 3.0)),
        point_value_twd=float(raw.get("point_value_twd", 200.0)),
        contracts=int(raw.get("contracts", 1)),
    )
    return settings, Path(raw.get("output_dir", "backtest_output/futures_session_trend_night"))


def main() -> None:
    parser = argparse.ArgumentParser(description="TX 2-2.11～2-2.14 夜盤走勢選點當沖回測")
    parser.add_argument(
        "--config", type=Path,
        default=Path("backtest/configs/futures_session_trend_night_v1.json"),
    )
    args = parser.parse_args()
    settings, output_dir = load_config(args.config)
    detail = run_backtest(settings)
    paths = export_reports(detail, settings, output_dir)
    print(summarize(detail.rename(columns={"session_date": "trade_date"}), ["strategy"]).to_string(index=False))
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
