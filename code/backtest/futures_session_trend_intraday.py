from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date, time
from pathlib import Path

import pandas as pd

from backtest.futures_turning_level_intraday import (
    FIVE_DAY_SIGNAL,
    SOURCE_VIEW,
    Settings,
    Tick,
    load_day_ticks,
    simulate_limit_trade,
    summarize,
)


@dataclass(frozen=True)
class TrendStrategyDefinition:
    code: str
    direction: str
    level_source: str
    description: str


STRATEGY_DEFINITIONS = {
    "2-2.7": TrendStrategyDefinition(
        "2-2.7", "LONG", "DAY",
        "五日核心多；日盤漲勢掛日盤支撐，日盤跌勢掛日盤壓力",
    ),
    "2-2.8": TrendStrategyDefinition(
        "2-2.8", "SHORT", "DAY",
        "五日核心空；日盤漲勢掛日盤支撐，日盤跌勢掛日盤壓力",
    ),
    "2-2.9": TrendStrategyDefinition(
        "2-2.9", "LONG", "NIGHT",
        "五日核心多；夜盤漲勢掛夜盤支撐，夜盤跌勢掛夜盤壓力",
    ),
    "2-2.10": TrendStrategyDefinition(
        "2-2.10", "SHORT", "NIGHT",
        "五日核心空；夜盤漲勢掛夜盤支撐，夜盤跌勢掛夜盤壓力",
    ),
}


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if pd.notna(result) else None


def normalize_trend(value: object) -> str | None:
    text = "" if value is None else str(value).strip()
    return text if text in {"漲勢盤", "跌勢盤"} else None


def core_direction(value: object) -> str | None:
    text = "" if value is None else str(value).strip()
    if text == "多":
        return "LONG"
    if text == "空":
        return "SHORT"
    return None


def select_entry_level(row: object, strategy: str) -> tuple[str | None, str | None, float | None]:
    definition = STRATEGY_DEFINITIONS[strategy]
    prefix = "day" if definition.level_source == "DAY" else "night"
    trend = normalize_trend(getattr(row, f"{prefix}_trend", None))
    if trend == "漲勢盤":
        return trend, "SUPPORT", _number(getattr(row, f"{prefix}_support", None))
    if trend == "跌勢盤":
        return trend, "RESISTANCE", _number(getattr(row, f"{prefix}_resistance", None))
    return None, None, None


def load_candidates(settings: Settings) -> pd.DataFrame:
    from common import db

    sql = f'''
        WITH history AS (
            SELECT futures_id, "日期" AS signal_date,
                TRIM("{FIVE_DAY_SIGNAL}"::text) AS signal_five_day,
                TRIM("日盤走勢"::text) AS day_trend,
                TRIM("夜盤走勢"::text) AS night_trend,
                "日盤轉折支撐" AS day_support,
                "日盤轉折壓力" AS day_resistance,
                "夜盤轉折支撐" AS night_support,
                "夜盤轉折壓力" AS night_resistance,
                CASE WHEN COUNT("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) = {settings.range_days}
                THEN AVG("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) END AS average_range_30,
                LEAD("日期") OVER (PARTITION BY futures_id ORDER BY "日期") AS trade_date
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        ), candidates AS (
            SELECT * FROM history WHERE trade_date BETWEEN ? AND ?
        ), opening_contract AS (
            SELECT k.trading_date, k.contract_date,
                ROW_NUMBER() OVER (
                    PARTITION BY k.trading_date
                    ORDER BY k.contract_date, k.bar_datetime
                ) AS contract_rank
            FROM raw_finmind.fm_taiwan_futures_kbar_1m k
            JOIN candidates c ON c.trade_date = k.trading_date
            WHERE k.futures_id = ?
              AND k.trading_session = 'position'
              AND k.contract_date ~ '^[0-9]{{6}}$'
              AND k.bar_datetime::time BETWEEN ? AND (? + INTERVAL '1 minute')
        )
        SELECT c.*, oc.contract_date
        FROM candidates c
        LEFT JOIN opening_contract oc
          ON oc.trading_date = c.trade_date AND oc.contract_rank = 1
        ORDER BY c.trade_date
    '''
    return db.sql_to_df(
        sql,
        params=(settings.futures_id, settings.start, settings.end,
                settings.futures_id, settings.entry_time, settings.entry_time),
        parse_dates=["signal_date", "trade_date"],
    )


def classify_entry(ticks: list[Tick], result: dict) -> str | None:
    if result.get("entry_status") != "ENTERED" or not ticks:
        return None
    return "OPENING_IMMEDIATE" if result.get("entry_source_row_no") == ticks[0].source_row_no else "INTRADAY_TOUCH"


def run_backtest(settings: Settings, candidates: pd.DataFrame | None = None,
                 tick_loader=load_day_ticks) -> pd.DataFrame:
    source = load_candidates(settings) if candidates is None else candidates.copy()
    rows: list[dict] = []
    for item in source.itertuples(index=False):
        signal = None if pd.isna(item.signal_five_day) else str(item.signal_five_day).strip()
        actual_direction = core_direction(signal)
        average_range = _number(item.average_range_30)
        contract = None if pd.isna(item.contract_date) else str(item.contract_date)
        ticks: list[Tick] | None = None

        for strategy, definition in STRATEGY_DEFINITIONS.items():
            trend, selected_level_type, entry_level = select_entry_level(item, strategy)
            base = {
                "strategy": strategy,
                "strategy_description": definition.description,
                "signal_column": FIVE_DAY_SIGNAL,
                "signal_date": pd.Timestamp(item.signal_date).date(),
                "trade_date": pd.Timestamp(item.trade_date).date(),
                "signal_direction": signal or None,
                "direction": definition.direction if actual_direction == definition.direction else None,
                "strategy_direction": definition.direction,
                "actual_core_direction": actual_direction,
                "level_source": definition.level_source,
                "session_trend": trend,
                "selected_level_type": selected_level_type,
                "entry_level": entry_level,
                "average_range_30": average_range,
                "contract_date": contract,
            }
            if actual_direction != definition.direction:
                rows.append({**base, "entry_status": "CORE_DIRECTION_MISMATCH", "exit_reason": "NO_ENTRY"})
                continue
            if trend is None:
                rows.append({**base, "entry_status": "MISSING_TREND", "exit_reason": "MISSING_DATA"})
                continue
            if entry_level is None or average_range is None or not contract:
                rows.append({**base, "entry_status": "MISSING_SOURCE_DATA", "exit_reason": "MISSING_DATA"})
                continue
            if ticks is None:
                ticks = tick_loader(settings, pd.Timestamp(item.trade_date).date(), contract)
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


def export_reports(detail: pd.DataFrame, settings: Settings, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = pd.to_datetime(detail["trade_date"]).max().date() if not detail.empty else settings.end
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    detail = detail.copy()
    detail["year"] = pd.to_datetime(detail["trade_date"]).dt.year

    summary = summarize(detail, ["strategy"])
    by_trend = summarize(
        detail[detail["session_trend"].notna()],
        ["strategy", "session_trend"],
    )
    by_entry_type = summarize(
        detail[detail["entry_type"].notna()],
        ["strategy", "entry_type"],
    )
    yearly = summarize(detail, ["strategy", "year"])

    paths = [
        output_dir / f"session_trend_detail_{suffix}.csv",
        output_dir / f"session_trend_summary_{suffix}.csv",
        output_dir / f"session_trend_by_trend_{suffix}.csv",
        output_dir / f"session_trend_by_entry_type_{suffix}.csv",
        output_dir / f"session_trend_yearly_{suffix}.csv",
        output_dir / f"session_trend_manifest_{suffix}.json",
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
            "signal_alignment": "訊號日的五日核心、日盤資料及其後完成夜盤，用於下一交易日日盤",
            "trend_level": "漲勢盤選支撐；跌勢盤選壓力；核心方向決定多空",
            "limit_fill": "多單價格小於等於掛單價成交；空單價格大於等於掛單價成交；首Tick已符合則以較優首Tick價成交",
            "entry_type": "OPENING_IMMEDIATE為首Tick立即成交；INTRADAY_TOUCH為盤中才觸價",
            "range": "訊號日含當日往回30個完整交易日之(日盤高-日盤低)平均",
            "exit": "成交後依Tick時序觸發15%停損或50%停利；否則日盤收盤前最後Tick出場",
        },
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def load_config(path: Path) -> tuple[Settings, Path]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    settings = Settings(
        start=date.fromisoformat(raw.get("start", "2018-01-01")),
        end=date.fromisoformat(raw.get("end", "2099-12-31")),
        futures_id=raw.get("futures_id", "TX"),
        entry_time=time.fromisoformat(raw.get("entry_time", "08:45:00")),
        forced_exit_time=time.fromisoformat(raw.get("forced_exit_time", "13:45:00")),
        range_days=int(raw.get("range_days", 30)),
        stop_loss_ratio=float(raw.get("stop_loss_ratio", 0.15)),
        take_profit_ratio=float(raw.get("take_profit_ratio", 0.50)),
        entry_filter_ratio=0.0,
        round_trip_cost_points=float(raw.get("round_trip_cost_points", 3.0)),
        point_value_twd=float(raw.get("point_value_twd", 200.0)),
        contracts=int(raw.get("contracts", 1)),
    )
    return settings, Path(raw.get("output_dir", "backtest_output/futures_session_trend"))


def main() -> None:
    parser = argparse.ArgumentParser(description="TX 2-2.7～2-2.10 日盤走勢選點當沖回測")
    parser.add_argument(
        "--config", type=Path,
        default=Path("backtest/configs/futures_session_trend_intraday_v1.json"),
    )
    args = parser.parse_args()
    settings, output_dir = load_config(args.config)
    detail = run_backtest(settings)
    paths = export_reports(detail, settings, output_dir)
    print(summarize(detail, ["strategy"]).to_string(index=False))
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
