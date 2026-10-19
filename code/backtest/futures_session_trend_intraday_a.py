from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, time
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd

from backtest.futures_session_trend_intraday import (
    TrendStrategyDefinition,
    core_direction,
    normalize_trend,
)
from backtest.futures_turning_level_intraday import (
    FIVE_DAY_SIGNAL,
    SOURCE_VIEW,
    Tick,
    load_day_ticks,
    summarize,
)


STRATEGY_DEFINITIONS = {
    "2-2-A.7": TrendStrategyDefinition(
        "2-2-A.7", "LONG", "DAY",
        "五日核心多；日盤漲勢掛日盤支撐，日盤跌勢掛日盤壓力",
    ),
    "2-2-A.8": TrendStrategyDefinition(
        "2-2-A.8", "SHORT", "DAY",
        "五日核心空；日盤漲勢掛日盤支撐，日盤跌勢掛日盤壓力",
    ),
    "2-2-A.9": TrendStrategyDefinition(
        "2-2-A.9", "LONG", "NIGHT",
        "五日核心多；夜盤漲勢掛夜盤支撐，夜盤跌勢掛夜盤壓力",
    ),
    "2-2-A.10": TrendStrategyDefinition(
        "2-2-A.10", "SHORT", "NIGHT",
        "五日核心空；夜盤漲勢掛夜盤支撐，夜盤跌勢掛夜盤壓力",
    ),
}


@dataclass(frozen=True)
class Settings:
    start: date
    end: date
    futures_id: str = "TX"
    entry_time: time = time(8, 45)
    forced_exit_time: time = time(13, 45)
    range_days: int = 30
    stop_loss_ratio: float = 0.25
    trailing_activation_ratio: float = 0.25
    trailing_giveback_ratio: float = 0.50
    take_profit_ratio: float = 0.50
    entry_filter_ratio: float = 0.30
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0
    contracts: int = 1

    def __post_init__(self) -> None:
        if self.start != date(2018, 1, 1):
            raise ValueError("完整回測的 start 固定為 2018-01-01")
        if self.start > self.end or self.entry_time > self.forced_exit_time:
            raise ValueError("日期或日盤時間範圍錯誤")
        ratios = (
            self.stop_loss_ratio, self.trailing_activation_ratio,
            self.trailing_giveback_ratio, self.take_profit_ratio,
        )
        if self.range_days < 1 or min(ratios) <= 0:
            raise ValueError("振幅天數及各項比例必須大於 0")
        if self.trailing_giveback_ratio > 1:
            raise ValueError("獲利回吐比例不可大於 1")
        if self.trailing_activation_ratio > self.take_profit_ratio:
            raise ValueError("移動停利啟動比例不可大於固定停利比例")
        if self.entry_filter_ratio < 0 or self.round_trip_cost_points < 0:
            raise ValueError("進場篩選與成本不可小於 0")
        if self.point_value_twd <= 0 or self.contracts < 1:
            raise ValueError("每點價值與口數必須大於 0")


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def select_entry_level(row: object, strategy: str) -> tuple[str | None, str | None, float | None]:
    definition = STRATEGY_DEFINITIONS[strategy]
    prefix = "day" if definition.level_source == "DAY" else "night"
    trend = normalize_trend(getattr(row, f"{prefix}_trend", None))
    if trend == "漲勢盤":
        return trend, "SUPPORT", _number(getattr(row, f"{prefix}_support", None))
    if trend == "跌勢盤":
        return trend, "RESISTANCE", _number(getattr(row, f"{prefix}_resistance", None))
    return None, None, None


def simulate_limit_trade(
    ticks: Sequence[Tick], direction: str, entry_level: float,
    average_range: float, stop_loss_ratio: float,
    trailing_activation_ratio: float, trailing_giveback_ratio: float,
    take_profit_ratio: float, round_trip_cost_points: float = 0.0,
) -> dict:
    """限價成交後依 Tick 時序執行固定停損、固定停利及浮盈回吐停利。"""
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction 必須是 LONG 或 SHORT")
    if not ticks:
        return {"entry_status": "MISSING_TICKS", "exit_reason": "MISSING_DATA"}

    touched: Callable[[Tick], bool] = (
        (lambda tick: tick.price <= entry_level)
        if direction == "LONG" else
        (lambda tick: tick.price >= entry_level)
    )
    entry_index = next((i for i, tick in enumerate(ticks) if touched(tick)), None)
    if entry_index is None:
        return {"entry_status": "NOT_TOUCHED", "exit_reason": "NO_ENTRY"}

    sign = 1.0 if direction == "LONG" else -1.0
    entry_tick = ticks[entry_index]
    entry_price = (
        min(entry_level, entry_tick.price)
        if direction == "LONG" else
        max(entry_level, entry_tick.price)
    )
    stop_price = entry_price - sign * average_range * stop_loss_ratio
    activation_distance = average_range * trailing_activation_ratio
    activation_price = entry_price + sign * activation_distance
    fixed_take_profit_price = entry_price + sign * average_range * take_profit_ratio

    best_price = entry_price
    best_profit_points = 0.0
    trailing_activated = False
    activation_tick: Tick | None = None
    trailing_stop_price: float | None = None
    exit_tick, exit_price, reason = ticks[-1], ticks[-1].price, "CLOSE_EXIT"

    for tick in ticks[entry_index + 1:]:
        if sign * (tick.price - stop_price) <= 0:
            exit_tick, exit_price, reason = tick, tick.price, "STOP_LOSS"
            break
        if sign * (tick.price - fixed_take_profit_price) >= 0:
            exit_tick, exit_price, reason = tick, fixed_take_profit_price, "FIXED_TAKE_PROFIT"
            best_price = tick.price
            best_profit_points = max(best_profit_points, sign * (tick.price - entry_price))
            break

        current_profit = sign * (tick.price - entry_price)
        if current_profit > best_profit_points:
            best_profit_points = current_profit
            best_price = tick.price
        if not trailing_activated and best_profit_points >= activation_distance:
            trailing_activated = True
            activation_tick = tick
        if trailing_activated:
            retained_profit = best_profit_points * (1.0 - trailing_giveback_ratio)
            trailing_stop_price = entry_price + sign * retained_profit
            if sign * (tick.price - trailing_stop_price) <= 0:
                exit_tick, exit_price, reason = tick, tick.price, "TRAILING_TAKE_PROFIT"
                break

    gross_points = sign * (exit_price - entry_price)
    return {
        "entry_status": "ENTERED",
        "entry_at": entry_tick.at,
        "entry_source_row_no": entry_tick.source_row_no,
        "theoretical_entry_price": entry_level,
        "actual_entry_price": entry_price,
        "stop_loss_price": stop_price,
        "trailing_activation_price": activation_price,
        "fixed_take_profit_price": fixed_take_profit_price,
        "trailing_activated": trailing_activated,
        "trailing_activation_at": activation_tick.at if activation_tick else None,
        "best_price_after_entry": best_price,
        "maximum_favorable_points": best_profit_points,
        "last_trailing_stop_price": trailing_stop_price,
        "exit_at": exit_tick.at,
        "exit_source_row_no": exit_tick.source_row_no,
        "actual_exit_price": exit_price,
        "exit_reason": reason,
        "gross_points": gross_points,
        "cost_points": round_trip_cost_points,
        "net_points": gross_points - round_trip_cost_points,
    }


def load_candidates(settings: Settings) -> pd.DataFrame:
    from common import db

    sql = f'''
        WITH history AS (
            SELECT futures_id, "日期" AS signal_date,
                TRIM("{FIVE_DAY_SIGNAL}"::text) AS signal_five_day,
                TRIM("日盤走勢"::text) AS day_trend,
                TRIM("夜盤走勢"::text) AS night_trend,
                "夜盤收" AS night_close,
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
    return (
        "OPENING_IMMEDIATE"
        if result.get("entry_source_row_no") == ticks[0].source_row_no
        else "INTRADAY_TOUCH"
    )


def run_backtest(settings: Settings, candidates: pd.DataFrame | None = None,
                 tick_loader=load_day_ticks) -> pd.DataFrame:
    source = load_candidates(settings) if candidates is None else candidates.copy()
    rows: list[dict] = []
    for item in source.itertuples(index=False):
        signal = None if pd.isna(item.signal_five_day) else str(item.signal_five_day).strip()
        actual_direction = core_direction(signal)
        average_range = _number(item.average_range_30)
        night_close = _number(item.night_close)
        contract = None if pd.isna(item.contract_date) else str(item.contract_date)
        ticks: list[Tick] | None = None

        for strategy, definition in STRATEGY_DEFINITIONS.items():
            trend, selected_level_type, entry_level = select_entry_level(item, strategy)
            entry_distance = (
                abs(entry_level - night_close)
                if entry_level is not None and night_close is not None else None
            )
            required_distance = (
                average_range * settings.entry_filter_ratio
                if average_range is not None else None
            )
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
                "night_close": night_close,
                "average_range_30": average_range,
                "entry_distance_from_night_close": entry_distance,
                "filter_distance_required": required_distance,
                "contract_date": contract,
            }
            if actual_direction != definition.direction:
                rows.append({**base, "entry_status": "CORE_DIRECTION_MISMATCH", "exit_reason": "NO_ENTRY"})
                continue
            if trend is None:
                rows.append({**base, "entry_status": "MISSING_TREND", "exit_reason": "MISSING_DATA"})
                continue
            if entry_level is None or average_range is None or night_close is None or not contract:
                rows.append({**base, "entry_status": "MISSING_SOURCE_DATA", "exit_reason": "MISSING_DATA"})
                continue
            if entry_distance < required_distance:
                rows.append({
                    **base, "entry_status": "DISTANCE_FILTERED", "exit_reason": "NO_ENTRY",
                    "gross_points": 0.0, "cost_points": 0.0, "net_points": 0.0,
                })
                continue
            if ticks is None:
                ticks = tick_loader(settings, pd.Timestamp(item.trade_date).date(), contract)
            result = simulate_limit_trade(
                ticks, definition.direction, entry_level, average_range,
                settings.stop_loss_ratio, settings.trailing_activation_ratio,
                settings.trailing_giveback_ratio, settings.take_profit_ratio,
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


def _summary(detail: pd.DataFrame, groups: list[str]) -> pd.DataFrame:
    # 共用摘要器把 MISSING_SOURCE_DATA 排除於可掛單分母；A 版另有
    # MISSING_TREND，統計時同樣視為來源資料不足，但保留明細原始狀態。
    normalized = detail.copy()
    normalized.loc[
        normalized["entry_status"] == "MISSING_TREND", "entry_status"
    ] = "MISSING_SOURCE_DATA"
    result = summarize(normalized, groups)
    counts = []
    for keys, frame in detail.groupby(groups, sort=True, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        entered = frame[frame["entry_status"] == "ENTERED"]
        row = dict(zip(groups, keys))
        row.update({
            "fixed_take_profit_count": int((entered["exit_reason"] == "FIXED_TAKE_PROFIT").sum()),
            "trailing_take_profit_count": int((entered["exit_reason"] == "TRAILING_TAKE_PROFIT").sum()),
            "trailing_activated_count": int(entered.get("trailing_activated", pd.Series(dtype=bool)).fillna(False).sum()),
        })
        counts.append(row)
    if counts:
        result = result.drop(columns=["take_profit_count"], errors="ignore").merge(
            pd.DataFrame(counts), on=groups, how="left"
        )
    return result


def export_reports(detail: pd.DataFrame, settings: Settings, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = pd.to_datetime(detail["trade_date"]).max().date() if not detail.empty else settings.end
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    detail = detail.copy()
    detail["year"] = pd.to_datetime(detail["trade_date"]).dt.year

    summary = _summary(detail, ["strategy"])
    by_trend = _summary(detail[detail["session_trend"].notna()], ["strategy", "session_trend"])
    by_entry_type = _summary(detail[detail["entry_type"].notna()], ["strategy", "entry_type"])
    yearly = _summary(detail, ["strategy", "year"])
    paths = [
        output_dir / f"session_trend_a_detail_{suffix}.csv",
        output_dir / f"session_trend_a_summary_{suffix}.csv",
        output_dir / f"session_trend_a_by_trend_{suffix}.csv",
        output_dir / f"session_trend_a_by_entry_type_{suffix}.csv",
        output_dir / f"session_trend_a_yearly_{suffix}.csv",
        output_dir / f"session_trend_a_manifest_{suffix}.json",
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
            "signal_alignment": "訊號日五日核心、日夜盤走勢與轉折點，用於下一交易日日盤",
            "trend_level": "漲勢盤選支撐；跌勢盤選壓力；核心方向決定多空",
            "distance_filter": "abs(進場點-昨日夜盤收) >= 30日平均振幅*30%",
            "limit_fill": "多單價格小於等於掛單價成交；空單價格大於等於掛單價成交；首Tick已符合則以較優首Tick價成交",
            "stop_loss": "成交價反向移動30日平均振幅25%時停損",
            "take_profit": "獲利達25%振幅後啟動浮盈回吐50%停利；達50%振幅則固定停利",
            "exit_priority": "逐Tick依序先檢查停損，再檢查50%固定停利，再更新並檢查移動停利；未觸發則收盤出場",
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
        stop_loss_ratio=float(raw.get("stop_loss_ratio", 0.25)),
        trailing_activation_ratio=float(raw.get("trailing_activation_ratio", 0.25)),
        trailing_giveback_ratio=float(raw.get("trailing_giveback_ratio", 0.50)),
        take_profit_ratio=float(raw.get("take_profit_ratio", 0.50)),
        entry_filter_ratio=float(raw.get("entry_filter_ratio", 0.30)),
        round_trip_cost_points=float(raw.get("round_trip_cost_points", 3.0)),
        point_value_twd=float(raw.get("point_value_twd", 200.0)),
        contracts=int(raw.get("contracts", 1)),
    )
    return settings, Path(raw.get("output_dir", "backtest_output/futures_session_trend_a"))


def main() -> None:
    parser = argparse.ArgumentParser(description="TX 2-2-A.7～2-2-A.10 日盤移動停利回測")
    parser.add_argument(
        "--config", type=Path,
        default=Path("backtest/configs/futures_session_trend_intraday_a_v1.json"),
    )
    args = parser.parse_args()
    settings, output_dir = load_config(args.config)
    detail = run_backtest(settings)
    paths = export_reports(detail, settings, output_dir)
    print(_summary(detail, ["strategy"]).to_string(index=False))
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
