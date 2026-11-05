from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence

import pandas as pd

from backtest.futures_combination_exit_backtest import (
    Settings as ExitSettings,
    _number,
    load_contract_map,
    load_day_ticks,
    simulate_open_trade,
    simulate_wait_entry_trade,
    summarize,
)
from backtest.futures_combination_signal_backtest import Settings as SignalSettings, load_source
from backtest.futures_retail_extreme_expansion_backtest import (
    Settings as RetailSettings,
    build_candidates,
    prepare_research_daily,
)
from backtest.futures_turning_level_intraday import Tick


ANALYSIS_NAME = "futures_retail_bounce_reentry_comparison_backtest"
VERSION = "1.0.0"


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    history_window: int = 252
    minimum_history: int = 60
    range_days: int = 30
    ma5_exclude_percentile: float = 80.0
    entry_distance_ratio: float = 5 / 12
    retail_threshold_pct: float = 20.0
    pre_entry_stop_ratio: float = 1 / 4
    bounce_ratio: float = 1 / 6
    execution_delay_minutes: float = 1.0
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0


VARIANTS = (
    ("CANCEL_DAY", "原規則：前30分鐘破失效價則取消當日交易"),
    ("DIRECT_ENTRY", "08:45起觸及5/12R即進場，不做破價過濾"),
    ("BOUNCE_1_6R", "破失效價後，由動態低點反彈1/6R，再延遲1分鐘進場"),
)


def build_target_candidates(settings: Settings) -> pd.DataFrame:
    signal_settings = SignalSettings(
        start=settings.start,
        end=settings.end,
        futures_id=settings.futures_id,
        core_column="策略_核心_五日價量",
        history_window=settings.history_window,
        minimum_history=settings.minimum_history,
        round_trip_cost_points=settings.round_trip_cost_points,
    )
    retail_settings = RetailSettings(
        start=settings.start,
        end=settings.end,
        futures_id=settings.futures_id,
        history_window=settings.history_window,
        minimum_history=settings.minimum_history,
        range_days=settings.range_days,
        ma5_exclude_percentile=settings.ma5_exclude_percentile,
        stop_loss_ratio=settings.pre_entry_stop_ratio,
        round_trip_cost_points=settings.round_trip_cost_points,
        point_value_twd=settings.point_value_twd,
    )
    daily = prepare_research_daily(load_source(signal_settings), retail_settings)
    candidates = build_candidates(daily, retail_settings)
    return candidates[
        (candidates["study"] == "A_ENTRY_DISTANCE")
        & (candidates["scenario"] == "A_ENTRY_5_12R")
        & (pd.to_numeric(candidates["retail_threshold_pct"], errors="coerce") == settings.retail_threshold_pct)
    ].copy()


def simulate_bounce_reentry(
    ticks: Sequence[Tick],
    night_close: float,
    average_range: float,
    settings: Settings,
    exit_settings: ExitSettings,
) -> dict:
    if not ticks:
        return {"entry_status": "MISSING_TICKS", "exit_reason": "MISSING_DATA"}

    target_entry_price = night_close - average_range * settings.entry_distance_ratio
    pre_entry_stop_price = target_entry_price - average_range * settings.pre_entry_stop_ratio
    bounce_distance = average_range * settings.bounce_ratio
    observation = [tick for tick in ticks if tick.at.time() < exit_settings.entry_allowed_time]
    breach_tick = next((tick for tick in observation if tick.price <= pre_entry_stop_price), None)

    # 前30分鐘沒有跌破失效價，完整沿用原本09:15後等待5/12R的規則。
    if breach_tick is None:
        result = simulate_wait_entry_trade(
            ticks,
            "LONG",
            night_close,
            average_range,
            entry_allowed_time=exit_settings.entry_allowed_time,
            entry_distance_ratio=settings.entry_distance_ratio,
            stop_enabled=False,
            stop_loss_ratio=settings.pre_entry_stop_ratio,
            activation_ratio=None,
            drawdown_ratio=None,
            round_trip_cost_points=settings.round_trip_cost_points,
        )
        return {**result, "reentry_mode": False, "bounce_distance_points": bounce_distance}

    base = {
        "night_close": night_close,
        "target_entry_price": target_entry_price,
        "pre_entry_stop_price": pre_entry_stop_price,
        "entry_allowed_at": datetime.combine(ticks[0].at.date(), exit_settings.entry_allowed_time),
        "pre_entry_stop_crossed": True,
        "pre_entry_stop_crossed_at": breach_tick.at,
        "reentry_mode": True,
        "bounce_distance_points": bounce_distance,
    }

    running_low = breach_tick.price
    running_low_at = breach_tick.at
    trigger_tick: Tick | None = None
    trigger_low = running_low
    trigger_low_at = running_low_at

    for tick in ticks:
        if tick.at < breach_tick.at:
            continue
        if tick.price < running_low:
            running_low = tick.price
            running_low_at = tick.at
        if tick.at > running_low_at and tick.price >= running_low + bounce_distance:
            trigger_tick = tick
            trigger_low = running_low
            trigger_low_at = running_low_at
            break

    if trigger_tick is None:
        return {
            **base,
            "lowest_price_after_breach": running_low,
            "lowest_price_after_breach_at": running_low_at,
            "entry_status": "NO_BOUNCE_TRIGGER",
            "exit_reason": "NO_ENTRY",
        }

    scheduled_entry_at = trigger_tick.at + timedelta(minutes=settings.execution_delay_minutes)
    eligible = [tick for tick in ticks if tick.at >= scheduled_entry_at]
    if not eligible or scheduled_entry_at.time() >= exit_settings.forced_exit_time:
        return {
            **base,
            "bounce_low_price": trigger_low,
            "bounce_low_at": trigger_low_at,
            "bounce_trigger_price": trigger_tick.price,
            "bounce_trigger_at": trigger_tick.at,
            "scheduled_entry_at": scheduled_entry_at,
            "entry_status": "NO_TIME_AFTER_TRIGGER",
            "exit_reason": "NO_ENTRY",
        }

    entry_tick = eligible[0]
    result = simulate_open_trade(
        eligible,
        "LONG",
        average_range,
        stop_enabled=False,
        stop_loss_ratio=settings.pre_entry_stop_ratio,
        activation_ratio=None,
        drawdown_ratio=None,
        round_trip_cost_points=settings.round_trip_cost_points,
    )
    return {
        **base,
        "bounce_low_price": trigger_low,
        "bounce_low_at": trigger_low_at,
        "bounce_trigger_price": trigger_tick.price,
        "bounce_trigger_at": trigger_tick.at,
        "actual_bounce_points": trigger_tick.price - trigger_low,
        "scheduled_entry_at": scheduled_entry_at,
        "breach_to_low_minutes": (trigger_low_at - breach_tick.at).total_seconds() / 60,
        "low_to_trigger_minutes": (trigger_tick.at - trigger_low_at).total_seconds() / 60,
        "breach_to_trigger_minutes": (trigger_tick.at - breach_tick.at).total_seconds() / 60,
        "entry_delay_from_trigger_minutes": (entry_tick.at - trigger_tick.at).total_seconds() / 60,
        "entry_vs_target_points": entry_tick.price - target_entry_price,
        "entry_vs_pre_stop_points": entry_tick.price - pre_entry_stop_price,
        "entry_vs_trigger_points": entry_tick.price - trigger_tick.price,
        "post_entry_broke_bounce_low": any(tick.price < trigger_low for tick in eligible),
        **result,
    }


def run_backtest(settings: Settings) -> pd.DataFrame:
    candidates = build_target_candidates(settings)
    exit_settings = ExitSettings(
        start=settings.start,
        end=settings.end,
        futures_id=settings.futures_id,
        range_days=settings.range_days,
        stop_loss_ratio=settings.pre_entry_stop_ratio,
        round_trip_cost_points=settings.round_trip_cost_points,
        point_value_twd=settings.point_value_twd,
    )
    contracts = load_contract_map(exit_settings)
    tick_cache: dict[tuple[date, str], list[Tick]] = {}
    rows: list[dict] = []
    descriptions = dict(VARIANTS)

    for item in candidates.itertuples(index=False):
        trade_date = item.交易日
        contract = contracts.get(trade_date)
        average_range = _number(item.平均振幅30)
        night_close = _number(item.夜盤收)
        common = {
            "signal_date": item.訊號日,
            "trade_date": trade_date,
            "signal_name": "核心多＋綜合散戶極度偏空＋排除正乖離過大",
            "direction": "LONG",
            "entry_distance_ratio": settings.entry_distance_ratio,
            "retail_threshold_pct": settings.retail_threshold_pct,
            "retail_percentile_pct": item.散戶多空比_歷史分位_pct,
            "ma5_bias_percentile_pct": item.MA5乖離_歷史分位_pct,
            "previous_day_close": item.日盤收,
            "contract_date": contract,
        }
        if not contract or average_range is None or night_close is None:
            for variant, description in VARIANTS:
                rows.append({**common, "variant": variant, "variant_description": description, "entry_status": "MISSING_REFERENCE", "exit_reason": "MISSING_DATA"})
            continue

        key = (trade_date, contract)
        if key not in tick_cache:
            tick_cache[key] = load_day_ticks(exit_settings, trade_date, contract)
        ticks = tick_cache[key]

        baseline = simulate_wait_entry_trade(
            ticks, "LONG", night_close, average_range,
            entry_allowed_time=exit_settings.entry_allowed_time,
            entry_distance_ratio=settings.entry_distance_ratio,
            stop_enabled=False,
            stop_loss_ratio=settings.pre_entry_stop_ratio,
            activation_ratio=None,
            drawdown_ratio=None,
            round_trip_cost_points=settings.round_trip_cost_points,
        )
        direct = simulate_wait_entry_trade(
            ticks, "LONG", night_close, average_range,
            entry_allowed_time=exit_settings.entry_time,
            entry_distance_ratio=settings.entry_distance_ratio,
            stop_enabled=False,
            stop_loss_ratio=settings.pre_entry_stop_ratio,
            activation_ratio=None,
            drawdown_ratio=None,
            round_trip_cost_points=settings.round_trip_cost_points,
        )
        bounce = (
            simulate_bounce_reentry(ticks, night_close, average_range, settings, exit_settings)
            if baseline.get("entry_status") == "CANCELLED_PRE_ENTRY_STOP"
            else {**baseline, "reentry_mode": False, "bounce_distance_points": average_range * settings.bounce_ratio}
        )
        results = {"CANCEL_DAY": baseline, "DIRECT_ENTRY": direct, "BOUNCE_1_6R": bounce}
        for variant, result in results.items():
            rows.append({**common, "variant": variant, "variant_description": descriptions[variant], **result})

    detail = pd.DataFrame(rows)
    detail["year"] = pd.to_datetime(detail["trade_date"]).dt.year
    detail["net_twd"] = pd.to_numeric(detail.get("net_points"), errors="coerce") * settings.point_value_twd
    return detail


def export_outputs(detail: pd.DataFrame, settings: Settings, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end = pd.to_datetime(detail["trade_date"]).max().date()
    suffix = f"{settings.start:%Y%m%d}_{end:%Y%m%d}"
    groups = ["variant", "variant_description", "entry_distance_ratio", "retail_threshold_pct"]
    summary = summarize(detail, groups)
    yearly = summarize(detail, groups + ["year"])
    status = detail.groupby(["variant", "entry_status"], dropna=False).size().rename("count").reset_index()
    rescued = detail[(detail["variant"] == "BOUNCE_1_6R") & (detail["reentry_mode"] == True)].copy()  # noqa: E712
    paths = {
        "detail": output_dir / f"retail_bounce_reentry_detail_{suffix}.csv",
        "summary": output_dir / f"retail_bounce_reentry_summary_{suffix}.csv",
        "yearly": output_dir / f"retail_bounce_reentry_yearly_{suffix}.csv",
        "entry_status": output_dir / f"retail_bounce_reentry_entry_status_{suffix}.csv",
        "rescued_detail": output_dir / f"retail_bounce_reentry_rescued_detail_{suffix}.csv",
        "manifest": output_dir / f"retail_bounce_reentry_manifest_{suffix}.json",
    }
    detail.to_csv(paths["detail"], index=False, encoding="utf-8-sig")
    summary.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths["yearly"], index=False, encoding="utf-8-sig")
    status.to_csv(paths["entry_status"], index=False, encoding="utf-8-sig")
    rescued.to_csv(paths["rescued_detail"], index=False, encoding="utf-8-sig")
    paths["manifest"].write_text(
        json.dumps(
            {
                "run_id": str(uuid.uuid4()),
                "analysis": ANALYSIS_NAME,
                "version": VERSION,
                "settings": {key: value.isoformat() if isinstance(value, date) else value for key, value in asdict(settings).items()},
                "variants": dict(VARIANTS),
                "bounce_rule": "前30分鐘破失效價後，自跌破以來動態最低價反彈1/6R即觸發；創新低則重算",
                "execution_rule": "觸發後等待完整1分鐘，以觸發時刻+1分鐘後第一筆成交價進場",
                "exit_rule": "進場後不設停損停利，持有至日盤收盤；每筆扣3點成本",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--cost-points", type=float, default=3.0)
    parser.add_argument("--output-dir", type=Path, default=Path("output/backtest/retail_bounce_reentry"))
    args = parser.parse_args()
    settings = Settings(start=args.start, end=args.end, round_trip_cost_points=args.cost_points)
    detail = run_backtest(settings)
    paths = export_outputs(detail, settings, args.output_dir)
    print(summarize(detail, ["variant", "variant_description"]).to_string(index=False))
    for name, path in paths.items():
        print(f"[retail_bounce_reentry] {name}: {path.resolve()}")


if __name__ == "__main__":
    main()
