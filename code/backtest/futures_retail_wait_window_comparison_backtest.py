from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from backtest.futures_combination_exit_backtest import (
    Settings as ExitSettings,
    _number,
    load_contract_map,
    load_day_ticks,
    simulate_wait_entry_trade,
    summarize,
)
from backtest.futures_combination_signal_backtest import Settings as SignalSettings, load_source
from backtest.futures_retail_extreme_expansion_backtest import (
    Settings as RetailSettings,
    build_candidates,
    prepare_research_daily,
)


ANALYSIS_NAME = "futures_retail_wait_window_comparison_backtest"
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
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0


WAIT_WINDOW_VARIANTS = (
    ("WAIT_30M", "前30分鐘不進場；先破失效價則取消"),
    ("NO_WAIT", "08:45起觸及進場價即進場；無前30分鐘取消"),
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
    tick_cache: dict[tuple[date, str], list] = {}
    rows: list[dict] = []

    for item in candidates.itertuples(index=False):
        trade_date = item.交易日
        contract = contracts.get(trade_date)
        average_range = _number(item.平均振幅30)
        night_close = _number(item.夜盤收)

        base = {
            "signal_date": item.訊號日,
            "trade_date": trade_date,
            "signal_name": "核心多＋綜合散戶極度偏空＋排除正乖離過大",
            "direction": "LONG",
            "entry_distance_ratio": settings.entry_distance_ratio,
            "retail_threshold_pct": settings.retail_threshold_pct,
            "retail_percentile_pct": item.散戶多空比_歷史分位_pct,
            "ma5_bias_percentile_pct": item.MA5乖離_歷史分位_pct,
            "contract_date": contract,
        }

        for variant, description in WAIT_WINDOW_VARIANTS:
            variant_base = {**base, "variant": variant, "variant_description": description}
            if not contract or average_range is None or night_close is None:
                rows.append({**variant_base, "entry_status": "MISSING_REFERENCE", "exit_reason": "MISSING_DATA"})
                continue

            key = (trade_date, contract)
            if key not in tick_cache:
                tick_cache[key] = load_day_ticks(exit_settings, trade_date, contract)

            # NO_WAIT 將允許進場時間改為日盤第一刻 08:45。
            # simulate_wait_entry_trade 對允許時間之前的資料才會檢查「先破失效價」，
            # 因此 08:45 起算時觀察區間為空，同時取消前30分鐘禁入與取消規則。
            entry_allowed_time = (
                exit_settings.entry_allowed_time if variant == "WAIT_30M" else exit_settings.entry_time
            )
            result = simulate_wait_entry_trade(
                tick_cache[key],
                "LONG",
                night_close,
                average_range,
                entry_allowed_time=entry_allowed_time,
                entry_distance_ratio=settings.entry_distance_ratio,
                stop_enabled=False,
                stop_loss_ratio=settings.pre_entry_stop_ratio,
                activation_ratio=None,
                drawdown_ratio=None,
                round_trip_cost_points=settings.round_trip_cost_points,
            )
            rows.append({**variant_base, **result})

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
    paths = {
        "detail": output_dir / f"retail_wait_window_detail_{suffix}.csv",
        "summary": output_dir / f"retail_wait_window_summary_{suffix}.csv",
        "yearly": output_dir / f"retail_wait_window_yearly_{suffix}.csv",
        "entry_status": output_dir / f"retail_wait_window_entry_status_{suffix}.csv",
        "manifest": output_dir / f"retail_wait_window_manifest_{suffix}.json",
    }
    detail.to_csv(paths["detail"], index=False, encoding="utf-8-sig")
    summary.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths["yearly"], index=False, encoding="utf-8-sig")
    status.to_csv(paths["entry_status"], index=False, encoding="utf-8-sig")
    paths["manifest"].write_text(
        json.dumps(
            {
                "run_id": str(uuid.uuid4()),
                "analysis": ANALYSIS_NAME,
                "version": VERSION,
                "settings": {
                    key: value.isoformat() if isinstance(value, date) else value
                    for key, value in asdict(settings).items()
                },
                "variants": dict(WAIT_WINDOW_VARIANTS),
                "control_rule": "除前30分鐘入場限制外，其餘訊號、5/12R進場、收盤出場及3點成本完全相同",
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
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/backtest/retail_wait_window"),
    )
    args = parser.parse_args()
    settings = Settings(start=args.start, end=args.end, round_trip_cost_points=args.cost_points)
    detail = run_backtest(settings)
    paths = export_outputs(detail, settings, args.output_dir)
    print(summarize(detail, ["variant", "variant_description"]).to_string(index=False))
    for name, path in paths.items():
        print(f"[retail_wait_window] {name}: {path.resolve()}")


if __name__ == "__main__":
    main()
