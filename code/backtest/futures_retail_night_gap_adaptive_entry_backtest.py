from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
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


ANALYSIS_NAME = "futures_retail_night_gap_adaptive_entry_backtest"
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
    retail_threshold_pct: float = 20.0
    gap_threshold_ratio: float = 1 / 6
    pre_entry_stop_ratio: float = 1 / 4
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0


DISTANCES = (
    ("ENTRY_1_3R", 1 / 3),
    ("ENTRY_5_12R", 5 / 12),
    ("ENTRY_1_2R", 1 / 2),
    ("ENTRY_7_12R", 7 / 12),
)

GAP_LABELS = {
    "NIGHT_DOWN": "夜盤跌（≤-1/6R）",
    "NIGHT_FLAT": "夜盤平（-1/6R～+1/6R）",
    "NIGHT_UP": "夜盤漲（≥+1/6R）",
}

POLICIES = {
    "FIXED_5_12R": {
        "label": "固定5/12R",
        "NIGHT_DOWN": 5 / 12,
        "NIGHT_FLAT": 5 / 12,
        "NIGHT_UP": 5 / 12,
    },
    "ADAPTIVE_LINEAR": {
        "label": "線性調整：跌1/3R、平5/12R、漲1/2R",
        "NIGHT_DOWN": 1 / 3,
        "NIGHT_FLAT": 5 / 12,
        "NIGHT_UP": 1 / 2,
    },
    "ADAPTIVE_UP_CONSERVATIVE": {
        "label": "只對夜盤漲保守：跌平5/12R、漲7/12R",
        "NIGHT_DOWN": 5 / 12,
        "NIGHT_FLAT": 5 / 12,
        "NIGHT_UP": 7 / 12,
    },
}


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
    selected = candidates[
        (candidates["study"] == "A_ENTRY_DISTANCE")
        & (candidates["scenario"] == "A_ENTRY_5_12R")
        & (pd.to_numeric(candidates["retail_threshold_pct"], errors="coerce") == settings.retail_threshold_pct)
    ].copy()
    return selected.sort_values("交易日").reset_index(drop=True)


def gap_group(gap_ratio: float | None, threshold: float) -> str | None:
    if gap_ratio is None:
        return None
    if gap_ratio <= -threshold:
        return "NIGHT_DOWN"
    if gap_ratio >= threshold:
        return "NIGHT_UP"
    return "NIGHT_FLAT"


def run_backtest(settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame]:
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
    cache = {}
    detail_rows: list[dict] = []
    path_rows: list[dict] = []

    for item in candidates.itertuples(index=False):
        trade_date = item.交易日
        contract = contracts.get(trade_date)
        average_range = _number(item.平均振幅30)
        night_close = _number(item.夜盤收)
        previous_day_close = _number(item.日盤收)
        gap_points = (
            night_close - previous_day_close
            if night_close is not None and previous_day_close is not None
            else None
        )
        gap_ratio = (
            gap_points / average_range
            if gap_points is not None and average_range is not None and average_range > 0
            else None
        )
        group = gap_group(gap_ratio, settings.gap_threshold_ratio)
        common = {
            "signal_date": item.訊號日,
            "trade_date": trade_date,
            "signal_name": "核心多＋綜合散戶極度偏空＋排除正乖離過大",
            "direction": "LONG",
            "retail_threshold_pct": settings.retail_threshold_pct,
            "retail_percentile_pct": item.散戶多空比_歷史分位_pct,
            "ma5_bias_percentile_pct": item.MA5乖離_歷史分位_pct,
            "previous_day_close": previous_day_close,
            "night_close": night_close,
            "average_range_30": average_range,
            "night_gap_points": gap_points,
            "night_gap_ratio": gap_ratio,
            "night_gap_group": group,
            "night_gap_group_label": GAP_LABELS.get(group),
            "contract_date": contract,
        }
        if not contract or average_range is None or night_close is None or previous_day_close is None or group is None:
            reason = "MISSING_REFERENCE"
            for scenario, distance in DISTANCES:
                detail_rows.append({**common, "study": "DISTANCE_MATRIX", "variant": scenario, "entry_distance_ratio": distance, "entry_status": reason, "exit_reason": "MISSING_DATA"})
            continue

        key = (trade_date, contract)
        if key not in cache:
            cache[key] = load_day_ticks(exit_settings, trade_date, contract)
        ticks = cache[key]
        if ticks:
            prices = [tick.price for tick in ticks]
            day_open, day_high, day_low, day_close = prices[0], max(prices), min(prices), prices[-1]
            path_rows.append({
                **common,
                "day_open": day_open,
                "day_high": day_high,
                "day_low": day_low,
                "day_close": day_close,
                "open_vs_night_points": day_open - night_close,
                "high_vs_night_points": day_high - night_close,
                "low_vs_night_points": day_low - night_close,
                "close_vs_night_points": day_close - night_close,
                "open_vs_night_ratio": (day_open - night_close) / average_range,
                "high_vs_night_ratio": (day_high - night_close) / average_range,
                "low_vs_night_ratio": (day_low - night_close) / average_range,
                "close_vs_night_ratio": (day_close - night_close) / average_range,
            })

        for scenario, distance in DISTANCES:
            result = simulate_wait_entry_trade(
                ticks,
                "LONG",
                night_close,
                average_range,
                entry_allowed_time=exit_settings.entry_allowed_time,
                entry_distance_ratio=distance,
                stop_enabled=False,
                stop_loss_ratio=settings.pre_entry_stop_ratio,
                activation_ratio=None,
                drawdown_ratio=None,
                round_trip_cost_points=settings.round_trip_cost_points,
            )
            detail_rows.append({
                **common,
                "study": "DISTANCE_MATRIX",
                "variant": scenario,
                "entry_distance_ratio": distance,
                **result,
            })

    detail = pd.DataFrame(detail_rows)
    if not detail.empty:
        detail["year"] = pd.to_datetime(detail["trade_date"]).dt.year
        detail["net_twd"] = pd.to_numeric(detail.get("net_points"), errors="coerce") * settings.point_value_twd
    return detail, pd.DataFrame(path_rows)


def build_policy_detail(matrix_detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for policy, rule in POLICIES.items():
        for group, distance in ((g, rule[g]) for g in GAP_LABELS):
            selected = matrix_detail[
                (matrix_detail["night_gap_group"] == group)
                & np.isclose(pd.to_numeric(matrix_detail["entry_distance_ratio"], errors="coerce"), distance)
            ].copy()
            selected["study"] = "POLICY_COMPARISON"
            selected["variant"] = policy
            selected["variant_description"] = rule["label"]
            rows.append(selected)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def summarize_paths(paths: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "night_gap_points", "night_gap_ratio",
        "open_vs_night_points", "high_vs_night_points", "low_vs_night_points", "close_vs_night_points",
        "open_vs_night_ratio", "high_vs_night_ratio", "low_vs_night_ratio", "close_vs_night_ratio",
    ]
    rows = []
    for group, frame in paths.groupby("night_gap_group", sort=True):
        row = {
            "night_gap_group": group,
            "night_gap_group_label": GAP_LABELS[group],
            "signal_count": len(frame),
            "next_day_close_up_rate_pct": float((frame["close_vs_night_points"] > 0).mean() * 100),
        }
        for metric in metrics:
            values = pd.to_numeric(frame[metric], errors="coerce")
            row[f"{metric}_mean"] = float(values.mean())
            row[f"{metric}_median"] = float(values.median())
        for name, distance in DISTANCES:
            row[f"touch_{name}_rate_pct"] = float((frame["low_vs_night_ratio"] <= -distance).mean() * 100)
        rows.append(row)
    return pd.DataFrame(rows)


def export_outputs(detail: pd.DataFrame, paths: pd.DataFrame, settings: Settings, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    policy_detail = build_policy_detail(detail)
    matrix_summary = summarize(detail, ["night_gap_group", "night_gap_group_label", "variant", "entry_distance_ratio"])
    policy_summary = summarize(policy_detail, ["variant", "variant_description"])
    policy_yearly = summarize(policy_detail, ["variant", "variant_description", "year"])
    path_summary = summarize_paths(paths)
    status = detail.groupby(["night_gap_group", "variant", "entry_status"], dropna=False).size().rename("count").reset_index()
    end = pd.to_datetime(detail["trade_date"]).max().date()
    suffix = f"{settings.start:%Y%m%d}_{end:%Y%m%d}"
    out = {
        "matrix_detail": output_dir / f"retail_night_gap_matrix_detail_{suffix}.csv",
        "matrix_summary": output_dir / f"retail_night_gap_matrix_summary_{suffix}.csv",
        "path_detail": output_dir / f"retail_night_gap_path_detail_{suffix}.csv",
        "path_summary": output_dir / f"retail_night_gap_path_summary_{suffix}.csv",
        "policy_detail": output_dir / f"retail_night_gap_policy_detail_{suffix}.csv",
        "policy_summary": output_dir / f"retail_night_gap_policy_summary_{suffix}.csv",
        "policy_yearly": output_dir / f"retail_night_gap_policy_yearly_{suffix}.csv",
        "entry_status": output_dir / f"retail_night_gap_entry_status_{suffix}.csv",
        "manifest": output_dir / f"retail_night_gap_manifest_{suffix}.json",
    }
    detail.to_csv(out["matrix_detail"], index=False, encoding="utf-8-sig")
    matrix_summary.to_csv(out["matrix_summary"], index=False, encoding="utf-8-sig")
    paths.to_csv(out["path_detail"], index=False, encoding="utf-8-sig")
    path_summary.to_csv(out["path_summary"], index=False, encoding="utf-8-sig")
    policy_detail.to_csv(out["policy_detail"], index=False, encoding="utf-8-sig")
    policy_summary.to_csv(out["policy_summary"], index=False, encoding="utf-8-sig")
    policy_yearly.to_csv(out["policy_yearly"], index=False, encoding="utf-8-sig")
    status.to_csv(out["entry_status"], index=False, encoding="utf-8-sig")
    manifest = {
        "run_id": str(uuid.uuid4()),
        "analysis": ANALYSIS_NAME,
        "version": VERSION,
        "settings": {k: v.isoformat() if isinstance(v, date) else v for k, v in asdict(settings).items()},
        "night_gap_definition": "(夜盤收－昨日日盤收)／近30日平均日盤振幅",
        "gap_groups": GAP_LABELS,
        "distance_matrix": dict(DISTANCES),
        "policies": POLICIES,
        "entry_rule": "08:45～09:15不進場；若前30分鐘先穿越該距離對應失效價，當日取消；否則09:15後等待實際觸價",
        "exit_rule": "進場後持有至日盤收盤；每筆扣固定來回成本",
        "anti_lookahead": "夜盤位移、昨收與平均振幅皆在隔日日盤開盤前已知",
    }
    out["manifest"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="夜盤相對昨收位移與自適應進場距離回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--cost-points", type=float, default=3.0)
    parser.add_argument("--output-dir", type=Path, default=Path("output/backtest/retail_night_gap"))
    args = parser.parse_args()
    settings = Settings(start=args.start, end=args.end, round_trip_cost_points=args.cost_points)
    detail, paths = run_backtest(settings)
    outputs = export_outputs(detail, paths, settings, args.output_dir)
    print("\n[夜盤位移分組 × 進場距離]\n")
    print(summarize(detail, ["night_gap_group", "variant", "entry_distance_ratio"]).to_string(index=False))
    print("\n[固定規則與自適應規則]\n")
    print(summarize(build_policy_detail(detail), ["variant", "variant_description"]).to_string(index=False))
    for name, path in outputs.items():
        print(f"[retail_night_gap] {name}: {path.resolve()}")


if __name__ == "__main__":
    main()
