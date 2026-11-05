from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
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


ANALYSIS_NAME = "futures_retail_dif_reentry_comparison_backtest"
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
    dif_fast_period: int = 12
    dif_slow_period: int = 26
    dif_history_calendar_days: int = 45
    execution_delay_minutes: int = 1


VARIANTS = (
    ("CANCEL_DAY", "原規則：前30分鐘破失效價則取消當日交易"),
    ("DIRECT_ENTRY", "08:45起觸及5/12R即進場，不做破價過濾"),
    ("DIF_REVERSAL_ENTRY", "破失效價後等兩根5分K DIF向上，再延遲1分鐘進場"),
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


def load_dif_history_ticks(
    settings: Settings,
    exit_settings: ExitSettings,
    trade_date: date,
    contract_date: str,
) -> list[Tick]:
    """載入同一契約先前日盤 tick，只用來暖機5分K DIF。"""
    from common import db

    frame = db.sql_to_df(
        '''
        SELECT trade_datetime, price, source_row_no
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ?
          AND contract_date = ?
          AND source_date >= ?
          AND source_date < ?
          AND trade_datetime::time BETWEEN ? AND ?
        ORDER BY trade_datetime, source_row_no
        ''',
        params=(
            settings.futures_id,
            contract_date,
            trade_date - timedelta(days=settings.dif_history_calendar_days),
            trade_date,
            exit_settings.entry_time,
            exit_settings.forced_exit_time,
        ),
        parse_dates=["trade_datetime"],
    )
    return [
        Tick(row.trade_datetime.to_pydatetime(), float(row.price), int(row.source_row_no))
        for row in frame.itertuples(index=False)
    ]


def _five_minute_closes(ticks: Sequence[Tick], session_open: time) -> pd.DataFrame:
    """將tick依每個日盤08:45起算切成5分鐘K，只使用各桶最後一筆成交。"""
    records: list[dict] = []
    for tick in ticks:
        session_start = datetime.combine(tick.at.date(), session_open)
        seconds = (tick.at - session_start).total_seconds()
        if seconds < 0:
            continue
        bucket = int(seconds // 300)
        close_at = session_start + timedelta(minutes=5 * (bucket + 1))
        records.append({"at": tick.at, "close_at": close_at, "price": tick.price})
    if not records:
        return pd.DataFrame(columns=["close_at", "close"])
    frame = pd.DataFrame(records).sort_values(["close_at", "at"])
    return frame.groupby("close_at", as_index=False).tail(1)[["close_at", "price"]].rename(columns={"price": "close"})


def find_dif_reversal(
    history_ticks: Sequence[Tick],
    day_ticks: Sequence[Tick],
    breach_at: datetime,
    settings: Settings,
    exit_settings: ExitSettings,
) -> dict | None:
    bars = _five_minute_closes([*history_ticks, *day_ticks], exit_settings.entry_time)
    if bars.empty:
        return None
    bars = bars.sort_values("close_at").reset_index(drop=True)
    close = pd.to_numeric(bars["close"], errors="coerce")
    bars["dif"] = (
        close.ewm(span=settings.dif_fast_period, adjust=False).mean()
        - close.ewm(span=settings.dif_slow_period, adjust=False).mean()
    )
    trade_day = breach_at.date()
    for index in range(2, len(bars)):
        row = bars.iloc[index]
        confirm_at = pd.Timestamp(row["close_at"]).to_pydatetime()
        if confirm_at.date() != trade_day or confirm_at <= breach_at:
            continue
        dif_2 = float(bars.iloc[index - 2]["dif"])
        dif_1 = float(bars.iloc[index - 1]["dif"])
        dif_0 = float(row["dif"])
        if all(math.isfinite(value) for value in (dif_2, dif_1, dif_0)) and dif_1 > dif_2 and dif_0 > dif_1:
            return {
                "dif_confirm_at": confirm_at,
                "dif_two_bars_back": dif_2,
                "dif_one_bar_back": dif_1,
                "dif_confirm_value": dif_0,
            }
    return None


def simulate_dif_reentry(
    day_ticks: Sequence[Tick],
    history_ticks: Sequence[Tick],
    night_close: float,
    average_range: float,
    settings: Settings,
    exit_settings: ExitSettings,
) -> dict:
    if not day_ticks:
        return {"entry_status": "MISSING_TICKS", "exit_reason": "MISSING_DATA"}

    target_entry_price = night_close - average_range * settings.entry_distance_ratio
    pre_entry_stop_price = target_entry_price - average_range * settings.pre_entry_stop_ratio
    observation = [tick for tick in day_ticks if tick.at.time() < exit_settings.entry_allowed_time]
    breach_tick = next((tick for tick in observation if tick.price <= pre_entry_stop_price), None)

    # 沒有在前30分鐘破失效價：完全沿用原本09:15後等待5/12R的規則。
    if breach_tick is None:
        result = simulate_wait_entry_trade(
            day_ticks,
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
        return {**result, "reentry_mode": False}

    base = {
        "night_close": night_close,
        "target_entry_price": target_entry_price,
        "pre_entry_stop_price": pre_entry_stop_price,
        "entry_allowed_at": datetime.combine(day_ticks[0].at.date(), exit_settings.entry_allowed_time),
        "pre_entry_stop_crossed": True,
        "pre_entry_stop_crossed_at": breach_tick.at,
        "reentry_mode": True,
    }
    reversal = find_dif_reversal(
        history_ticks,
        day_ticks,
        breach_tick.at,
        settings,
        exit_settings,
    )
    if reversal is None:
        return {**base, "entry_status": "NO_DIF_REVERSAL", "exit_reason": "NO_ENTRY"}

    confirm_at = reversal["dif_confirm_at"]
    # 第二根5分K收盤已知後，保留完整1分鐘給資料、網路與下單流程；
    # 以其後第二個1分鐘區間的第一筆成交模擬入場，即確認時刻+1分鐘。
    scheduled_entry_at = confirm_at + timedelta(minutes=settings.execution_delay_minutes)
    eligible = [tick for tick in day_ticks if tick.at >= scheduled_entry_at]
    if not eligible or scheduled_entry_at.time() >= exit_settings.forced_exit_time:
        return {
            **base,
            **reversal,
            "scheduled_entry_at": scheduled_entry_at,
            "entry_status": "NO_TIME_AFTER_CONFIRMATION",
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
    reference_ticks = [tick for tick in day_ticks if breach_tick.at <= tick.at <= confirm_at]
    reversal_low = min((tick.price for tick in reference_ticks), default=breach_tick.price)
    post_entry_broke_reversal_low = any(tick.price < reversal_low for tick in eligible)
    return {
        **base,
        **reversal,
        "scheduled_entry_at": scheduled_entry_at,
        "dif_wait_minutes": (confirm_at - breach_tick.at).total_seconds() / 60,
        "entry_delay_from_confirm_minutes": (entry_tick.at - confirm_at).total_seconds() / 60,
        "entry_vs_target_points": entry_tick.price - target_entry_price,
        "entry_vs_pre_stop_points": entry_tick.price - pre_entry_stop_price,
        "reversal_reference_low": reversal_low,
        "post_entry_broke_reversal_low": post_entry_broke_reversal_low,
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
    day_cache: dict[tuple[date, str], list[Tick]] = {}
    history_cache: dict[tuple[date, str], list[Tick]] = {}
    rows: list[dict] = []

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
            "contract_date": contract,
        }
        if not contract or average_range is None or night_close is None:
            for variant, description in VARIANTS:
                rows.append({**common, "variant": variant, "variant_description": description, "entry_status": "MISSING_REFERENCE", "exit_reason": "MISSING_DATA"})
            continue

        key = (trade_date, contract)
        if key not in day_cache:
            day_cache[key] = load_day_ticks(exit_settings, trade_date, contract)
        ticks = day_cache[key]

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

        if baseline.get("entry_status") == "CANCELLED_PRE_ENTRY_STOP":
            if key not in history_cache:
                history_cache[key] = load_dif_history_ticks(settings, exit_settings, trade_date, contract)
            dif_reentry = simulate_dif_reentry(
                ticks,
                history_cache[key],
                night_close,
                average_range,
                settings,
                exit_settings,
            )
        else:
            dif_reentry = {**baseline, "reentry_mode": False}

        results = {
            "CANCEL_DAY": baseline,
            "DIRECT_ENTRY": direct,
            "DIF_REVERSAL_ENTRY": dif_reentry,
        }
        descriptions = dict(VARIANTS)
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
    rescued = detail[(detail["variant"] == "DIF_REVERSAL_ENTRY") & (detail["reentry_mode"] == True)].copy()  # noqa: E712
    paths = {
        "detail": output_dir / f"retail_dif_reentry_detail_{suffix}.csv",
        "summary": output_dir / f"retail_dif_reentry_summary_{suffix}.csv",
        "yearly": output_dir / f"retail_dif_reentry_yearly_{suffix}.csv",
        "entry_status": output_dir / f"retail_dif_reentry_entry_status_{suffix}.csv",
        "rescued_detail": output_dir / f"retail_dif_reentry_rescued_detail_{suffix}.csv",
        "manifest": output_dir / f"retail_dif_reentry_manifest_{suffix}.json",
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
                "dif_rule": "完整5分K的DIF連續上升兩次：DIF[t-1] > DIF[t-2] 且 DIF[t] > DIF[t-1]",
                "execution_rule": "第二根確認5分K收盤後保留完整1分鐘，以確認時刻+1分鐘後第一筆成交價進場",
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
    parser.add_argument("--output-dir", type=Path, default=Path("output/backtest/retail_dif_reentry"))
    args = parser.parse_args()
    settings = Settings(start=args.start, end=args.end, round_trip_cost_points=args.cost_points)
    detail = run_backtest(settings)
    paths = export_outputs(detail, settings, args.output_dir)
    print(summarize(detail, ["variant", "variant_description"]).to_string(index=False))
    for name, path in paths.items():
        print(f"[retail_dif_reentry] {name}: {path.resolve()}")


if __name__ == "__main__":
    main()
