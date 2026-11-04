from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

from backtest.futures_combination_signal_backtest import (
    Settings as SignalSettings,
    build_signals,
    load_source,
    prepare_daily,
)
from backtest.futures_turning_level_intraday import Tick


ANALYSIS_NAME = "futures_combination_exit_backtest"
VERSION = "1.1.0"


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    entry_time: time = time(8, 45)
    entry_allowed_time: time = time(9, 15)
    forced_exit_time: time = time(13, 45)
    range_days: int = 30
    entry_distance_ratio: float = 1 / 2
    stop_loss_ratio: float = 1 / 4
    new_activation_ratio: float = 1 / 3
    new_drawdown_ratio: float = 1 / 6
    old_activation_ratio: float = 1 / 4
    old_drawdown_ratio: float = 1 / 5
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0
    contracts: int = 1

    def __post_init__(self) -> None:
        if self.end and self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if not self.entry_time < self.entry_allowed_time <= self.forced_exit_time:
            raise ValueError("進場觀察起訖與尾盤時間設定錯誤")
        ratios = (
            self.entry_distance_ratio, self.stop_loss_ratio, self.new_activation_ratio, self.new_drawdown_ratio,
            self.old_activation_ratio, self.old_drawdown_ratio,
        )
        if self.range_days < 1 or min(ratios) <= 0:
            raise ValueError("振幅天數與各比例必須大於 0")
        if self.new_drawdown_ratio >= self.new_activation_ratio:
            raise ValueError("新版回撤距離必須小於啟動距離，才能在啟動時鎖住獲利")
        if self.round_trip_cost_points < 0 or self.point_value_twd <= 0 or self.contracts < 1:
            raise ValueError("成本不可為負；每點價值與口數必須大於 0")


VARIANTS = {
    "A_CLOSE_ONLY": {"stop": False, "activation": None, "drawdown": None},
    "B_STOP_ONLY": {"stop": True, "activation": None, "drawdown": None},
    "C_OLD_2_1": {"stop": True, "activation": 1 / 4, "drawdown": 1 / 5},
    "D_NEW_1_3_1_6": {"stop": True, "activation": 1 / 3, "drawdown": 1 / 6},
}


TARGET_SIGNALS = (
    "核心多＋法人買＋散戶偏空＋排除正乖離過大",
    "核心多＋綜合散戶極度偏空＋排除正乖離過大",
    "核心空＋夜盤漲＋正乖離Q80",
    "核心空＋夜盤極端跌＋負乖離Q20",
    "核心多＋夜盤極端漲＋正乖離Q80",
)


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def prepare_candidates(source: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    signal_settings = SignalSettings(
        start=settings.start, end=settings.end, futures_id=settings.futures_id,
        core_column="策略_核心_五日價量",
        round_trip_cost_points=settings.round_trip_cost_points,
    )
    ordered_source = source.sort_values("日期").drop_duplicates("日期", keep="last").copy()
    day_range = (
        pd.to_numeric(ordered_source["日盤高"], errors="coerce")
        - pd.to_numeric(ordered_source["日盤低"], errors="coerce")
    )
    ordered_source["平均振幅30"] = day_range.rolling(
        settings.range_days, min_periods=settings.range_days
    ).mean()
    range_by_date = ordered_source.set_index(pd.to_datetime(ordered_source["日期"]))["平均振幅30"]
    daily = prepare_daily(ordered_source, signal_settings)
    daily["平均振幅30"] = pd.to_datetime(daily["訊號日"]).map(range_by_date)
    signals = build_signals(daily, signal_settings)
    if signals.empty:
        return signals
    selected = signals[signals["組合訊號"].isin(TARGET_SIGNALS)].copy()
    selected = selected[selected["平均振幅30"].notna() & pd.to_numeric(selected["夜盤收"], errors="coerce").notna()].copy()
    selected["交易日"] = pd.to_datetime(selected["隔日日期"]).dt.date
    selected["訊號日"] = pd.to_datetime(selected["訊號日"]).dt.date
    return selected.sort_values(["交易日", "組合訊號"]).reset_index(drop=True)


def load_contract_map(settings: Settings) -> dict[date, str]:
    from common import db

    frame = db.sql_to_df('''
        SELECT DISTINCT ON (trading_date)
            trading_date, contract_date
        FROM raw_finmind.fm_taiwan_futures_kbar_1m
        WHERE futures_id = ?
          AND trading_date BETWEEN ? AND ?
          AND trading_session = 'position'
          AND contract_date ~ '^[0-9]{6}$'
          AND bar_datetime::time BETWEEN ? AND (? + INTERVAL '1 minute')
        ORDER BY trading_date, contract_date, bar_datetime
    ''', params=(
        settings.futures_id, settings.start, settings.end or date.today(),
        settings.entry_time, settings.entry_time,
    ), parse_dates=["trading_date"])
    return {
        pd.Timestamp(row.trading_date).date(): str(row.contract_date)
        for row in frame.itertuples(index=False)
    }


def load_day_ticks(settings: Settings, trade_date: date, contract_date: str) -> list[Tick]:
    from common import db

    frame = db.sql_to_df('''
        SELECT trade_datetime, price, source_row_no
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ? AND contract_date = ? AND source_date = ?
          AND trade_datetime >= ? AND trade_datetime <= ?
        ORDER BY trade_datetime, source_row_no
    ''', params=(
        settings.futures_id, contract_date, trade_date,
        datetime.combine(trade_date, settings.entry_time),
        datetime.combine(trade_date, settings.forced_exit_time),
    ), parse_dates=["trade_datetime"])
    return [
        Tick(row.trade_datetime.to_pydatetime(), float(row.price), int(row.source_row_no))
        for row in frame.itertuples(index=False)
    ]


def simulate_open_trade(
    ticks: Sequence[Tick], direction: str, average_range: float,
    *, stop_enabled: bool, stop_loss_ratio: float = 1 / 4,
    activation_ratio: float | None,
    drawdown_ratio: float | None, round_trip_cost_points: float,
) -> dict:
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction 必須是 LONG 或 SHORT")
    if not ticks:
        return {"entry_status": "MISSING_TICKS", "exit_reason": "MISSING_DATA"}
    if average_range <= 0:
        return {"entry_status": "MISSING_RANGE", "exit_reason": "MISSING_DATA"}
    if (activation_ratio is None) != (drawdown_ratio is None):
        raise ValueError("activation_ratio 與 drawdown_ratio 必須同時提供或同時省略")

    sign = 1.0 if direction == "LONG" else -1.0
    entry_tick = ticks[0]
    entry_price = entry_tick.price
    stop_price = entry_price - sign * average_range * stop_loss_ratio if stop_enabled else None
    activation_distance = average_range * activation_ratio if activation_ratio else None
    activation_price = entry_price + sign * activation_distance if activation_distance else None
    drawdown_distance = average_range * drawdown_ratio if drawdown_ratio else None

    best_price = entry_price
    best_profit = 0.0
    worst_adverse = 0.0
    activated = False
    activation_tick: Tick | None = None
    trailing_price: float | None = None
    exit_index = len(ticks) - 1
    exit_price = ticks[-1].price
    exit_reason = "CLOSE_EXIT"

    for index, tick in enumerate(ticks[1:], start=1):
        current_profit = sign * (tick.price - entry_price)
        worst_adverse = max(worst_adverse, -current_profit)

        if stop_price is not None and sign * (tick.price - stop_price) <= 0:
            exit_index, exit_price, exit_reason = index, tick.price, "STOP_LOSS"
            break

        if current_profit > best_profit:
            best_profit = current_profit
            best_price = tick.price
        if activation_distance is not None and not activated and best_profit >= activation_distance:
            activated = True
            activation_tick = tick
        if activated and drawdown_distance is not None:
            trailing_price = best_price - sign * drawdown_distance
            if sign * (tick.price - trailing_price) <= 0:
                exit_index, exit_price, exit_reason = index, tick.price, "TRAILING_TAKE_PROFIT"
                break

    gross_points = sign * (exit_price - entry_price)
    close_gross_points = sign * (ticks[-1].price - entry_price)
    future_prices = [tick.price for tick in ticks[exit_index + 1:]]
    post_exit_best = (
        max(future_prices) if direction == "LONG" and future_prices
        else min(future_prices) if direction == "SHORT" and future_prices
        else None
    )
    post_exit_best_profit = (
        sign * (post_exit_best - entry_price) if post_exit_best is not None else None
    )
    return {
        "entry_status": "ENTERED",
        "entry_at": entry_tick.at,
        "entry_price": entry_price,
        "average_range_30": average_range,
        "stop_loss_price": stop_price,
        "trailing_activation_price": activation_price,
        "trailing_drawdown_points": drawdown_distance,
        "trailing_activated": activated,
        "trailing_activation_at": activation_tick.at if activation_tick else None,
        "best_price_before_exit": best_price,
        "maximum_favorable_points": best_profit,
        "maximum_adverse_points": worst_adverse,
        "last_trailing_stop_price": trailing_price,
        "exit_at": ticks[exit_index].at,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "gross_points": gross_points,
        "cost_points": round_trip_cost_points,
        "net_points": gross_points - round_trip_cost_points,
        "close_exit_gross_points": close_gross_points,
        "close_exit_net_points": close_gross_points - round_trip_cost_points,
        "post_exit_best_profit_points": post_exit_best_profit,
        "trailing_exit_then_new_extreme": bool(
            exit_reason == "TRAILING_TAKE_PROFIT"
            and post_exit_best_profit is not None
            and post_exit_best_profit > best_profit
        ),
    }


def simulate_wait_entry_trade(
    ticks: Sequence[Tick], direction: str, night_close: float, average_range: float,
    *, entry_allowed_time: time = time(9, 15), entry_distance_ratio: float = 1 / 2,
    stop_enabled: bool, stop_loss_ratio: float = 1 / 4,
    activation_ratio: float | None, drawdown_ratio: float | None,
    round_trip_cost_points: float,
) -> dict:
    """前30分鐘只觀察；未先穿越預定停損時，09:15後等待逆向1/2R進場。"""
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction 必須是 LONG 或 SHORT")
    if not ticks:
        return {"entry_status": "MISSING_TICKS", "exit_reason": "MISSING_DATA"}
    if average_range <= 0 or not math.isfinite(night_close):
        return {"entry_status": "MISSING_REFERENCE", "exit_reason": "MISSING_DATA"}

    sign = 1.0 if direction == "LONG" else -1.0
    target_entry_price = night_close - sign * average_range * entry_distance_ratio
    pre_entry_stop_price = target_entry_price - sign * average_range * stop_loss_ratio
    observation = [tick for tick in ticks if tick.at.time() < entry_allowed_time]
    stop_crossed = any(sign * (tick.price - pre_entry_stop_price) <= 0 for tick in observation)
    base = {
        "night_close": night_close,
        "target_entry_price": target_entry_price,
        "pre_entry_stop_price": pre_entry_stop_price,
        "entry_allowed_at": datetime.combine(ticks[0].at.date(), entry_allowed_time),
        "pre_entry_stop_crossed": stop_crossed,
    }
    if stop_crossed:
        return {**base, "entry_status": "CANCELLED_PRE_ENTRY_STOP", "exit_reason": "NO_ENTRY"}

    eligible = [tick for tick in ticks if tick.at.time() >= entry_allowed_time]
    entry_index = next(
        (index for index, tick in enumerate(eligible) if sign * (tick.price - target_entry_price) <= 0),
        None,
    )
    if entry_index is None:
        return {**base, "entry_status": "TARGET_NOT_REACHED", "exit_reason": "NO_ENTRY"}

    # 以09:15後第一筆實際穿越成交價進場，不倒填理論掛價。
    result = simulate_open_trade(
        eligible[entry_index:], direction, average_range,
        stop_enabled=stop_enabled, stop_loss_ratio=stop_loss_ratio,
        activation_ratio=activation_ratio, drawdown_ratio=drawdown_ratio,
        round_trip_cost_points=round_trip_cost_points,
    )
    return {**base, **result}


def run_backtest(
    settings: Settings, candidates: pd.DataFrame | None = None,
    contract_map: dict[date, str] | None = None, tick_loader=load_day_ticks,
) -> pd.DataFrame:
    if candidates is None:
        signal_settings = SignalSettings(
            start=settings.start, end=settings.end, futures_id=settings.futures_id,
            core_column="策略_核心_五日價量",
            round_trip_cost_points=settings.round_trip_cost_points,
        )
        candidates = prepare_candidates(load_source(signal_settings), settings)
    if contract_map is None:
        contract_map = load_contract_map(settings)

    variant_rules = {
        "A_CLOSE_ONLY": {"stop": False, "activation": None, "drawdown": None},
        "B_STOP_ONLY": {"stop": True, "activation": None, "drawdown": None},
        "C_OLD_2_1": {
            "stop": True, "activation": settings.old_activation_ratio,
            "drawdown": settings.old_drawdown_ratio,
        },
        "D_NEW_1_3_1_6": {
            "stop": True, "activation": settings.new_activation_ratio,
            "drawdown": settings.new_drawdown_ratio,
        },
    }
    rows: list[dict] = []
    tick_cache: dict[tuple[date, str], list[Tick]] = {}
    for item in candidates.itertuples(index=False):
        trade_date = item.交易日
        contract = contract_map.get(trade_date)
        average_range = _number(item.平均振幅30)
        night_close = _number(item.夜盤收)
        base = {
            "signal_date": item.訊號日,
            "trade_date": trade_date,
            "signal_family": item.組合家族,
            "signal_name": item.組合訊號,
            "direction": item.交易方向,
            "contract_date": contract,
        }
        if not contract or average_range is None or night_close is None:
            reason = "MISSING_CONTRACT" if not contract else ("MISSING_RANGE" if average_range is None else "MISSING_NIGHT_CLOSE")
            for variant in variant_rules:
                rows.append({**base, "variant": variant, "entry_status": reason, "exit_reason": "MISSING_DATA"})
            continue
        cache_key = (trade_date, contract)
        if cache_key not in tick_cache:
            tick_cache[cache_key] = tick_loader(settings, trade_date, contract)
        ticks = tick_cache[cache_key]
        for variant, rule in variant_rules.items():
            result = simulate_wait_entry_trade(
                ticks, item.交易方向, night_close, average_range,
                entry_allowed_time=settings.entry_allowed_time,
                entry_distance_ratio=settings.entry_distance_ratio,
                stop_enabled=bool(rule["stop"]),
                stop_loss_ratio=settings.stop_loss_ratio,
                activation_ratio=rule["activation"],
                drawdown_ratio=rule["drawdown"],
                round_trip_cost_points=settings.round_trip_cost_points,
            )
            rows.append({**base, "variant": variant, **result})
    detail = pd.DataFrame(rows)
    if not detail.empty:
        detail["year"] = pd.to_datetime(detail["trade_date"]).dt.year
        detail["net_twd"] = pd.to_numeric(detail.get("net_points"), errors="coerce") * settings.point_value_twd * settings.contracts
    return detail


def _max_losing_streak(points: pd.Series) -> int:
    longest = current = 0
    for value in points:
        current = current + 1 if value < 0 else 0
        longest = max(longest, current)
    return longest


def summarize(detail: pd.DataFrame, groups: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    for keys, frame in detail.groupby(groups, sort=True, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        entered = frame[frame["entry_status"].eq("ENTERED")].sort_values("trade_date")
        net = pd.to_numeric(entered.get("net_points"), errors="coerce").dropna()
        gross = pd.to_numeric(entered.get("gross_points"), errors="coerce").dropna()
        wins, losses = net[net > 0], net[net < 0]
        equity = net.cumsum()
        drawdown = equity - equity.cummax()
        activated = entered.get("trailing_activated", pd.Series(False, index=entered.index)).fillna(False).astype(bool)
        trailing_exits = entered["exit_reason"].eq("TRAILING_TAKE_PROFIT")
        rows.append({
            **dict(zip(groups, keys)),
            "signal_count": int(len(frame)),
            "entry_count": int(len(entered)),
            "missing_count": int((frame["entry_status"] != "ENTERED").sum()),
            "win_rate_pct": float((net > 0).mean() * 100) if len(net) else np.nan,
            "total_net_points": float(net.sum()),
            "average_net_points": float(net.mean()) if len(net) else np.nan,
            "median_net_points": float(net.median()) if len(net) else np.nan,
            "profit_factor": float(wins.sum() / -losses.sum()) if losses.sum() < 0 else np.nan,
            "max_drawdown_points": float(drawdown.min()) if len(drawdown) else np.nan,
            "max_losing_streak": _max_losing_streak(net),
            "stop_loss_count": int(entered["exit_reason"].eq("STOP_LOSS").sum()),
            "trailing_activation_count": int(activated.sum()),
            "trailing_activation_rate_pct": float(activated.mean() * 100) if len(entered) else np.nan,
            "trailing_exit_count": int(trailing_exits.sum()),
            "close_exit_count": int(entered["exit_reason"].eq("CLOSE_EXIT").sum()),
            "average_mfe_points": float(pd.to_numeric(entered.get("maximum_favorable_points"), errors="coerce").mean()),
            "average_mae_points": float(pd.to_numeric(entered.get("maximum_adverse_points"), errors="coerce").mean()),
            "trailing_exit_then_new_extreme_count": int(entered.get("trailing_exit_then_new_extreme", pd.Series(False, index=entered.index)).fillna(False).sum()),
            "vs_close_exit_total_points": float((net - pd.to_numeric(entered.get("close_exit_net_points"), errors="coerce")).sum()),
        })
    result = pd.DataFrame(rows)
    if not result.empty:
        numeric = result.select_dtypes(include="number").columns
        result[numeric] = result[numeric].round(4)
    return result


def export_outputs(detail: pd.DataFrame, settings: Settings, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = pd.to_datetime(detail["trade_date"]).max().date() if not detail.empty else (settings.end or date.today())
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    summary = summarize(detail, ["signal_name", "direction", "variant"])
    yearly = summarize(detail, ["signal_name", "direction", "variant", "year"])
    diagnostics = detail[detail["entry_status"].eq("ENTERED")].groupby(
        ["signal_name", "variant", "exit_reason"], dropna=False
    ).agg(
        trades=("trade_date", "size"),
        average_net_points=("net_points", "mean"),
        average_mfe_points=("maximum_favorable_points", "mean"),
        average_mae_points=("maximum_adverse_points", "mean"),
    ).reset_index().round(4)
    paths = {
        "detail": output_dir / f"combination_exit_detail_{suffix}.csv",
        "summary": output_dir / f"combination_exit_summary_{suffix}.csv",
        "yearly": output_dir / f"combination_exit_yearly_{suffix}.csv",
        "diagnostics": output_dir / f"combination_exit_diagnostics_{suffix}.csv",
        "manifest": output_dir / f"combination_exit_manifest_{suffix}.json",
    }
    detail.to_csv(paths["detail"], index=False, encoding="utf-8-sig")
    summary.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths["yearly"], index=False, encoding="utf-8-sig")
    diagnostics.to_csv(paths["diagnostics"], index=False, encoding="utf-8-sig")
    paths["manifest"].write_text(json.dumps({
        "run_id": str(uuid.uuid4()), "analysis": ANALYSIS_NAME, "version": VERSION,
        "settings": {k: v.isoformat() if isinstance(v, (date, time)) else v for k, v in asdict(settings).items()},
        "target_signals": TARGET_SIGNALS,
        "variants": {
            "A_CLOSE_ONLY": {"stop": False, "activation": None, "drawdown": None},
            "B_STOP_ONLY": {"stop": True, "activation": None, "drawdown": None},
            "C_OLD_2_1": {"stop": True, "activation": settings.old_activation_ratio, "drawdown": settings.old_drawdown_ratio},
            "D_NEW_1_3_1_6": {"stop": True, "activation": settings.new_activation_ratio, "drawdown": settings.new_drawdown_ratio},
        },
        "definitions": {
            "range": "訊號日含當日往回30個完整交易日的(日盤高-日盤低)平均",
            "entry": "以夜盤收為基準，做多向下1/2R、做空向上1/2R；08:45至09:15只觀察，09:15後才以第一筆實際觸價成交",
            "pre_entry_cancel": "前30分鐘若先觸及理論掛價再延伸1/4R的預定停損價，當日取消進場",
            "new_exit": "停損1/4R；獲利達1/3R後，從進場後最佳價固定回撤1/6R出場；否則尾盤出場",
            "anti_lookahead": "訊號、夜盤收及平均振幅只使用進場日前資料；進出場依逐筆成交時間順序",
            "fill": "停損與保利以第一筆穿越門檻的實際成交價計算，固定來回成本另扣",
        },
        "output_rows": {"detail": len(detail), "summary": len(summary), "yearly": len(yearly), "diagnostics": len(diagnostics)},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="五組期貨組合訊號 × 四種出場法逐筆回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--cost-points", type=float, default=3.0)
    parser.add_argument("--output-dir", type=Path, default=Path("output/backtest/combination_exit"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(start=args.start, end=args.end, round_trip_cost_points=args.cost_points)
    detail = run_backtest(settings)
    paths = export_outputs(detail, settings, args.output_dir)
    print(summarize(detail, ["signal_name", "direction", "variant"]).to_string(index=False))
    for name, path in paths.items():
        print(f"[combination_exit] {name}: {path.resolve()}")


if __name__ == "__main__":
    main()
