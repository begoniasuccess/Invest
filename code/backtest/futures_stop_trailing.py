from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from enum import Enum
from pathlib import Path
from typing import Sequence

import pandas as pd
import numpy as np


SOURCE_VIEW = "report.v_futures_report_daily_corrected"
STRATEGY_CODE = "futures_stop_trailing_2_1"
STRATEGY_VERSION = "1.0"
ALLOWED_SIGNAL_COLUMNS = {"策略_核心_昨價量", "策略_核心_五日價量"}


class ThresholdMode(str, Enum):
    POINTS = "POINTS"
    PRICE_PERCENT = "PRICE_PERCENT"
    PROFIT_GIVEBACK_PERCENT = "PROFIT_GIVEBACK_PERCENT"


@dataclass(frozen=True)
class Threshold:
    mode: ThresholdMode
    value: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.value) or self.value <= 0:
            raise ValueError("門檻值必須是大於 0 的有限數字")

    @classmethod
    def from_dict(cls, value: dict) -> "Threshold":
        return cls(ThresholdMode(str(value["mode"]).upper()), float(value["value"]))


@dataclass(frozen=True)
class StrategyParameters:
    gap_limit: Threshold
    trailing_activation: Threshold
    trailing_drawdown: Threshold
    stop_loss: Threshold
    entry_slippage_points: float = 0.0
    exit_slippage_points: float = 0.0
    fee_cost_amount: float = 0.0

    def __post_init__(self) -> None:
        if self.gap_limit.mode is ThresholdMode.PROFIT_GIVEBACK_PERCENT:
            raise ValueError("跳空限制不支援最大浮盈回吐比例")
        if self.trailing_activation.mode is ThresholdMode.PROFIT_GIVEBACK_PERCENT:
            raise ValueError("停利啟動不支援最大浮盈回吐比例")
        if self.stop_loss.mode is ThresholdMode.PROFIT_GIVEBACK_PERCENT:
            raise ValueError("固定停損不支援最大浮盈回吐比例")
        if min(self.entry_slippage_points, self.exit_slippage_points, self.fee_cost_amount) < 0:
            raise ValueError("滑價與費用不可小於 0")
        if (
            self.trailing_drawdown.mode is ThresholdMode.PROFIT_GIVEBACK_PERCENT
            and self.trailing_drawdown.value > 100
        ):
            raise ValueError("最大浮盈回吐比例不可超過 100%")

    @classmethod
    def from_dict(cls, value: dict) -> "StrategyParameters":
        return cls(
            gap_limit=Threshold.from_dict(value["gap_limit"]),
            trailing_activation=Threshold.from_dict(value["trailing_activation"]),
            trailing_drawdown=Threshold.from_dict(value["trailing_drawdown"]),
            stop_loss=Threshold.from_dict(value["stop_loss"]),
            entry_slippage_points=float(value.get("entry_slippage_points", 0)),
            exit_slippage_points=float(value.get("exit_slippage_points", 0)),
            fee_cost_amount=float(value.get("fee_cost_amount", 0)),
        )

    def canonical_dict(self) -> dict:
        result = asdict(self)
        for key in ("gap_limit", "trailing_activation", "trailing_drawdown", "stop_loss"):
            result[key]["mode"] = result[key]["mode"].value
        return result

    @property
    def parameter_id(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @property
    def execution_key(self) -> tuple:
        """跳空只影響是否進場，不影響進場後結果。"""
        return (
            self.trailing_activation.mode.value, self.trailing_activation.value,
            self.trailing_drawdown.mode.value, self.trailing_drawdown.value,
            self.stop_loss.mode.value, self.stop_loss.value,
            self.entry_slippage_points, self.exit_slippage_points, self.fee_cost_amount,
        )


@dataclass(frozen=True)
class BacktestSettings:
    start: date
    end: date
    long_parameters: tuple[StrategyParameters, ...]
    short_parameters: tuple[StrategyParameters, ...]
    signal_column: str = "策略_核心_五日價量"
    futures_id: str = "TX"
    entry_time: time = time(8, 45)
    forced_exit_time: time = time(13, 45)

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if self.signal_column not in ALLOWED_SIGNAL_COLUMNS:
            raise ValueError(f"signal_column 只允許：{sorted(ALLOWED_SIGNAL_COLUMNS)}")
        if not self.long_parameters:
            raise ValueError("多單至少需要一組策略參數")
        if not self.short_parameters:
            raise ValueError("空單至少需要一組策略參數")

    @property
    def parameter_count(self) -> int:
        return len(self.long_parameters) + len(self.short_parameters)


@dataclass(frozen=True)
class Tick:
    at: datetime
    price: float
    source_row_no: int


def _distance(threshold: Threshold, reference_price: float) -> float:
    if threshold.mode is ThresholdMode.POINTS:
        return threshold.value
    if threshold.mode is ThresholdMode.PRICE_PERCENT:
        return reference_price * threshold.value / 100.0
    raise ValueError("此門檻必須使用點數或價格百分比")


def gap_allows_entry(direction: str, day_open: float, previous_full_close: float,
                     threshold: Threshold) -> bool:
    distance = _distance(threshold, previous_full_close)
    gap_points = day_open - previous_full_close
    # 規格是「超過」才不做單，所以恰好等於門檻仍進場。
    return gap_points <= distance if direction == "LONG" else gap_points >= -distance


def _adverse_fill(price: float, direction: str, slippage: float, *, is_entry: bool) -> float:
    if not slippage:
        return price
    if direction == "LONG":
        return price + slippage if is_entry else price - slippage
    return price - slippage if is_entry else price + slippage


def simulate_ticks(
    ticks: Sequence[Tick],
    direction: str,
    parameters: StrategyParameters,
    *,
    theoretical_open: float | None = None,
) -> dict:
    """依成交先後掃描一次；停損及停利均採觸及即觸發。"""
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction 必須是 LONG 或 SHORT")
    if not ticks:
        return {
            "exit_reason": "MISSING_DATA", "resolution_level": "NO_DATA",
            "is_confirmed": False, "ambiguous_reason": "日盤無 Tick",
            "data_quality_status": "MISSING",
        }

    first = ticks[0]
    entry_price = _adverse_fill(first.price, direction, parameters.entry_slippage_points,
                                is_entry=True)
    sign = 1.0 if direction == "LONG" else -1.0
    activation_distance = _distance(parameters.trailing_activation, entry_price)
    stop_distance = _distance(parameters.stop_loss, entry_price)
    activation_price = entry_price + sign * activation_distance
    stop_price = entry_price - sign * stop_distance
    prices = np.fromiter((tick.price for tick in ticks), dtype=np.float64, count=len(ticks))
    indices = np.arange(len(prices))
    stop_hits = indices[(indices > 0) & (sign * (prices - stop_price) <= 0)]
    activation_hits = indices[(indices > 0) & (sign * (prices - activation_price) >= 0)]
    stop_index = int(stop_hits[0]) if len(stop_hits) else None
    activation_index = int(activation_hits[0]) if len(activation_hits) else None
    trailing_index: int | None = None

    if activation_index is not None and (stop_index is None or activation_index < stop_index):
        segment = prices[activation_index:]
        if direction == "LONG":
            running_best = np.maximum.accumulate(segment)
        else:
            running_best = np.minimum.accumulate(segment)
        if parameters.trailing_drawdown.mode is ThresholdMode.PROFIT_GIVEBACK_PERCENT:
            maximum_profit = sign * (running_best - entry_price)
            drawdown_distance = maximum_profit * parameters.trailing_drawdown.value / 100.0
        elif parameters.trailing_drawdown.mode is ThresholdMode.POINTS:
            drawdown_distance = np.full_like(running_best, parameters.trailing_drawdown.value)
        else:
            drawdown_distance = running_best * parameters.trailing_drawdown.value / 100.0
        trailing_price = running_best - sign * drawdown_distance
        trailing_hits = np.flatnonzero(sign * (segment - trailing_price) <= 0)
        if len(trailing_hits):
            trailing_index = activation_index + int(trailing_hits[0])

    # 同一筆價格若同時碰停損與移動停利，依實際下單保守採停損。
    if stop_index is not None and (trailing_index is None or stop_index <= trailing_index):
        exit_index = stop_index
        exit_reason = "STOP_LOSS"
    elif trailing_index is not None:
        exit_index = trailing_index
        exit_reason = "TAKE_PROFIT"
    else:
        exit_index = len(ticks) - 1
        exit_reason = "CLOSE_EXIT"

    exit_tick = ticks[exit_index]
    activation_tick = ticks[activation_index] if activation_index is not None else None
    stop_tick = ticks[stop_index] if exit_reason == "STOP_LOSS" else None
    best_slice = prices[:exit_index + 1]
    best_price = float(best_slice.max() if direction == "LONG" else best_slice.min())
    trailing_activated = activation_index is not None and activation_index <= exit_index

    raw_exit = exit_tick.price
    simulated_exit = _adverse_fill(raw_exit, direction, parameters.exit_slippage_points,
                                   is_entry=False)
    gross_points = sign * (raw_exit - first.price)
    net_points = sign * (simulated_exit - entry_price)
    return {
        "entry_candidate_at": first.at,
        "actual_entry_at": first.at,
        "actual_entry_source_row_no": first.source_row_no,
        "theoretical_entry_price": theoretical_open if theoretical_open is not None else first.price,
        "simulated_entry_price": entry_price,
        "take_profit_first_at": activation_tick.at if activation_tick else None,
        "take_profit_source_row_no": activation_tick.source_row_no if activation_tick else None,
        "stop_loss_first_at": stop_tick.at if stop_tick else None,
        "stop_loss_source_row_no": stop_tick.source_row_no if stop_tick else None,
        "exit_reason": exit_reason,
        "resolution_level": "TICK",
        "is_confirmed": True,
        "ambiguous_reason": None,
        "actual_exit_at": exit_tick.at,
        "actual_exit_source_row_no": exit_tick.source_row_no,
        "theoretical_exit_price": raw_exit,
        "simulated_exit_price": simulated_exit,
        "gross_points": gross_points,
        "slippage_cost_points": parameters.entry_slippage_points + parameters.exit_slippage_points,
        "fee_cost_amount": parameters.fee_cost_amount,
        "net_points": net_points,
        "data_quality_status": "OK",
        "details": {
            "activation_price": activation_price,
            "stop_loss_price": stop_price,
            "best_price": best_price,
            "trailing_activated": trailing_activated,
        },
    }


def load_candidates(settings: BacktestSettings) -> pd.DataFrame:
    """載入訊號並選取開盤時已知、當日有成交的最近月份契約。"""
    from common import db

    sql = f'''
        WITH ordered AS (
            SELECT
                "日期" AS signal_date,
                TRIM("{settings.signal_column}"::text) AS signal_direction,
                "全日收" AS previous_full_close,
                LEAD("日期") OVER (PARTITION BY futures_id ORDER BY "日期") AS trade_date
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        ), signals AS (
            SELECT * FROM ordered
            WHERE signal_date BETWEEN ? AND ?
              AND signal_direction IN ('多', '空')
              AND trade_date IS NOT NULL
              AND previous_full_close IS NOT NULL
        ), opening_contract AS (
            SELECT k.trading_date, k.contract_date, k.open AS day_open,
                   ROW_NUMBER() OVER (
                       PARTITION BY k.trading_date
                       ORDER BY k.contract_date, k.bar_datetime
                   ) AS contract_rank
            FROM raw_finmind.fm_taiwan_futures_kbar_1m k
            JOIN signals s ON s.trade_date = k.trading_date
            WHERE k.futures_id = ?
              AND k.trading_session = 'position'
              AND k.contract_date ~ '^[0-9]{{6}}$'
              AND k.bar_datetime::time >= ?
              AND k.bar_datetime::time < (? + INTERVAL '1 minute')
        )
        SELECT s.signal_date, s.trade_date, s.signal_direction,
               s.previous_full_close, oc.contract_date, oc.day_open
        FROM signals s
        LEFT JOIN opening_contract oc
          ON oc.trading_date = s.trade_date AND oc.contract_rank = 1
        ORDER BY s.trade_date
    '''
    return db.sql_to_df(
        sql,
        params=(settings.futures_id, settings.start, settings.end,
                settings.futures_id, settings.entry_time, settings.entry_time),
        parse_dates=["signal_date", "trade_date"],
    )


def load_day_ticks(settings: BacktestSettings, trade_date: date,
                   contract_date: str) -> list[Tick]:
    """單日單契約只查一次，供同日所有參數組合重複使用。"""
    from common import db

    start_at = datetime.combine(trade_date, settings.entry_time)
    end_at = datetime.combine(trade_date, settings.forced_exit_time)
    sql = '''
        SELECT trade_datetime, price, source_row_no
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ? AND contract_date = ?
          AND source_date = ?
          AND trade_datetime >= ? AND trade_datetime <= ?
        ORDER BY trade_datetime, source_row_no
    '''
    frame = db.sql_to_df(
        sql, params=(settings.futures_id, contract_date, trade_date, start_at, end_at),
        parse_dates=["trade_datetime"],
    )
    return [Tick(row.trade_datetime.to_pydatetime(), float(row.price), int(row.source_row_no))
            for row in frame.itertuples(index=False)]


def run_backtests(settings: BacktestSettings,
                  candidates: pd.DataFrame | None = None) -> pd.DataFrame:
    """行情逐日載入一次，同時執行全部參數組合。"""
    source = load_candidates(settings) if candidates is None else candidates.copy()
    rows: list[dict] = []
    for item in source.itertuples(index=False):
        direction = "LONG" if item.signal_direction == "多" else "SHORT"
        parameter_pool = (
            settings.long_parameters if direction == "LONG" else settings.short_parameters
        )
        missing_contract = pd.isna(item.contract_date) or pd.isna(item.day_open)
        ticks: list[Tick] | None = None
        execution_cache: dict[tuple, dict] = {}

        for parameters in parameter_pool:
            base = {
                "parameter_id": f"{direction.lower()}_{parameters.parameter_id}",
                "signal_date": pd.Timestamp(item.signal_date).date(),
                "trade_date": pd.Timestamp(item.trade_date).date(),
                "signal_direction": item.signal_direction,
                "direction": direction,
                "previous_full_close": float(item.previous_full_close),
                "day_open": None if pd.isna(item.day_open) else float(item.day_open),
                "contract_date": None if pd.isna(item.contract_date) else str(item.contract_date),
                "parameters": json.dumps(parameters.canonical_dict(), ensure_ascii=False),
            }
            if missing_contract:
                rows.append({**base, "entry_status": "MISSING_DATA",
                             "exit_reason": "MISSING_DATA", "net_points": None,
                             "data_quality_status": "MISSING"})
                continue
            if not gap_allows_entry(direction, float(item.day_open),
                                    float(item.previous_full_close), parameters.gap_limit):
                rows.append({**base, "entry_status": "GAP_FILTERED",
                             "exit_reason": "NO_ENTRY", "net_points": 0.0,
                             "data_quality_status": "OK"})
                continue
            if ticks is None:
                ticks = load_day_ticks(settings, pd.Timestamp(item.trade_date).date(),
                                       str(item.contract_date))
            result = execution_cache.get(parameters.execution_key)
            if result is None:
                result = simulate_ticks(ticks, direction, parameters,
                                        theoretical_open=float(item.day_open))
                execution_cache[parameters.execution_key] = result
            rows.append({**base, "entry_status": "ENTERED", **result})
    return pd.DataFrame(rows)


def calculate_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for parameter_id, frame in detail.groupby("parameter_id", sort=False):
        entered = frame[(frame["entry_status"] == "ENTERED") & frame["net_points"].notna()].copy()
        points = pd.to_numeric(entered["net_points"], errors="coerce")
        equity = pd.concat([pd.Series([0.0]), points.reset_index(drop=True)]).cumsum()
        drawdown = equity - equity.cummax()
        wins = points[points > 0].sum()
        losses = -points[points < 0].sum()
        rows.append({
            "parameter_id": parameter_id,
            "direction": frame.iloc[0]["direction"],
            "parameters": frame.iloc[0]["parameters"],
            "signal_count": len(frame),
            "entered_count": len(entered),
            "gap_filtered_count": int((frame["entry_status"] == "GAP_FILTERED").sum()),
            "missing_count": int((frame["data_quality_status"] == "MISSING").sum()),
            "win_rate_pct": round(float((points > 0).mean() * 100), 2) if len(points) else None,
            "total_net_points": round(float(points.sum()), 4),
            "average_net_points": round(float(points.mean()), 4) if len(points) else None,
            "median_net_points": round(float(points.median()), 4) if len(points) else None,
            "profit_factor": round(float(wins / losses), 4) if losses > 0 else None,
            "max_drawdown_points": round(float(drawdown.min()), 4) if len(drawdown) else None,
            "stop_loss_count": int((entered["exit_reason"] == "STOP_LOSS").sum()),
            "take_profit_count": int((entered["exit_reason"] == "TAKE_PROFIT").sum()),
            "close_exit_count": int((entered["exit_reason"] == "CLOSE_EXIT").sum()),
        })
    return pd.DataFrame(rows).sort_values(
        ["total_net_points", "profit_factor"], ascending=[False, False], na_position="last"
    )


def export_reports(detail: pd.DataFrame, summary: pd.DataFrame, settings: BacktestSettings,
                   output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}"
    detail_path = output_dir / f"futures_stop_trailing_2_1_directional_detail_{suffix}.csv"
    summary_path = output_dir / f"futures_stop_trailing_2_1_directional_summary_{suffix}.csv"
    detail.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return detail_path, summary_path


def _thresholds(spec: dict) -> list[Threshold]:
    if "mode" in spec:
        values = spec.get("values", [spec.get("value")])
        return [Threshold(ThresholdMode(str(spec["mode"]).upper()), float(value))
                for value in values]
    result: list[Threshold] = []
    for mode, values in spec.items():
        result.extend(Threshold(ThresholdMode(str(mode).upper()), float(value))
                      for value in values)
    return result


def _build_parameter_grid(grid: dict, costs: dict) -> tuple[StrategyParameters, ...]:
    parameter_sets = tuple(
        StrategyParameters(
            gap_limit=gap,
            trailing_activation=activation,
            trailing_drawdown=drawdown,
            stop_loss=stop,
            entry_slippage_points=float(costs.get("entry_slippage_points", 0)),
            exit_slippage_points=float(costs.get("exit_slippage_points", 0)),
            fee_cost_amount=float(costs.get("fee_cost_amount", 0)),
        )
        for gap, activation, drawdown, stop in itertools.product(
            _thresholds(grid["gap_limit"]),
            _thresholds(grid["trailing_activation"]),
            _thresholds(grid["trailing_drawdown"]),
            _thresholds(grid["stop_loss"]),
        )
    )
    unique = {value.parameter_id: value for value in parameter_sets}
    return tuple(unique.values())


def load_config(path: Path) -> tuple[BacktestSettings, Path]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    grids = raw["parameter_grid"]
    if "long" not in grids or "short" not in grids:
        raise ValueError("parameter_grid 必須分別包含 long 與 short")
    shared = raw.get("costs", {})
    settings = BacktestSettings(
        start=date.fromisoformat(raw["start"]), end=date.fromisoformat(raw["end"]),
        long_parameters=_build_parameter_grid(grids["long"], shared),
        short_parameters=_build_parameter_grid(grids["short"], shared),
        signal_column=raw.get("signal_column", "策略_核心_五日價量"),
        futures_id=raw.get("futures_id", "TX"),
        entry_time=time.fromisoformat(raw.get("entry_time", "08:45:00")),
        forced_exit_time=time.fromisoformat(raw.get("forced_exit_time", "13:45:00")),
    )
    return settings, Path(raw.get("output_dir", "../output/backtest"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="2-1 固定停損／移動停利參數化回測")
    parser.add_argument("--config", type=Path,
                        default=Path("backtest/configs/futures_stop_trailing_2_1.json"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings, output_dir = load_config(args.config)
    print(
        f"參數組合：多單 {len(settings.long_parameters):,} 組、"
        f"空單 {len(settings.short_parameters):,} 組，合計 {settings.parameter_count:,} 組"
    )
    detail = run_backtests(settings)
    summary = calculate_summary(detail)
    detail_path, summary_path = export_reports(detail, summary, settings, output_dir)
    print(summary.head(20).to_string(index=False))
    print(f"\n明細：{detail_path.resolve()}")
    print(f"摘要：{summary_path.resolve()}")


if __name__ == "__main__":
    main()
