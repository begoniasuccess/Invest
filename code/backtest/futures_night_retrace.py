from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "backtest" / "configs" / "futures_night_retrace_v1.json"
STRATEGY_NAME = "futures_night_retrace_long_v1"
STRATEGY_VERSION = "1.0.0"


CANDIDATE_SQL = """
WITH ordered AS (
    SELECT
        r."日期" AS group_date,
        r."日盤收" AS base_day_close,
        r."夜盤收" AS night_close,
        LEAD(r."日期") OVER w AS trade_date,
        LEAD(r."日盤收") OVER w AS report_day_close
    FROM report.v_futures_report_daily_corrected AS r
    WHERE r.futures_id = ?
    WINDOW w AS (PARTITION BY r.futures_id ORDER BY r."日期")
), opening_contract AS (
    SELECT trading_date, contract_date, day_open
    FROM (
        SELECT
            k.trading_date,
            k.contract_date,
            k.open AS day_open,
            ROW_NUMBER() OVER (
                PARTITION BY k.trading_date
                ORDER BY k.contract_date, k.bar_datetime
            ) AS contract_rank
        FROM raw_finmind.fm_taiwan_futures_kbar_1m AS k
        WHERE k.futures_id = ?
          AND k.trading_session = 'position'
          AND k.contract_date ~ '^[0-9]{6}$'
          AND k.trading_date BETWEEN ? AND ? + 10
          AND k.bar_datetime::time = ?
    ) AS ranked
    WHERE contract_rank = 1
)
SELECT
    o.group_date, o.trade_date, o.base_day_close, o.night_close,
    o.report_day_close, c.contract_date, c.day_open
FROM ordered AS o
LEFT JOIN opening_contract AS c ON c.trading_date = o.trade_date
WHERE o.group_date BETWEEN ? AND ?
  AND o.night_close > o.base_day_close
ORDER BY o.group_date
"""


BAR_SQL = """
WITH opening_contract AS (
    SELECT trading_date, contract_date
    FROM (
        SELECT
            k.trading_date,
            k.contract_date,
            ROW_NUMBER() OVER (
                PARTITION BY k.trading_date
                ORDER BY k.contract_date, k.bar_datetime
            ) AS contract_rank
        FROM raw_finmind.fm_taiwan_futures_kbar_1m AS k
        WHERE k.futures_id = ?
          AND k.trading_session = 'position'
          AND k.contract_date ~ '^[0-9]{6}$'
          AND k.trading_date BETWEEN ? AND ?
          AND k.bar_datetime::time = ?
    ) AS ranked
    WHERE contract_rank = 1
)
SELECT
    k.trading_date, k.contract_date, k.bar_datetime,
    k.open, k.high, k.low, k.close
FROM raw_finmind.fm_taiwan_futures_kbar_1m AS k
JOIN opening_contract AS c
  ON c.trading_date = k.trading_date
 AND c.contract_date = k.contract_date
WHERE k.futures_id = ?
  AND k.trading_session = 'position'
  AND k.trading_date BETWEEN ? AND ?
  AND k.bar_datetime::time BETWEEN ? AND ?
ORDER BY k.trading_date, k.bar_datetime
"""


@dataclass(frozen=True)
class XBand:
    label: str
    minimum_pct: float
    maximum_pct: float | None

    def contains(self, values: pd.Series) -> pd.Series:
        result = values >= self.minimum_pct
        if self.maximum_pct is not None:
            result &= values < self.maximum_pct
        return result


@dataclass(frozen=True)
class Config:
    futures_id: str
    start_date: date
    end_date: date
    entry_start_time: time
    last_entry_time: time
    forced_exit_time: time
    x_bands: tuple[XBand, ...]
    y_retrace_pct_values: tuple[float, ...]
    z_stop_pct_values: tuple[float, ...]
    entry_slippage_points: float
    exit_slippage_points: float
    fee_tax_points: float
    train_end: date
    validation_end: date
    minimum_train_entries: int
    detail_top_n: int
    output_dir: Path
    write_database: bool
    export_csv: bool


@dataclass(frozen=True)
class DayBars:
    at: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    last_entry_index: int


def _parse_time(value: str) -> time:
    return time.fromisoformat(value)


def _float_tuple(values: Iterable[Any], name: str) -> tuple[float, ...]:
    result = tuple(float(value) for value in values)
    if not result or any(not math.isfinite(value) or value <= 0 for value in result):
        raise ValueError(f"{name} 必須是非空且全部大於 0 的有限數字")
    return tuple(sorted(set(result)))


def load_config(path: str | Path = DEFAULT_CONFIG) -> Config:
    config_path = Path(path)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    start_date = date.fromisoformat(raw["start_date"])
    end_date = date.fromisoformat(raw["end_date"])
    train_end = date.fromisoformat(raw.get("train_end", "2023-12-31"))
    validation_end = date.fromisoformat(raw.get("validation_end", "2025-12-31"))
    if not start_date <= train_end < validation_end <= end_date:
        raise ValueError("日期必須符合 start_date <= train_end < validation_end <= end_date")

    x_bands = tuple(
        XBand(
            label=str(item["label"]),
            minimum_pct=float(item["min_pct"]),
            maximum_pct=None if item.get("max_pct") is None else float(item["max_pct"]),
        )
        for item in raw["x_bands"]
    )
    if not x_bands:
        raise ValueError("x_bands 不可為空")
    for band in x_bands:
        if band.maximum_pct is not None and band.minimum_pct >= band.maximum_pct:
            raise ValueError(f"x 分組上下限錯誤：{band.label}")

    entry_start = _parse_time(raw.get("entry_start_time", "08:45:00"))
    last_entry = _parse_time(raw.get("last_entry_time", "13:20:00"))
    forced_exit = _parse_time(raw.get("forced_exit_time", "13:25:00"))
    if not entry_start < last_entry < forced_exit:
        raise ValueError("時間必須符合 entry_start_time < last_entry_time < forced_exit_time")

    output_dir = Path(raw.get("output_dir", "../output/backtest/night_retrace"))
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    costs = raw.get("costs", {})
    return Config(
        futures_id=str(raw.get("futures_id", "TX")),
        start_date=start_date,
        end_date=end_date,
        entry_start_time=entry_start,
        last_entry_time=last_entry,
        forced_exit_time=forced_exit,
        x_bands=x_bands,
        y_retrace_pct_values=_float_tuple(raw["y_retrace_pct_values"], "y_retrace_pct_values"),
        z_stop_pct_values=_float_tuple(raw["z_stop_pct_values"], "z_stop_pct_values"),
        entry_slippage_points=float(costs.get("entry_slippage_points", 1.0)),
        exit_slippage_points=float(costs.get("exit_slippage_points", 1.0)),
        fee_tax_points=float(costs.get("fee_tax_points", 1.0)),
        train_end=train_end,
        validation_end=validation_end,
        minimum_train_entries=int(raw.get("minimum_train_entries", 80)),
        detail_top_n=int(raw.get("detail_top_n", 20)),
        output_dir=output_dir,
        write_database=bool(raw.get("write_database", True)),
        export_csv=bool(raw.get("export_csv", True)),
    )


def load_candidates(config: Config) -> pd.DataFrame:
    from common.db import sql_to_df

    frame = sql_to_df(
        CANDIDATE_SQL,
        params=(
            config.futures_id, config.futures_id, config.start_date, config.end_date,
            config.entry_start_time,
            config.start_date, config.end_date,
        ),
        parse_dates=["group_date", "trade_date"],
    )
    for column in ["base_day_close", "night_close", "report_day_close", "day_open"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["night_return_pct"] = (
        (frame["night_close"] - frame["base_day_close"]) / frame["base_day_close"] * 100.0
    )
    return frame


def load_bars(config: Config, first_trade_date: date, last_trade_date: date) -> pd.DataFrame:
    from common.db import sql_to_df

    frame = sql_to_df(
        BAR_SQL,
        params=(
            config.futures_id, first_trade_date, last_trade_date,
            config.entry_start_time, config.futures_id,
            first_trade_date, last_trade_date,
            config.entry_start_time, config.forced_exit_time,
        ),
        parse_dates=["trading_date", "bar_datetime"],
    )
    for column in ["open", "high", "low", "close"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def parameter_id(x_band: XBand, y_pct: float, z_pct: float) -> str:
    raw = json.dumps(
        {"x": [x_band.minimum_pct, x_band.maximum_pct], "y": y_pct, "z": z_pct},
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def prepare_day_bars(frame: pd.DataFrame, last_entry_time: time) -> DayBars:
    at = pd.to_datetime(frame["bar_datetime"]).to_numpy()
    eligible = np.fromiter(
        (pd.Timestamp(value).time() <= last_entry_time for value in at),
        dtype=bool,
        count=len(at),
    )
    last_entry_index = int(np.flatnonzero(eligible)[-1]) if eligible.any() else -1
    return DayBars(
        at=at,
        open=frame["open"].to_numpy(dtype=float),
        high=frame["high"].to_numpy(dtype=float),
        low=frame["low"].to_numpy(dtype=float),
        close=frame["close"].to_numpy(dtype=float),
        last_entry_index=last_entry_index,
    )


def simulate_day(
    candidate: Any,
    bars: pd.DataFrame | DayBars | None,
    y_pct: float,
    z_pct: float,
    config: Config,
) -> dict[str, Any]:
    base = float(candidate.base_day_close)
    night_close = float(candidate.night_close)
    entry_limit = base * (1.0 + (float(candidate.night_return_pct) - y_pct) / 100.0)
    result = {
        "group_date": pd.Timestamp(candidate.group_date).date(),
        "trade_date": None if pd.isna(candidate.trade_date) else pd.Timestamp(candidate.trade_date).date(),
        "contract_date": None if pd.isna(candidate.contract_date) else str(candidate.contract_date),
        "base_day_close": base,
        "night_close": night_close,
        "night_return_pct": float(candidate.night_return_pct),
        "y_retrace_pct": y_pct,
        "z_stop_pct": z_pct,
        "entry_limit_price": entry_limit,
        "entry_status": "NO_FILL",
        "exit_reason": "NO_ENTRY",
        "gross_points": 0.0,
        "net_points": 0.0,
        "ambiguous_bar": False,
        "data_quality": "OK",
    }
    if bars is None or (isinstance(bars, pd.DataFrame) and bars.empty) or pd.isna(candidate.contract_date):
        return {**result, "entry_status": "MISSING_DATA", "exit_reason": "MISSING_DATA", "data_quality": "MISSING"}
    day = prepare_day_bars(bars, config.last_entry_time) if isinstance(bars, pd.DataFrame) else bars
    if day.last_entry_index < 0:
        return {**result, "entry_status": "MISSING_DATA", "exit_reason": "MISSING_DATA", "data_quality": "MISSING"}

    hit_positions = np.flatnonzero(day.low[:day.last_entry_index + 1] <= entry_limit)
    if not len(hit_positions):
        return result

    entry_pos = int(hit_positions[0])
    opened_below_limit = float(day.open[entry_pos]) <= entry_limit
    raw_entry = min(float(day.open[entry_pos]), entry_limit) if opened_below_limit else entry_limit
    simulated_entry = raw_entry + config.entry_slippage_points
    stop_price = simulated_entry * (1.0 - z_pct / 100.0)
    target_price = night_close

    exit_reason = "CLOSE_EXIT"
    raw_exit = float(day.close[-1])
    exit_at = pd.Timestamp(day.at[-1])
    exit_offset = len(day.at) - 1
    ambiguous = False

    for offset in range(entry_pos, len(day.at)):
        stop_hit = float(day.low[offset]) <= stop_price
        target_hit = float(day.high[offset]) >= target_price
        if offset == entry_pos and not opened_below_limit:
            # 限價在棒內才成交時，棒內高點可能早於成交；不把同棒高點當停利。
            target_hit = False
        if stop_hit and target_hit:
            exit_reason, raw_exit, exit_at, ambiguous = "STOP_LOSS", stop_price, pd.Timestamp(day.at[offset]), True
            exit_offset = offset
            break
        if stop_hit:
            exit_reason, raw_exit, exit_at = "STOP_LOSS", stop_price, pd.Timestamp(day.at[offset])
            exit_offset = offset
            break
        if target_hit:
            exit_reason, raw_exit, exit_at = "TAKE_PROFIT", target_price, pd.Timestamp(day.at[offset])
            exit_offset = offset
            break

    simulated_exit = raw_exit - config.exit_slippage_points
    gross_points = raw_exit - raw_entry
    net_points = simulated_exit - simulated_entry - config.fee_tax_points
    return {
        **result,
        "entry_status": "ENTERED",
        "exit_reason": exit_reason,
        "entry_at": pd.Timestamp(day.at[entry_pos]),
        "entry_price": raw_entry,
        "simulated_entry_price": simulated_entry,
        "stop_price": stop_price,
        "target_price": target_price,
        "exit_at": exit_at,
        "exit_price": raw_exit,
        "simulated_exit_price": simulated_exit,
        "gross_points": gross_points,
        "net_points": net_points,
        "mfe_points": float(day.high[entry_pos:exit_offset + 1].max() - simulated_entry),
        "mae_points": float(day.low[entry_pos:exit_offset + 1].min() - simulated_entry),
        "ambiguous_bar": ambiguous,
    }


def build_base_executions(config: Config, candidates: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    bar_groups = {
        pd.Timestamp(key).date(): prepare_day_bars(group.reset_index(drop=True), config.last_entry_time)
        for key, group in bars.groupby(bars["trading_date"].dt.date, sort=False)
    }
    rows: list[dict[str, Any]] = []
    yz_pairs = tuple(itertools.product(config.y_retrace_pct_values, config.z_stop_pct_values))
    total = len(candidates)
    for day_no, candidate in enumerate(candidates.itertuples(index=False), start=1):
        trade_date = None if pd.isna(candidate.trade_date) else pd.Timestamp(candidate.trade_date).date()
        day_bars = bar_groups.get(trade_date)
        for y_pct, z_pct in yz_pairs:
            rows.append(simulate_day(candidate, day_bars, y_pct, z_pct, config))
        if day_no % 250 == 0 or day_no == total:
            print(f"已模擬 {day_no:,}/{total:,} 個夜盤上漲樣本")
    return pd.DataFrame(rows)


def _segment_mask(frame: pd.DataFrame, segment: str, config: Config) -> pd.Series:
    dates = pd.to_datetime(frame["trade_date"]).dt.date
    if segment == "TRAIN":
        return dates <= config.train_end
    if segment == "VALIDATION":
        return (dates > config.train_end) & (dates <= config.validation_end)
    if segment == "TEST":
        return dates > config.validation_end
    return pd.Series(True, index=frame.index)


def _max_drawdown(points: pd.Series) -> float | None:
    if points.empty:
        return None
    equity = pd.concat([pd.Series([0.0]), points.reset_index(drop=True)]).cumsum()
    return float((equity - equity.cummax()).min())


def _longest_losing_streak(points: pd.Series) -> int:
    longest = current = 0
    for loss in (points < 0):
        current = current + 1 if loss else 0
        longest = max(longest, current)
    return longest


def summarize_slice(frame: pd.DataFrame) -> dict[str, Any]:
    signals = len(frame)
    entered = frame[(frame["entry_status"] == "ENTERED") & frame["net_points"].notna()].copy()
    points = pd.to_numeric(entered["net_points"], errors="coerce").dropna()
    wins = float(points[points > 0].sum())
    losses = float(-points[points < 0].sum())
    years = entered.assign(year=pd.to_datetime(entered["trade_date"]).dt.year).groupby("year")["net_points"].sum()
    return {
        "signal_count": signals,
        "entered_count": len(entered),
        "entry_rate_pct": len(entered) / signals * 100.0 if signals else None,
        "missing_count": int((frame["entry_status"] == "MISSING_DATA").sum()),
        "win_rate_pct": float((points > 0).mean() * 100.0) if len(points) else None,
        "total_net_points": float(points.sum()),
        "average_net_points": float(points.mean()) if len(points) else None,
        "median_net_points": float(points.median()) if len(points) else None,
        "profit_factor": wins / losses if losses > 0 else None,
        "max_drawdown_points": _max_drawdown(points),
        "max_losing_streak": _longest_losing_streak(points),
        "take_profit_count": int((entered["exit_reason"] == "TAKE_PROFIT").sum()),
        "stop_loss_count": int((entered["exit_reason"] == "STOP_LOSS").sum()),
        "close_exit_count": int((entered["exit_reason"] == "CLOSE_EXIT").sum()),
        "ambiguous_count": int(entered["ambiguous_bar"].sum()),
        "positive_year_count": int((years > 0).sum()),
        "year_count": int(len(years)),
    }


def build_summaries(config: Config, executions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, Any]] = []
    yearly_rows: list[dict[str, Any]] = []
    for band in config.x_bands:
        band_frame = executions[band.contains(executions["night_return_pct"])]
        for y_pct, z_pct in itertools.product(config.y_retrace_pct_values, config.z_stop_pct_values):
            parameter_frame = band_frame[
                (band_frame["y_retrace_pct"] == y_pct) & (band_frame["z_stop_pct"] == z_pct)
            ].sort_values("trade_date")
            pid = parameter_id(band, y_pct, z_pct)
            definition = {
                "parameter_id": pid, "x_label": band.label,
                "x_min_pct": band.minimum_pct, "x_max_pct": band.maximum_pct,
                "y_retrace_pct": y_pct, "z_stop_pct": z_pct,
            }
            for segment in ["OVERALL", "TRAIN", "VALIDATION", "TEST"]:
                sliced = parameter_frame[_segment_mask(parameter_frame, segment, config)]
                summary_rows.append({**definition, "segment": segment, **summarize_slice(sliced)})
            for year, year_frame in parameter_frame.groupby(pd.to_datetime(parameter_frame["trade_date"]).dt.year):
                yearly_rows.append({**definition, "result_year": int(year), **summarize_slice(year_frame)})
    summary = pd.DataFrame(summary_rows)
    yearly = pd.DataFrame(yearly_rows)
    return summary, yearly


def build_selection(config: Config, summary: pd.DataFrame) -> pd.DataFrame:
    metrics = [
        "entered_count", "win_rate_pct", "total_net_points", "average_net_points",
        "profit_factor", "max_drawdown_points", "positive_year_count", "year_count",
    ]
    base = summary[["parameter_id", "x_label", "x_min_pct", "x_max_pct", "y_retrace_pct", "z_stop_pct"]].drop_duplicates()
    for segment in ["TRAIN", "VALIDATION", "TEST", "OVERALL"]:
        part = summary[summary["segment"] == segment][["parameter_id", *metrics]].copy()
        part = part.rename(columns={column: f"{segment.lower()}_{column}" for column in metrics})
        base = base.merge(part, on="parameter_id", how="left")
    eligible = base["train_entered_count"].fillna(0) >= config.minimum_train_entries
    base["eligible"] = eligible
    dd = base["train_max_drawdown_points"].abs().clip(lower=1.0)
    base["train_return_drawdown_ratio"] = base["train_total_net_points"] / dd
    validation_bonus = np.where(base["validation_total_net_points"].fillna(0) > 0, 1.0, 0.0)
    base["robust_score"] = np.where(
        eligible,
        base["train_return_drawdown_ratio"].fillna(-999.0)
        + pd.to_numeric(base["train_profit_factor"], errors="coerce").fillna(0.0)
        + validation_bonus,
        -999.0,
    )
    return base.sort_values(
        ["eligible", "robust_score", "validation_total_net_points", "train_total_net_points"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True).assign(rank=lambda value: np.arange(1, len(value) + 1))


def build_detail(config: Config, executions: pd.DataFrame, selection: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for item in selection[selection["eligible"]].head(config.detail_top_n).itertuples(index=False):
        band = next(value for value in config.x_bands if value.label == item.x_label)
        chosen = executions[
            band.contains(executions["night_return_pct"])
            & (executions["y_retrace_pct"] == item.y_retrace_pct)
            & (executions["z_stop_pct"] == item.z_stop_pct)
        ].copy()
        chosen.insert(0, "parameter_id", item.parameter_id)
        chosen.insert(0, "rank", item.rank)
        chosen.insert(2, "x_label", item.x_label)
        rows.append(chosen)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _db_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        value = value.item()
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return value


def _insert_frame(cursor: Any, table: str, frame: pd.DataFrame, columns: list[str]) -> None:
    if frame.empty:
        return
    placeholders = ",".join(["%s"] * len(columns))
    rows = [tuple(_db_value(row[column]) for column in columns) for _, row in frame.iterrows()]
    cursor.executemany(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})", rows)


def save_database(run_id: uuid.UUID, config: Config, summary: pd.DataFrame,
                  yearly: pd.DataFrame, detail: pd.DataFrame, diagnostics: dict[str, Any]) -> None:
    from common.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass('research.night_retrace_backtest_run')")
            if cursor.fetchone()[0] is None:
                raise RuntimeError("請先執行 db_migrate/20260913_create_night_retrace_backtest.sql")
            cursor.execute(
                """INSERT INTO research.night_retrace_backtest_run
                (run_id,strategy_name,strategy_version,futures_id,start_date,end_date,config,status,
                 candidate_days,bar_rows,missing_days)
                VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,'running',%s,%s,%s)""",
                (run_id, STRATEGY_NAME, STRATEGY_VERSION, config.futures_id,
                 config.start_date, config.end_date, json.dumps(config_to_dict(config), ensure_ascii=False),
                 diagnostics["candidate_days"], diagnostics["bar_rows"], diagnostics["missing_days"]),
            )
            summary_db = summary.copy(); summary_db.insert(0, "run_id", run_id)
            yearly_db = yearly.copy(); yearly_db.insert(0, "run_id", run_id)
            detail_db = detail.copy(); detail_db.insert(0, "run_id", run_id)
            summary_cols = ["run_id", *summary.columns]
            yearly_cols = ["run_id", *yearly.columns]
            detail_cols = [
                "run_id", "rank", "parameter_id", "x_label", "group_date", "trade_date",
                "contract_date", "base_day_close", "night_close", "night_return_pct",
                "y_retrace_pct", "z_stop_pct", "entry_limit_price", "entry_status",
                "exit_reason", "entry_at", "entry_price", "simulated_entry_price", "stop_price",
                "target_price", "exit_at", "exit_price", "simulated_exit_price", "gross_points",
                "net_points", "mfe_points", "mae_points", "ambiguous_bar", "data_quality",
            ]
            _insert_frame(cursor, "research.night_retrace_parameter_summary", summary_db, summary_cols)
            _insert_frame(cursor, "research.night_retrace_yearly_summary", yearly_db, yearly_cols)
            _insert_frame(cursor, "research.night_retrace_trade_detail", detail_db, detail_cols)
            cursor.execute(
                "UPDATE research.night_retrace_backtest_run SET status='completed',finished_at=CURRENT_TIMESTAMP WHERE run_id=%s",
                (run_id,),
            )
        conn.commit()


def config_to_dict(config: Config) -> dict[str, Any]:
    return {
        "futures_id": config.futures_id, "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(), "entry_start_time": config.entry_start_time.isoformat(),
        "last_entry_time": config.last_entry_time.isoformat(), "forced_exit_time": config.forced_exit_time.isoformat(),
        "x_bands": [{"label": b.label, "min_pct": b.minimum_pct, "max_pct": b.maximum_pct} for b in config.x_bands],
        "y_retrace_pct_values": config.y_retrace_pct_values, "z_stop_pct_values": config.z_stop_pct_values,
        "costs": {"entry_slippage_points": config.entry_slippage_points,
                  "exit_slippage_points": config.exit_slippage_points, "fee_tax_points": config.fee_tax_points},
        "train_end": config.train_end.isoformat(), "validation_end": config.validation_end.isoformat(),
        "minimum_train_entries": config.minimum_train_entries, "detail_top_n": config.detail_top_n,
    }


def export_reports(run_id: uuid.UUID, config: Config, summary: pd.DataFrame,
                   yearly: pd.DataFrame, selection: pd.DataFrame, detail: pd.DataFrame,
                   diagnostics: dict[str, Any]) -> list[Path]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{config.start_date:%Y%m%d}_{config.end_date:%Y%m%d}"
    paths = [
        config.output_dir / f"night_retrace_parameter_summary_{suffix}.csv",
        config.output_dir / f"night_retrace_yearly_summary_{suffix}.csv",
        config.output_dir / f"night_retrace_selection_{suffix}.csv",
        config.output_dir / f"night_retrace_top_detail_{suffix}.csv",
    ]
    for frame, path in zip([summary, yearly, selection, detail], paths):
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    manifest = config.output_dir / f"night_retrace_manifest_{suffix}.json"
    manifest.write_text(json.dumps({
        "run_id": str(run_id), "strategy_name": STRATEGY_NAME,
        "strategy_version": STRATEGY_VERSION, "config": config_to_dict(config),
        "diagnostics": diagnostics, "generated_at": datetime.now().isoformat(timespec="seconds"),
        "report_files": [path.name for path in paths],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return [*paths, manifest]


def run(config: Config) -> dict[str, Any]:
    run_id = uuid.uuid4()
    candidates = load_candidates(config)
    if candidates.empty:
        raise RuntimeError("指定期間沒有夜盤上漲樣本")
    trade_dates = pd.to_datetime(candidates["trade_date"], errors="coerce").dropna()
    if trade_dates.empty:
        raise RuntimeError("夜盤上漲樣本都缺少下一交易日")
    bars = load_bars(config, trade_dates.min().date(), trade_dates.max().date())
    executions = build_base_executions(config, candidates, bars)
    summary, yearly = build_summaries(config, executions)
    selection = build_selection(config, summary)
    detail = build_detail(config, executions, selection)
    candidate_dates = set(pd.to_datetime(candidates["trade_date"]).dropna().dt.date)
    bar_dates = set(pd.to_datetime(bars["trading_date"]).dropna().dt.date)
    diagnostics = {
        "candidate_days": len(candidates), "bar_rows": len(bars),
        "missing_days": len(candidate_dates - bar_dates),
        "base_execution_rows": len(executions), "parameter_count": len(selection),
        "eligible_parameter_count": int(selection["eligible"].sum()),
        "detail_parameter_count": min(config.detail_top_n, int(selection["eligible"].sum())),
    }
    outputs: list[Path] = []
    if config.write_database:
        save_database(run_id, config, summary, yearly, detail, diagnostics)
    if config.export_csv:
        outputs = export_reports(run_id, config, summary, yearly, selection, detail, diagnostics)
    return {"run_id": str(run_id), "diagnostics": diagnostics, "outputs": outputs,
            "selection": selection}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="夜盤上漲後回檔掛多、夜收停利、固定停損參數回測")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--no-db", action="store_true")
    parser.add_argument("--no-csv", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base = load_config(args.config)
    config = Config(**{**base.__dict__, "write_database": base.write_database and not args.no_db,
                       "export_csv": base.export_csv and not args.no_csv})
    result = run(config)
    print(f"\n回測完成：run_id={result['run_id']}")
    for key, value in result["diagnostics"].items():
        print(f"{key}: {value:,}" if isinstance(value, int) else f"{key}: {value}")
    print("\n訓練期篩選前 20 組：")
    columns = ["rank", "x_label", "y_retrace_pct", "z_stop_pct", "train_entered_count",
               "train_total_net_points", "validation_total_net_points", "test_total_net_points",
               "overall_max_drawdown_points"]
    print(result["selection"][columns].head(20).to_string(index=False))
    for path in result["outputs"]:
        print(f"報告：{path.resolve()}")


if __name__ == "__main__":
    main()
