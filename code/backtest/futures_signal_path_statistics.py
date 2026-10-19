from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.futures_stop_trailing import ALLOWED_SIGNAL_COLUMNS, Tick


SOURCE_VIEW = "report.v_futures_report_daily_corrected"


@dataclass(frozen=True)
class StatisticsSettings:
    start: date
    end: date
    signal_column: str
    futures_id: str
    entry_time: time
    forced_exit_time: time
    point_thresholds: tuple[float, ...]
    percent_thresholds: tuple[float, ...]
    trailing_activation_points: tuple[float, ...]
    trailing_drawdown_points: tuple[float, ...]
    trailing_drawdown_percents: tuple[float, ...]
    output_dir: Path

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if self.signal_column not in ALLOWED_SIGNAL_COLUMNS:
            raise ValueError(f"signal_column 只允許：{sorted(ALLOWED_SIGNAL_COLUMNS)}")
        for values in (
            self.point_thresholds, self.percent_thresholds,
            self.trailing_activation_points, self.trailing_drawdown_points,
            self.trailing_drawdown_percents,
        ):
            if not values or any(not np.isfinite(value) or value <= 0 for value in values):
                raise ValueError("所有統計門檻都必須是大於 0 的有限數字")


def load_config(path: Path) -> StatisticsSettings:
    raw = json.loads(path.read_text(encoding="utf-8"))
    thresholds = raw["thresholds"]
    trailing = raw["trailing_analysis"]
    return StatisticsSettings(
        start=date.fromisoformat(raw["start"]),
        end=date.fromisoformat(raw["end"]),
        signal_column=raw.get("signal_column", "策略_核心_五日價量"),
        futures_id=raw.get("futures_id", "TX"),
        entry_time=time.fromisoformat(raw.get("entry_time", "08:45:00")),
        forced_exit_time=time.fromisoformat(raw.get("forced_exit_time", "13:45:00")),
        point_thresholds=tuple(sorted({float(v) for v in thresholds["points"]})),
        percent_thresholds=tuple(sorted({float(v) for v in thresholds["percents"]})),
        trailing_activation_points=tuple(sorted({float(v) for v in trailing["activation_points"]})),
        trailing_drawdown_points=tuple(sorted({float(v) for v in trailing["drawdown_points"]})),
        trailing_drawdown_percents=tuple(sorted({float(v) for v in trailing["drawdown_percents"]})),
        output_dir=Path(raw.get("output_dir", "../output/backtest/signal_path_statistics")),
    )


def load_signal_candidates(settings: StatisticsSettings) -> pd.DataFrame:
    """只使用訊號日已知欄位，並選擇隔日開盤時有成交的最近月契約。"""
    from common import db

    sql = f'''
        WITH ordered AS (
            SELECT
                "日期" AS signal_date,
                TRIM("{settings.signal_column}"::text) AS signal_direction,
                "全日收" AS previous_full_close,
                "全20MA乖離率" AS previous_ma20_deviation,
                "全MA方向_20" AS previous_ma20_direction,
                "全位置" AS previous_position,
                "全振幅_開盤_pct" AS previous_range_pct,
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
        SELECT s.*, oc.contract_date, oc.day_open
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


def load_daily_baseline(settings: StatisticsSettings) -> pd.DataFrame:
    """方向統計的全交易日／無明確訊號比較組，不讀 Tick。"""
    from common import db

    sql = f'''
        WITH ordered AS (
            SELECT
                "日期" AS signal_date,
                CASE
                    WHEN TRIM("{settings.signal_column}"::text) IN ('多', '空')
                        THEN TRIM("{settings.signal_column}"::text)
                    ELSE '無明確訊號'
                END AS signal_direction,
                LEAD("日期") OVER (PARTITION BY futures_id ORDER BY "日期") AS trade_date,
                LEAD("日盤開") OVER (PARTITION BY futures_id ORDER BY "日期") AS day_open,
                LEAD("日盤收") OVER (PARTITION BY futures_id ORDER BY "日期") AS day_close
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        )
        SELECT * FROM ordered
        WHERE signal_date BETWEEN ? AND ? AND trade_date IS NOT NULL
          AND day_open IS NOT NULL AND day_close IS NOT NULL
        ORDER BY trade_date
    '''
    return db.sql_to_df(sql, params=(settings.futures_id, settings.start, settings.end),
                        parse_dates=["signal_date", "trade_date"])


def load_day_ticks(settings: StatisticsSettings, trade_date: date,
                   contract_date: str) -> list[Tick]:
    from common import db

    start_at = datetime.combine(trade_date, settings.entry_time)
    end_at = datetime.combine(trade_date, settings.forced_exit_time)
    sql = '''
        SELECT trade_datetime, price, source_row_no
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ? AND contract_date = ? AND source_date = ?
          AND trade_datetime >= ? AND trade_datetime <= ?
        ORDER BY trade_datetime, source_row_no
    '''
    frame = db.sql_to_df(
        sql, params=(settings.futures_id, contract_date, trade_date, start_at, end_at),
        parse_dates=["trade_datetime"],
    )
    return [Tick(row.trade_datetime.to_pydatetime(), float(row.price), int(row.source_row_no))
            for row in frame.itertuples(index=False)]


def _first_index(mask: np.ndarray) -> int | None:
    hits = np.flatnonzero(mask)
    return int(hits[0]) if len(hits) else None


def _first_hit_label(favorable_index: int | None, adverse_index: int | None) -> str:
    if favorable_index is None and adverse_index is None:
        return "NEITHER"
    if adverse_index is None or (
        favorable_index is not None and favorable_index < adverse_index
    ):
        return "FAVORABLE_FIRST"
    if favorable_index is None or adverse_index < favorable_index:
        return "ADVERSE_FIRST"
    return "SAME_TICK"


def _path_metrics(ticks: list[Tick], direction: str) -> tuple[dict, np.ndarray, np.ndarray]:
    prices = np.fromiter((tick.price for tick in ticks), dtype=np.float64, count=len(ticks))
    sign = 1.0 if direction == "LONG" else -1.0
    signed_path = sign * (prices - prices[0])
    mfe_index = int(np.argmax(signed_path))
    mae_index = int(np.argmax(-signed_path))
    result = {
        "entry_price": float(prices[0]),
        "close_price": float(prices[-1]),
        "open_to_close_points": float(signed_path[-1]),
        "direction_hit": bool(signed_path[-1] > 0),
        "mfe_points": float(signed_path[mfe_index]),
        "mae_points": float(-signed_path[mae_index]),
        "mfe_pct": float(signed_path[mfe_index] / prices[0] * 100),
        "mae_pct": float(-signed_path[mae_index] / prices[0] * 100),
        "mfe_at": ticks[mfe_index].at,
        "mae_at": ticks[mae_index].at,
        "tick_count": len(ticks),
    }
    return result, prices, signed_path


def _threshold_rows(base: dict, entry_price: float, signed_path: np.ndarray,
                    settings: StatisticsSettings) -> list[dict]:
    rows: list[dict] = []
    specs = [("POINTS", value, value) for value in settings.point_thresholds]
    specs += [("PRICE_PERCENT", value, entry_price * value / 100)
              for value in settings.percent_thresholds]
    for mode, value, distance in specs:
        favorable_index = _first_index(signed_path >= distance)
        adverse_index = _first_index(signed_path <= -distance)
        rows.append({
            **base, "threshold_mode": mode, "threshold_value": value,
            "threshold_points": distance,
            "favorable_hit": favorable_index is not None,
            "adverse_hit": adverse_index is not None,
            "first_hit": _first_hit_label(favorable_index, adverse_index),
            "favorable_first_index": favorable_index,
            "adverse_first_index": adverse_index,
        })
    return rows


def _trailing_rows(base: dict, ticks: list[Tick], prices: np.ndarray,
                   signed_path: np.ndarray, settings: StatisticsSettings) -> list[dict]:
    rows: list[dict] = []
    sign = 1.0 if base["direction"] == "LONG" else -1.0
    for activation in settings.trailing_activation_points:
        activation_index = _first_index(signed_path >= activation)
        if activation_index is None:
            for mode, values in (
                ("POINTS", settings.trailing_drawdown_points),
                ("PRICE_PERCENT", settings.trailing_drawdown_percents),
            ):
                for drawdown in values:
                    rows.append({
                        **base, "activation_points": activation,
                        "activation_hit": False, "activation_at": None,
                        "drawdown_mode": mode, "drawdown_value": drawdown,
                        "drawdown_hit": False, "drawdown_at": None,
                        "best_profit_before_drawdown": None,
                        "new_best_after_drawdown": None,
                        "close_profit_points": float(signed_path[-1]),
                    })
            continue

        segment_prices = prices[activation_index:]
        if sign > 0:
            running_best_price = np.maximum.accumulate(segment_prices)
        else:
            running_best_price = np.minimum.accumulate(segment_prices)
        running_best_profit = sign * (running_best_price - prices[0])
        pullback_points = sign * (running_best_price - segment_prices)
        specs = [("POINTS", value, np.full_like(prices[activation_index:], value))
                 for value in settings.trailing_drawdown_points]
        specs += [("PRICE_PERCENT", value, running_best_price * value / 100)
                  for value in settings.trailing_drawdown_percents]
        for mode, value, distances in specs:
            local_index = _first_index(pullback_points >= distances)
            absolute_index = activation_index + local_index if local_index is not None else None
            best_before = (float(running_best_profit[local_index])
                           if local_index is not None else None)
            new_best = None
            if absolute_index is not None:
                prior_best_price = running_best_price[local_index]
                remaining = prices[absolute_index + 1:]
                new_best = bool(
                    len(remaining) and (
                        remaining.max() > prior_best_price if sign > 0
                        else remaining.min() < prior_best_price
                    )
                )
            rows.append({
                **base, "activation_points": activation,
                "activation_hit": True, "activation_at": ticks[activation_index].at,
                "drawdown_mode": mode, "drawdown_value": value,
                "drawdown_hit": absolute_index is not None,
                "drawdown_at": ticks[absolute_index].at if absolute_index is not None else None,
                "best_profit_before_drawdown": best_before,
                "new_best_after_drawdown": new_best,
                "close_profit_points": float(signed_path[-1]),
            })
    return rows


def calculate_daily_statistics(settings: StatisticsSettings,
                               candidates: pd.DataFrame | None = None
                               ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source = load_signal_candidates(settings) if candidates is None else candidates.copy()
    daily_rows: list[dict] = []
    threshold_rows: list[dict] = []
    trailing_rows: list[dict] = []
    for sequence_no, item in enumerate(source.itertuples(index=False), start=1):
        signal_date = pd.Timestamp(item.signal_date).date()
        trade_date = pd.Timestamp(item.trade_date).date()
        direction = "LONG" if item.signal_direction == "多" else "SHORT"
        base = {
            "signal_date": signal_date, "trade_date": trade_date,
            "signal_direction": item.signal_direction, "direction": direction,
            "previous_full_close": float(item.previous_full_close),
            "day_open": None if pd.isna(item.day_open) else float(item.day_open),
            "contract_date": None if pd.isna(item.contract_date) else str(item.contract_date),
            "previous_ma20_deviation": item.previous_ma20_deviation,
            "previous_ma20_direction": item.previous_ma20_direction,
            "previous_position": item.previous_position,
            "previous_range_pct": item.previous_range_pct,
        }
        if pd.isna(item.contract_date) or pd.isna(item.day_open):
            daily_rows.append({**base, "data_quality_status": "MISSING_OPENING_CONTRACT"})
            continue
        ticks = load_day_ticks(settings, trade_date, str(item.contract_date))
        if not ticks:
            daily_rows.append({**base, "data_quality_status": "MISSING_TICK"})
            continue
        metrics, prices, signed_path = _path_metrics(ticks, direction)
        gap_points = float(item.day_open) - float(item.previous_full_close)
        gap_pct = gap_points / float(item.previous_full_close) * 100
        signed_gap_pct = gap_pct if direction == "LONG" else -gap_pct
        row = {
            **base, **metrics, "gap_points": gap_points, "gap_pct": gap_pct,
            "signed_gap_pct": signed_gap_pct, "data_quality_status": "OK",
        }
        daily_rows.append(row)
        event_base = {key: row[key] for key in (
            "signal_date", "trade_date", "signal_direction", "direction",
            "entry_price", "open_to_close_points", "mfe_points", "mae_points",
        )}
        threshold_rows.extend(_threshold_rows(event_base, metrics["entry_price"],
                                              signed_path, settings))
        trailing_rows.extend(_trailing_rows(event_base, ticks, prices, signed_path, settings))
        if sequence_no % 100 == 0:
            print(f"[基礎統計] 已處理 {sequence_no:,}/{len(source):,} 個訊號日")
    return pd.DataFrame(daily_rows), pd.DataFrame(threshold_rows), pd.DataFrame(trailing_rows)


def _describe_rows(frame: pd.DataFrame, group_columns: list[str],
                   value_columns: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    grouped = frame.groupby(group_columns, dropna=False, sort=False, observed=False)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_columns, keys))
        row["sample_count"] = len(group)
        for column in value_columns:
            values = pd.to_numeric(group[column], errors="coerce").dropna()
            row.update({
                f"{column}_mean": values.mean(), f"{column}_median": values.median(),
                f"{column}_q10": values.quantile(.10), f"{column}_q25": values.quantile(.25),
                f"{column}_q75": values.quantile(.75), f"{column}_q90": values.quantile(.90),
            })
        rows.append(row)
    return pd.DataFrame(rows)


def build_summaries(daily: pd.DataFrame, thresholds: pd.DataFrame,
                    trailing: pd.DataFrame, baseline: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ok = daily[daily["data_quality_status"] == "OK"].copy()
    direction_summary = _describe_rows(
        ok, ["direction"], ["open_to_close_points", "mfe_points", "mae_points"]
    )
    win_rates = ok.groupby("direction")["direction_hit"].mean().mul(100)
    direction_summary["direction_hit_rate_pct"] = direction_summary["direction"].map(win_rates)

    threshold_summary = thresholds.groupby(
        ["direction", "threshold_mode", "threshold_value"], as_index=False
    ).agg(
        sample_count=("trade_date", "size"),
        favorable_hit_rate_pct=("favorable_hit", lambda x: x.mean() * 100),
        adverse_hit_rate_pct=("adverse_hit", lambda x: x.mean() * 100),
        favorable_first_rate_pct=("first_hit", lambda x: (x == "FAVORABLE_FIRST").mean() * 100),
        adverse_first_rate_pct=("first_hit", lambda x: (x == "ADVERSE_FIRST").mean() * 100),
        neither_rate_pct=("first_hit", lambda x: (x == "NEITHER").mean() * 100),
    )

    bins = [-np.inf, 0, .4, .6, .8, 1.0, 1.2, np.inf]
    labels = ["反向跳空", "0~0.4%", "0.4~0.6%", "0.6~0.8%",
              "0.8~1.0%", "1.0~1.2%", "1.2%以上"]
    ok["gap_bucket"] = pd.cut(ok["signed_gap_pct"], bins=bins, labels=labels,
                              right=False)
    gap_summary = _describe_rows(
        ok, ["direction", "gap_bucket"],
        ["open_to_close_points", "mfe_points", "mae_points"],
    )
    gap_wins = ok.groupby(["direction", "gap_bucket"], observed=False)["direction_hit"].mean().mul(100)
    gap_summary["direction_hit_rate_pct"] = [
        gap_wins.get((row.direction, row.gap_bucket), np.nan)
        for row in gap_summary.itertuples(index=False)
    ]

    trailing_summary = trailing.groupby(
        ["direction", "activation_points", "drawdown_mode", "drawdown_value"],
        as_index=False,
    ).agg(
        sample_count=("trade_date", "size"),
        activation_hit_rate_pct=("activation_hit", lambda x: x.mean() * 100),
        drawdown_hit_rate_pct=("drawdown_hit", lambda x: x.mean() * 100),
        new_best_after_drawdown_rate_pct=(
            "new_best_after_drawdown",
            lambda x: pd.Series(x).dropna().astype(bool).mean() * 100
            if len(pd.Series(x).dropna()) else np.nan,
        ),
        median_best_profit_before_drawdown=("best_profit_before_drawdown", "median"),
        mean_close_profit_points=("close_profit_points", "mean"),
    )

    environment_frames: list[pd.DataFrame] = []
    environment_specs = {
        "MA20方向": "previous_ma20_direction",
        "前日位置": "previous_position",
    }
    deviation = pd.to_numeric(ok["previous_ma20_deviation"], errors="coerce")
    ok["MA20上下"] = np.select(
        [deviation >= 0, deviation < 0], ["收盤在MA20上", "收盤在MA20下"],
        default=None,
    )
    environment_specs["MA20上下"] = "MA20上下"
    range_values = pd.to_numeric(ok["previous_range_pct"], errors="coerce")
    try:
        ok["前日波動分組"] = pd.qcut(range_values, 3, labels=["低波動", "中波動", "高波動"])
        environment_specs["前日波動"] = "前日波動分組"
    except ValueError:
        pass
    for dimension, column in environment_specs.items():
        summary = _describe_rows(
            ok.dropna(subset=[column]), ["direction", column],
            ["open_to_close_points", "mfe_points", "mae_points"],
        )
        summary.insert(1, "environment_dimension", dimension)
        summary = summary.rename(columns={column: "environment_value"})
        environment_frames.append(summary)

    base = baseline.copy()
    base["open_to_close_points"] = base["day_close"] - base["day_open"]
    base["up_day"] = base["open_to_close_points"] > 0
    baseline_summary = base.groupby("signal_direction", as_index=False).agg(
        sample_count=("trade_date", "size"),
        mean_open_to_close_points=("open_to_close_points", "mean"),
        median_open_to_close_points=("open_to_close_points", "median"),
        up_day_rate_pct=("up_day", lambda x: x.mean() * 100),
    )
    all_row = pd.DataFrame([{
        "signal_direction": "全部交易日", "sample_count": len(base),
        "mean_open_to_close_points": base["open_to_close_points"].mean(),
        "median_open_to_close_points": base["open_to_close_points"].median(),
        "up_day_rate_pct": base["up_day"].mean() * 100,
    }])

    quality = daily.groupby("data_quality_status", as_index=False).agg(
        sample_count=("trade_date", "size"), min_trade_date=("trade_date", "min"),
        max_trade_date=("trade_date", "max"),
    )
    return {
        "direction_summary": direction_summary,
        "threshold_summary": threshold_summary,
        "gap_bucket_summary": gap_summary,
        "trailing_summary": trailing_summary,
        "environment_summary": pd.concat(environment_frames, ignore_index=True),
        "baseline_summary": pd.concat([all_row, baseline_summary], ignore_index=True),
        "data_quality_summary": quality,
    }


def export_reports(settings: StatisticsSettings, daily: pd.DataFrame,
                   thresholds: pd.DataFrame, trailing: pd.DataFrame,
                   summaries: dict[str, pd.DataFrame]) -> list[Path]:
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}"
    frames = {
        "daily_path_detail": daily,
        "threshold_first_hit_detail": thresholds,
        "trailing_pullback_detail": trailing,
        **summaries,
    }
    paths: list[Path] = []
    for name, frame in frames.items():
        path = settings.output_dir / f"futures_signal_{name}_{suffix}.csv"
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        paths.append(path)
    manifest = settings.output_dir / f"futures_signal_manifest_{suffix}.json"
    manifest.write_text(json.dumps({
        "start": settings.start.isoformat(), "end": settings.end.isoformat(),
        "signal_column": settings.signal_column, "futures_id": settings.futures_id,
        "files": [path.name for path in paths],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    paths.append(manifest)
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="台指期訊號隔日日盤價格路徑基礎統計")
    parser.add_argument("--config", type=Path,
                        default=Path("backtest/configs/futures_signal_path_statistics.json"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_config(args.config)
    print(f"分析期間：{settings.start}～{settings.end}")
    candidates = load_signal_candidates(settings)
    baseline = load_daily_baseline(settings)
    print(f"有效多空訊號：{len(candidates):,} 天")
    daily, thresholds, trailing = calculate_daily_statistics(settings, candidates)
    summaries = build_summaries(daily, thresholds, trailing, baseline)
    paths = export_reports(settings, daily, thresholds, trailing, summaries)
    print("\n方向摘要：")
    print(summaries["direction_summary"].to_string(index=False))
    print("\n資料品質：")
    print(summaries["data_quality_summary"].to_string(index=False))
    print(f"\n輸出資料夾：{settings.output_dir.resolve()}")
    print(f"輸出檔案：{len(paths)} 個；請將整個資料夾壓縮後提供分析。")


if __name__ == "__main__":
    main()
