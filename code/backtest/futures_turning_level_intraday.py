from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd

SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
STRATEGIES = tuple("ABCDEFGHIJKL")
YESTERDAY_SIGNAL = "策略_核心_昨價量"
FIVE_DAY_SIGNAL = "策略_核心_五日價量"


@dataclass(frozen=True)
class StrategyDefinition:
    code: str
    signal_column: str
    level_source: str
    distance_filter: bool


STRATEGY_DEFINITIONS = {
    code: StrategyDefinition(
        code,
        YESTERDAY_SIGNAL if code in "ACEGIK" else FIVE_DAY_SIGNAL,
        "DAY" if code in "ABGH" else "NIGHT" if code in "CDIJ" else "COMBINED",
        code in "GHIJKL",
    )
    for code in STRATEGIES
}


@dataclass(frozen=True)
class Tick:
    at: datetime
    price: float
    source_row_no: int


@dataclass(frozen=True)
class Settings:
    start: date
    end: date
    futures_id: str = "TX"
    entry_time: time = time(8, 45)
    forced_exit_time: time = time(13, 45)
    range_days: int = 30
    stop_loss_ratio: float = 0.15
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
        if self.range_days < 1 or min(self.stop_loss_ratio, self.take_profit_ratio) <= 0:
            raise ValueError("振幅天數、停損與停利比例必須大於 0")
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


def strategy_direction(row: object, strategy: str) -> tuple[str | None, str | None]:
    definition = STRATEGY_DEFINITIONS[strategy]
    attribute = "signal_yesterday" if definition.signal_column == YESTERDAY_SIGNAL else "signal_five_day"
    signal = str(getattr(row, attribute, "")).strip()
    if signal == "多":
        return signal, "LONG"
    if signal == "空":
        return signal, "SHORT"
    return signal or None, None


def strategy_entry_level(row: object, strategy: str, direction: str) -> float | None:
    source = STRATEGY_DEFINITIONS[strategy].level_source
    day_support = _number(getattr(row, "day_support", None))
    day_resistance = _number(getattr(row, "day_resistance", None))
    night_support = _number(getattr(row, "night_support", None))
    night_resistance = _number(getattr(row, "night_resistance", None))
    if direction == "LONG":
        if source == "DAY":
            return day_support
        if source == "NIGHT":
            return night_support
        values = [v for v in (day_support, night_support) if v is not None]
        return min(values) if len(values) == 2 else None
    if direction == "SHORT":
        if source == "DAY":
            return day_resistance
        if source == "NIGHT":
            return night_resistance
        values = [v for v in (day_resistance, night_resistance) if v is not None]
        return max(values) if len(values) == 2 else None
    raise ValueError("direction 必須是 LONG 或 SHORT")


def filter_allows_entry(strategy: str, entry_level: float, night_close: float,
                        average_range: float, ratio: float) -> bool:
    if not STRATEGY_DEFINITIONS[strategy].distance_filter:
        return True
    return abs(entry_level - night_close) >= average_range * ratio


def simulate_limit_trade(
    ticks: Sequence[Tick], direction: str, entry_level: float,
    average_range: float, stop_loss_ratio: float, take_profit_ratio: float,
    round_trip_cost_points: float = 0.0,
) -> dict:
    """依 Tick 時序回測限價進場；成交後才啟動停損與停利。"""
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction 必須是 LONG 或 SHORT")
    if not ticks:
        return {"entry_status": "MISSING_TICKS", "exit_reason": "MISSING_DATA"}
    touched: Callable[[Tick], bool] = ((lambda t: t.price <= entry_level) if direction == "LONG"
                                      else (lambda t: t.price >= entry_level))
    entry_index = next((i for i, tick in enumerate(ticks) if touched(tick)), None)
    if entry_index is None:
        return {"entry_status": "NOT_TOUCHED", "exit_reason": "NO_ENTRY"}
    sign = 1.0 if direction == "LONG" else -1.0
    entry_tick = ticks[entry_index]
    entry_price = min(entry_level, entry_tick.price) if direction == "LONG" else max(entry_level, entry_tick.price)
    stop_price = entry_price - sign * average_range * stop_loss_ratio
    take_price = entry_price + sign * average_range * take_profit_ratio
    exit_tick, exit_price, reason = ticks[-1], ticks[-1].price, "CLOSE_EXIT"
    for tick in ticks[entry_index + 1:]:
        if sign * (tick.price - stop_price) <= 0:
            exit_tick, exit_price, reason = tick, tick.price, "STOP_LOSS"
            break
        if sign * (tick.price - take_price) >= 0:
            exit_tick, exit_price, reason = tick, take_price, "TAKE_PROFIT"
            break
    gross = sign * (exit_price - entry_price)
    return {
        "entry_status": "ENTERED", "entry_at": entry_tick.at,
        "entry_source_row_no": entry_tick.source_row_no,
        "theoretical_entry_price": entry_level, "actual_entry_price": entry_price,
        "stop_loss_price": stop_price, "take_profit_price": take_price,
        "exit_at": exit_tick.at, "exit_source_row_no": exit_tick.source_row_no,
        "actual_exit_price": exit_price, "exit_reason": reason,
        "gross_points": gross, "cost_points": round_trip_cost_points,
        "net_points": gross - round_trip_cost_points,
    }


def load_candidates(settings: Settings) -> pd.DataFrame:
    from common import db
    sql = f'''
        WITH history AS (
            SELECT futures_id, "日期" AS signal_date,
                TRIM("{YESTERDAY_SIGNAL}"::text) AS signal_yesterday,
                TRIM("{FIVE_DAY_SIGNAL}"::text) AS signal_five_day,
                "夜盤收" AS night_close,
                "日盤轉折支撐" AS day_support, "日盤轉折壓力" AS day_resistance,
                "夜盤轉折支撐" AS night_support, "夜盤轉折壓力" AS night_resistance,
                CASE WHEN COUNT("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) = {settings.range_days}
                THEN AVG("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) END AS average_range_30,
                LEAD("日期") OVER (PARTITION BY futures_id ORDER BY "日期") AS trade_date
            FROM {SOURCE_VIEW} WHERE futures_id = ?
        ), candidates AS (
            SELECT * FROM history WHERE trade_date BETWEEN ? AND ?
        ), opening_contract AS (
            SELECT k.trading_date, k.contract_date,
                ROW_NUMBER() OVER (PARTITION BY k.trading_date ORDER BY k.contract_date, k.bar_datetime) AS contract_rank
            FROM raw_finmind.fm_taiwan_futures_kbar_1m k
            JOIN candidates c ON c.trade_date = k.trading_date
            WHERE k.futures_id = ? AND k.trading_session = 'position'
              AND k.contract_date ~ '^[0-9]{{6}}$'
              AND k.bar_datetime::time BETWEEN ? AND (? + INTERVAL '1 minute')
        )
        SELECT c.*, oc.contract_date FROM candidates c
        LEFT JOIN opening_contract oc ON oc.trading_date = c.trade_date AND oc.contract_rank = 1
        ORDER BY c.trade_date
    '''
    return db.sql_to_df(sql, params=(settings.futures_id, settings.start, settings.end,
                                     settings.futures_id, settings.entry_time, settings.entry_time),
                        parse_dates=["signal_date", "trade_date"])


def load_day_ticks(settings: Settings, trade_date: date, contract_date: str) -> list[Tick]:
    from common import db
    frame = db.sql_to_df('''
        SELECT trade_datetime, price, source_row_no
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ? AND contract_date = ? AND source_date = ?
          AND trade_datetime >= ? AND trade_datetime <= ?
        ORDER BY trade_datetime, source_row_no
    ''', params=(settings.futures_id, contract_date, trade_date,
                 datetime.combine(trade_date, settings.entry_time),
                 datetime.combine(trade_date, settings.forced_exit_time)),
        parse_dates=["trade_datetime"])
    return [Tick(r.trade_datetime.to_pydatetime(), float(r.price), int(r.source_row_no))
            for r in frame.itertuples(index=False)]


def run_backtest(settings: Settings, candidates: pd.DataFrame | None = None,
                 tick_loader=load_day_ticks) -> pd.DataFrame:
    source = load_candidates(settings) if candidates is None else candidates.copy()
    rows: list[dict] = []
    for item in source.itertuples(index=False):
        avg_range, night_close = _number(item.average_range_30), _number(item.night_close)
        contract = None if pd.isna(item.contract_date) else str(item.contract_date)
        ticks: list[Tick] | None = None
        for strategy in STRATEGIES:
            definition = STRATEGY_DEFINITIONS[strategy]
            signal, direction = strategy_direction(item, strategy)
            entry = strategy_entry_level(item, strategy, direction) if direction else None
            base = {
                "strategy": strategy, "signal_column": definition.signal_column,
                "level_source": definition.level_source,
                "distance_filter_enabled": definition.distance_filter,
                "signal_date": pd.Timestamp(item.signal_date).date(),
                "trade_date": pd.Timestamp(item.trade_date).date(),
                "signal_direction": signal, "direction": direction,
                "contract_date": contract, "night_close": night_close,
                "average_range_30": avg_range, "entry_level": entry,
                "filter_distance_required": avg_range * settings.entry_filter_ratio if avg_range is not None else None,
                "entry_distance_from_night_close": abs(entry - night_close) if entry is not None and night_close is not None else None,
            }
            if direction is None:
                rows.append({**base, "entry_status": "NO_DIRECTION_SIGNAL", "exit_reason": "NO_ENTRY"})
                continue
            if entry is None or avg_range is None or not contract or (definition.distance_filter and night_close is None):
                rows.append({**base, "entry_status": "MISSING_SOURCE_DATA", "exit_reason": "MISSING_DATA"})
                continue
            if not filter_allows_entry(strategy, entry, night_close, avg_range, settings.entry_filter_ratio):
                rows.append({**base, "entry_status": "DISTANCE_FILTERED", "exit_reason": "NO_ENTRY",
                             "gross_points": 0.0, "cost_points": 0.0, "net_points": 0.0})
                continue
            if ticks is None:
                ticks = tick_loader(settings, pd.Timestamp(item.trade_date).date(), contract)
            rows.append({**base, **simulate_limit_trade(
                ticks, direction, entry, avg_range, settings.stop_loss_ratio,
                settings.take_profit_ratio, settings.round_trip_cost_points)})
    detail = pd.DataFrame(rows)
    if not detail.empty:
        detail["net_twd"] = pd.to_numeric(detail.get("net_points"), errors="coerce") * settings.point_value_twd * settings.contracts
    return detail


def _max_losing_streak(points: pd.Series) -> int:
    longest = current = 0
    for value in points:
        current = current + 1 if value < 0 else 0
        longest = max(longest, current)
    return longest


def summarize(detail: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    rows = []
    for keys, frame in detail.groupby(group_columns, sort=True, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        signalled = frame[frame["direction"].isin(["LONG", "SHORT"])]
        eligible = signalled[~signalled["entry_status"].isin(["MISSING_SOURCE_DATA", "DISTANCE_FILTERED"])]
        entered = frame[frame["entry_status"] == "ENTERED"]
        sort_columns = [column for column in ("trade_date", "entry_at") if column in entered.columns]
        if sort_columns:
            entered = entered.sort_values(sort_columns)
        points = pd.to_numeric(
            entered["net_points"] if "net_points" in entered else pd.Series(dtype=float),
            errors="coerce",
        ).dropna()
        gross_points = pd.to_numeric(
            entered["gross_points"] if "gross_points" in entered else pd.Series(dtype=float),
            errors="coerce",
        )
        net_twd = pd.to_numeric(
            entered["net_twd"] if "net_twd" in entered else pd.Series(dtype=float),
            errors="coerce",
        )
        equity = pd.concat([pd.Series([0.0]), points.reset_index(drop=True)]).cumsum()
        drawdown = equity - equity.cummax()
        profit, loss = float(points[points > 0].sum()), float(-points[points < 0].sum())
        market_days = int(frame["trade_date"].nunique())
        months = int(pd.to_datetime(frame["trade_date"]).dt.to_period("M").nunique())
        row = dict(zip(group_columns, keys))
        row.update({
            "market_day_count": market_days, "signal_count": len(signalled),
            "eligible_order_count": len(eligible), "entry_count": len(entered),
            "entry_rate_per_signal_pct": round(len(entered) / len(signalled) * 100, 2) if len(signalled) else None,
            "entry_rate_per_eligible_order_pct": round(len(entered) / len(eligible) * 100, 2) if len(eligible) else None,
            "entry_frequency_per_market_day_pct": round(len(entered) / market_days * 100, 2) if market_days else None,
            "average_entries_per_month": round(len(entered) / months, 2) if months else None,
            "distance_filtered_count": int((frame["entry_status"] == "DISTANCE_FILTERED").sum()),
            "not_touched_count": int((frame["entry_status"] == "NOT_TOUCHED").sum()),
            "missing_count": int(frame["entry_status"].isin(["MISSING_SOURCE_DATA", "MISSING_TICKS"]).sum()),
            "win_count": int((points > 0).sum()), "loss_count": int((points < 0).sum()),
            "breakeven_count": int((points == 0).sum()),
            "win_rate_pct": round(float((points > 0).mean() * 100), 2) if len(points) else None,
            "total_gross_points": round(float(gross_points.sum()), 2),
            "total_net_points": round(float(points.sum()), 2),
            "average_net_points": round(float(points.mean()), 2) if len(points) else None,
            "median_net_points": round(float(points.median()), 2) if len(points) else None,
            "total_net_twd": round(float(net_twd.sum()), 0),
            "profit_factor": round(profit / loss, 4) if loss else None,
            "max_drawdown_points": round(float(drawdown.min()), 2) if len(drawdown) else None,
            "max_losing_streak": _max_losing_streak(points),
            "stop_loss_count": int((entered["exit_reason"] == "STOP_LOSS").sum()),
            "take_profit_count": int((entered["exit_reason"] == "TAKE_PROFIT").sum()),
            "close_exit_count": int((entered["exit_reason"] == "CLOSE_EXIT").sum()),
        })
        rows.append(row)
    return pd.DataFrame(rows)


def export_reports(detail: pd.DataFrame, settings: Settings, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = pd.to_datetime(detail["trade_date"]).max().date() if not detail.empty else settings.end
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    detail = detail.copy()
    detail["year"] = pd.to_datetime(detail["trade_date"]).dt.year
    overall = summarize(detail, ["strategy"]); overall.insert(1, "direction", "ALL")
    by_direction = summarize(detail[detail["direction"].notna()], ["strategy", "direction"])
    summary = pd.concat([overall, by_direction], ignore_index=True).sort_values(["strategy", "direction"])
    yearly_overall = summarize(detail, ["strategy", "year"]); yearly_overall.insert(1, "direction", "ALL")
    yearly_direction = summarize(detail[detail["direction"].notna()], ["strategy", "direction", "year"])
    yearly = pd.concat([yearly_overall, yearly_direction], ignore_index=True).sort_values(["strategy", "year", "direction"])
    paths = [output_dir / f"turning_level_detail_{suffix}.csv",
             output_dir / f"turning_level_summary_{suffix}.csv",
             output_dir / f"turning_level_yearly_{suffix}.csv",
             output_dir / f"turning_level_manifest_{suffix}.json"]
    detail.to_csv(paths[0], index=False, encoding="utf-8-sig")
    summary.to_csv(paths[1], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths[2], index=False, encoding="utf-8-sig")
    paths[3].write_text(json.dumps({
        "source_view": SOURCE_VIEW, "actual_data_end": actual_end.isoformat(),
        "settings": {**asdict(settings), "start": settings.start.isoformat(), "end": settings.end.isoformat(),
                     "entry_time": settings.entry_time.isoformat(), "forced_exit_time": settings.forced_exit_time.isoformat()},
        "strategies": {k: asdict(v) for k, v in STRATEGY_DEFINITIONS.items()},
        "definitions": {
            "signal_date": "前一報表交易日的核心策略、轉折點、夜盤收，用於下一交易日日盤",
            "range": "訊號日含當日往回30個完整交易日之(日盤高-日盤低)平均",
            "distance_filter": "G-L要求 abs(進場點-訊號日夜盤收) >= 30日平均振幅*30%",
            "exit_priority": "成交後依Tick時序觸發15%停損或50%停利；否則13:45前最後Tick出場",
        }}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
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
        entry_filter_ratio=float(raw.get("entry_filter_ratio", 0.30)),
        round_trip_cost_points=float(raw.get("round_trip_cost_points", 3.0)),
        point_value_twd=float(raw.get("point_value_twd", 200.0)), contracts=int(raw.get("contracts", 1)))
    return settings, Path(raw.get("output_dir", "backtest_output/futures_turning_level"))


def main() -> None:
    parser = argparse.ArgumentParser(description="TX 日/夜盤轉折支撐壓力 A-L 當沖回測")
    parser.add_argument("--config", type=Path,
                        default=Path("backtest/configs/futures_turning_level_intraday_v2.json"))
    args = parser.parse_args()
    settings, output_dir = load_config(args.config)
    detail = run_backtest(settings)
    paths = export_reports(detail, settings, output_dir)
    print(summarize(detail, ["strategy"]).to_string(index=False))
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
