from datetime import datetime, timedelta

import numpy as np

from backtest.futures_signal_path_statistics import (
    StatisticsSettings,
    _first_hit_label,
    _path_metrics,
    _threshold_rows,
    _trailing_rows,
)
from backtest.futures_stop_trailing import Tick


def ticks(*prices: float) -> list[Tick]:
    start = datetime(2026, 9, 10, 8, 45)
    return [Tick(start + timedelta(seconds=index), price, index + 1)
            for index, price in enumerate(prices)]


def settings() -> StatisticsSettings:
    return StatisticsSettings(
        start=datetime(2024, 1, 1).date(), end=datetime(2026, 9, 10).date(),
        signal_column="策略_核心_五日價量", futures_id="TX",
        entry_time=datetime.strptime("08:45", "%H:%M").time(),
        forced_exit_time=datetime.strptime("13:45", "%H:%M").time(),
        point_thresholds=(100.0, 150.0), percent_thresholds=(0.5,),
        trailing_activation_points=(100.0,), trailing_drawdown_points=(75.0,),
        trailing_drawdown_percents=(0.2,), output_dir=None,
    )


def test_long_path_metrics() -> None:
    values = ticks(1000, 1100, 950, 1080)
    result, _, _ = _path_metrics(values, "LONG")
    assert result["mfe_points"] == 100
    assert result["mae_points"] == 50
    assert result["open_to_close_points"] == 80


def test_short_path_metrics() -> None:
    values = ticks(1000, 900, 1050, 920)
    result, _, _ = _path_metrics(values, "SHORT")
    assert result["mfe_points"] == 100
    assert result["mae_points"] == 50
    assert result["open_to_close_points"] == 80


def test_first_hit_order() -> None:
    assert _first_hit_label(2, 4) == "FAVORABLE_FIRST"
    assert _first_hit_label(5, 3) == "ADVERSE_FIRST"
    assert _first_hit_label(None, None) == "NEITHER"


def test_threshold_rows() -> None:
    signed = np.array([0, 100, -150, 50], dtype=float)
    rows = _threshold_rows({"direction": "LONG"}, 1000, signed, settings())
    point100 = next(row for row in rows
                    if row["threshold_mode"] == "POINTS" and row["threshold_value"] == 100)
    assert point100["favorable_hit"]
    assert point100["adverse_hit"]
    assert point100["first_hit"] == "FAVORABLE_FIRST"


def test_trailing_rebound_to_new_best() -> None:
    values = ticks(1000, 1100, 1025, 1150)
    metrics, prices, signed = _path_metrics(values, "LONG")
    rows = _trailing_rows(
        {"direction": "LONG", "trade_date": datetime(2026, 9, 10).date()},
        values, prices, signed, settings(),
    )
    row = next(item for item in rows
               if item["drawdown_mode"] == "POINTS" and item["drawdown_value"] == 75)
    assert row["activation_hit"]
    assert row["drawdown_hit"]
    assert row["new_best_after_drawdown"]
