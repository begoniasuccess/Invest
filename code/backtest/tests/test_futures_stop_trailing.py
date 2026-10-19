from datetime import datetime
from pathlib import Path

import pytest

from backtest.futures_stop_trailing import (
    StrategyParameters,
    Threshold,
    ThresholdMode,
    Tick,
    gap_allows_entry,
    load_config,
    simulate_ticks,
)


def params(*, drawdown_mode=ThresholdMode.POINTS, drawdown=100) -> StrategyParameters:
    return StrategyParameters(
        gap_limit=Threshold(ThresholdMode.POINTS, 500),
        trailing_activation=Threshold(ThresholdMode.POINTS, 125),
        trailing_drawdown=Threshold(drawdown_mode, drawdown),
        stop_loss=Threshold(ThresholdMode.POINTS, 125),
    )


def ticks(*prices: float) -> list[Tick]:
    return [Tick(datetime(2026, 9, 10, 8, 45, index), price, index + 1)
            for index, price in enumerate(prices)]


def test_gap_filter_is_strictly_greater_than() -> None:
    threshold = Threshold(ThresholdMode.POINTS, 500)
    assert gap_allows_entry("LONG", 47500, 47000, threshold)
    assert not gap_allows_entry("LONG", 47501, 47000, threshold)
    assert gap_allows_entry("SHORT", 46500, 47000, threshold)
    assert not gap_allows_entry("SHORT", 46499, 47000, threshold)


def test_long_stop_loss_touch() -> None:
    result = simulate_ticks(ticks(47000, 46900, 46875), "LONG", params())
    assert result["exit_reason"] == "STOP_LOSS"
    assert result["net_points"] == -125


def test_long_trailing_uses_best_price() -> None:
    result = simulate_ticks(ticks(47000, 47125, 47300, 47201, 47200), "LONG", params())
    assert result["exit_reason"] == "TAKE_PROFIT"
    assert result["net_points"] == 200
    assert result["details"]["best_price"] == 47300


def test_short_trailing() -> None:
    result = simulate_ticks(ticks(47000, 46875, 46700, 46799, 46800), "SHORT", params())
    assert result["exit_reason"] == "TAKE_PROFIT"
    assert result["net_points"] == 200


def test_close_exit() -> None:
    result = simulate_ticks(ticks(47000, 47050, 47020), "LONG", params())
    assert result["exit_reason"] == "CLOSE_EXIT"
    assert result["net_points"] == 20


def test_price_percent_stop() -> None:
    value = StrategyParameters(
        gap_limit=Threshold(ThresholdMode.PRICE_PERCENT, 1),
        trailing_activation=Threshold(ThresholdMode.PRICE_PERCENT, 1),
        trailing_drawdown=Threshold(ThresholdMode.PRICE_PERCENT, 0.2),
        stop_loss=Threshold(ThresholdMode.PRICE_PERCENT, 0.25),
    )
    result = simulate_ticks(ticks(40000, 39900), "LONG", value)
    assert result["exit_reason"] == "STOP_LOSS"


def test_profit_giveback_percent() -> None:
    result = simulate_ticks(
        ticks(47000, 47125, 47300, 47151, 47150), "LONG",
        params(drawdown_mode=ThresholdMode.PROFIT_GIVEBACK_PERCENT, drawdown=50),
    )
    assert result["exit_reason"] == "TAKE_PROFIT"
    assert result["net_points"] == 150


def test_invalid_giveback_location() -> None:
    with pytest.raises(ValueError):
        StrategyParameters(
            gap_limit=Threshold(ThresholdMode.PROFIT_GIVEBACK_PERCENT, 50),
            trailing_activation=Threshold(ThresholdMode.POINTS, 125),
            trailing_drawdown=Threshold(ThresholdMode.POINTS, 100),
            stop_loss=Threshold(ThresholdMode.POINTS, 125),
        )


def test_directional_grid_is_split() -> None:
    settings, _ = load_config(
        Path(__file__).parents[1] / "configs" / "futures_stop_trailing_2_1.json"
    )
    assert len(settings.long_parameters) == 162
    assert len(settings.short_parameters) == 162
    assert settings.parameter_count == 324
