from datetime import datetime, timedelta
from types import SimpleNamespace

from backtest.futures_turning_level_intraday import (
    Tick, filter_allows_entry, simulate_limit_trade, strategy_levels,
)


def ticks(*prices):
    start = datetime(2026, 1, 2, 8, 45)
    return [Tick(start + timedelta(seconds=i), price, i + 1)
            for i, price in enumerate(prices)]


def levels():
    return SimpleNamespace(day_support=100, day_resistance=130,
                           night_support=90, night_resistance=140)


def test_strategy_levels_are_reversed_for_short():
    assert strategy_levels(levels(), "A", "LONG") == (100, 130)
    assert strategy_levels(levels(), "A", "SHORT") == (130, 100)
    assert strategy_levels(levels(), "B", "LONG") == (90, 140)
    assert strategy_levels(levels(), "C", "LONG") == (90, 140)
    assert strategy_levels(levels(), "F", "SHORT") == (140, 90)


def test_d_to_f_filter_uses_absolute_night_close_distance():
    assert filter_allows_entry("A", 970, 1000, 100, 0.30)
    assert filter_allows_entry("D", 970, 1000, 100, 0.30)
    assert not filter_allows_entry("D", 971, 1000, 100, 0.30)


def test_long_limit_entry_then_fixed_take_profit():
    result = simulate_limit_trade(ticks(110, 100, 105, 151), "LONG", 100, 180,
                                  average_range=100, stop_loss_ratio=.15,
                                  take_profit_ratio=.50, round_trip_cost_points=3)
    assert result["entry_status"] == "ENTERED"
    assert result["exit_reason"] == "TAKE_PROFIT"
    assert result["gross_points"] == 51
    assert result["net_points"] == 48


def test_turning_target_can_exit_before_fixed_take_profit():
    result = simulate_limit_trade(ticks(105, 100, 131), "LONG", 100, 130,
                                  average_range=100, stop_loss_ratio=.15,
                                  take_profit_ratio=.50)
    assert result["exit_reason"] == "TURNING_TARGET"


def test_stop_and_close_exit_are_sequential():
    stopped = simulate_limit_trade(ticks(105, 100, 84, 140), "LONG", 100, 180,
                                   100, .15, .50)
    assert stopped["exit_reason"] == "STOP_LOSS"
    closed = simulate_limit_trade(ticks(105, 100, 110), "LONG", 100, 180,
                                  100, .15, .50)
    assert closed["exit_reason"] == "CLOSE_EXIT"
    assert closed["gross_points"] == 10


def test_short_strategy_and_unfilled_order():
    short = simulate_limit_trade(ticks(120, 130, 90), "SHORT", 130, 90,
                                 100, .15, .50)
    assert short["exit_reason"] == "TURNING_TARGET"
    assert short["gross_points"] == 40
    missed = simulate_limit_trade(ticks(120, 125), "SHORT", 130, 90,
                                  100, .15, .50)
    assert missed["entry_status"] == "NOT_TOUCHED"
