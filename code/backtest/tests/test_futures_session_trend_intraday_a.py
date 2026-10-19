from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pandas as pd

from backtest.futures_session_trend_intraday_a import (
    STRATEGY_DEFINITIONS,
    Settings,
    run_backtest,
    simulate_limit_trade,
)
from backtest.futures_turning_level_intraday import Tick


def ticks(*prices: float) -> list[Tick]:
    start = datetime(2026, 1, 5, 8, 45)
    return [Tick(start + timedelta(seconds=i), price, i + 1) for i, price in enumerate(prices)]


def test_strategy_definitions() -> None:
    assert set(STRATEGY_DEFINITIONS) == {"2-2-A.7", "2-2-A.8", "2-2-A.9", "2-2-A.10"}


def test_fixed_stop_is_25_percent_of_range() -> None:
    result = simulate_limit_trade(ticks(1000, 975), "LONG", 1000, 100, .25, .25, .5, .5)
    assert result["exit_reason"] == "STOP_LOSS"
    assert result["net_points"] == -25


def test_fixed_take_profit_is_50_percent_of_range() -> None:
    result = simulate_limit_trade(ticks(1000, 1050), "LONG", 1000, 100, .25, .25, .5, .5)
    assert result["exit_reason"] == "FIXED_TAKE_PROFIT"
    assert result["net_points"] == 50


def test_long_trailing_gives_back_half_of_maximum_profit() -> None:
    result = simulate_limit_trade(
        ticks(1000, 1025, 1040, 1021, 1020), "LONG", 1000, 100, .25, .25, .5, .5,
    )
    assert result["exit_reason"] == "TRAILING_TAKE_PROFIT"
    assert result["net_points"] == 20
    assert result["last_trailing_stop_price"] == 1020


def test_short_trailing_gives_back_half_of_maximum_profit() -> None:
    result = simulate_limit_trade(
        ticks(1000, 975, 960, 979, 980), "SHORT", 1000, 100, .25, .25, .5, .5,
    )
    assert result["exit_reason"] == "TRAILING_TAKE_PROFIT"
    assert result["net_points"] == 20
    assert result["last_trailing_stop_price"] == 980


def test_distance_filter_uses_yesterday_night_close() -> None:
    candidate = pd.DataFrame([vars(SimpleNamespace(
        signal_date=date(2026, 1, 2), trade_date=date(2026, 1, 5),
        signal_five_day="多", day_trend="漲勢盤", night_trend="漲勢盤",
        night_close=1000, day_support=1029, day_resistance=1100,
        night_support=970, night_resistance=1100,
        average_range_30=100, contract_date="202601",
    ))])
    settings = Settings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidate, lambda *_: ticks(1000, 970, 1000))
    assert result[result.strategy == "2-2-A.7"].iloc[0].entry_status == "DISTANCE_FILTERED"
    assert result[result.strategy == "2-2-A.9"].iloc[0].entry_status == "ENTERED"
