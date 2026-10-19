from datetime import date, datetime, timedelta
from types import SimpleNamespace
import unittest

import pandas as pd

from backtest.futures_session_trend_intraday import (
    STRATEGY_DEFINITIONS,
    Settings,
    core_direction,
    run_backtest,
    select_entry_level,
)
from backtest.futures_turning_level_intraday import Tick


def row(**overrides):
    values = dict(
        signal_date=date(2026, 1, 2), trade_date=date(2026, 1, 5),
        signal_five_day="多", day_trend="漲勢盤", night_trend="跌勢盤",
        day_support=100, day_resistance=130,
        night_support=90, night_resistance=140,
        average_range_30=100, contract_date="202601",
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def ticks(*prices):
    start = datetime(2026, 1, 5, 8, 45)
    return [Tick(start + timedelta(seconds=i), price, i) for i, price in enumerate(prices)]


def test_four_strategy_definitions_are_exact():
    assert set(STRATEGY_DEFINITIONS) == {"2-2.7", "2-2.8", "2-2.9", "2-2.10"}
    assert STRATEGY_DEFINITIONS["2-2.7"].direction == "LONG"
    assert STRATEGY_DEFINITIONS["2-2.8"].direction == "SHORT"
    assert STRATEGY_DEFINITIONS["2-2.9"].level_source == "NIGHT"
    assert STRATEGY_DEFINITIONS["2-2.10"].level_source == "NIGHT"


def test_trend_selects_support_or_resistance_independent_of_direction():
    sample = row()
    assert select_entry_level(sample, "2-2.7") == ("漲勢盤", "SUPPORT", 100)
    assert select_entry_level(sample, "2-2.8") == ("漲勢盤", "SUPPORT", 100)
    assert select_entry_level(sample, "2-2.9") == ("跌勢盤", "RESISTANCE", 140)
    assert select_entry_level(sample, "2-2.10") == ("跌勢盤", "RESISTANCE", 140)


def test_core_direction_parser():
    assert core_direction("多") == "LONG"
    assert core_direction("空") == "SHORT"
    assert core_direction("觀望") is None


def test_run_only_activates_matching_core_direction_and_classifies_opening_fill():
    candidates = pd.DataFrame([vars(row())])
    settings = Settings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, lambda *_: ticks(95, 100, 151))
    assert len(result) == 4
    assert set(result[result.entry_status == "ENTERED"].strategy) == {"2-2.7", "2-2.9"}
    assert set(result[result.entry_status == "CORE_DIRECTION_MISMATCH"].strategy) == {"2-2.8", "2-2.10"}
    day = result[result.strategy == "2-2.7"].iloc[0]
    assert day.actual_entry_price == 95
    assert day.entry_type == "OPENING_IMMEDIATE"


def test_short_bullish_session_uses_support_as_limit_floor():
    candidates = pd.DataFrame([vars(row(
        signal_five_day="空", day_trend="漲勢盤", night_trend="漲勢盤",
    ))])
    settings = Settings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, lambda *_: ticks(110, 100, 50))
    entered = result[result.entry_status == "ENTERED"]
    assert set(entered.strategy) == {"2-2.8", "2-2.10"}
    day = entered[entered.strategy == "2-2.8"].iloc[0]
    assert day.entry_level == 100
    assert day.actual_entry_price == 110
    assert day.entry_type == "OPENING_IMMEDIATE"


def test_missing_trend_and_missing_selected_level_do_not_trade():
    candidates = pd.DataFrame([
        vars(row(day_trend=None)),
        vars(row(night_trend="跌勢盤", night_resistance=None)),
    ])
    settings = Settings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, lambda *_: ticks(100))
    assert result[(result.strategy == "2-2.7") & (result.entry_status == "MISSING_TREND")].shape[0] == 1
    assert result[(result.strategy == "2-2.9") & (result.entry_status == "MISSING_SOURCE_DATA")].shape[0] == 1


class TestFunctions(unittest.TestCase):
    pass


for _name, _function in list(globals().items()):
    if _name.startswith("test_") and callable(_function):
        setattr(TestFunctions, _name, staticmethod(_function))
