from datetime import date, datetime, timedelta
from types import SimpleNamespace
import unittest

import pandas as pd

from backtest.futures_session_trend_night import (
    NightSettings,
    STRATEGY_DEFINITIONS,
    run_backtest,
    select_entry_level,
    session_window,
)
from backtest.futures_turning_level_intraday import Tick


def row(**overrides):
    values = dict(
        signal_date=date(2026, 1, 2), signal_five_day="多",
        day_trend="漲勢盤", day_support=100, day_resistance=130,
        previous_night_date=date(2026, 1, 1), previous_night_trend="跌勢盤",
        previous_night_support=90, previous_night_resistance=140,
        average_range_30=100, contract_date="202601",
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def ticks(*prices):
    start = datetime(2026, 1, 2, 17)
    return [Tick(start + timedelta(seconds=i), price, i) for i, price in enumerate(prices)]


def test_four_night_strategies_are_exact():
    assert set(STRATEGY_DEFINITIONS) == {"2-2.11", "2-2.12", "2-2.13", "2-2.14"}
    assert STRATEGY_DEFINITIONS["2-2.11"].direction == "LONG"
    assert STRATEGY_DEFINITIONS["2-2.12"].direction == "SHORT"
    assert STRATEGY_DEFINITIONS["2-2.13"].level_source == "PREVIOUS_NIGHT"


def test_window_crosses_midnight():
    settings = NightSettings(date(2018, 1, 1), date(2099, 12, 31))
    start, end = session_window(date(2026, 1, 2), settings)
    assert start == datetime(2026, 1, 2, 17)
    assert end == datetime(2026, 1, 3, 5)


def test_day_and_previous_night_levels_are_not_mixed():
    sample = row()
    assert select_entry_level(sample, "2-2.11") == ("漲勢盤", "SUPPORT", 100, date(2026, 1, 2))
    assert select_entry_level(sample, "2-2.13") == ("跌勢盤", "RESISTANCE", 140, date(2026, 1, 1))


def test_matching_core_only_and_window_start_price_improvement():
    candidates = pd.DataFrame([vars(row())])
    settings = NightSettings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, lambda *_: ticks(95, 100, 151))
    assert len(result) == 4
    assert set(result[result.entry_status == "ENTERED"].strategy) == {"2-2.11", "2-2.13"}
    assert set(result[result.entry_status == "CORE_DIRECTION_MISMATCH"].strategy) == {"2-2.12", "2-2.14"}
    assert set(result[result.entry_status == "ENTERED"].entry_type) == {"WINDOW_START_IMMEDIATE"}


def test_short_uses_support_as_limit_floor_when_trend_is_bullish():
    candidates = pd.DataFrame([vars(row(
        signal_five_day="空", previous_night_trend="漲勢盤",
    ))])
    settings = NightSettings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, lambda *_: ticks(110, 100, 50))
    night = result[result.strategy == "2-2.14"].iloc[0]
    assert night.entry_status == "ENTERED"
    assert night.entry_level == 90
    assert night.actual_entry_price == 110


def test_summary_signal_denominator_excludes_core_mismatch():
    candidates = pd.DataFrame([vars(row())])
    settings = NightSettings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, lambda *_: ticks(95, 100, 151))
    assert result[result.strategy == "2-2.12"].direction.isna().all()


class TestFunctions(unittest.TestCase):
    pass


for _name, _function in list(globals().items()):
    if _name.startswith("test_") and callable(_function):
        setattr(TestFunctions, _name, staticmethod(_function))
