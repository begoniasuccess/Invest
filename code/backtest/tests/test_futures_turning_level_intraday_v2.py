from datetime import date, datetime, timedelta
from types import SimpleNamespace
import unittest

import pandas as pd

from backtest.futures_turning_level_intraday import (
    STRATEGY_DEFINITIONS,
    Settings,
    Tick,
    filter_allows_entry,
    run_backtest,
    simulate_limit_trade,
    strategy_direction,
    strategy_entry_level,
    summarize,
)


def row(**overrides):
    values = dict(
        signal_date=date(2026, 1, 2), trade_date=date(2026, 1, 5),
        signal_yesterday="多", signal_five_day="空", night_close=110,
        day_support=100, day_resistance=130,
        night_support=90, night_resistance=140,
        average_range_30=100, contract_date="202601",
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def ticks(*prices):
    start = datetime(2026, 1, 5, 8, 45)
    return [Tick(start + timedelta(seconds=i), price, i) for i, price in enumerate(prices)]


def test_strategy_map_matches_a_to_l_specification():
    assert STRATEGY_DEFINITIONS["A"].signal_column == "策略_核心_昨價量"
    assert STRATEGY_DEFINITIONS["B"].signal_column == "策略_核心_五日價量"
    assert [STRATEGY_DEFINITIONS[x].level_source for x in "ABCDEF"] == [
        "DAY", "DAY", "NIGHT", "NIGHT", "COMBINED", "COMBINED"]
    assert all(not STRATEGY_DEFINITIONS[x].distance_filter for x in "ABCDEF")
    assert all(STRATEGY_DEFINITIONS[x].distance_filter for x in "GHIJKL")


def test_two_signal_columns_control_their_own_strategies():
    assert strategy_direction(row(), "A") == ("多", "LONG")
    assert strategy_direction(row(), "B") == ("空", "SHORT")
    assert strategy_direction(row(signal_yesterday="資料不足"), "A") == ("資料不足", None)


def test_entry_levels_and_combined_choice():
    sample = row()
    assert strategy_entry_level(sample, "A", "LONG") == 100
    assert strategy_entry_level(sample, "A", "SHORT") == 130
    assert strategy_entry_level(sample, "C", "LONG") == 90
    assert strategy_entry_level(sample, "D", "SHORT") == 140
    assert strategy_entry_level(sample, "E", "LONG") == 90
    assert strategy_entry_level(sample, "F", "SHORT") == 140
    assert strategy_entry_level(row(night_support=None), "E", "LONG") is None


def test_only_g_to_l_apply_distance_filter():
    assert filter_allows_entry("A", 105, 110, 100, .30)
    assert not filter_allows_entry("G", 105, 110, 100, .30)
    assert filter_allows_entry("G", 80, 110, 100, .30)


def test_long_take_profit_uses_exact_target_and_cost():
    result = simulate_limit_trade(ticks(110, 100, 151), "LONG", 100, 100, .15, .50, 3)
    assert result["actual_entry_price"] == 100
    assert result["take_profit_price"] == 150
    assert result["actual_exit_price"] == 150
    assert result["exit_reason"] == "TAKE_PROFIT"
    assert result["net_points"] == 47


def test_short_stop_uses_first_observed_tick_and_close_fallback():
    stopped = simulate_limit_trade(ticks(120, 130, 146), "SHORT", 130, 100, .15, .50)
    assert stopped["exit_reason"] == "STOP_LOSS"
    assert stopped["actual_exit_price"] == 146
    assert stopped["gross_points"] == -16
    closed = simulate_limit_trade(ticks(90, 100, 95), "LONG", 100, 100, .15, .50)
    assert closed["actual_entry_price"] == 90
    assert closed["exit_reason"] == "CLOSE_EXIT"
    assert closed["gross_points"] == 5


def test_not_touched_and_missing_ticks_are_distinct():
    assert simulate_limit_trade([], "LONG", 100, 100, .15, .50)["entry_status"] == "MISSING_TICKS"
    assert simulate_limit_trade(ticks(110, 105), "LONG", 100, 100, .15, .50)["entry_status"] == "NOT_TOUCHED"


def test_run_creates_twelve_rows_and_reuses_one_tick_load():
    candidates = pd.DataFrame([vars(row())])
    calls = []
    def loader(settings, trade_date, contract):
        calls.append((trade_date, contract))
        return ticks(110, 90, 150)
    settings = Settings(date(2018, 1, 1), date(2099, 12, 31))
    result = run_backtest(settings, candidates, loader)
    assert list(result["strategy"]) == list("ABCDEFGHIJKL")
    assert len(calls) == 1
    assert result.loc[result.strategy == "A", "direction"].item() == "LONG"
    assert result.loc[result.strategy == "B", "direction"].item() == "SHORT"


def test_summary_has_frequency_and_profit_statistics():
    detail = pd.DataFrame([
        dict(strategy="A", trade_date=date(2026, 1, 2), direction="LONG", entry_status="ENTERED",
             entry_at=datetime(2026, 1, 2, 9), exit_reason="TAKE_PROFIT", net_points=10,
             gross_points=13, net_twd=2000),
        dict(strategy="A", trade_date=date(2026, 1, 3), direction="LONG", entry_status="NOT_TOUCHED",
             entry_at=pd.NaT, exit_reason="NO_ENTRY", net_points=None, gross_points=None, net_twd=None),
    ])
    result = summarize(detail, ["strategy"]).iloc[0]
    assert result.entry_count == 1
    assert result.entry_rate_per_signal_pct == 50
    assert result.entry_frequency_per_market_day_pct == 50
    assert result.win_rate_pct == 100
    assert result.total_net_points == 10


def test_summary_handles_no_entries():
    detail = pd.DataFrame([dict(
        strategy="A", trade_date=date(2026, 1, 2), direction="LONG",
        entry_status="NOT_TOUCHED", exit_reason="NO_ENTRY")])
    result = summarize(detail, ["strategy"]).iloc[0]
    assert result.entry_count == 0
    assert result.total_net_points == 0


def test_start_is_fixed_at_2018():
    try:
        Settings(date(2019, 1, 1), date(2099, 12, 31))
    except ValueError as error:
        assert "2018-01-01" in str(error)
    else:
        raise AssertionError("非2018起始日應被拒絕")


class TestFunctions(unittest.TestCase):
    pass


for _name, _function in list(globals().items()):
    if _name.startswith("test_") and callable(_function):
        setattr(TestFunctions, _name, staticmethod(_function))


if __name__ == "__main__":
    unittest.main()
