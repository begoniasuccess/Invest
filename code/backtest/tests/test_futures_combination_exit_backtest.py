from datetime import date, datetime, timedelta

import pandas as pd

from backtest.futures_combination_exit_backtest import Settings, run_backtest, simulate_open_trade, simulate_wait_entry_trade, summarize
from backtest.futures_turning_level_intraday import Tick


def ticks(prices: list[float]) -> list[Tick]:
    start = datetime(2026, 1, 2, 8, 45)
    return [Tick(start + timedelta(seconds=i), price, i) for i, price in enumerate(prices)]


def test_new_rule_activates_at_one_third_and_exits_after_one_sixth_drawdown() -> None:
    result = simulate_open_trade(
        ticks([1000, 1020, 1034, 1050, 1033]), "LONG", 100,
        stop_enabled=True, activation_ratio=1 / 3, drawdown_ratio=1 / 6,
        round_trip_cost_points=3,
    )
    assert result["trailing_activated"] is True
    assert result["trailing_activation_price"] == 1000 + 100 / 3
    assert result["exit_reason"] == "TRAILING_TAKE_PROFIT"
    assert result["gross_points"] == 33
    assert result["net_points"] == 30


def test_new_rule_does_not_activate_before_one_third() -> None:
    result = simulate_open_trade(
        ticks([1000, 1032, 1010]), "LONG", 100,
        stop_enabled=True, activation_ratio=1 / 3, drawdown_ratio=1 / 6,
        round_trip_cost_points=0,
    )
    assert result["trailing_activated"] is False
    assert result["exit_reason"] == "CLOSE_EXIT"
    assert result["gross_points"] == 10


def test_short_rule_is_symmetric() -> None:
    result = simulate_open_trade(
        ticks([1000, 966, 950, 967]), "SHORT", 100,
        stop_enabled=True, activation_ratio=1 / 3, drawdown_ratio=1 / 6,
        round_trip_cost_points=0,
    )
    assert result["trailing_activated"] is True
    assert result["exit_reason"] == "TRAILING_TAKE_PROFIT"
    assert result["gross_points"] == 33


def test_stop_precedes_activation() -> None:
    result = simulate_open_trade(
        ticks([1000, 974, 1040]), "LONG", 100,
        stop_enabled=True, activation_ratio=1 / 3, drawdown_ratio=1 / 6,
        round_trip_cost_points=0,
    )
    assert result["exit_reason"] == "STOP_LOSS"
    assert result["gross_points"] == -26


def test_wait_entry_cancels_when_first_30_minutes_cross_planned_stop() -> None:
    result = simulate_wait_entry_trade(
        ticks([1000, 924, 960]), "LONG", 1000, 100,
        stop_enabled=True, activation_ratio=None, drawdown_ratio=None,
        round_trip_cost_points=0,
    )
    assert result["target_entry_price"] == 950
    assert result["pre_entry_stop_price"] == 925
    assert result["entry_status"] == "CANCELLED_PRE_ENTRY_STOP"


def test_wait_entry_uses_first_actual_fill_after_0915() -> None:
    day = datetime(2026, 1, 2, 8, 45)
    stream = [
        Tick(day, 1000, 0),
        Tick(day + timedelta(minutes=20), 940, 1),
        Tick(day + timedelta(minutes=30), 945, 2),
        Tick(day + timedelta(minutes=31), 970, 3),
    ]
    result = simulate_wait_entry_trade(
        stream, "LONG", 1000, 100,
        stop_enabled=False, activation_ratio=None, drawdown_ratio=None,
        round_trip_cost_points=0,
    )
    assert result["entry_status"] == "ENTERED"
    assert result["entry_price"] == 945
    assert result["exit_price"] == 970


def test_wait_entry_does_not_fill_when_target_not_reached_after_0915() -> None:
    day = datetime(2026, 1, 2, 8, 45)
    stream = [Tick(day, 1000, 0), Tick(day + timedelta(minutes=30), 951, 1)]
    result = simulate_wait_entry_trade(
        stream, "LONG", 1000, 100,
        stop_enabled=False, activation_ratio=None, drawdown_ratio=None,
        round_trip_cost_points=0,
    )
    assert result["entry_status"] == "TARGET_NOT_REACHED"


def test_run_reuses_ticks_and_outputs_four_variants() -> None:
    settings = Settings(start=date(2026, 1, 2), end=date(2026, 1, 2))
    candidates = pd.DataFrame([{
        "訊號日": date(2026, 1, 1), "交易日": date(2026, 1, 2),
        "組合家族": "test", "組合訊號": "signal", "交易方向": "LONG",
        "平均振幅30": 100.0, "夜盤收": 1000.0,
    }])
    calls = []

    def loader(*args):
        calls.append(args)
        day = datetime(2026, 1, 2, 8, 45)
        return [Tick(day, 1000, 0), Tick(day + timedelta(minutes=30), 950, 1), Tick(day + timedelta(minutes=31), 990, 2)]

    detail = run_backtest(
        settings, candidates=candidates, contract_map={date(2026, 1, 2): "202601"},
        tick_loader=loader,
    )
    assert len(calls) == 1
    assert len(detail) == 4
    assert set(detail["variant"]) == {"A_CLOSE_ONLY", "B_STOP_ONLY", "C_OLD_2_1", "D_NEW_1_3_1_6"}
    summary = summarize(detail, ["signal_name", "variant"])
    assert len(summary) == 4
    assert {"profit_factor", "max_drawdown_points", "trailing_activation_rate_pct"}.issubset(summary.columns)
