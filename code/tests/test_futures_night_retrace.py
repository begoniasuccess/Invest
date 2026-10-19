from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from backtest.futures_night_retrace import Config, XBand, simulate_day


def config() -> Config:
    return Config(
        futures_id="TX", start_date=date(2024, 1, 1), end_date=date(2026, 1, 2),
        entry_start_time=time(8, 45), last_entry_time=time(13, 20),
        forced_exit_time=time(13, 25), x_bands=(XBand("x", 0.5, 1.0),),
        y_retrace_pct_values=(0.5,), z_stop_pct_values=(0.5,),
        entry_slippage_points=0, exit_slippage_points=0, fee_tax_points=0,
        train_end=date(2024, 12, 31), validation_end=date(2025, 12, 31),
        minimum_train_entries=1, detail_top_n=1, output_dir=Path("out"),
        write_database=False, export_csv=False,
    )


def candidate():
    return SimpleNamespace(
        group_date=pd.Timestamp("2024-01-02"), trade_date=pd.Timestamp("2024-01-03"),
        contract_date="202401", base_day_close=10000.0, night_close=10100.0,
        night_return_pct=1.0,
    )


def bars(rows):
    return pd.DataFrame([
        {"bar_datetime": pd.Timestamp(datetime(2024, 1, 3, hour, minute)),
         "open": o, "high": h, "low": l, "close": c}
        for hour, minute, o, h, l, c in rows
    ])


def test_limit_fill_then_take_profit():
    data = bars([(8, 45, 10080, 10090, 10040, 10060), (8, 46, 10060, 10110, 10055, 10100)])
    result = simulate_day(candidate(), data, 0.5, 0.5, config())
    assert abs(result["entry_price"] - 10050) < 1e-9
    assert result["exit_reason"] == "TAKE_PROFIT"
    assert abs(result["gross_points"] - 50) < 1e-9


def test_same_bar_stop_and_target_uses_stop():
    data = bars([(8, 45, 10040, 10110, 9980, 10080), (13, 25, 10080, 10080, 10080, 10080)])
    result = simulate_day(candidate(), data, 0.5, 0.5, config())
    assert result["exit_reason"] == "STOP_LOSS"
    assert result["ambiguous_bar"] is True


def test_no_fill_returns_no_entry():
    data = bars([(8, 45, 10080, 10110, 10060, 10090), (13, 25, 10090, 10100, 10080, 10095)])
    result = simulate_day(candidate(), data, 0.5, 0.5, config())
    assert result["entry_status"] == "NO_FILL"
    assert result["net_points"] == 0
