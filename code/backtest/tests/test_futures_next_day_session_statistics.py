from datetime import date

import pandas as pd

from backtest.futures_next_day_session_statistics import (
    Settings,
    classify_night_change,
    prepare_detail,
    summarize,
)


def sample() -> pd.DataFrame:
    return pd.DataFrame({
        "signal_date": [date(2026, 1, 2), date(2026, 1, 5)],
        "target_date": [date(2026, 1, 5), date(2026, 1, 6)],
        "core_direction": ["多", "空"],
        "signal_day_trend": ["漲勢盤", "跌勢盤"],
        "signal_night_trend": ["跌勢盤", "漲勢盤"],
        "signal_day_close": [1000, 1000],
        "signal_night_close": [990, 1010],
        "average_range_30": [100, 100],
        "target_day_open": [995, 1005],
        "target_day_high": [1030, 1020],
        "target_day_low": [980, 970],
        "target_day_close": [1020, 980],
    })


def test_night_change_boundaries() -> None:
    assert classify_night_change(-1.01) == "明顯下跌"
    assert classify_night_change(-1.0) == "小幅下跌"
    assert classify_night_change(0.0) == "小幅上漲"
    assert classify_night_change(1.0) == "明顯上漲"


def test_prepare_detail_formulas_and_direction_hits() -> None:
    detail = prepare_detail(sample())
    assert round(detail.iloc[0].low_close_rebound_pct, 6) == round(40 / 1020 * 100, 6)
    assert round(detail.iloc[0].high_close_pullback_pct, 6) == round(10 / 1020 * 100, 6)
    assert detail.core_direction_hit.tolist() == [True, True]
    assert detail.night_change_bucket.tolist() == ["小幅下跌", "明顯上漲"]


def test_summary_contains_percentiles_and_sample_flag() -> None:
    detail = prepare_detail(sample())
    result = summarize(detail, ["core_direction"], 100)
    assert set(result.sample_status) == {"LOW_SAMPLE"}
    assert "low_close_rebound_p50_pct" in result.columns
    assert "high_close_pullback_range_p90_pct" in result.columns


def test_start_is_fixed_at_2018() -> None:
    try:
        Settings(date(2019, 1, 1), date(2026, 1, 1))
    except ValueError:
        return
    raise AssertionError("start 應固定為 2018-01-01")
