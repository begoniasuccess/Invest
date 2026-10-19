import pandas as pd

from analysis.futures_night_bucket import build_daily_features, build_summaries


EDGES = (-4, -3, -2, -1, 0, 1, 2, 3, 4)


def _source() -> pd.DataFrame:
    return pd.DataFrame({
        "group_date": pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"]),
        "result_date": pd.to_datetime(["2026-01-05", "2026-01-06", None]),
        "base_day_close": [100.0, 100.0, 100.0],
        "night_open": [100.0, 100.0, 100.0],
        "night_high": [101.0, 100.0, 100.0],
        "night_low": [99.0, 98.0, 100.0],
        "night_close": [100.0, 99.0, 100.0],
        "night_source": ["test", "test", "test"],
        "result_day_open": [101.0, 98.0, None],
        "result_day_high": [104.0, 102.0, None],
        "result_day_low": [99.0, 97.0, None],
        "result_day_close": [103.0, 101.0, None],
    })


def test_build_daily_features_uses_lower_inclusive_upper_exclusive_buckets():
    daily, diagnostics = build_daily_features(_source(), EDGES)
    assert diagnostics == {"source_rows": 3, "valid_rows": 2, "excluded_rows": 1, "missing_next_day": 1, "missing_next_day_ohlc": 1}
    assert daily["bucket_id"].tolist() == [6, 5]
    assert daily["next_intraday_return_pct"].round(6).tolist() == [1.980198, 3.061224]
    assert daily["filled_base_day_close"].tolist() == [True, True]


def test_build_summaries_outputs_all_and_yearly_metrics():
    daily, _ = build_daily_features(_source(), EDGES)
    summary, yearly = build_summaries(daily)
    assert len(summary) == 20
    assert len(yearly) == 20
    assert set(summary["metric_name"]) == {
        "night_return_pct", "next_open_return_pct", "next_close_return_pct",
        "next_intraday_return_pct", "next_range_prev_close_pct", "next_range_open_pct",
        "night_close_to_next_open_pct", "max_up_from_open_pct",
        "max_down_from_open_pct", "close_location_pct",
    }

