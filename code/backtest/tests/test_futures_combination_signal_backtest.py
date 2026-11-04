from datetime import date

import numpy as np
import pandas as pd

from backtest.futures_combination_signal_backtest import Settings, build_signals, prepare_daily, summarize


def sample_source(rows: int = 180) -> pd.DataFrame:
    index = np.arange(rows, dtype=float)
    dates = pd.bdate_range("2022-01-03", periods=rows)
    core = np.where(index.astype(int) % 2 == 0, "多", "空")
    trend = np.where(index.astype(int) % 2 == 0, "跌勢盤", "漲勢盤")
    day_close = 15000 + index * 2
    night_open = day_close.copy()
    night_close = day_close + np.where(index.astype(int) % 2 == 0, -20, 20)
    return pd.DataFrame({
        "日期": dates, "策略_核心_五日價量": core, "夜盤走勢": trend,
        "夜盤開": night_open, "夜盤高": np.maximum(night_open, night_close) + 30,
        "夜盤低": np.minimum(night_open, night_close) - 30, "夜盤收": night_close,
        "日盤開": day_close - 5, "日盤高": day_close + 50,
        "日盤低": day_close - 50, "日盤收": day_close,
        "日盤轉折支撐": day_close - 100, "日盤轉折壓力": day_close + 100,
        "夜盤轉折支撐": day_close - 100, "夜盤轉折壓力": day_close + 100,
        "全位置": np.where(index.astype(int) % 4 < 2, "低檔", "高檔"),
        "全5MA乖離率": np.sin(index / 5.0) * 0.05,
        "全20均上升幅度_pct": np.where(core == "多", 0.01, -0.01),
        "法人總買超_億": np.where(core == "多", 10, -10),
        "散戶多空比_微台": np.where(core == "多", -index, index),
        "散戶多空比_小台": np.where(core == "多", -index / 2, index / 2),
        "主力新建多單": index,
    })


def test_alignment_and_no_current_value_in_percentile() -> None:
    settings = Settings(start=date(2022, 4, 1), end=date(2022, 9, 30), minimum_history=20, history_window=60)
    daily = prepare_daily(sample_source(), settings)
    row = daily.iloc[0]
    source = sample_source()
    signal_index = source.index[source["日期"].eq(row["訊號日"])][0]
    assert row["隔日日期"] == source.loc[signal_index + 1, "日期"]
    assert row["隔日日盤開"] == source.loc[signal_index + 1, "日盤開"]
    assert pd.notna(row["夜盤漲跌_pct_歷史分位_pct"])
    assert pd.notna(row["MA5乖離率_pct_歷史分位_pct"])


def test_builds_expected_families_and_directional_pnl() -> None:
    settings = Settings(start=date(2022, 4, 1), end=date(2022, 9, 30), minimum_history=20, history_window=60)
    detail = build_signals(prepare_daily(sample_source(), settings), settings)
    assert {
        "A核心×夜盤反向", "C夜盤收盤位置確認", "D核心×位置", "F籌碼共振",
        "J保留組合×排除追價", "K保留組合×極端乖離", "L保留組合×非極端乖離",
    }.issubset(set(detail["組合家族"]))
    long_rows = detail[detail["交易方向"].eq("LONG")]
    assert np.allclose(
        long_rows["方向損益點數"],
        long_rows["隔日開收點數"] - settings.round_trip_cost_points,
    )


def test_summary_contains_multiple_testing_control() -> None:
    settings = Settings(start=date(2022, 4, 1), end=date(2022, 9, 30), minimum_history=20, history_window=60, minimum_group_size=10)
    detail = build_signals(prepare_daily(sample_source(), settings), settings)
    summary, yearly = summarize(detail, settings)
    assert len(summary) > 0 and len(yearly) > 0
    assert {"命中率FDR_q值", "平均損益FDR_q值", "正式通過", "年度獲利比例_pct"}.issubset(summary.columns)


def test_ma5_bias_filter_is_direction_aware() -> None:
    settings = Settings(start=date(2022, 4, 1), end=date(2022, 9, 30), minimum_history=20, history_window=60)
    detail = build_signals(prepare_daily(sample_source(), settings), settings)
    long_filtered = detail[
        detail["組合家族"].eq("J保留組合×排除追價")
        & detail["交易方向"].eq("LONG")
    ]
    short_filtered = detail[
        detail["組合家族"].eq("J保留組合×排除追價")
        & detail["交易方向"].eq("SHORT")
    ]
    assert not ((long_filtered["MA5乖離率_pct"] > 0) & (long_filtered["MA5乖離率_pct_歷史分位_pct"] >= 80)).any()
    assert not ((short_filtered["MA5乖離率_pct"] < 0) & (short_filtered["MA5乖離率_pct_歷史分位_pct"] <= 20)).any()
