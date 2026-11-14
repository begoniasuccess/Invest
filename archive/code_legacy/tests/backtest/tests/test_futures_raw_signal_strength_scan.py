from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from backtest.futures_raw_signal_strength_scan import (
    BUCKET_LABELS,
    Settings,
    _rolling_percentile,
    _strength_bucket,
    build_bucket_summary,
    build_signal_features,
    prepare_detail,
    scan_period,
)


class TestRawSignalStrengthScan(unittest.TestCase):
    @staticmethod
    def _source(size: int = 90) -> pd.DataFrame:
        index = np.arange(size, dtype=float)
        dates = pd.bdate_range("2022-01-03", periods=size)
        close = 10000 + index * 2
        frame = pd.DataFrame({
            "futures_id": "TX", "日期": dates,
            "全日收": close, "全成交量": 1000 + index * 5,
            "全5均量": 980 + index * 5, "全價漲跌_pct": np.sin(index / 8),
            "全量增減_昨日_pct": np.cos(index / 9),
            "全量增減_五日_pct": np.sin(index / 10),
            "全5均上升幅度_pct": index / 10000,
            "全10均上升幅度_pct": index / 12000,
            "全20均上升幅度_pct": index / 15000,
            "全5MA乖離率": np.sin(index / 20) / 100,
            "全布林上軌": close + 100, "全布林下軌": close - 100,
            "法人總買超_億": index - 45,
            "散戶多空比_微台": np.where(index % 7 == 0, np.nan, index - 40),
            "散戶多空比_小台": index - 42,
            "主力新建多單": index - 30, "主力新建空單": 40 - index,
            "未沖銷_全": 50000 + index,
            "call_月選": 100 + index, "put_月選": 120 + index,
            "call_週三": 90 + index, "put_週三": 100 + index,
            "call_週五": 110 + index, "put_週五": 100 + index,
            "日盤開": close, "日盤高": close + 80, "日盤低": close - 60,
            "日盤收": close + np.sin(index / 4) * 40, "夜盤收": close - 10,
        })
        return frame

    def test_rolling_percentile_excludes_current_and_future(self) -> None:
        original = pd.Series([1, 2, 3, 4, 5, 100, 7, 8], dtype=float)
        first = _rolling_percentile(original, window=5, minimum_history=5)
        changed = original.copy()
        changed.iloc[6:] = -999
        second = _rolling_percentile(changed, window=5, minimum_history=5)
        self.assertAlmostEqual(first.iloc[5], 100.0)
        self.assertAlmostEqual(first.iloc[5], second.iloc[5])
        self.assertTrue(first.iloc[:5].isna().all())

    def test_signal_features_and_weekly_option_selection(self) -> None:
        source = self._source()
        features, metadata = build_signal_features(source)
        self.assertEqual(len(metadata), 19)
        self.assertAlmostEqual(
            features.loc[0, "主力淨多空"],
            source.loc[0, "主力新建多單"] - source.loc[0, "主力新建空單"],
        )
        # 2022-01-03 為星期一，短週應使用週三。
        expected = np.log(source.loc[0, "call_週三"] / source.loc[0, "put_週三"])
        self.assertAlmostEqual(features.loc[0, "主要短週CallPut對數比"], expected)

    def test_prepare_detail_aligns_next_trading_day(self) -> None:
        source = self._source()
        settings = Settings(
            start=date(2022, 1, 3), split_date=date(2022, 3, 1),
            history_window=20, minimum_history=10, minimum_group_size=10,
        )
        detail, _ = prepare_detail(source, settings)
        row = detail[(detail["訊號日"] == source.loc[20, "日期"])
                     & (detail["指標代碼"] == "法人總買超")].iloc[0]
        self.assertEqual(row["隔日日期"], source.loc[21, "日期"])
        expected = (source.loc[21, "日盤收"] - source.loc[21, "日盤開"]) / source.loc[21, "日盤開"] * 100
        self.assertAlmostEqual(row["隔日開收報酬_pct"], expected)
        self.assertFalse(pd.isna(row["歷史分位_pct"]))

    def test_bucket_summary_and_tests(self) -> None:
        rows = []
        for i in range(250):
            percentile = (i % 100) + 0.5
            bucket = _strength_bucket(pd.Series([percentile])).iloc[0]
            rows.append({
                "訊號日": pd.Timestamp("2020-01-01") + pd.Timedelta(days=i),
                "隔日日期": pd.Timestamp("2020-01-02") + pd.Timedelta(days=i),
                "指標類別": "測試", "指標代碼": "人工趨勢", "公式": "test",
                "天然方向": True, "原始值": percentile, "歷史分位_pct": percentile,
                "強度層": bucket, "原始正負": "正",
                "隔日開收報酬_pct": percentile / 100,
                "隔日上行MFE_pct": percentile / 80,
                "隔日下行MFE_pct": (100 - percentile) / 80,
                "隔日日內振幅_pct": 1.0, "隔日收盤位置_pct": percentile,
                "隔日開盤跳空_日收_pct": 0.0, "隔日開盤跳空_夜收_pct": 0.0,
            })
        detail = pd.DataFrame(rows)
        settings = Settings(
            start=date(2019, 1, 1), split_date=date(2023, 1, 1),
            minimum_group_size=20,
        )
        summary = build_bucket_summary(detail)
        self.assertEqual(set(summary["強度層"]), set(BUCKET_LABELS))
        tests = scan_period(detail, settings)
        match = tests[(tests["結果指標"] == "隔日開收報酬_pct")
                      & (tests["檢定類型"] == "Spearman分位趨勢")].iloc[0]
        self.assertGreater(match["效果值"], 0.99)
        self.assertTrue(match["FDR顯著"])


if __name__ == "__main__":
    unittest.main()
