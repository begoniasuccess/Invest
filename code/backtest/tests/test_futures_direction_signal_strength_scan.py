from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from backtest.futures_direction_signal_strength_scan import (
    add_direction_validation,
    build_direction_bucket_summary,
    build_direction_ranking,
    prepare_direction_detail,
    scan_direction_period,
)
from backtest.futures_raw_signal_strength_scan import Settings


class TestFuturesDirectionSignalStrengthScan(unittest.TestCase):
    @staticmethod
    def _source(size: int = 1500) -> pd.DataFrame:
        rng = np.random.default_rng(20261103)
        dates = pd.bdate_range("2018-01-02", periods=size)
        micro = rng.normal(0, 20, size)
        day_open = np.full(size, 10000.0)
        # 訊號日微台值越高，下一交易日日盤越偏空。
        day_move = np.r_[0.0, -micro[:-1] * 4.0] + rng.normal(0, 12, size)
        day_close = day_open + day_move
        day_high = np.maximum(day_open, day_close) + 45
        day_low = np.minimum(day_open, day_close) - 45
        night_open = day_close.copy()
        night_close = night_open - micro * 3.0 + rng.normal(0, 10, size)
        night_high = np.maximum(night_open, night_close) + 35
        night_low = np.minimum(night_open, night_close) - 35
        return pd.DataFrame({
            "日期": dates,
            "全日收": day_close,
            "散戶多空比_微台": micro,
            "散戶多空比_小台": micro * 0.5,
            "日盤開": day_open,
            "日盤高": day_high,
            "日盤低": day_low,
            "日盤收": day_close,
            "夜盤開": night_open,
            "夜盤高": night_high,
            "夜盤低": night_low,
            "夜盤收": night_close,
        })

    @staticmethod
    def _settings() -> Settings:
        return Settings(
            start=date(2018, 1, 2), end=date(2023, 9, 29),
            split_date=date(2022, 1, 1), history_window=60,
            minimum_history=30, minimum_group_size=20,
            minimum_year_size=10,
        )

    def test_prepare_uses_next_day_and_keeps_pure_micro_signal(self) -> None:
        source = self._source()
        detail, metadata = prepare_direction_detail(source, self._settings())
        self.assertIn("散戶多空比_微台", metadata)
        row = detail[
            detail["指標代碼"].eq("散戶多空比_微台")
            & detail["訊號日"].eq(source.loc[100, "日期"])
        ].iloc[0]
        expected = (source.loc[101, "日盤收"] - source.loc[101, "日盤開"]) / source.loc[101, "日盤開"] * 100
        self.assertEqual(row["隔日日期"], source.loc[101, "日期"])
        self.assertAlmostEqual(row["隔日開收報酬_pct"], expected)
        self.assertAlmostEqual(row["原始值"], source.loc[100, "散戶多空比_微台"])

    def test_direction_scan_finds_negative_monotonic_effect(self) -> None:
        detail, _ = prepare_direction_detail(self._source(), self._settings())
        tests = add_direction_validation(
            scan_direction_period(detail, self._settings()), detail, self._settings()
        )
        match = tests[
            tests["指標代碼"].eq("散戶多空比_微台")
            & tests["結果指標"].eq("隔日方向效率_pct")
            & tests["檢定類型"].eq("Spearman強度趨勢")
        ].iloc[0]
        self.assertLess(match["效果值"], -0.80)
        self.assertEqual(match["方向判定"], "指標越高越偏空")
        self.assertTrue(match["重複驗證通過"])

        ranking = build_direction_ranking(tests)
        micro = ranking[
            ranking["指標代碼"].eq("散戶多空比_微台")
            & ranking["預測時段"].eq("隔日日盤")
        ].iloc[0]
        self.assertEqual(micro["綜合方向判定"], "指標越高越偏空")
        self.assertGreater(micro["穩定方向證據數"], 0)

    def test_night_close_relative_to_day_close_is_ranked_separately(self) -> None:
        source = self._source()
        detail, _ = prepare_direction_detail(source, self._settings())
        row = detail[
            detail["指標代碼"].eq("散戶多空比_微台")
            & detail["訊號日"].eq(source.loc[100, "日期"])
        ].iloc[0]
        expected = (source.loc[100, "夜盤收"] - source.loc[100, "日盤收"]) / source.loc[100, "日盤收"] * 100
        self.assertAlmostEqual(row["夜盤收相對日盤收_pct"], expected)

        tests = add_direction_validation(
            scan_direction_period(detail, self._settings()), detail, self._settings()
        )
        ranking = build_direction_ranking(tests)
        night = ranking[
            ranking["指標代碼"].eq("散戶多空比_微台")
            & ranking["預測時段"].eq("當日夜盤")
        ].iloc[0]
        self.assertEqual(night["主要結果指標"], "夜盤收相對日盤收_pct")
        self.assertLess(night["主要結果_rho"], -0.80)
        self.assertEqual(night["綜合方向判定"], "指標越高越偏空")

    def test_highest_bucket_is_more_bearish_than_lowest_bucket(self) -> None:
        detail, _ = prepare_direction_detail(self._source(), self._settings())
        summary = build_direction_bucket_summary(detail)
        micro = summary[summary["指標代碼"].eq("散戶多空比_微台")].set_index("強度層")
        self.assertLess(
            micro.loc["Q5最高20%", "隔日方向效率平均_pct"],
            micro.loc["Q1最低20%", "隔日方向效率平均_pct"],
        )


if __name__ == "__main__":
    unittest.main()
