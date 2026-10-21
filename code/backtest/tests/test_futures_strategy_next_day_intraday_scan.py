from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from backtest.futures_strategy_next_day_field_scan import Settings
from backtest.futures_strategy_next_day_intraday_scan import (
    prepare_intraday_data,
    scan_directional_performance,
    scan_group_differences,
)


class TestStrategyNextDayIntradayScan(unittest.TestCase):
    def setUp(self) -> None:
        n = 900
        dates = pd.bdate_range("2018-01-01", periods=n)
        strategy = np.where(np.arange(n) % 2 == 0, "多", "空")
        # 第 j 日行情由第 j-1 日訊號決定，模擬真正隔日關係。
        previous = np.concatenate((["多"], strategy[:-1]))
        open_price = 10000 + np.arange(n, dtype=float)
        close_price = open_price + np.where(previous == "多", 60.0, -60.0)
        high_price = np.maximum(open_price, close_price) + 40.0
        low_price = np.minimum(open_price, close_price) - 40.0
        self.source = pd.DataFrame({
            "日期": dates, "futures_id": "TX", "策略_A": strategy,
            "日盤開": open_price, "日盤高": high_price,
            "日盤低": low_price, "日盤收": close_price,
            "夜盤收": open_price - 5.0,
        })
        self.settings = Settings(
            start=date(2018, 1, 1), split_date=date(2019, 7, 1),
            minimum_group_size=50, minimum_year_size=10,
        )

    def test_prepares_only_intraday_outcomes(self) -> None:
        prepared = prepare_intraday_data(self.source)
        self.assertIn("open_to_close_pct", prepared)
        self.assertIn("intraday_range_pct", prepared)
        self.assertNotIn("日盤收__隔日變化", prepared)

    def test_group_scan_finds_open_to_close_difference(self) -> None:
        result = scan_group_differences(prepare_intraday_data(self.source), self.settings)
        match = result[(result["策略欄位"] == "策略_A") &
                       (result["結果代碼"] == "open_to_close_pct")]
        self.assertEqual(len(match), 1)
        self.assertTrue(bool(match.iloc[0]["重複驗證通過"]))

    def test_directional_scan_finds_profitable_signal(self) -> None:
        result = scan_directional_performance(prepare_intraday_data(self.source), self.settings)
        match = result[(result["策略欄位"] == "策略_A") &
                       (result["進場代碼"] == "day_open")]
        self.assertEqual(len(match), 1)
        self.assertGreater(match.iloc[0]["方向化平均收盤報酬_pct"], 0)
        self.assertTrue(bool(match.iloc[0]["方向策略候選"]))


if __name__ == "__main__":
    unittest.main()
