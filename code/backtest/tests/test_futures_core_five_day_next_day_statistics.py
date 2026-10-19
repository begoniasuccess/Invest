from __future__ import annotations

import unittest

import pandas as pd

from backtest.futures_core_five_day_next_day_statistics import (
    SIGNAL_COLUMN,
    calculate_summary,
    calculate_tests,
    prepare_detail,
)


class TestCoreFiveDayNextDayStatistics(unittest.TestCase):
    def setUp(self) -> None:
        self.source = pd.DataFrame({
            "signal_date": pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]),
            "target_date": pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]),
            "core_direction": ["多", "多", "空", "空"],
            "signal_day_close": [100, 100, 100, 100],
            "signal_night_close": [101, 101, 99, 99],
            "target_day_high": [105, 104, 99, 98],
            "target_day_low": [99, 100, 94, 95],
            "target_day_close": [103, 102, 96, 97],
        })

    def test_detail_has_two_denominators_and_next_trading_date(self) -> None:
        detail = prepare_detail(self.source)
        self.assertEqual(len(detail), 8)
        self.assertEqual(set(detail["基準"]), {"前一日日盤收", "前一日夜盤收"})
        friday = detail[(detail["訊號日"] == pd.Timestamp("2026-01-02")) &
                        (detail["基準"] == "前一日日盤收")].iloc[0]
        self.assertEqual(friday["隔日日盤日期"], pd.Timestamp("2026-01-05"))
        self.assertAlmostEqual(friday["收盤漲跌幅_pct"], 3.0)

    def test_summary_groups_long_and_short(self) -> None:
        summary = calculate_summary(prepare_detail(self.source))
        row = summary[(summary["基準"] == "前一日日盤收") &
                      (summary[SIGNAL_COLUMN] == "多")].iloc[0]
        self.assertEqual(row["樣本數"], 2)
        self.assertAlmostEqual(row["平均漲跌幅_pct"], 2.5)
        self.assertAlmostEqual(row["上漲機率_pct"], 100.0)

    def test_tests_include_continuous_and_probability_tests(self) -> None:
        tests = calculate_tests(prepare_detail(self.source))
        self.assertEqual(len(tests), 14)
        self.assertIn("Welch t（平均數）", set(tests["檢定"]))
        self.assertIn("Mann-Whitney U（分布）", set(tests["檢定"]))
        self.assertIn("兩比例 z（比例）", set(tests["檢定"]))
        self.assertTrue(tests["BH校正p值"].between(0, 1).all())


if __name__ == "__main__":
    unittest.main()
