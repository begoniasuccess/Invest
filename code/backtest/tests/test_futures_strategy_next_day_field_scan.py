from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from backtest.futures_strategy_next_day_field_scan import (
    Settings,
    add_validation,
    build_shortlist,
    build_targets,
    detect_strategy_columns,
    scan_period,
)


class TestStrategyNextDayFieldScan(unittest.TestCase):
    def setUp(self) -> None:
        n = 800
        dates = pd.bdate_range("2018-01-01", periods=n)
        strategy = np.where(np.arange(n) % 2 == 0, "多", "空")
        signal = np.where(strategy == "多", 2.0, -2.0)
        rng = np.random.default_rng(42)
        next_day_metric = np.r_[0.0, signal[:-1] + rng.normal(0, 0.15, n - 1)]
        self.frame = pd.DataFrame({
            "日期": dates,
            "futures_id": "TX",
            "策略_A": strategy,
            "策略_只有多": "多",
            "日價漲跌_pct": next_day_metric,
            "日盤收": 10000 + np.arange(n) + next_day_metric * 20,
            "日盤走勢": np.where(next_day_metric > 0, "漲勢盤", "跌勢盤"),
        })
        self.settings = Settings(
            start=date(2018, 1, 1), split_date=date(2019, 7, 1),
            minimum_group_size=50, minimum_year_size=10,
        )

    def test_detects_only_strategy_with_both_groups(self) -> None:
        self.assertEqual(detect_strategy_columns(self.frame), ["策略_A"])

    def test_build_targets_uses_next_existing_row(self) -> None:
        prepared, metadata = build_targets(self.frame)
        self.assertIn("日價漲跌_pct__隔日原值", metadata)
        self.assertIn("日盤收__隔日變化", metadata)
        self.assertEqual(prepared.loc[0, "隔日日期"], self.frame.loc[1, "日期"])

    def test_scan_and_validation_find_stable_effect(self) -> None:
        prepared, metadata = build_targets(self.frame)
        full = scan_period(prepared, metadata, self.settings)
        results = add_validation(full, prepared, metadata, self.settings)
        shortlist = build_shortlist(results)
        match = shortlist[
            (shortlist["策略欄位"] == "策略_A") &
            (shortlist["來源欄位"] == "日價漲跌_pct") &
            (shortlist["目標型態"] == "隔日原值")
        ]
        self.assertEqual(len(match), 1)
        self.assertTrue(bool(match.iloc[0]["重複驗證通過"]))


if __name__ == "__main__":
    unittest.main()
