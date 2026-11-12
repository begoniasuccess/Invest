from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from analysis.futures_daily_amplitude_violent_combo_scan import (
    CORE_FEATURES,
    Settings,
    build_rule_detail,
    build_rules,
    build_summary,
    build_yearly,
)


class TestFuturesDailyAmplitudeViolentComboScan(unittest.TestCase):
    @staticmethod
    def _frame() -> pd.DataFrame:
        count = 1600
        dates = pd.bdate_range("2018-01-02", periods=count)
        index = np.arange(count)
        a = (index * 17) % 100
        b = (index * 29) % 100
        c = (index * 43) % 100
        event = ((a >= 80) & (b >= 80)) | ((index % 23) == 0)
        frame = pd.DataFrame({
            "訊號日": dates - pd.offsets.BDay(1),
            "結果日": dates,
            "range_ratio": np.where(event, 1.8, 0.9),
            "劇烈以上": event.astype(int),
        })
        values = [a, b, c]
        for feature, percentile in zip(CORE_FEATURES, values):
            frame[f"{feature}_原始值"] = percentile / 100
            frame[f"{feature}_歷史分位_pct"] = percentile
            frame[f"{feature}_強度層"] = pd.cut(
                percentile,
                bins=[-1, 20, 40, 60, 80, 100],
                labels=["Q1最低20%", "Q2偏低", "Q3中間", "Q4偏高", "Q5最高20%"],
            )
        return frame

    def test_builds_fourteen_simple_rules(self) -> None:
        rules = build_rules()
        self.assertEqual(len(rules), 14)
        self.assertEqual(sum(len(rule.features) == 1 for rule in rules), 6)
        self.assertEqual(sum(len(rule.features) == 2 for rule in rules), 6)
        self.assertEqual(sum(len(rule.features) == 3 for rule in rules), 2)

    def test_summary_detects_high_risk_pair(self) -> None:
        frame = self._frame()
        rules = build_rules()
        settings = Settings(
            start=date(2018, 1, 1),
            split_date=date(2021, 1, 1),
            minimum_rule_sample=10,
            minimum_year_rule_sample=3,
        )
        yearly = build_yearly(frame, rules, settings)
        summary = build_summary(frame, rules, yearly, settings)
        pair = summary[
            summary["規則名稱"].eq("近期振幅＋夜盤振幅皆為Q5")
        ].iloc[0]
        self.assertGreater(pair["全期_Lift"], 3.0)
        self.assertGreater(pair["驗證期_事件率_pct"], pair["驗證期_母體事件率_pct"])
        self.assertTrue(pair["探索驗證皆高於母體"])

    def test_detail_has_one_row_per_day_and_rule(self) -> None:
        frame = self._frame()
        rules = build_rules()
        detail = build_rule_detail(frame, rules)
        self.assertEqual(len(detail), len(frame) * len(rules))
        self.assertEqual(detail["規則代碼"].nunique(), 14)


if __name__ == "__main__":
    unittest.main()
