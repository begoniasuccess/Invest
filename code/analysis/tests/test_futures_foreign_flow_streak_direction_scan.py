from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from analysis.futures_daily_amplitude_foreign_flow_scan import (
    FOREIGN_COLUMN,
    TURNOVER_COLUMN,
    Settings,
    build_foreign_feature,
)
from analysis.futures_foreign_flow_direction_scan import prepare_direction_frame
from analysis.futures_foreign_flow_streak_direction_scan import (
    add_streak_features,
    build_classifier_summary,
    build_rules,
    build_summary,
    build_yearly,
)


class TestFuturesForeignFlowStreakDirectionScan(unittest.TestCase):
    @staticmethod
    def _settings() -> Settings:
        return Settings(
            start=date(2018, 1, 1), end=date(2025, 12, 31),
            split_date=date(2022, 1, 1), history_window=80,
            minimum_history=20, minimum_rule_sample=10,
            minimum_year_rule_sample=3,
        )

    @staticmethod
    def _source(count: int = 2100) -> pd.DataFrame:
        dates = pd.bdate_range("2017-01-02", periods=count)
        block = (np.arange(count) // 12) % 2
        foreign = np.where(block == 0, -180.0, 180.0)
        day_open = np.full(count, 10000.0)
        previous_foreign = np.r_[0.0, foreign[:-1]]
        day_close = day_open + np.where(previous_foreign > 0, 100.0, -100.0)
        trend = np.where(previous_foreign > 0, "漲勢盤", "跌勢盤")
        return pd.DataFrame({
            "日期": dates,
            FOREIGN_COLUMN: foreign,
            TURNOVER_COLUMN: 2500.0,
            "日盤開": day_open,
            "日盤收": day_close,
            "日盤走勢": trend,
        })

    def _frame(self) -> tuple[pd.DataFrame, Settings]:
        settings = self._settings()
        source = self._source()
        foreign = build_foreign_feature(source, settings)
        direction = prepare_direction_frame(source, foreign, settings)
        return add_streak_features(source, direction, settings), settings

    def test_builds_sixteen_rules(self) -> None:
        rules = build_rules(self._settings())
        self.assertEqual(len(rules), 16)
        self.assertEqual(sum(rule.require_extreme for rule in rules), 4)

    def test_streak_and_strict_features(self) -> None:
        frame, _ = self._frame()
        self.assertTrue(frame["連續2日買超"].any())
        self.assertTrue(frame["連續5日賣超"].any())
        self.assertTrue(frame["連續3日大買"].any())
        self.assertLessEqual(frame["連續3日大買"].sum(), frame["連續3日買超"].sum())

    def test_detects_streak_direction_signal(self) -> None:
        frame, settings = self._frame()
        rules = build_rules(settings)
        yearly = build_yearly(frame, rules, settings)
        summary = build_summary(frame, rules, yearly, settings)
        self.assertTrue(summary["連續外資方向候選"].any())
        classifier = build_classifier_summary(frame, settings)
        row = classifier[
            classifier["條件名稱"].eq("連續3日同向")
            & classifier["方向定義"].eq("開收方向")
            & classifier["期間"].eq("驗證期")
        ].iloc[0]
        self.assertGreater(row["正確率_pct"], 80.0)


if __name__ == "__main__":
    unittest.main()
