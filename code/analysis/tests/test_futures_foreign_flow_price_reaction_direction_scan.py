from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from analysis.futures_daily_amplitude_foreign_flow_scan import Settings
from analysis.futures_foreign_flow_price_reaction_direction_scan import (
    add_price_reaction_features,
    build_classifier_summary,
    build_rules,
    build_summary,
    build_yearly,
)


class TestFuturesForeignFlowPriceReactionDirectionScan(unittest.TestCase):
    @staticmethod
    def _settings() -> Settings:
        return Settings(
            start=date(2018, 1, 1), split_date=date(2022, 1, 1),
            minimum_rule_sample=10, minimum_year_rule_sample=3,
        )

    @staticmethod
    def _source_and_streak(count: int = 1800) -> tuple[pd.DataFrame, pd.DataFrame]:
        dates = pd.bdate_range("2017-01-02", periods=count)
        index = np.arange(count)
        buy = (index // 8) % 2 == 0
        absorbed = index % 4 < 2
        day_open = np.full(count, 10000.0)
        day_close = day_open + np.where(
            buy,
            np.where(absorbed, -50.0, 50.0),
            np.where(absorbed, 50.0, -50.0),
        )
        # Price absorption predicts a next-day reversal; confirmation predicts continuation.
        next_up = np.where(absorbed, ~buy, buy)
        next_up = np.where(index % 17 == 0, ~next_up, next_up)
        streak = pd.DataFrame({
            "訊號日": dates,
            "結果日": pd.Series(dates).shift(-1),
            "連續3日大買": buy,
            "連續3日大賣": ~buy,
            "隔日開收報酬_pct": np.where(next_up, 1.0, -1.0),
            "隔日開收方向": np.where(next_up, "上漲", "下跌"),
            "隔日日盤走勢": np.where(next_up, "漲勢盤", "跌勢盤"),
        })
        source = pd.DataFrame({
            "日期": dates,
            "日盤開": day_open,
            "日盤收": day_close,
        })
        return source, streak.iloc[:-1].copy()

    def test_builds_sixteen_rules(self) -> None:
        rules = build_rules()
        self.assertEqual(len(rules), 16)
        self.assertEqual(sum(rule.reaction_type == "absorb" for rule in rules), 8)

    def test_price_reaction_uses_signal_day_only(self) -> None:
        source, streak = self._source_and_streak(20)
        frame = add_price_reaction_features(source, streak)
        row = frame.iloc[5]
        expected_1d = (
            source.loc[5, "日盤收"] / source.loc[5, "日盤開"] - 1.0
        ) * 100.0
        expected_3d = (
            source.loc[5, "日盤收"] / source.loc[3, "日盤開"] - 1.0
        ) * 100.0
        self.assertAlmostEqual(row["當日日盤開收反應_pct"], expected_1d)
        self.assertAlmostEqual(row["近3日日盤價格反應_pct"], expected_3d)

    def test_detects_price_reaction_signal(self) -> None:
        source, streak = self._source_and_streak()
        frame = add_price_reaction_features(source, streak)
        settings = self._settings()
        rules = build_rules()
        yearly = build_yearly(frame, rules, settings)
        summary = build_summary(frame, rules, yearly, settings)
        self.assertTrue(summary["價格反應方向候選"].any())
        classifier = build_classifier_summary(frame, settings)
        validation = classifier[
            classifier["價格反應期間"].eq(1)
            & classifier["價格反應類型"].eq("背離吸收")
            & classifier["方向定義"].eq("開收方向")
            & classifier["期間"].eq("驗證期")
        ].iloc[0]
        self.assertGreater(validation["正確率_pct"], 90.0)


if __name__ == "__main__":
    unittest.main()
