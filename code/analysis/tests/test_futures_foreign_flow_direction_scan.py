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
from analysis.futures_foreign_flow_direction_scan import (
    build_bucket_stats,
    build_classifier_summary,
    build_rules,
    build_summary,
    build_yearly,
    prepare_direction_frame,
)


class TestFuturesForeignFlowDirectionScan(unittest.TestCase):
    @staticmethod
    def _settings() -> Settings:
        return Settings(
            start=date(2018, 1, 1),
            end=date(2024, 12, 31),
            split_date=date(2022, 1, 1),
            history_window=80,
            minimum_history=20,
            minimum_rule_sample=10,
            minimum_year_rule_sample=3,
        )

    @staticmethod
    def _source(count: int = 1800) -> pd.DataFrame:
        dates = pd.bdate_range("2017-01-02", periods=count)
        index = np.arange(count)
        foreign = np.sin(index / 13.0) * 150
        future_up = np.r_[False, foreign[:-1] > 0]
        day_open = np.full(count, 10000.0)
        day_close = day_open + np.where(future_up, 80.0, -80.0)
        trend = np.where(future_up, "漲勢盤", "跌勢盤")
        return pd.DataFrame({
            "日期": dates,
            FOREIGN_COLUMN: foreign,
            TURNOVER_COLUMN: 2500.0,
            "日盤開": day_open,
            "日盤收": day_close,
            "日盤走勢": trend,
        })

    def test_next_day_target_is_shifted(self) -> None:
        settings = self._settings()
        source = self._source(400)
        foreign = build_foreign_feature(source, settings)
        frame = prepare_direction_frame(source, foreign, settings)
        row = frame.iloc[0]
        source_row = source[source["日期"].eq(row["結果日"])].iloc[0]
        expected = "上漲" if source_row["日盤收"] > source_row["日盤開"] else "下跌"
        self.assertEqual(row["隔日開收方向"], expected)

    def test_builds_four_direction_rules_and_five_buckets(self) -> None:
        settings = self._settings()
        source = self._source()
        foreign = build_foreign_feature(source, settings)
        frame = prepare_direction_frame(source, foreign, settings)
        self.assertEqual([rule.rule_id for rule in build_rules(settings)], ["D01", "D02", "D03", "D04"])
        buckets = build_bucket_stats(frame, settings)
        self.assertEqual(len(buckets[buckets["期間"].eq("全期")]), 5)

    def test_detects_directional_foreign_signal(self) -> None:
        settings = self._settings()
        source = self._source()
        foreign = build_foreign_feature(source, settings)
        frame = prepare_direction_frame(source, foreign, settings)
        rules = build_rules(settings)
        yearly = build_yearly(frame, rules, settings)
        summary = build_summary(frame, rules, yearly, settings)
        self.assertTrue(summary["方向候選"].any())
        classifier = build_classifier_summary(frame, settings)
        validation_close = classifier[
            classifier["方向定義"].eq("開收方向") & classifier["期間"].eq("驗證期")
        ].iloc[0]
        self.assertGreater(validation_close["正確率_pct"], 70.0)


if __name__ == "__main__":
    unittest.main()
