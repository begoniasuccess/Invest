from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from analysis.futures_daily_amplitude_feature_scan import (
    Settings,
    add_target_flags,
    add_yearly_consistency,
    build_bucket_summary,
    build_ranking,
    build_target_distribution,
    build_yearly,
)
from backtest.futures_raw_signal_strength_scan import BUCKET_LABELS


class TestFuturesDailyAmplitudeFeatureScan(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            start=date(2018, 1, 1), split_date=date(2023, 1, 1),
            minimum_group_size=10, minimum_year_size=40,
        )

    @staticmethod
    def _targets() -> pd.DataFrame:
        return pd.DataFrame({
            "trading_date": pd.date_range("2024-01-01", periods=5),
            "range_ratio": [1.0, 1.6, 2.1, 1.8, 0.8],
            "amplitude_label": ["一般振幅", "劇烈振幅", "極端振幅", "劇烈振幅", "一般振幅"],
            "path_pattern_label": ["偏多震盪", "強單邊多", "強單邊空", "先殺後拉", "強單邊多"],
        })

    @staticmethod
    def _detail() -> pd.DataFrame:
        rows = []
        dates = pd.bdate_range("2018-01-02", periods=1800)
        for index, result_date in enumerate(dates):
            bucket_id = index % 5
            bucket = BUCKET_LABELS[bucket_id]
            event = int(bucket_id == 4 or (bucket_id == 3 and index % 3 == 0))
            rows.append({
                "訊號日": result_date - pd.offsets.BDay(1),
                "結果日": result_date,
                "指標類別": "測試", "指標代碼": "趨勢特徵", "公式": "測試",
                "原始值": float(bucket_id), "歷史分位_pct": bucket_id * 20 + 10,
                "強度層": bucket, "range_ratio": 1.6 if event else 1.0,
                "劇烈以上": event, "劇烈強單邊": event,
                "劇烈強單邊多": event, "劇烈強單邊空": 0,
                "劇烈非單邊": 0,
            })
        return pd.DataFrame(rows)

    def test_target_flags_are_mutually_consistent(self) -> None:
        flags = add_target_flags(self._targets())
        self.assertEqual(flags["劇烈以上"].sum(), 3)
        self.assertEqual(flags["劇烈強單邊"].sum(), 2)
        self.assertEqual(flags["劇烈強單邊多"].sum(), 1)
        self.assertEqual(flags["劇烈強單邊空"].sum(), 1)
        self.assertEqual(flags["劇烈非單邊"].sum(), 1)

    def test_distribution_uses_all_targets(self) -> None:
        summary = build_target_distribution(self._targets())
        full = summary[(summary["期間"] == "全期") & (summary["分組"] == "研究目標")]
        self.assertEqual(set(full["標籤"]), {
            "劇烈以上", "劇烈強單邊", "劇烈強單邊多", "劇烈強單邊空", "劇烈非單邊",
        })

    def test_bucket_lift_and_ranking_detect_strong_top_bucket(self) -> None:
        detail = self._detail()
        buckets = build_bucket_summary(detail)
        target = buckets[buckets["研究目標"] == "劇烈以上"].set_index("強度層")
        self.assertGreater(target.loc[BUCKET_LABELS[-1], "Lift"], 3.0)
        ranking = build_ranking(detail, self.settings)
        row = ranking[ranking["研究目標"] == "劇烈以上"].iloc[0]
        self.assertGreater(row["全期_趨勢效果"], 0)
        self.assertLess(row["全期_趨勢q值"], 0.05)
        self.assertTrue(row["前後期趨勢同方向"])

    def test_yearly_consistency_promotes_stable_feature(self) -> None:
        detail = self._detail()
        ranking = build_ranking(detail, self.settings)
        yearly = build_yearly(detail, self.settings)
        result = add_yearly_consistency(ranking, yearly)
        row = result[result["研究目標"] == "劇烈以上"].iloc[0]
        self.assertGreaterEqual(row["年度趨勢一致率"], 0.60)
        self.assertTrue(row["第一階段候選"])


if __name__ == "__main__":
    unittest.main()
