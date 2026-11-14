from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from backtest.futures_open_to_night_close_convergence import (
    Settings,
    build_anchor_position_summary,
    build_gap_matrix,
    build_night_gap_summary,
    prepare_detail,
)


class TestOpenToNightCloseConvergence(unittest.TestCase):
    def _source(self, *, day_close: float, night_close: float,
                next_open: float, next_high: float, next_low: float,
                next_close: float) -> pd.DataFrame:
        return pd.DataFrame({
            "futures_id": ["TX", "TX"],
            "日期": pd.to_datetime(["2026-01-05", "2026-01-06"]),
            "日盤開": [99.0, next_open],
            "日盤高": [101.0, next_high],
            "日盤低": [98.0, next_low],
            "日盤收": [day_close, next_close],
            "夜盤收": [night_close, next_close],
        })

    def test_downward_convergence_metrics(self) -> None:
        source = self._source(
            day_close=105.0, night_close=100.0,
            next_open=110.0, next_high=113.0, next_low=104.0, next_close=106.0,
        )
        settings = Settings(start=date(2026, 1, 5), end=date(2026, 1, 5))
        row = prepare_detail(source, settings).iloc[0]
        self.assertEqual(row["夜收方向"], "向下")
        self.assertEqual(row["盤中最大向夜收_點"], 6.0)
        self.assertAlmostEqual(row["盤中收回原始距離_pct"], 60.0)
        self.assertEqual(row["收盤向夜收移動_點"], 4.0)
        self.assertAlmostEqual(row["收盤收回原始距離_pct"], 40.0)
        self.assertTrue(row["收盤往夜收方向"])
        self.assertTrue(row["收盤比開盤更接近夜收"])

    def test_upward_convergence_and_night_outer_position(self) -> None:
        source = self._source(
            day_close=100.0, night_close=110.0,
            next_open=105.0, next_high=109.0, next_low=101.0, next_close=108.0,
        )
        settings = Settings(start=date(2026, 1, 5), end=date(2026, 1, 5))
        row = prepare_detail(source, settings).iloc[0]
        self.assertEqual(row["夜收方向"], "向上")
        self.assertEqual(row["開盤錨點位置"], "日收與夜收之間")
        self.assertEqual(row["盤中最大向夜收_點"], 4.0)
        self.assertAlmostEqual(row["盤中收回原始距離_pct"], 80.0)
        self.assertAlmostEqual(row["收盤收回原始距離_pct"], 60.0)

    def test_night_outer_position(self) -> None:
        source = self._source(
            day_close=100.0, night_close=90.0,
            next_open=85.0, next_high=91.0, next_low=82.0, next_close=88.0,
        )
        settings = Settings(start=date(2026, 1, 5), end=date(2026, 1, 5))
        row = prepare_detail(source, settings).iloc[0]
        self.assertEqual(row["開盤錨點位置"], "夜收外側")
        self.assertTrue(row["盤中收回至少100pct"])

    def test_summary_builders(self) -> None:
        rows = []
        for i in range(80):
            rows.append({
                "futures_id": "TX",
                "日期": pd.Timestamp("2025-01-01") + pd.Timedelta(days=i),
                "日盤開": 100.0,
                "日盤高": 102.0,
                "日盤低": 98.0,
                "日盤收": 100.0 + (i % 3),
                "夜盤收": 99.5 if i % 2 == 0 else 100.5,
            })
        source = pd.DataFrame(rows)
        settings = Settings(start=date(2025, 1, 1), minimum_sample_size=10)
        detail = prepare_detail(source, settings)
        self.assertGreater(len(build_night_gap_summary(detail)), 0)
        self.assertGreater(len(build_anchor_position_summary(detail, 10)), 0)
        self.assertGreater(len(build_gap_matrix(detail, 10)), 0)


if __name__ == "__main__":
    unittest.main()
