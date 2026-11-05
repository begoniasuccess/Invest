from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from analysis.futures_daily_amplitude_pattern import (
    Settings,
    _amplitude_label,
    _path_pattern,
    build_daily_patterns,
)


class TestFuturesDailyAmplitudePattern(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            start=date(2026, 1, 1),
            range_days=2,
            minimum_bars=3,
        )

    @staticmethod
    def _day(day: str, prices: list[tuple[float, float, float, float]]) -> list[dict]:
        rows = []
        times = ["08:45", "11:00", "13:45"]
        for at, (open_, high, low, close) in zip(times, prices):
            rows.append({
                "trading_date": day,
                "contract_date": "202601",
                "bar_datetime": f"{day} {at}",
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 10,
            })
        return rows

    def test_threshold_labels(self) -> None:
        self.assertEqual(_amplitude_label(0.5, self.settings), "低振幅")
        self.assertEqual(_amplitude_label(1.25, self.settings), "明顯放大")
        self.assertEqual(_amplitude_label(1.50, self.settings), "劇烈振幅")
        self.assertEqual(_amplitude_label(2.00, self.settings), "極端振幅")

    def test_path_patterns_cover_ideal_and_reversal_shapes(self) -> None:
        self.assertEqual(_path_pattern(0.85, 0.90, "LOW_FIRST", self.settings), "強單邊多")
        self.assertEqual(_path_pattern(0.15, 0.10, "HIGH_FIRST", self.settings), "強單邊空")
        self.assertEqual(_path_pattern(0.75, 0.20, "HIGH_FIRST", self.settings), "先拉後殺")
        self.assertEqual(_path_pattern(0.25, 0.80, "LOW_FIRST", self.settings), "先殺後拉")

    def test_previous_average_excludes_current_day_and_keeps_all_dates(self) -> None:
        rows = []
        rows += self._day("2026-01-02", [(100, 102, 100, 102), (102, 104, 102, 104), (104, 104, 103, 104)])
        rows += self._day("2026-01-05", [(100, 101, 99, 100), (100, 103, 100, 103), (103, 103, 102, 103)])
        rows += self._day("2026-01-06", [(100, 110, 100, 109), (109, 110, 108, 109), (109, 110, 109, 110)])
        result = build_daily_patterns(pd.DataFrame(rows), self.settings)
        self.assertEqual(len(result), 3)
        third = result.iloc[2]
        self.assertAlmostEqual(third["average_range_previous"], 4.0)
        self.assertAlmostEqual(third["range_ratio"], 2.5)
        self.assertEqual(third["amplitude_label"], "極端振幅")
        self.assertEqual(third["path_pattern_label"], "強單邊多")
        self.assertTrue(third["valid_for_research"])

    def test_last_extreme_determines_session_trend(self) -> None:
        rows = self._day(
            "2026-01-02",
            [(100, 105, 99, 104), (104, 105, 100, 101), (101, 103, 99, 100)],
        )
        result = build_daily_patterns(pd.DataFrame(rows), self.settings)
        self.assertEqual(result.iloc[0]["extreme_order"], "HIGH_FIRST")
        self.assertEqual(result.iloc[0]["session_trend_label"], "跌勢盤")


if __name__ == "__main__":
    unittest.main()
