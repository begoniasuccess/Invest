from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from backtest.futures_day_night_next_day_layered import (
    Settings,
    build_layered,
    compare_opposite_same,
    prepare_detail,
)


class TestDayNightNextDayLayered(unittest.TestCase):
    def setUp(self) -> None:
        n = 600
        dates = pd.bdate_range("2018-01-01", periods=n)
        day_close = 10000 + np.arange(n, dtype=float)
        # 偶數日：日漲夜跌（反向）；奇數日：日漲夜漲（同向）。
        night_close = day_close + np.where(np.arange(n) % 2 == 0, -60.0, 60.0)
        target_open = np.r_[10000.0, (day_close[:-1] + night_close[:-1]) / 2]
        target_close = target_open.copy()
        target_high = target_open + 80.0
        target_low = target_open - 80.0
        self.source = pd.DataFrame({
            "futures_id": "TX", "日期": dates,
            "日盤開": target_open, "日盤高": target_high,
            "日盤低": target_low, "日盤收": day_close,
            "夜盤收": night_close,
        })
        self.settings = Settings(
            start=date(2018, 1, 10), minimum_sample_size=20
        )

    def test_alignment_and_position(self) -> None:
        detail = prepare_detail(self.source, self.settings)
        first = detail.iloc[0]
        source_index = self.source.index[self.source["日期"] == first["訊號日"]][0]
        self.assertEqual(first["隔日日盤日期"], self.source.loc[source_index + 1, "日期"])
        self.assertIn(first["隔日收盤位置"], {"兩者之間", "夜收外側", "日收外側"})

    def test_layered_contains_opposite_ratio(self) -> None:
        detail = prepare_detail(self.source, self.settings)
        layered = build_layered(detail, 20)
        self.assertIn("反向×夜盤幅度×反向比率", set(layered["分層類型"]))

    def test_comparison_has_fdr_columns(self) -> None:
        detail = prepare_detail(self.source, self.settings)
        result = compare_opposite_same(detail, self.settings)
        self.assertGreater(len(result), 0)
        self.assertIn("FDR_q值", result)
        self.assertIn("隔日收盤在兩者之間", set(result["比較結果"]))


if __name__ == "__main__":
    unittest.main()
