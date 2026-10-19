import sys
import types
import unittest

import numpy as np
import pandas as pd

fake_common = types.ModuleType("common")
fake_db = types.ModuleType("common.db")
fake_common.db = fake_db
sys.modules.setdefault("common", fake_common)
sys.modules.setdefault("common.db", fake_db)

from machine_learning import yu_key_k_stage2 as model


def config():
    return {
        "date_column": "日期",
        "taiex": {
            "relation": "t", "fields": ["開盤價", "最高價", "最低價", "收盤價", "成交量"],
            "column_map": {"open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價", "volume": "成交量"},
        },
        "futures": {
            "relation": "f", "entity_filter": {"futures_id": "TX"},
            "mapped_fields": [
                {"field": "全日開", "session": "全日", "kind": "開盤"},
                {"field": "全日高", "session": "全日", "kind": "高點"},
                {"field": "全日低", "session": "全日", "kind": "低點"},
                {"field": "全日收", "session": "全日", "kind": "收盤"},
            ],
        },
        "features": {"load_calendar_days": 200, "windows": [5, 10, 20, 60], "atr_window": 10},
        "scoring": {
            "volume_window": 20, "shape_percentile_window": 60,
            "pivot_windows": [5, 10, 20, 60], "large_volume_ratio": 1.3,
            "long_body_atr": .8, "long_shadow_atr": .45,
            "formation_weights": {"volume": 2, "pivot": 2, "body": 1.5, "shadow": 1.5},
            "validity": {
                "break_buffer_atr": .1, "touch_buffer_atr": .15, "age_half_life_rows": 40,
                "distance_decay_atr": 6, "break_penalty": .18, "cross_penalty": .08,
                "formation_share": .55, "recency_share": .25, "proximity_share": .20,
            },
        },
        "output": {"top_key_k": 10, "minimum_formation_score": 2, "minimum_validity_score": 1},
        "matching": {
            "candidate_calendar_days": 180, "exact_tolerance_points": 15,
            "price_tolerance_points": 60, "age_decay_calendar_days": 60,
            "top_matches_per_point": 5,
            "weights": {"price": 4, "explicit_date": 6, "kind": 2, "recency": .5},
        },
    }


class YuKeyKStage2Test(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2025-12-01", periods=70)
        base = 20000 + np.arange(70) * 10.0
        self.taiex = pd.DataFrame({
            "日期": dates, "開盤價": base, "最高價": base + 80,
            "最低價": base - 60, "收盤價": base + 20, "成交量": 100.0,
        })
        self.futures = pd.DataFrame({
            "日期": dates, "futures_id": "TX", "全日開": base + 1000,
            "全日高": base + 1100, "全日低": base + 900, "全日收": base + 1020,
        })
        source_i, analysis_i = 35, 45
        self.source_date = dates[source_i]
        self.analysis_date = dates[analysis_i]
        self.yu_point = float(self.futures.iloc[source_i]["全日高"])
        self.labels = pd.DataFrame({
            "分析日期": [self.analysis_date], "進場點": [self.yu_point],
            "進場依據": [f"{self.source_date.month}/{self.source_date.day}高點"], "進場附註": [None],
            "停利1_點位": [np.nan], "停利1_依據": [None], "停利1_附註": [None],
            "停利2_點位": [np.nan], "停利2_依據": [None], "停利2_附註": [None],
            "停損1_點位": [np.nan], "停損1_依據": [None], "停損1_附註": [None],
            "停損2_點位": [np.nan], "停損2_依據": [None], "停損2_附註": [None],
        })

    def test_date_parser_never_returns_future_date(self):
        analysis = pd.Timestamp("2026-01-05")
        dates = model.extract_reference_dates("12/30高點、1/3低點", analysis)
        self.assertEqual(dates, [pd.Timestamp("2025-12-30"), pd.Timestamp("2026-01-03")])
        self.assertTrue(all(x <= analysis for x in dates))

    def test_explicit_date_and_price_find_correct_k(self):
        points, matches, labels, _ = model.compare_history(
            self.labels, self.taiex, self.futures,
            self.analysis_date, self.analysis_date, config(),
        )
        best = matches.loc[matches["候選排名"] == 1].iloc[0]
        self.assertEqual(best["加權關鍵K日期"], self.source_date)
        self.assertEqual(best["點位類型"], "高點")
        self.assertEqual(best["配對可信度"], "高")
        self.assertEqual(labels.iloc[0]["監督標籤"], 1)

    def test_future_market_rows_do_not_change_match(self):
        old = model.compare_history(
            self.labels, self.taiex, self.futures,
            self.analysis_date, self.analysis_date, config(),
        )[1]
        future_t = self.taiex.copy()
        future_f = self.futures.copy()
        mask = future_t["日期"] > self.analysis_date
        future_t.loc[mask, ["最高價", "最低價", "收盤價", "成交量"]] = [99999, 1, 88888, 999999]
        future_f.loc[future_f["日期"] > self.analysis_date, ["全日開", "全日高", "全日低", "全日收"]] = 77777
        new = model.compare_history(
            self.labels, future_t, future_f,
            self.analysis_date, self.analysis_date, config(),
        )[1]
        cols = ["候選排名", "加權關鍵K日期", "TX來源欄位", "TX關鍵點位", "配對分數"]
        pd.testing.assert_frame_equal(old[cols].reset_index(drop=True), new[cols].reset_index(drop=True))

    def test_no_date_weak_match_is_not_forced_positive(self):
        labels = self.labels.copy()
        labels["進場依據"] = "5日均"
        labels["進場點"] = self.yu_point + 45
        _, matches, positives, _ = model.compare_history(
            labels, self.taiex, self.futures,
            self.analysis_date, self.analysis_date, config(),
        )
        best = matches.loc[matches["候選排名"] == 1].iloc[0]
        self.assertIn(best["配對可信度"], ["低", "不採用"])
        self.assertTrue(positives.empty)


if __name__ == "__main__":
    unittest.main()
