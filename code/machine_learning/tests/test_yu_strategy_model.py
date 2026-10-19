import unittest
from types import ModuleType
import sys

import numpy as np
import pandas as pd

fake_common = ModuleType("common")
fake_db = ModuleType("common.db")
fake_common.db = fake_db
sys.modules.setdefault("common", fake_common)
sys.modules.setdefault("common.db", fake_db)

from machine_learning import yu_strategy_model as model


def test_config():
    return {
        "date_column": "日期",
        "direction_features": ["動能", "策略"],
        "direction_model": {"minimum_training_rows": 4, "max_depth": 2, "min_samples_leaf": 1, "random_seed": 42},
        "candidate": {
            "lookback_rows": 5, "match_tolerance_points": 20, "cluster_tolerance_points": 10,
            "reference_price_field": "全日收", "volume_field": "全成交量", "round_steps": [50],
            "raw_fields": [
                {"field": "全日開", "session": "全日", "kind": "開盤"},
                {"field": "全日收", "session": "全日", "kind": "收盤"},
                {"field": "全日高", "session": "全日", "kind": "高點"},
                {"field": "全日低", "session": "全日", "kind": "低點"},
            ],
            "current_fields": [{"field": "全5MA", "session": "全日", "kind": "5日均"}],
        },
    }


class YuStrategyModelTest(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2026-01-02", periods=12)
        close = np.arange(10000, 10120, 10, dtype=float)
        self.report = pd.DataFrame({
            "日期": dates, "全日開": close - 5, "全日收": close,
            "全日高": close + 30, "全日低": close - 40, "全5MA": close - 15,
            "全成交量": np.arange(1000, 1120, 10), "動能": np.tile([-1.0, 1.0], 6),
            "策略": np.tile(["空", "多"], 6),
        })

    def test_candidates_never_use_future_rows(self):
        target = self.report.iloc[5]["日期"]
        candidates = model.generate_candidates(self.report, target, test_config())
        self.assertFalse(candidates.empty)
        self.assertLessEqual(candidates["來源日期"].max(), target)

    def test_reason_date_and_kind_break_equal_price_tie(self):
        target = self.report.iloc[5]["日期"]
        candidates = model.generate_candidates(self.report, target, test_config())
        source = self.report.iloc[2]
        plan = pd.Series({
            "分析日期": target, "方案序號": 1, "分析方向_標籤": "多", "交易方向_標籤": "多",
            "樣本型態": "新交易計畫", "進場點": source["全日收"],
            "進場依據": f"{source['日期'].month}/{source['日期'].day}收盤", "資料品質狀態": "OK",
        })
        match = model.match_entry_to_candidate(plan, candidates, test_config())
        self.assertEqual(match["比對狀態"], "命中")
        self.assertEqual(match["點位類型"], "收盤")
        self.assertEqual(pd.Timestamp(match["來源日期"]), source["日期"])

    def test_existing_position_is_not_treated_as_new_point(self):
        plan = pd.Series({
            "分析日期": self.report.iloc[-1]["日期"], "方案序號": 1,
            "分析方向_標籤": "多", "交易方向_標籤": "多", "樣本型態": "既有持倉",
            "進場點": 10000, "進場依據": "抱單中", "資料品質狀態": "OK",
        })
        match = model.match_entry_to_candidate(plan, pd.DataFrame(), test_config())
        self.assertIn("既有持倉", match["比對狀態"])

    def test_walk_forward_uses_only_prior_labels(self):
        labels = pd.DataFrame({
            "分析日期": self.report["日期"], "分析方向_標籤": np.tile(["空", "多"], 6),
            "交易方向_標籤": np.tile(["空", "多"], 6),
        })
        samples = labels.merge(self.report, left_on="分析日期", right_on="日期")
        results, rules = model.walk_forward_direction(samples, test_config())
        self.assertTrue(results.iloc[:4]["交易預測"].isna().all())
        self.assertTrue(results.iloc[4:]["交易預測"].notna().all())
        self.assertIn("Yu 交易方向", rules)

    def test_candidate_rankings_are_split_by_direction(self):
        target = self.report.iloc[-1]["日期"]
        candidates = model.generate_candidates(self.report, target, test_config())
        matches = pd.DataFrame({
            "分析日期": [self.report.iloc[2]["日期"], self.report.iloc[3]["日期"]],
            "比對狀態": ["命中", "命中"], "交易方向": ["多", "空"],
            "點位類型": ["收盤", "高點"], "盤別": ["全日", "全日"],
        })
        ranked = model.candidate_rankings_for_date(target, candidates, matches, top_n=5)
        self.assertEqual(set(ranked["預測交易方向"]), {"多", "空"})
        self.assertEqual(len(ranked), 10)

    def test_current_direction_prediction_excludes_same_date_label(self):
        labels = pd.DataFrame({
            "分析日期": self.report["日期"], "分析方向_標籤": np.tile(["空", "多"], 6),
            "交易方向_標籤": np.tile(["空", "多"], 6),
        })
        samples = labels.merge(self.report, left_on="分析日期", right_on="日期")
        target = self.report.iloc[-1]["日期"]
        prediction = model.predict_direction_for_date(
            samples, self.report.iloc[[-1]], target, test_config()
        )
        self.assertEqual(len(prediction), 1)
        self.assertIn(prediction.iloc[0]["交易預測"], {"多", "空", "無"})


if __name__ == "__main__":
    unittest.main()
