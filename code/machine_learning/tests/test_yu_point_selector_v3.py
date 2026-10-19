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

from machine_learning import yu_point_selector_v3 as model


def config():
    return {
        "date_column": "日期", "labels_sheet": "sheet",
        "futures": {
            "relation": "f", "entity_filter": {"futures_id": "TX"},
            "reference_price_field": "全日收", "range_field": "全振幅", "volume_field": "全成交量",
            "mapped_fields": [
                {"field": "全日開", "session": "全日", "kind": "開盤"},
                {"field": "全日收", "session": "全日", "kind": "收盤"},
                {"field": "全日高", "session": "全日", "kind": "高點"},
                {"field": "全日低", "session": "全日", "kind": "低點"},
            ],
            "current_fields": [{"field": "全5MA", "session": "全日", "kind": "5日均"}],
        },
        "taiex": {
            "relation": "t", "key_fields": ["開盤價", "收盤價", "最高價", "最低價", "成交量", "20日均量", "實體_pct", "日振幅_昨收_pct", "跳空缺口_昨收_pct", "K線型態", "趨勢"],
            "recent_dates_always_keep": 3, "maximum_key_dates": 10, "pivot_window": 2, "minimum_key_score": 3,
        },
        "candidate": {
            "lookback_rows": 12, "cluster_tolerance_points": 20, "match_tolerance_points": 35,
            "minimum_entry_distance_points": 15, "minimum_actionable_distance_points": 300,
            "range_distance_multiplier": 2, "maximum_actionable_distance_points": 1400,
            "recency_half_life_rows": 10, "round_steps": [50, 100], "top_per_scenario": 5,
        },
        "selector": {
            "minimum_training_plans": 4,
            "model": {"C": 1, "max_iter": 500, "random_seed": 42},
            "tx_report_fields": ["動能"], "taiex_report_fields": ["趨勢"],
            "yu_context_fields": ["分析方向"],
            "feature_groups": {
                "candidate_core": ["進場情境", "主要盤別", "主要類型", "依據數", "距參考收盤絕對點數"],
                "yu_history": ["歷史同情境樣本數", "歷史同情境類型盤別選用次數", "歷史同情境類型盤別選用率", "歷史同情境類型選用次數"],
            },
            "feature_sets": [
                {"name": "A", "groups": ["candidate_core"]},
                {"name": "B", "groups": ["candidate_core", "yu_history", "tx_report", "taiex_report"]},
            ],
        },
    }


class YuPointSelectorV3Test(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2026-01-02", periods=12)
        close = np.arange(30000, 30120, 10, dtype=float)
        self.futures = pd.DataFrame({
            "日期": dates, "futures_id": "TX", "全日開": close - 20, "全日收": close,
            "全日高": close + 100, "全日低": close - 100, "全振幅": 200,
            "全成交量": 10000, "全5MA": close - 30, "動能": np.tile([-1, 1], 6),
        })
        self.taiex = pd.DataFrame({
            "日期": dates, "開盤價": close / 2, "收盤價": close / 2 + 5,
            "最高價": close / 2 + 50, "最低價": close / 2 - 50,
            "成交量": [100, 100, 300, 100, 100, 100, 100, 100, 100, 100, 100, 100],
            "20日均量": 100, "實體_pct": .005, "日振幅_昨收_pct": .01,
            "跳空缺口_昨收_pct": 0, "K線型態": "中實體K", "趨勢": np.tile(["空", "多"], 6),
        })
        self.labels = pd.DataFrame({
            "分析日期": dates, "方案序號": 1, "交易方向": "多", "分析方向": "多",
            "進場點": close - 100, "進場條件": "回測支撐", "進場依據": "前低", "進場附註": "",
        })

    def test_samples_label_candidate_near_yu_entry(self):
        samples = model.build_candidate_samples(self.labels, self.futures, self.taiex, config())
        first = samples.loc[samples["分析日期"] == self.labels.iloc[0]["分析日期"]]
        self.assertTrue((first["是否命中Yu點位"] == 1).any())
        self.assertEqual(first["歷史同情境樣本數"].max(), 0)

    def test_history_features_do_not_see_same_day_label(self):
        labels = pd.concat([self.labels, self.labels.iloc[[0]].assign(方案序號=2)], ignore_index=True)
        samples = model.build_candidate_samples(labels, self.futures, self.taiex, config())
        first_day = samples.loc[samples["分析日期"] == self.labels.iloc[0]["分析日期"]]
        self.assertEqual(first_day["歷史同情境樣本數"].max(), 0)
        second_day = samples.loc[samples["分析日期"] == self.labels.iloc[1]["分析日期"]]
        self.assertEqual(second_day["歷史同情境樣本數"].max(), 2)

    def test_forbidden_yu_target_field_is_rejected(self):
        bad = config()
        bad["selector"]["yu_context_fields"] = ["進場點"]
        import json, tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as handle:
            json.dump(bad, handle, ensure_ascii=False)
            name = handle.name
        with self.assertRaises(ValueError):
            model.load_config(name)

    def test_walk_forward_has_no_prediction_before_minimum_plans(self):
        samples = model.build_candidate_samples(self.labels, self.futures, self.taiex, config())
        details, metrics = model.walk_forward_experiments(
            samples, config(), self.labels.iloc[0]["分析日期"], self.labels.iloc[-1]["分析日期"]
        )
        self.assertFalse(metrics.empty)
        self.assertTrue((metrics["訓練計畫數"] >= 4).all())
        self.assertEqual(set(metrics["特徵組合"]), {"A", "B"})

    def test_summary_reports_top_k(self):
        metrics = pd.DataFrame({
            "特徵組合": ["A", "A"], "可命中候選存在": [1, 1],
            "Top1命中": [1, 0], "Top3命中": [1, 1], "Top5命中": [1, 1],
            "倒數排名MRR": [1, .5], "Top1差距點數": [0, 20], "輸入欄位數": [5, 5],
            "僅研究用途": [False, False],
        })
        summary = model.summarize_experiments(metrics)
        self.assertEqual(summary.iloc[0]["Top1命中率_pct"], 50)
        self.assertEqual(summary.iloc[0]["Top3命中率_pct"], 100)


if __name__ == "__main__":
    unittest.main()
