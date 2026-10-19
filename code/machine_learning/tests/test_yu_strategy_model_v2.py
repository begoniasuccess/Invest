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

from machine_learning import yu_strategy_model_v2 as model


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
            "recent_dates_always_keep": 3, "maximum_key_dates": 10,
            "pivot_window": 2, "minimum_key_score": 3.0,
        },
        "direction_features": ["taiex__趨勢", "tx__動能"],
        "direction_model": {"minimum_training_rows": 4, "max_depth": 2, "min_samples_leaf": 1, "random_seed": 42},
        "candidate": {
            "lookback_rows": 12, "cluster_tolerance_points": 20, "match_tolerance_points": 35,
            "minimum_entry_distance_points": 15, "minimum_actionable_distance_points": 300,
            "range_distance_multiplier": 2.0, "maximum_actionable_distance_points": 1400,
            "recency_half_life_rows": 10, "round_steps": [50, 100], "top_per_scenario": 5,
        },
    }


class YuStrategyV2Test(unittest.TestCase):
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
            "成交量": [100, 100, 100, 300, 100, 100, 100, 100, 100, 100, 100, 100],
            "20日均量": 100, "實體_pct": 0.005, "日振幅_昨收_pct": 0.01,
            "跳空缺口_昨收_pct": 0.0, "K線型態": "中實體K",
            "趨勢": np.tile(["空", "多"], 6),
        })

    def test_taiex_selects_date_but_candidate_uses_futures_price(self):
        target = self.futures.iloc[-1]["日期"]
        raw = model.generate_raw_candidates(self.futures, self.taiex, target, config())
        key_date = self.taiex.iloc[3]["日期"]
        mapped = raw.loc[(raw["來源日期"] == key_date) & (raw["點位類型"] == "收盤")]
        self.assertFalse(mapped.empty)
        self.assertEqual(mapped.iloc[0]["候選點位"], self.futures.iloc[3]["全日收"])
        self.assertNotEqual(mapped.iloc[0]["候選點位"], self.taiex.iloc[3]["收盤價"])

    def test_clusters_merge_nearby_levels(self):
        target = self.futures.iloc[-1]["日期"]
        raw = model.generate_raw_candidates(self.futures, self.taiex, target, config())
        clusters = model.cluster_candidates(raw, config())
        self.assertLess(len(clusters), len(raw))
        self.assertTrue((clusters["依據數"] > 1).any())

    def test_four_scenarios_have_correct_side(self):
        clusters = pd.DataFrame({"距參考收盤點數": [-100, 100]})
        self.assertEqual(model.scenario_compatible(clusters, "回測做多", config()).tolist(), [True, False])
        self.assertEqual(model.scenario_compatible(clusters, "突破做多", config()).tolist(), [False, True])
        self.assertEqual(model.scenario_compatible(clusters, "反彈做空", config()).tolist(), [False, True])
        self.assertEqual(model.scenario_compatible(clusters, "跌破做空", config()).tolist(), [True, False])

    def test_infer_scenario_from_text_then_price_fallback(self):
        self.assertEqual(model.infer_scenario(pd.Series({"交易方向": "多", "進場條件": "突破前高", "進場依據": "", "進場附註": ""}), 30000), "突破做多")
        self.assertEqual(model.infer_scenario(pd.Series({"交易方向": "空", "進場條件": "跌破支撐", "進場依據": "", "進場附註": ""}), 30000), "跌破做空")
        self.assertEqual(model.infer_scenario(pd.Series({"交易方向": "多", "進場條件": "", "進場依據": "", "進場附註": "", "進場點": 29900}), 30000), "回測做多")

    def test_prediction_ranking_differs_by_scenario(self):
        target = self.futures.iloc[-1]["日期"]
        raw = model.generate_raw_candidates(self.futures, self.taiex, target, config())
        clusters = model.cluster_candidates(raw, config())
        ranked = model.rank_clusters(clusters, pd.DataFrame(), self.futures.iloc[-1], target, config())
        self.assertEqual(set(ranked["進場情境"]), set(model.SCENARIOS))
        below = ranked.loc[ranked["進場情境"].isin(["回測做多", "跌破做空"]), "距參考收盤點數"]
        above = ranked.loc[ranked["進場情境"].isin(["突破做多", "反彈做空"]), "距參考收盤點數"]
        self.assertTrue((below < 0).all())
        self.assertTrue((above > 0).all())

    def test_evaluation_start_does_not_remove_prior_training(self):
        combined = model.merge_direction_report(self.taiex, self.futures, config())
        labels = pd.DataFrame({
            "分析日期": self.futures["日期"],
            "分析方向": np.tile(["空", "多"], 6),
            "交易方向": np.tile(["空", "多"], 6),
        })
        samples = model.build_direction_samples(labels, combined, config())
        start = self.futures.iloc[8]["日期"]
        results, _, _ = model.direction_walk_forward(samples, config(), evaluation_start=start)
        self.assertEqual(results.iloc[0]["分析日期"], start)
        self.assertNotEqual(results.iloc[0]["交易判斷路徑"], "訓練資料不足（0/4）")

    def test_source_summary_is_split_by_scenario(self):
        matches = pd.DataFrame({
            "分析日期": pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-04"]),
            "比對狀態": ["命中", "命中", "未命中：超過容許點數"],
            "進場情境": ["回測做多", "反彈做空", "回測做多"],
            "主要類型": ["收盤", "高點", "低點"], "主要盤別": ["全日", "日盤", "全日"],
            "差距點數": [3.0, 5.0, 80.0],
        })
        summary = model.summarize_historical_matches(matches)
        self.assertEqual(summary["命中筆數"].sum(), 2)
        self.assertEqual(set(summary["進場情境"]), {"回測做多", "反彈做空"})


if __name__ == "__main__":
    unittest.main()
