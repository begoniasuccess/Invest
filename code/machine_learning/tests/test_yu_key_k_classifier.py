import sys
import types
import unittest

import numpy as np
import pandas as pd

fake_common = types.ModuleType("common"); fake_db = types.ModuleType("common.db"); fake_common.db = fake_db
sys.modules.setdefault("common", fake_common); sys.modules.setdefault("common.db", fake_db)

from machine_learning import yu_key_k_classifier as model


def config():
    return {
        "date_column": "日期",
        "database_sources": {},
        "formation_features": ["量比_20", "紅黑方向"],
        "feature_sets": {"formation": {"groups": ["formation"]}},
        "models": {
            "logistic": {"type": "logistic", "C": 1, "max_iter": 1000},
            "forest": {"type": "random_forest", "n_estimators": 50, "min_samples_leaf": 2, "max_depth": 3},
        },
        "validation": {"minimum_train_rows": 30, "test_block_rows": 7, "classification_threshold": .5,
                       "top_k": [10], "importance_holdout_rows": 20, "importance_repeats": 2},
    }


class KeyKClassifierTest(unittest.TestCase):
    def setUp(self):
        rng = np.random.default_rng(9); n = 90
        signal = rng.normal(size=n)
        self.data = pd.DataFrame({
            "日期": pd.bdate_range("2025-01-01", periods=n),
            "量比_20": signal,
            "紅黑方向": np.where(signal > 0, "紅K", "黑K"),
            model.TARGET: (signal > .2).astype(int),
        })
        self.data = model.build_dataset(self.data, pd.DataFrame({"日期": []}), "日期")

    def test_walk_forward_never_trains_through_test_date(self):
        out = model.expanding_predictions(self.data, ["量比_20", "紅黑方向"], "logistic", config())
        self.assertTrue((out["訓練截止日"] < out["日期"]).all())
        self.assertEqual(len(out), 60)

    def test_feature_set_comparison_returns_both_models(self):
        sets = model.resolve_feature_sets(self.data, config())
        summary, predictions, importance = model.compare(self.data, sets, config())
        self.assertEqual(set(summary["模型"]), {"logistic", "forest"})
        self.assertFalse(predictions.empty)
        self.assertFalse(importance.empty)


if __name__ == "__main__":
    unittest.main()
