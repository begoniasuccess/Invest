import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
import sys
import types

import numpy as np
import pandas as pd

# 測試環境不需要連 PostgreSQL；若未裝 psycopg，提供最小匯入替身。
if "psycopg" not in sys.modules:
    psycopg = types.ModuleType("psycopg")
    psycopg_types = types.ModuleType("psycopg.types")
    psycopg_json = types.ModuleType("psycopg.types.json")
    psycopg_json.Jsonb = lambda value: value
    sys.modules.update({"psycopg": psycopg, "psycopg.types": psycopg_types,
                        "psycopg.types.json": psycopg_json})
fake_db = types.ModuleType("common.db")
fake_common = types.ModuleType("common")
fake_common.db = fake_db
sys.modules.setdefault("common", fake_common)
sys.modules.setdefault("common.db", fake_db)

from machine_learning import futures_direction_model as model


def config():
    return {
        "model_key": "test", "model_name": "test", "target_market": "TX",
        "target_name": "direction", "version_no": 1, "algorithm": "random_forest",
        "source_relation": "source_view", "date_column": "日期",
        "entity_filter": {"futures_id": "TX"}, "feature_columns": ["動能", "型態"],
        "target_config": {"price_column": "日盤收", "flat_threshold_pct": 0.0,
                          "labels": {"-1": "跌", "0": "平", "1": "漲"}},
        "model_params": {"n_estimators": 20, "max_depth": 3, "min_samples_leaf": 1, "n_jobs": 1},
        "training_config": {"minimum_training_rows": 20, "random_seed": 42},
    }


class FuturesDirectionModelTest(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2025-01-01", periods=80)
        momentum = np.tile([-1.0, 1.0], 40)
        prices = [100.0]
        for signal in momentum[:-1]:
            prices.append(prices[-1] * (1.01 if signal > 0 else 0.99))
        self.raw = pd.DataFrame({
            "日期": dates, "日盤收": prices, "動能": momentum,
            "型態": np.where(momentum > 0, "強", "弱"),
        })

    def test_sample_target_is_next_trading_row(self):
        samples = model.make_samples(self.raw, config())
        self.assertEqual(samples.loc[0, "feature_date"], self.raw.loc[0, "日期"])
        self.assertEqual(samples.loc[0, "target_date"], self.raw.loc[1, "日期"])
        self.assertTrue(pd.isna(samples.iloc[-1]["actual_class"]))

    def test_prediction_training_cutoff_precedes_target(self):
        target = self.raw.iloc[-1]["日期"].date()
        with patch.object(model, "load_source", return_value=self.raw):
            predictions, _, _ = model.predict_dates(config(), [target])
        self.assertEqual(len(predictions), 1)
        self.assertLess(predictions[0].trained_through_date, target)
        self.assertLess(predictions[0].feature_date, target)

    def test_unsafe_relation_rejected(self):
        with self.assertRaises(ValueError):
            model.quote_relation("view; DROP TABLE x")

    def test_decimal_and_none_are_normalized_as_numeric(self):
        train = pd.DataFrame({
            "數值": [Decimal("1.5"), None, Decimal("2.5")],
            "文字": ["多", None, "空"],
        })
        predict = pd.DataFrame({"數值": [Decimal("3.5")], "文字": [None]})
        train_out, predict_out = model.normalize_feature_frames(
            train, predict, ["數值", "文字"]
        )
        self.assertTrue(pd.api.types.is_float_dtype(train_out["數值"]))
        self.assertTrue(pd.isna(train_out.loc[1, "數值"]))
        self.assertEqual(predict_out.loc[0, "數值"], 3.5)
        self.assertTrue(pd.isna(predict_out.loc[0, "文字"]))

    def test_decimal_feature_snapshot_is_json_serializable(self):
        row = pd.Series({"數值": Decimal("123.45"), "空值": None})
        snapshot = model._feature_snapshot(row, ["數值", "空值"])
        self.assertEqual(snapshot, {"數值": 123.45, "空值": None})
        self.assertIn('"數值":123.45', model.canonical_json(snapshot))

    def test_dates_are_grouped_by_calendar_month(self):
        dates = [
            date(2025, 2, 3), date(2025, 1, 31),
            date(2025, 2, 4), date(2025, 3, 3),
        ]
        groups = model.group_dates_by_month(dates)
        self.assertEqual(groups, [
            [date(2025, 1, 31)],
            [date(2025, 2, 3), date(2025, 2, 4)],
            [date(2025, 3, 3)],
        ])

    def test_resume_skips_only_saved_dates(self):
        dates = [date(2025, 6, 23), date(2025, 6, 24), date(2025, 6, 25)]
        self.assertEqual(
            model.missing_target_dates(dates, {date(2025, 6, 23), date(2025, 6, 25)}),
            [date(2025, 6, 24)],
        )


if __name__ == "__main__":
    unittest.main()
