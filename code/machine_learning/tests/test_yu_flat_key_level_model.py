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

from machine_learning import yu_flat_key_level_model as model


def config():
    return {
        "date_column": "日期",
        "taiex": {"relation": "t", "fields": ["開盤價", "最高價", "最低價", "收盤價", "成交量"],
                  "column_map": {"open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價", "volume": "成交量"}},
        "futures": {"relation": "f", "entity_filter": {"futures_id": "TX"}, "mapped_fields": [
            {"field": "全日開", "session": "全日", "kind": "開盤"},
            {"field": "全日高", "session": "全日", "kind": "高點"},
            {"field": "全日低", "session": "全日", "kind": "低點"},
            {"field": "全日收", "session": "全日", "kind": "收盤"}]},
        "features": {"load_calendar_days": 200, "windows": [5, 10, 20, 60], "atr_window": 10},
        "scoring": {"volume_window": 20, "shape_percentile_window": 60, "pivot_windows": [5, 10, 20, 60],
                    "large_volume_ratio": 1.3, "long_body_atr": .8, "long_shadow_atr": .45,
                    "formation_weights": {"volume": 2, "pivot": 2, "body": 1.5, "shadow": 1.5},
                    "validity": {"break_buffer_atr": .1, "touch_buffer_atr": .15, "age_half_life_rows": 40,
                                 "distance_decay_atr": 6, "break_penalty": .18, "cross_penalty": .08,
                                 "formation_share": .55, "recency_share": .25, "proximity_share": .20}},
        "output": {"top_key_k": 20, "minimum_formation_score": 2, "minimum_validity_score": 1},
        "flat_labels": {"deduplicate_points": 2, "candidate_deduplicate_points": 2, "hit_tolerance_points": 15},
    }


def label_frame(dates):
    base = {"停利1_點位": np.nan, "停利1_依據": None, "停利1_附註": None,
            "停利2_點位": np.nan, "停利2_依據": None, "停利2_附註": None,
            "停損1_點位": np.nan, "停損1_依據": None, "停損1_附註": None,
            "停損2_點位": np.nan, "停損2_依據": None, "停損2_附註": None}
    rows = []
    for date, point, basis in [(dates[0], 21000, "1/2收盤"), (dates[1], 21001, "1/2收盤"),
                               (dates[1], 22000, "60分K大量")]:
        rows.append({"分析日期": date, "進場點": point, "進場依據": basis, "進場附註": None, **base})
    return pd.DataFrame(rows)


class YuFlatKeyLevelModelTest(unittest.TestCase):
    def test_flatten_deduplicates_repeated_point_without_weight(self):
        dates = [pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-05")]
        flat = model.flatten_yu_labels(label_frame(dates), dates[0], dates[1], config())
        daily = flat[flat["時間尺度"] == "日K"]
        self.assertEqual(len(daily), 1)
        self.assertEqual(daily.iloc[0]["首次標記日期"], dates[0])
        self.assertEqual(daily.iloc[0]["原始引用次數"], 2)
        self.assertEqual(len(flat[flat["時間尺度"] == "60分K"]), 1)

    def test_label_is_unavailable_before_first_seen(self):
        flat = pd.DataFrame({"Yu扁平點位ID": ["YL-1"], "時間尺度": ["日K"],
                             "Yu點位": [21000.0], "首次標記日期": [pd.Timestamp("2026-01-05")]})
        candidates = pd.DataFrame({"TX關鍵點位": [21000.0], "關鍵K排名": [1], "關鍵K目前有效度": [1.0]})
        old, _ = model.label_daily_candidates(pd.Timestamp("2026-01-02"), candidates, flat, config())
        new, _ = model.label_daily_candidates(pd.Timestamp("2026-01-05"), candidates, flat, config())
        self.assertEqual(old.iloc[0]["監督標籤"], 0)
        self.assertEqual(new.iloc[0]["監督標籤"], 1)

    def test_hit_requires_same_scale_and_tolerance(self):
        flat = pd.DataFrame({
            "Yu扁平點位ID": ["D", "M"], "時間尺度": ["日K", "60分K"],
            "Yu點位": [21000.0, 22000.0],
            "首次標記日期": [pd.Timestamp("2026-01-02")] * 2,
        })
        candidates = pd.DataFrame({"TX關鍵點位": [21010.0, 22000.0], "關鍵K排名": [1, 2], "關鍵K目前有效度": [2, 1]})
        result, _ = model.label_daily_candidates(pd.Timestamp("2026-01-05"), candidates, flat, config())
        self.assertEqual(result["監督標籤"].tolist(), [1, 0])


if __name__ == "__main__":
    unittest.main()
