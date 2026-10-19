import sys
import types
import unittest

import numpy as np
import pandas as pd

fake_common = types.ModuleType("common"); fake_db = types.ModuleType("common.db"); fake_common.db = fake_db
sys.modules.setdefault("common", fake_common); sys.modules.setdefault("common.db", fake_db)

from machine_learning import yu_key_k_formation_candidates as model


def config():
    return {
        "date_column": "日期",
        "taiex": {"relation": "t", "fields": ["開盤價", "最高價", "最低價", "收盤價", "成交量"],
                  "column_map": {"open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價", "volume": "成交量"}},
        "futures": {"relation": "f", "entity_filter": {"futures_id": "TX"}, "mapped_fields": [
            {"field": "全日開", "session": "全日", "kind": "開盤"}, {"field": "全日高", "session": "全日", "kind": "高點"},
            {"field": "全日低", "session": "全日", "kind": "低點"}, {"field": "全日收", "session": "全日", "kind": "收盤"}]},
        "features": {"load_calendar_days": 300, "windows": [5, 10, 20, 60], "atr_window": 10},
        "scoring": {"volume_window": 20, "shape_percentile_window": 60, "pivot_windows": [5, 10, 20, 60],
                    "large_volume_ratio": 1.3, "long_body_atr": .8, "long_shadow_atr": .45,
                    "formation_weights": {"volume": 2, "pivot": 2, "body": 1.5, "shadow": 1.5},
                    "validity": {"break_buffer_atr": .1, "touch_buffer_atr": .15, "age_half_life_rows": 40,
                                 "distance_decay_atr": 6, "break_penalty": .18, "cross_penalty": .08,
                                 "formation_share": .55, "recency_share": .25, "proximity_share": .20}},
        "formation_candidates": {"minimum_score": 2, "top_k": 100, "reason_score_threshold": .8},
    }


class FormationCandidateTest(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2026-01-02", periods=80); base = 20000 + np.arange(80) * 5.0
        self.taiex = pd.DataFrame({"日期": dates, "開盤價": base, "最高價": base + 80,
                                   "最低價": base - 60, "收盤價": base + 20, "成交量": 100.0})
        self.taiex.loc[35, ["開盤價", "收盤價", "最高價", "最低價", "成交量"]] = [20150, 20400, 20420, 20120, 350]
        self.futures = pd.DataFrame({"日期": dates, "futures_id": "TX", "全日開": base * 2,
                                     "全日高": base * 2 + 200, "全日低": base * 2 - 180, "全日收": base * 2 + 30})

    def test_future_rows_never_change_formation_score(self):
        cutoff = self.taiex.iloc[50]["日期"]
        old = model.build_candidates_as_of(self.taiex, self.futures, cutoff, config())[0]
        changed = self.taiex.copy(); changed.loc[51:, ["最高價", "最低價", "收盤價", "成交量"]] = [99999, 1, 88888, 999999]
        new = model.build_candidates_as_of(changed, self.futures, cutoff, config())[0]
        pd.testing.assert_series_equal(old["形成重要度"], new["形成重要度"])

    def test_old_k_formation_score_does_not_decay_when_cutoff_advances(self):
        early = model.build_candidates_as_of(self.taiex.iloc[:51], self.futures.iloc[:51], self.taiex.iloc[50]["日期"], config())[0]
        late = model.build_candidates_as_of(self.taiex, self.futures, self.taiex.iloc[-1]["日期"], config())[0]
        source = self.taiex.iloc[35]["日期"]
        a = early.loc[early["日期"].eq(source), "形成重要度"].iloc[0]
        b = late.loc[late["日期"].eq(source), "形成重要度"].iloc[0]
        self.assertAlmostEqual(a, b)

    def test_yu_reference_becomes_positive_without_future_feature(self):
        formation = model.build_candidates_as_of(self.taiex, self.futures, self.taiex.iloc[-1]["日期"], config())[0]
        source = self.taiex.iloc[35]["日期"]
        refs = pd.DataFrame({"來源K日期": [source], "首次標記日期": [self.taiex.iloc[40]["日期"]],
                             "Yu點位": [40000], "Yu點位角色": ["進場"], "Yu原始依據": ["來源高點"], "Yu標註ID": ["x"]})
        table = model.build_k_training_table(formation, refs, self.taiex.iloc[20]["日期"], self.taiex.iloc[60]["日期"], config())
        row = table.loc[table["日期"].eq(source)].iloc[0]
        self.assertEqual(row["Yu曾標記關鍵K"], 1)
        self.assertEqual(row["特徵可用截止日"], source)


if __name__ == "__main__": unittest.main()
