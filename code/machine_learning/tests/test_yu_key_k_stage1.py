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

from machine_learning import yu_key_k_stage1 as model


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
    }


class YuKeyKStage1Test(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2026-01-02", periods=80)
        base = 20000 + np.arange(80) * 5.0
        self.taiex = pd.DataFrame({
            "日期": dates, "開盤價": base, "最高價": base + 80,
            "最低價": base - 60, "收盤價": base + 20, "成交量": 100.0,
        })
        self.taiex.loc[35, ["開盤價", "收盤價", "最高價", "最低價", "成交量"]] = [20150, 20400, 20420, 20120, 350]
        self.futures = pd.DataFrame({
            "日期": dates, "futures_id": "TX", "全日開": base * 2,
            "全日高": base * 2 + 200, "全日低": base * 2 - 180, "全日收": base * 2 + 30,
        })

    def test_future_rows_never_change_old_cutoff_result(self):
        cutoff = self.taiex.iloc[60]["日期"]
        old = model.analyze_as_of(self.taiex.iloc[:61], self.futures.iloc[:61], cutoff, config())
        future_t = self.taiex.copy()
        future_t.loc[61:, ["最高價", "最低價", "收盤價", "成交量"]] = [99999, 1, 88888, 999999]
        new = model.analyze_as_of(future_t, self.futures, cutoff, config())
        pd.testing.assert_frame_equal(old[1].reset_index(drop=True), new[1].reset_index(drop=True))
        pd.testing.assert_frame_equal(old[2].reset_index(drop=True), new[2].reset_index(drop=True))

    def test_volume_spike_is_identified_from_prior_data(self):
        cutoff = self.taiex.iloc[50]["日期"]
        snapshot, _, _ = model.analyze_as_of(self.taiex, self.futures, cutoff, config())
        spike = snapshot.loc[snapshot["日期"] == self.taiex.iloc[35]["日期"]].iloc[0]
        self.assertGreater(spike["量比_20"], 3)
        self.assertIn("大量K", spike["形成原因"])

    def test_current_asof_high_uses_no_future_confirmation(self):
        cutoff = self.taiex.iloc[35]["日期"]
        snapshot, _, _ = model.analyze_as_of(self.taiex, self.futures, cutoff, config())
        row = snapshot.iloc[-1]
        self.assertEqual(row["是否截至當日20日新高"], 1)
        self.assertEqual(row["距今交易日數"], 0)

    def test_mapping_uses_same_date_tx_price(self):
        cutoff = self.taiex.iloc[50]["日期"]
        _, key_k, levels = model.analyze_as_of(self.taiex, self.futures, cutoff, config())
        source_date = key_k.iloc[0]["日期"]
        expected = self.futures.loc[self.futures["日期"] == source_date, "全日高"].iloc[0]
        mapped = levels.loc[
            (levels["加權關鍵K日期"] == source_date) & (levels["TX來源欄位"] == "全日高"),
            "TX關鍵點位",
        ].iloc[0]
        self.assertEqual(mapped, expected)
        self.assertEqual(
            levels.loc[levels["TX來源欄位"] == "全日高", "點位角色"].iloc[0], "壓力候選"
        )
        self.assertEqual(
            levels.loc[levels["TX來源欄位"] == "全日低", "點位角色"].iloc[0], "支撐候選"
        )

    def test_status_can_change_only_when_cutoff_advances(self):
        source = self.taiex.iloc[35]["日期"]
        early, _, _ = model.analyze_as_of(self.taiex, self.futures, source, config())
        advanced = self.taiex.copy()
        advanced.loc[50, ["開盤價", "最高價", "最低價", "收盤價"]] = [20500, 20700, 20480, 20650]
        later, _, _ = model.analyze_as_of(advanced, self.futures, self.taiex.iloc[60]["日期"], config())
        early_status = early.loc[early["日期"] == source, "目前狀態"].iloc[0]
        later_status = later.loc[later["日期"] == source, "目前狀態"].iloc[0]
        self.assertEqual(early_status, "仍在K棒區間")
        self.assertNotEqual(early_status, later_status)


if __name__ == "__main__":
    unittest.main()
