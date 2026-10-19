import sys
import tempfile
import types
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

fake_common = types.ModuleType("common")
fake_db = types.ModuleType("common.db")
fake_common.db = fake_db
sys.modules.setdefault("common", fake_common)
sys.modules.setdefault("common.db", fake_db)

from machine_learning import yu_key_level_rule_discovery as model


def config():
    return {
        "date_column": "日期",
        "taiex": {"column_map": {"open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價", "volume": "成交量"}},
        "futures": {"column_map": {"open": "日盤開", "high": "日盤高", "low": "日盤低", "close": "日盤收"}},
        "features": {"windows": [5, 10, 20, 60, 120], "atr_window": 10},
        "scoring": {
            "volume_window": 20, "shape_percentile_window": 60, "pivot_windows": [5, 10, 20, 60],
            "large_volume_ratio": 1.3, "long_body_atr": .8, "long_shadow_atr": .45,
            "formation_weights": {"volume": 2, "pivot": 2, "body": 1.5, "shadow": 1.5},
        },
        "discovery": {
            "candidate_calendar_days": 180, "exact_tolerance_points": 5, "price_tolerance_points": 30,
            "age_decay_calendar_days": 60, "accepted_confidence": ["高", "中"],
            "weights": {"price": 4, "explicit_date": 8, "kind": 3, "recency": .25},
            "threshold_quantiles": [.5, .75, .9],
            "rule_features": {"大量K": ["量比_20"], "長紅黑K": ["實體_ATR倍數"]},
            "validation_fraction": .25, "label_maturity_calendar_days": 0,
            "minimum_rule_positive_samples": 1,
            "stable_min_validation_positives": 1, "stable_min_lift": 1.0,
            "stable_min_coverage": .1, "pivot_touch_buffer_atr": .15,
        },
        "mapping": {"hit_tolerances": [25, 50]},
    }


class YuRuleDiscoveryTest(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2025-10-01", periods=90)
        base = 28000 + np.arange(len(dates)) * 7.0
        self.taiex = pd.DataFrame({
            "日期": dates, "開盤價": base, "最高價": base + 80,
            "最低價": base - 60, "收盤價": base + 30,
            "成交量": np.where(np.arange(len(dates)) == 45, 500, 100),
        })
        self.futures = pd.DataFrame({
            "日期": dates, "日盤開": base + 110, "日盤高": base + 190,
            "日盤低": base + 50, "日盤收": base + 140,
        })

    def test_normalize_structured_taiex_points(self):
        analysis_date = self.taiex.iloc[55]["日期"]
        source_date = self.taiex.iloc[45]["日期"]
        frame = pd.DataFrame({
            "分析日期": [analysis_date], "壓力1_點位": [self.taiex.iloc[45]["最高價"]],
            "壓力1_依據": [f"{source_date.month}/{source_date.day}大量K高點"],
            "支撐1_點位": [self.taiex.iloc[45]["開盤價"]],
            "支撐1_依據": [f"{source_date.month}/{source_date.day}大量K開盤"],
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.csv"; frame.to_csv(path, index=False, encoding="utf-8-sig")
            points = model.normalize_taiex_labels([path], analysis_date, analysis_date)
        self.assertEqual(len(points), 2)
        self.assertTrue(points["明示參考日期"].str.contains(f"{source_date:%Y-%m-%d}").all())
        self.assertTrue(points["規則文字標籤"].str.contains("大量K").all())

    def test_clause_parser_does_not_leak_rule_to_another_date(self):
        analysis_date = pd.Timestamp("2025-11-26")
        frame = pd.DataFrame({
            "分析日期": [analysis_date],
            "壓力1_點位": [27816],
            "壓力1_依據": ["長下影線低點 11/05 27373/下殺K棒開盤 11/14 27816"],
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.csv"; frame.to_csv(path, index=False, encoding="utf-8-sig")
            points = model.normalize_taiex_labels([path], analysis_date, analysis_date)
        row = points.iloc[0]
        self.assertEqual(row["明示參考日期"], "2025-11-14")
        self.assertEqual(row["明示點位類型"], "開盤")
        self.assertNotIn("長下影", str(row["規則文字標籤"]))
        self.assertEqual(row["Yu點位子句"], "下殺K棒開盤 11/14 27816")

    def test_daily_description_labels_same_day_point(self):
        analysis_date = pd.Timestamp("2026-01-05")
        frame = pd.DataFrame({
            "分析日期": [analysis_date], "價量描述": ["爆近一年的大量，今天開收高低非常重要"],
            "線型描述": ["多頭趨勢"], "壓力1_點位": [30105],
            "壓力1_依據": ["01/05 高點30339/收盤30105"],
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.csv"; frame.to_csv(path, index=False, encoding="utf-8-sig")
            points = model.normalize_taiex_labels([path], analysis_date, analysis_date)
        row = points.iloc[0]
        self.assertEqual(row["明示參考日期"], "2026-01-05")
        self.assertIn("大量K", row["規則文字標籤"])

    def test_structured_point_can_use_reason_without_repeating_number(self):
        analysis_date = pd.Timestamp("2026-01-07")
        frame = pd.DataFrame({
            "分析日期": [analysis_date], "支撐1_點位": [30105],
            "支撐1_依據": ["1/5收盤，大量K"],
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.csv"; frame.to_csv(path, index=False, encoding="utf-8-sig")
            points = model.normalize_taiex_labels([path], analysis_date, analysis_date)
        row = points.iloc[0]
        self.assertEqual(row["子句解析狀態"], "點位由結構欄提供")
        self.assertEqual(row["明示參考日期"], "2026-01-05")
        self.assertEqual(row["明示點位類型"], "收盤")
        self.assertIn("大量K", row["規則文字標籤"])

    def test_view_gap_and_non_overlap_gap_are_both_kept(self):
        formation = pd.DataFrame({
            "日期": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "開盤價": [100, 115], "最高價": [110, 120], "最低價": [90, 112],
            "收盤價": [105, 118], "跳空缺口": [np.nan, 10], "ATR": [20, 20],
        })
        enriched = model.add_gap_features(formation, config())
        self.assertEqual(enriched.iloc[1]["View開盤跳空點數"], 10)
        self.assertEqual(enriched.iloc[1]["跳空缺口絕對點數"], 2)
        self.assertEqual(enriched.iloc[1]["View與未重疊缺口方向一致"], 1)

    def test_gap_event_uses_two_trading_days(self):
        market = pd.DataFrame({
            "日期": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-05"]),
            "開盤價": [100, 115, 108], "最高價": [110, 120, 112],
            "最低價": [90, 112, 100], "收盤價": [105, 118, 102],
            "成交量": [100, 100, 100], "跳空缺口": [np.nan, 10, -10],
        })
        events = model.build_gap_events(market, pd.Timestamp("2026-01-05"), config())
        up = events.loc[events["缺口方向"].eq("向上")].iloc[0]
        self.assertEqual(up["缺口前交易日"], pd.Timestamp("2026-01-01"))
        self.assertEqual(up["缺口形成日"], pd.Timestamp("2026-01-02"))
        self.assertEqual(up["缺口下緣"], 110)
        self.assertEqual(up["缺口上緣"], 112)
        self.assertEqual(up["首次完全回補日"], pd.Timestamp("2026-01-05"))

    def test_gap_definitions_compare_distinct_boundaries(self):
        market = pd.DataFrame({
            "日期": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "開盤價": [100, 115], "最高價": [110, 120], "最低價": [90, 112],
            "收盤價": [105, 118], "成交量": [100, 100], "跳空缺口": [np.nan, 10],
        })
        candidates = model.build_gap_definition_candidates(market, pd.Timestamp("2026-01-02"), config())
        edges = candidates.set_index("缺口定義")[["缺口下緣", "缺口上緣"]]
        self.assertEqual(tuple(edges.loc["昨收至今開"]), (105, 115))
        self.assertEqual(tuple(edges.loc["昨收至今收"]), (105, 118))
        self.assertEqual(tuple(edges.loc["前後實體邊界"]), (105, 115))
        self.assertEqual(tuple(edges.loc["前後全K未重疊"]), (110, 112))

    def test_reference_parts_are_not_swing_labels(self):
        source_date = self.taiex.iloc[45]["日期"]
        matches = pd.DataFrame([{"Yu點位ID": "P1", "分析日期": self.taiex.iloc[55]["日期"],
                                "來源K日期": source_date, "候選點位類型": "低點",
                                "規則文字標籤": "大量K", "是否採用正例": 1}])
        training = model.build_k_training(self.taiex, matches, source_date, self.taiex.iloc[60]["日期"], config())
        row = training.loc[training["日期"].eq(source_date)].iloc[0]
        self.assertEqual(row["Yu引用部位_低點"], 1)
        self.assertEqual(row["Yu形成原因_波段低K"], 0)

    def test_pivot_snapshot_never_uses_rows_after_analysis_date(self):
        analysis_date = self.taiex.iloc[55]["日期"]
        source_date = self.taiex.iloc[45]["日期"]
        matches = pd.DataFrame([{
            "Yu點位ID": "P1", "分析日期": analysis_date, "來源K日期": source_date,
            "候選點位類型": "低點", "Yu加權點位": self.taiex.iloc[45]["最低價"],
            "加權候選點位": self.taiex.iloc[45]["最低價"], "是否採用正例": 1,
        }])
        old = model.build_pivot_reference_snapshots(self.taiex, matches, config())
        changed = self.taiex.copy()
        changed.loc[changed["日期"] > analysis_date, ["最高價", "最低價", "收盤價"]] = 99999
        new = model.build_pivot_reference_snapshots(changed, matches, config())
        pd.testing.assert_frame_equal(old, new)

    def test_matching_never_uses_future_rows(self):
        analysis_date = self.taiex.iloc[55]["日期"]
        source = self.taiex.iloc[45]
        points = pd.DataFrame([{
            "Yu點位ID": "P1", "分析日期": analysis_date, "點位角色": "壓力",
            "Yu加權點位": source["最高價"], "Yu原始依據": f"{source['日期'].month}/{source['日期'].day}大量K高點",
            "來源檔": "x", "來源欄位": "壓力1", "明示參考日期": f"{source['日期']:%Y-%m-%d}",
            "明示點位類型": "高點", "規則文字標籤": "大量K|波段高低", "是否K棒型點位": 1,
        }])
        old = model.match_yu_levels(points, self.taiex, config())
        altered = self.taiex.copy()
        altered.loc[altered["日期"] > analysis_date, ["開盤價", "最高價", "最低價", "收盤價"]] = 99999
        new = model.match_yu_levels(points, altered, config())
        cols = ["來源K日期", "候選點位類型", "加權候選點位", "點位誤差", "配對可信度"]
        pd.testing.assert_frame_equal(old[cols], new[cols])
        self.assertEqual(old.iloc[0]["來源K日期"], source["日期"])
        self.assertEqual(old.iloc[0]["候選點位類型"], "高點")

    def test_threshold_output_calls_unselected_rows_unlabeled(self):
        training = pd.DataFrame({
            "日期": pd.bdate_range("2026-01-01", periods=8),
            "Yu曾標記關鍵K": [0, 0, 1, 0, 0, 1, 0, 1],
            "Yu規則_大量K": [0, 0, 1, 0, 0, 1, 0, 1],
            "Yu規則_長紅黑K": [0, 0, 0, 1, 0, 0, 1, 0],
            "量比_20": [0.7, 0.9, 1.8, 1.0, 1.1, 2.0, 1.2, 2.1],
            "實體_ATR倍數": [.1, .2, .3, .8, .2, .4, 1.0, .3],
        })
        result = model.discover_thresholds(training, pd.Timestamp("2026-08-31"), config())
        self.assertFalse(result.empty)
        self.assertTrue(result["未標記說明"].str.contains("不等於真正負例").all())
        self.assertIn("樣本外_正例覆蓋率", result.columns)
        self.assertTrue(result["規則答案欄位"].str.startswith("Yu規則_").all())

    def test_mapping_comparison_recovers_source_same_kind(self):
        analysis_date = self.taiex.iloc[55]["日期"]
        source_date = self.taiex.iloc[45]["日期"]
        matches = pd.DataFrame([{
            "Yu點位ID": "P1", "分析日期": analysis_date, "來源K日期": source_date,
            "候選點位類型": "高點", "Yu加權點位": self.taiex.iloc[45]["最高價"],
            "加權候選點位": self.taiex.iloc[45]["最高價"], "是否採用正例": 1,
        }])
        future_points = pd.DataFrame([{
            "期貨點位ID": "F1", "分析日期": analysis_date, "期貨點位角色": "進場",
            "Yu期貨點位": self.futures.iloc[45]["日盤高"],
            "期貨原始依據": f"{source_date.month}/{source_date.day}高點",
            "明示參考日期": f"{source_date:%Y-%m-%d}", "明示點位類型": "高點",
            "規則文字標籤": "",
        }])
        _, summary = model.compare_mapping_methods(matches, future_points, self.taiex, self.futures, config())
        best = summary.iloc[0]
        self.assertEqual(best["映射方法"], "來源K同部位TX_日盤")
        self.assertEqual(best["中位絕對誤差"], 0)

    def test_mapping_rejects_unaligned_kind(self):
        analysis_date = self.taiex.iloc[55]["日期"]
        source_date = self.taiex.iloc[45]["日期"]
        matches = pd.DataFrame([{
            "Yu點位ID": "P1", "分析日期": analysis_date, "來源K日期": source_date,
            "候選點位類型": "收盤", "Yu加權點位": self.taiex.iloc[45]["收盤價"],
            "加權候選點位": self.taiex.iloc[45]["收盤價"], "是否採用正例": 1,
        }])
        future_points = pd.DataFrame([{
            "期貨點位ID": "F1", "分析日期": analysis_date, "期貨點位角色": "進場",
            "Yu期貨點位": self.futures.iloc[45]["日盤高"], "期貨原始依據": "歷史高點",
            "明示參考日期": f"{source_date:%Y-%m-%d}", "明示點位類型": "高點",
            "規則文字標籤": "",
        }])
        detail, summary = model.compare_mapping_methods(matches, future_points, self.taiex, self.futures, config())
        self.assertTrue(detail.empty)
        self.assertTrue(summary.empty)


if __name__ == "__main__":
    unittest.main()
