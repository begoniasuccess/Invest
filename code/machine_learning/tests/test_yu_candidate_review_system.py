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

from machine_learning import yu_candidate_review_system as model


def config():
    return {
        "name": "test", "version": 1, "date_column": "日期",
        "taiex": {
            "fields": ["開盤價", "最高價", "最低價", "收盤價", "成交量"],
            "column_map": {"open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價", "volume": "成交量"},
            "dynamic_fields": [{"field": "5MA", "kind": "5MA", "deviation_ratios": [-.02, .02]}],
        },
        "futures": {
            "mapped_fields": [
                {"field": "全日開", "kind": "開盤"}, {"field": "全日高", "kind": "高點"},
                {"field": "全日低", "kind": "低點"}, {"field": "全日收", "kind": "收盤"},
            ]
        },
        "features": {"load_calendar_days": 500, "windows": [5, 10, 20, 60, 120], "atr_window": 10},
        "scoring": {
            "volume_window": 20, "shape_percentile_window": 60, "pivot_windows": [5, 10, 20, 60],
            "large_volume_ratio": 1.3, "long_body_atr": .8, "long_shadow_atr": .45,
            "formation_weights": {"volume": 2, "pivot": 2, "body": 1.5, "shadow": 1.5},
            "validity": {"age_half_life_rows": 40, "cross_penalty": .08},
        },
        "importance_scoring": {
            "weights": {"shape": .25, "point": .20, "formation": .20, "validity": .20, "overlap": .15},
            "grade_thresholds": {"S": 85, "A": 70, "B": 55, "C": 40},
        },
        "candidate": {
            "lookback_trading_rows": 200, "large_volume_ratio": 1.3, "large_volume_percentile": .8,
            "long_body_atr": .8, "long_body_percentile": .85,
            "long_shadow_atr": .45, "long_shadow_percentile": .85,
            "pivot_windows": [10, 20, 60], "minimum_gap_points": 20, "minimum_gap_atr": .08,
            "cluster_tolerance_points": 20, "cluster_tolerance_atr": .05,
            "cluster_max_width_points": 35, "cluster_max_width_atr": .1,
            "touch_buffer_atr": .12, "break_buffer_atr": .1,
            "confirmed_break_closes": 2, "overtested_cross_count": 5,
            "external_round_step": 500, "external_round_levels_each_side": 1,
        },
        "review": {
            "maximum_zones": 30, "maximum_distance_atr": 8,
            "include_categories": ["STATIC_K_LEVEL", "DYNAMIC_INDICATOR", "EXTERNAL_MARKET"],
            "excluded_statuses": ["INVALIDATED", "OVERTESTED"],
        },
        "known_label_validation": {"match_tolerance_points": 35},
    }


def strict_config():
    result = config()
    result["candidate"].update({
        "selection_mode": "strict",
        "strict": {
            "minimum_signal_families": 2,
            "large_volume_ratio": 1.5, "large_volume_percentile": .9,
            "long_body_atr": 1.0, "long_body_percentile": .9,
            "long_shadow_atr": .65, "long_shadow_percentile": .9,
            "pivot_windows": [20, 60],
            "extreme_volume_ratio": 2.0, "extreme_body_atr": 1.5,
            "extreme_shadow_atr": 1.0, "extreme_percentile": .97,
        },
    })
    return result


class YuCandidateReviewSystemTest(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2026-01-01", periods=90)
        base = 30000 + np.arange(90) * 3.0
        self.taiex = pd.DataFrame({
            "日期": dates, "開盤價": base, "最高價": base + 60,
            "最低價": base - 50, "收盤價": base + 20,
            "成交量": np.where(np.arange(90) == 45, 600, 100),
            "5MA": base - 10,
        })
        self.taiex.loc[46, ["開盤價", "最高價", "最低價", "收盤價"]] += 150
        self.futures = pd.DataFrame({
            "日期": dates, "全日開": base + 100, "全日高": base + 160,
            "全日低": base + 50, "全日收": base + 120,
        })

    def test_raw_candidates_keep_multiple_sources(self):
        cutoff = self.taiex.iloc[70]["日期"]
        raw = model.build_raw_taiex_levels(self.taiex, cutoff, config())
        source = raw.loc[raw["source_date"].eq(self.taiex.iloc[45]["日期"])]
        self.assertIn("大量K", set(source["source_rule"]))
        self.assertGreaterEqual(source["source_part"].nunique(), 6)
        self.assertIn("全K中關", set(source["source_part"]))
        self.assertIn("實K中關", set(source["source_part"]))
        self.assertTrue(raw["raw_level_id"].is_unique)

    def test_strict_mode_rejects_ordinary_single_signal(self):
        row = pd.Series({
            "量比_20": 1.6, "成交量百分位_60": .91,
            "實體_ATR倍數": .2, "實體百分位_60": .4, "實體點數": 10,
            "上影_ATR倍數": .1, "上影百分位_60": .3,
            "下影_ATR倍數": .1, "下影百分位_60": .3,
            "是否截至當日20日新高": 0, "是否截至當日20日新低": 0,
            "是否截至當日60日新高": 0, "是否截至當日60日新低": 0,
        })
        self.assertEqual(model._formation_hits(row, strict_config()), [])

    def test_strict_mode_keeps_two_signal_families(self):
        row = pd.Series({
            "量比_20": 1.6, "成交量百分位_60": .91,
            "實體_ATR倍數": 1.1, "實體百分位_60": .92, "實體點數": 10,
            "上影_ATR倍數": .1, "上影百分位_60": .3,
            "下影_ATR倍數": .1, "下影百分位_60": .3,
            "是否截至當日20日新高": 0, "是否截至當日20日新低": 0,
            "是否截至當日60日新高": 0, "是否截至當日60日新低": 0,
        })
        self.assertEqual(model._formation_hits(row, strict_config()), ["大量K", "長紅K"])

    def test_strict_mode_keeps_only_extreme_single_signal(self):
        row = pd.Series({
            "量比_20": 2.1, "成交量百分位_60": .98,
            "實體_ATR倍數": .2, "實體百分位_60": .4, "實體點數": -10,
            "上影_ATR倍數": .1, "上影百分位_60": .3,
            "下影_ATR倍數": .1, "下影百分位_60": .3,
            "是否截至當日20日新高": 0, "是否截至當日20日新低": 0,
            "是否截至當日60日新高": 0, "是否截至當日60日新低": 0,
        })
        self.assertEqual(model._formation_hits(row, strict_config()), ["大量K"])

    def test_gap_uses_previous_close_and_current_open(self):
        cutoff = self.taiex.iloc[50]["日期"]
        raw = model.build_raw_taiex_levels(self.taiex, cutoff, config())
        gaps = raw.loc[raw["source_date"].eq(self.taiex.iloc[46]["日期"]) & raw["source_rule"].str.contains("跳空")]
        self.assertEqual(set(gaps["source_part"]), {"昨收缺口緣", "今開缺口緣"})
        self.assertIn(float(self.taiex.iloc[45]["收盤價"]), set(gaps["level_value"]))
        self.assertIn(float(self.taiex.iloc[46]["開盤價"]), set(gaps["level_value"]))

    def test_cluster_preserves_every_raw_source_link(self):
        raw = pd.DataFrame([
            {"raw_level_id": "A", "market": "TAIEX", "source_category": "STATIC_K_LEVEL", "level_value": 100,
             "formation_score": 1, "validity_score": .8, "source_date": pd.Timestamp("2026-01-01"),
             "source_rule": "大量K", "current_status": "ACTIVE", "distance_from_current": -10},
            {"raw_level_id": "B", "market": "TAIEX", "source_category": "STATIC_K_LEVEL", "level_value": 112,
             "formation_score": 2, "validity_score": .7, "source_date": pd.Timestamp("2026-01-02"),
             "source_rule": "長上影", "current_status": "TOUCHED", "distance_from_current": 2},
            {"raw_level_id": "C", "market": "TAIEX", "source_category": "STATIC_K_LEVEL", "level_value": 160,
             "formation_score": 1, "validity_score": .6, "source_date": pd.Timestamp("2026-01-03"),
             "source_rule": "長紅K", "current_status": "ACTIVE", "distance_from_current": 50},
        ])
        zones, links = model.cluster_levels(raw, 100, config())
        self.assertEqual(len(zones), 2)
        self.assertEqual(len(links), 3)
        first = zones.loc[zones["source_count"].eq(2)].iloc[0]
        self.assertEqual(first["representative_level"], 112)
        self.assertIn(first["importance_grade"], {"S", "A", "B", "C", "D"})
        self.assertGreaterEqual(first["importance_score"], 0)
        self.assertLessEqual(first["importance_score"], 100)

    def test_yu_candle_profile_separates_direction_and_importance(self):
        cmap = config()["taiex"]["column_map"]
        red = pd.Series({"開盤價": 100, "最高價": 130, "最低價": 100, "收盤價": 130, "ATR": 20})
        black = pd.Series({"開盤價": 130, "最高價": 130, "最低價": 100, "收盤價": 100, "ATR": 20})
        red_profile = model._candle_profile(red, cmap)
        black_profile = model._candle_profile(black, cmap)
        self.assertEqual(red_profile["candle_shape"], "大紅實體K")
        self.assertGreater(red_profile["direction_strength"], 0)
        self.assertEqual(black_profile["candle_shape"], "大黑實體K")
        self.assertLess(black_profile["direction_strength"], 0)
        self.assertEqual(red_profile["shape_prior_score"], black_profile["shape_prior_score"])

    def test_black_shadow_strength_is_mirror_of_red(self):
        cmap = config()["taiex"]["column_map"]
        red_lower = pd.Series({"開盤價": 110, "最高價": 130, "最低價": 80, "收盤價": 130, "ATR": 40})
        black_lower = pd.Series({"開盤價": 130, "最高價": 130, "最低價": 80, "收盤價": 110, "ATR": 40})
        red_score = model._candle_profile(red_lower, cmap)["shape_prior_score"]
        black_score = model._candle_profile(black_lower, cmap)["shape_prior_score"]
        self.assertGreater(red_score, black_score)

    def test_cluster_does_not_chain_into_wide_zone(self):
        raw = pd.DataFrame([
            {"raw_level_id": str(i), "market": "TAIEX", "source_category": "STATIC_K_LEVEL",
             "level_value": value, "formation_score": 1, "validity_score": .8,
             "source_date": pd.Timestamp("2026-01-01"), "source_rule": "大量K",
             "current_status": "ACTIVE_SUPPORT", "distance_from_current": value - 200}
            for i, value in enumerate([100, 118, 136, 154])
        ])
        zones, _ = model.cluster_levels(raw, 100, config())
        self.assertGreater(len(zones), 1)
        self.assertLessEqual((zones["zone_upper"] - zones["zone_lower"]).max(), 35)

    def test_future_rows_do_not_change_asof_output(self):
        cutoff = self.taiex.iloc[60]["日期"]
        old = model.analyze_as_of(self.taiex, self.futures, cutoff, config())
        changed = self.taiex.copy()
        changed.loc[changed["日期"] > cutoff, ["開盤價", "最高價", "最低價", "收盤價", "成交量"]] = 999999
        new = model.analyze_as_of(changed, self.futures, cutoff, config())
        for left, right in zip(old, new):
            pd.testing.assert_frame_equal(left.reset_index(drop=True), right.reset_index(drop=True))

    def test_review_separates_key_and_trade_labels(self):
        zones = pd.DataFrame([{
            "zone_id": "Z1", "market": "TAIEX", "zone_lower": 100,
            "zone_upper": 110, "representative_level": 105, "source_count": 2,
            "rule_count": 2, "formation_score": 2, "validity_score": .8,
            "current_status": "ACTIVE", "distance_from_current": 5,
            "representative_method": "test", "source_rules": "大量K|長上影", "source_dates": "2026-01-01",
        }])
        review = model.build_review_template(zones, pd.Timestamp("2026-02-01"))
        self.assertIn("is_key_level", review.columns)
        self.assertIn("is_trade_candidate", review.columns)
        self.assertIn("review_action", review.columns)

    def test_review_contains_only_taiex_and_is_capped(self):
        cutoff = self.taiex.iloc[70]["日期"]
        _, _, _, review = model.analyze_as_of(self.taiex, self.futures, cutoff, config())
        self.assertLessEqual(len(review), 30)
        self.assertEqual(set(review["market"]), {"TAIEX"})
        self.assertEqual(set(review["source_category"]), {"STATIC_K_LEVEL"})

    def test_dynamic_deviation_levels_are_generated_separately(self):
        cutoff = self.taiex.iloc[70]["日期"]
        raw = model.build_raw_taiex_levels(self.taiex, cutoff, config())
        dynamic = raw.loc[raw["source_category"].eq("DYNAMIC_INDICATOR")]
        self.assertIn("5MA+2%乖離點", set(dynamic["source_part"]))
        self.assertIn("5MA-2%乖離點", set(dynamic["source_part"]))

    def test_point_clause_prevents_category_pollution(self):
        point = {
            "Yu原始依據": "今日高點32196／5MA乖離率2%為32391",
            "Yu點位子句": "今日高點32196",
            "明示點位類型": "高點",
        }
        self.assertEqual(model._known_category(point), "STATIC_K_LEVEL")
        point["Yu點位子句"] = "5MA乖離率2%為32391"
        self.assertEqual(model._known_category(point), "DYNAMIC_INDICATOR")

    def test_cleaned_category_has_priority_over_text(self):
        point = {
            "已整理點位分類": "K棒固定點位",
            "Yu點位子句": "同一段文字另有5MA與選擇權",
        }
        self.assertEqual(model._known_category(point), "STATIC_K_LEVEL")
        point["已整理點位分類"] = "均線或指標點位"
        self.assertEqual(model._known_category(point), "DYNAMIC_INDICATOR")
        point["已整理點位分類"] = "外部參考點位"
        self.assertEqual(model._known_category(point), "EXTERNAL_MARKET")

    def test_cleaned_workbook_reads_named_sheet_and_columns(self):
        from machine_learning import yu_key_level_rule_discovery as discovery

        cleaned = pd.DataFrame([
            {
                "整理後點位ID": "CLEAN-1", "分析日期": "2026-01-05",
                "點位角色": "壓力", "點位": 30123, "點位分類": "K棒固定點位",
                "點位類型": "高點", "Yu點位子句": "高點30123",
                "Yu原始依據": "高點30123", "明示參考日期": "2026-01-02",
            },
            {
                "整理後點位ID": "CLEAN-2", "分析日期": "2026-01-05",
                "點位角色": "支撐", "點位": 29900, "點位分類": "均線或指標點位",
                "點位類型": "5MA", "Yu點位子句": "5MA 29900",
                "Yu原始依據": "5MA 29900", "明示參考日期": "",
            },
        ])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cleaned.xlsx"
            with pd.ExcelWriter(path) as writer:
                pd.DataFrame({"說明": ["摘要"]}).to_excel(writer, sheet_name="整理摘要", index=False)
                cleaned.to_excel(writer, sheet_name="加權點位整理", index=False)
            result = discovery.normalize_taiex_labels(
                [path], pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-31")
            )
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["已整理點位分類"], "K棒固定點位")
        self.assertEqual(result.iloc[0]["Yu加權點位"], 30123)
        self.assertEqual(result.iloc[1]["是否K棒型點位"], 0)

    def test_dynamic_and_external_do_not_compete_for_static_review(self):
        cutoff = self.taiex.iloc[70]["日期"]
        _, zones, _, review = model.analyze_as_of(self.taiex, self.futures, cutoff, config())
        dynamic = model.select_reference_levels(zones, "DYNAMIC_INDICATOR", config())
        external = model.select_reference_levels(zones, "EXTERNAL_MARKET", config())
        self.assertTrue(review["source_category"].eq("STATIC_K_LEVEL").all())
        self.assertTrue(dynamic["source_category"].eq("DYNAMIC_INDICATOR").all())
        self.assertTrue(external["source_category"].eq("EXTERNAL_MARKET").all())

    def test_static_review_balances_above_and_below_when_available(self):
        rows = []
        for i, distance in enumerate([-40, -30, -20, -10, 10, 20, 30, 40]):
            rows.append({
                "zone_id": f"Z{i}", "market": "TAIEX", "source_category": "STATIC_K_LEVEL",
                "source_count": 2, "rule_count": 1, "formation_score": 1,
                "validity_score": .8, "current_status": "ACTIVE_SUPPORT",
                "distance_from_current": distance,
            })
        zones = pd.DataFrame(rows)
        zones.attrs["current_atr"] = 100
        selected = model.select_review_zones(zones, config(), limit=6)
        self.assertEqual(int(selected["distance_from_current"].lt(0).sum()), 3)
        self.assertEqual(int(selected["distance_from_current"].gt(0).sum()), 3)


if __name__ == "__main__":
    unittest.main()
