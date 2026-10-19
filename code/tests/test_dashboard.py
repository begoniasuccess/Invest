import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import sys

import pandas as pd
import common

from dashboard.config import INDICATORS, MARKETS
from dashboard.cycle_service import build_cycle_statistics
from dashboard.data_service import _ohlc_sql, build_distribution, prepare_market_frame
from dashboard.kline_service import KLINE_INTERVALS, _kline_bounds_sql, _kline_sql, prepare_kline_frame
from dashboard.raw_service import PAGE_SIZES, RAW_REPORTS, get_report_config, load_raw_report


class DashboardCalculationTests(unittest.TestCase):
    def test_kline_sources_include_volume(self):
        self.assertEqual(KLINE_INTERVALS["week"], "週 K")
        self.assertIn('"Trading_Volume" AS 成交量', _kline_sql("taiex"))
        self.assertIn("日成交量 AS 成交量", _kline_sql("tx_day"))
        self.assertIn("夜成交量 AS 成交量", _kline_sql("tx_night"))
        self.assertIn("COALESCE(日成交量, 0) + COALESCE(夜成交量, 0)", _kline_sql("tx_full"))
        self.assertIn("MAX(date)", _kline_bounds_sql("taiex"))
        self.assertIn("MAX(日期)", _kline_bounds_sql("tx_full"))

    def test_weekly_kline_uses_first_open_last_close_and_summed_volume(self):
        raw = pd.DataFrame({
            "日期": ["2026-08-10", "2026-08-11", "2026-08-14", "2026-08-17"],
            "開盤": [100, 105, 108, 120], "最高": [110, 112, 115, 130],
            "最低": [95, 101, 106, 118], "收盤": [105, 108, 114, 126],
            "成交量": [10, 20, 30, 40],
        })
        weekly = prepare_kline_frame(raw, "week")
        first = weekly.iloc[0]
        self.assertEqual(first["日期"], pd.Timestamp("2026-08-14"))
        self.assertEqual(float(first["開盤"]), 100)
        self.assertEqual(float(first["最高"]), 115)
        self.assertEqual(float(first["最低"]), 95)
        self.assertEqual(float(first["收盤"]), 114)
        self.assertEqual(float(first["成交量"]), 60)

    def test_monthly_kline_uses_last_real_trading_date(self):
        raw = pd.DataFrame({
            "日期": ["2026-07-30", "2026-07-31", "2026-08-03"],
            "開盤": [100, 105, 110], "最高": [108, 112, 118],
            "最低": [98, 103, 109], "收盤": [105, 110, 117], "成交量": [10, 20, 30],
        })
        monthly = prepare_kline_frame(raw, "month")
        self.assertEqual(monthly.iloc[0]["日期"], pd.Timestamp("2026-07-31"))
        self.assertEqual(float(monthly.iloc[0]["成交量"]), 30)

    def test_yearly_kline_uses_first_open_last_close_and_summed_volume(self):
        raw = pd.DataFrame({
            "日期": ["2025-01-02", "2025-12-31", "2026-01-05"],
            "開盤": [100, 120, 140], "最高": [110, 135, 150],
            "最低": [90, 115, 138], "收盤": [105, 130, 148], "成交量": [10, 20, 30],
        })
        yearly = prepare_kline_frame(raw, "year")
        first = yearly.iloc[0]
        self.assertEqual(KLINE_INTERVALS["year"], "年 K")
        self.assertEqual(first["日期"], pd.Timestamp("2025-12-31"))
        self.assertEqual(float(first["開盤"]), 100)
        self.assertEqual(float(first["最高"]), 135)
        self.assertEqual(float(first["最低"]), 90)
        self.assertEqual(float(first["收盤"]), 130)
        self.assertEqual(float(first["成交量"]), 30)

    def test_raw_report_sources_are_allowlisted(self):
        self.assertEqual(RAW_REPORTS["taiex"].view, "v_taiex_report")
        self.assertEqual(RAW_REPORTS["futures"].view, "v_futures_dashboard_turning")
        self.assertEqual(get_report_config("futures").date_column, "日期")
        self.assertIn(30, PAGE_SIZES)
        self.assertIn(240, PAGE_SIZES)
        with self.assertRaisesRegex(ValueError, "不支援"):
            get_report_config("unknown")

    def test_raw_report_search_and_sort_apply_to_full_query(self):
        query_to_df = Mock()
        query_single_value = Mock(return_value=1)
        query_to_df.side_effect = [
            pd.DataFrame(columns=["日期", "收盤價", "法人籌碼"]),
            pd.DataFrame({"min_date": ["2026-01-01"], "max_date": ["2026-12-31"]}),
            pd.DataFrame({"日期": ["2026-08-18"], "收盤價": [45000], "法人籌碼": ["買超"]}),
        ]
        fake_db = SimpleNamespace(query_to_df=query_to_df, query_single_value=query_single_value)
        with patch.object(common, "db", fake_db, create=True), patch.dict(sys.modules, {"common.db": fake_db}):
            frame, metadata = load_raw_report(
                "taiex", "2026-01-01", "2026-12-31", 240, 0,
                "收盤價", "asc", "法人籌碼", "買_%",
            )
        count_sql, count_params = query_single_value.call_args.args
        data_sql, data_params = query_to_df.call_args_list[-1].args
        self.assertIn('CAST("法人籌碼" AS TEXT) ILIKE ?', count_sql)
        self.assertEqual(count_params[-1], "%買\\_\\%%")
        self.assertIn('ORDER BY "收盤價" ASC NULLS LAST, "日期" DESC', data_sql)
        self.assertEqual(data_params[-2:], (240, 0))
        self.assertEqual(metadata["sort_column"], "收盤價")
        self.assertEqual(metadata["search_text"], "買_%")
        self.assertEqual(len(frame), 1)

    def test_tx_full_market_reads_view_ohlc(self):
        self.assertEqual(MARKETS["tx_full"], "台指期全日")
        sql = _ohlc_sql("tx_full")
        self.assertIn("FROM v_futures_report_daily", sql)
        self.assertIn("全日開 AS 開盤", sql)
        self.assertIn("全日高 AS 最高", sql)
        self.assertIn("全日低 AS 最低", sql)
        self.assertIn("全日收 AS 收盤", sql)

    def test_indicator_calculation(self):
        raw = pd.DataFrame(
            {
                "日期": ["2026-01-02", "2026-01-05"],
                "開盤": [100.0, 110.0],
                "最高": [115.0, 125.0],
                "最低": [95.0, 105.0],
                "收盤": [110.0, 120.0],
            }
        )
        frame, invalid = prepare_market_frame(raw)
        self.assertEqual(invalid, 0)
        self.assertEqual(frame.loc[1, "昨收"], 110.0)
        self.assertEqual(frame.loc[1, "range_points"], 20.0)
        self.assertAlmostEqual(frame.loc[1, "range_prev_pct"], 20 / 110 * 100)
        self.assertEqual(frame.loc[1, "close_open_points"], 10.0)
        self.assertEqual(frame.loc[1, "close_prev_points"], 10.0)

    def test_positive_point_boundaries(self):
        frame = pd.DataFrame({"年": [2026] * 4, "range_points": [0.0, 24.999, 25.0, 500.0]})
        table, summary = build_distribution(frame, INDICATORS["range_points"])
        counts = table.set_index("級距")["總計"]
        self.assertEqual(counts["0~25"], 2)
        self.assertEqual(counts["25~50"], 1)
        self.assertEqual(counts[">=500"], 1)
        self.assertEqual(summary["count"], 4)

    def test_signed_boundaries_put_zero_in_positive_bucket(self):
        frame = pd.DataFrame({"年": [2026] * 5, "close_open_points": [-501.0, -500.0, -0.01, 0.0, 500.0]})
        table, _ = build_distribution(frame, INDICATORS["close_open_points"])
        counts = table.set_index("級距")["總計"]
        self.assertEqual(counts["<-500"], 1)
        self.assertEqual(counts["-500~-475"], 1)
        self.assertEqual(counts["-25~0"], 1)
        self.assertEqual(counts["0~25"], 1)
        self.assertEqual(counts[">=500"], 1)

    def test_invalid_ohlc_is_removed(self):
        raw = pd.DataFrame(
            {
                "日期": ["2026-01-02", "2026-01-05"],
                "開盤": [100.0, 100.0],
                "最高": [110.0, 90.0],
                "最低": [90.0, 80.0],
                "收盤": [105.0, 100.0],
            }
        )
        frame, invalid = prepare_market_frame(raw)
        self.assertEqual(invalid, 1)
        self.assertEqual(len(frame), 1)

    def test_dynamic_point_bins_follow_data_range(self):
        frame = pd.DataFrame({"年": [2025, 2026, 2026], "close_open_points": [-423.0, 0.0, 587.0]})
        table, summary = build_distribution(frame, INDICATORS["close_open_points"], 50.0)
        self.assertEqual(table.iloc[0]["級距"], "-450~-400")
        self.assertEqual(table.iloc[-1]["級距"], "550~600")
        self.assertEqual(int(table["總計"].sum()), 3)
        self.assertEqual(summary["mode_bucket"], "-450~-400")

    def test_dynamic_percent_bins_use_decimal_labels(self):
        frame = pd.DataFrame({"年": [2026, 2026], "close_prev_pct": [-3.27, 5.13]})
        table, _ = build_distribution(frame, INDICATORS["close_prev_pct"], 0.5)
        self.assertEqual(table.iloc[0]["級距"], "-3.5%~-3.0%")
        self.assertEqual(table.iloc[-1]["級距"], "5.0%~5.5%")
        self.assertEqual(int(table["總計"].sum()), 2)

    def test_dynamic_bins_reject_excessive_bucket_count(self):
        frame = pd.DataFrame({"年": [2026, 2026], "range_points": [0.0, 500.0]})
        with self.assertRaisesRegex(ValueError, "級距過小"):
            build_distribution(frame, INDICATORS["range_points"], 1)

    def test_only_sparse_head_and_tail_buckets_are_merged(self):
        counts = [1, 2, 10, 1, 1980, 10, 3, 4]
        values = [10, 60, 110, 160, 210, 260, 310, 360]
        frame = pd.DataFrame(
            {
                "年": [2026] * sum(counts),
                "range_points": [value for value, count in zip(values, counts) for _ in range(count)],
            }
        )
        table, _ = build_distribution(frame, INDICATORS["range_points"], 50)
        totals = table.set_index("級距")["總計"]

        self.assertEqual(table.iloc[0]["級距"], "<100")
        self.assertEqual(int(totals["<100"]), 3)
        self.assertEqual(int(totals["150~200"]), 1)  # 中間低於 0.25% 仍保留
        self.assertEqual(table.iloc[-1]["級距"], ">=300")
        self.assertEqual(int(totals[">=300"]), 7)

    def test_weekday_cycle_statistics(self):
        frame = pd.DataFrame(
            {
                "日期": pd.to_datetime(["2026-08-10", "2026-08-17", "2026-08-11", "2026-08-12"]),
                "close_prev_points": [10.0, -5.0, 0.0, 20.0],
                "close_prev_pct": [1.0, -0.5, 0.0, 2.0],
            }
        )
        table, summary = build_cycle_statistics(frame, "weekday")
        monday = table.set_index("週期").loc["星期一"]
        self.assertEqual(int(monday["上漲次數"]), 1)
        self.assertEqual(int(monday["下跌次數"]), 1)
        self.assertAlmostEqual(float(monday["上漲機率"]), 50.0)
        self.assertEqual(summary["flat_count"], 1)

    def test_iso_week_cycle_labels(self):
        frame = pd.DataFrame(
            {
                "日期": pd.to_datetime(["2026-01-01", "2026-12-31"]),
                "close_prev_points": [10.0, -10.0],
                "close_prev_pct": [0.1, -0.1],
            }
        )
        table, _ = build_cycle_statistics(frame, "week")
        self.assertEqual(len(table), 53)
        self.assertEqual(table.iloc[0]["週期"], "1W")
        self.assertEqual(table.iloc[-1]["週期"], "53W")


if __name__ == "__main__":
    unittest.main()
