from pathlib import Path
import unittest

from common.report_columns import order_taiex_report_columns


ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "db_migrate" / "split_taiex_core_strategy_20260909.sql"
SQL = SQL_PATH.read_text(encoding="utf-8")
NORMALIZED_SQL = " ".join(SQL.split())


class TaiexDualCoreColumnTests(unittest.TestCase):
    def test_sql_calculates_both_core_variants(self):
        self.assertIn(
            'public.fn_futures_core_strategy( "策略_價量昨日_判斷", "趨勢", "位置", "策略_籌碼_判定" ) AS "策略_核心_昨價量"',
            NORMALIZED_SQL,
        )
        self.assertIn(
            'public.fn_futures_core_strategy( "策略_價量五日_判斷", "趨勢", "位置", "策略_籌碼_判定" ) AS "策略_核心_五日價量"',
            NORMALIZED_SQL,
        )

    def test_legacy_core_remains_five_day_compatible(self):
        self.assertIn(
            'public.fn_futures_core_strategy( "策略_價量五日_判斷", "趨勢", "位置", "策略_籌碼_判定" ) AS "策略_核心"',
            NORMALIZED_SQL,
        )
        self.assertIn('"策略_核心" IS DISTINCT FROM "策略_核心_五日價量"', SQL)

    def test_report_order_shows_new_columns_and_hides_legacy_alias(self):
        source = [
            "日期",
            "策略_核心",
            "策略_核心_五日價量",
            "策略_線型_籌碼",
            "策略_核心_昨價量",
        ]
        self.assertEqual(
            order_taiex_report_columns(source),
            ["日期", "策略_線型_籌碼", "策略_核心_昨價量", "策略_核心_五日價量"],
        )


if __name__ == "__main__":
    unittest.main()
