from pathlib import Path
import unittest

from common.report_columns import order_futures_report_columns


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "db_migrate" / "update_dual_core_and_gap_rules_20260910.sql").read_text(
    encoding="utf-8"
)


class StrategyRules20260910Tests(unittest.TestCase):
    def test_core_function_uses_all_three_priority_inputs(self):
        self.assertIn("CREATE OR REPLACE FUNCTION public.fn_report_core_strategy", SQL)
        self.assertIn("institutional_chip = '買超'", SQL)
        self.assertIn("institutional_chip = '賣超'", SQL)
        self.assertIn("WHEN price_strategy = '多' AND trend_value = '多' THEN '多'", SQL)
        self.assertIn("WHEN price_strategy = '空' AND trend_value = '空' THEN '空'", SQL)

    def test_both_reports_expose_dual_core(self):
        self.assertGreaterEqual(SQL.count('AS "策略_核心_昨價量"'), 2)
        self.assertGreaterEqual(SQL.count('AS "策略_核心_五日價量"'), 2)
        self.assertIn('"策略_核心" IS DISTINCT FROM "策略_核心_五日價量"', SQL)

    def test_equal_price_is_neutral_not_missing(self):
        self.assertIn("price_value = '價同' OR volume_value = '量同' THEN '無'", SQL)

    def test_line_chip_rule_does_not_return_neutral_for_complete_input(self):
        function_body = SQL.split(
            "CREATE OR REPLACE FUNCTION public.fn_report_line_chip_strategy", 1
        )[1].split("$$;", 1)[0]
        self.assertNotIn("THEN '無'", function_body)

    def test_option_source_supports_forced_retry(self):
        source = (ROOT / "module" / "finMind.py").read_text(encoding="utf-8")
        function = source.split("def get_tw_option_daily(", 1)[1].split(") -> pd.DataFrame:", 1)[0]
        self.assertIn("force_refresh", function)

    def test_futures_output_removes_legacy_core_and_places_new_columns_before_created_at(self):
        columns = [
            "日期",
            "策略_核心",
            "策略_核心_五日價量",
            "策略_線型_籌碼",
            "策略_核心_昨價量",
            "建立時間",
        ]
        self.assertEqual(
            order_futures_report_columns(columns),
            [
                "日期",
                "策略_線型_籌碼",
                "策略_核心_昨價量",
                "策略_核心_五日價量",
                "建立時間",
            ],
        )


if __name__ == "__main__":
    unittest.main()
