from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "db_migrate" / "add_taiex_strategy_columns_20260818.sql"
SQL = SQL_PATH.read_text(encoding="utf-8")


class TaiexStrategyUpdateTests(unittest.TestCase):
    def test_new_strategy_columns_are_present_in_requested_order(self):
        columns = [
            'AS "策略_價量昨日_線型"',
            'AS "策略_價量五日_線型"',
            'AS "策略_價量昨日_籌碼"',
            'AS "策略_價量五日_籌碼"',
            'AS "策略_線型_籌碼"',
            'AS "策略_核心"',
        ]
        positions = [SQL.index(column) for column in columns]
        self.assertEqual(positions, sorted(positions))

    def test_chip_strategy_matches_requested_rules(self):
        self.assertIn("src.%2$I = ''買超'' AND src.%1$I = ''空單'' THEN ''多''", SQL)
        self.assertIn("src.%2$I = ''賣超'' AND src.%1$I = ''多單'' THEN ''空''", SQL)
        self.assertIn("ELSE ''無''::text END AS %3$I", SQL)

    def test_chip_strategy_is_inserted_after_retail_chip(self):
        self.assertIn("WHEN column_name = '散戶籌碼'", SQL)
        self.assertIn("'散戶籌碼', '法人籌碼', '策略_籌碼_判定'", SQL)

    def test_taiex_fields_replace_futures_strategy_fields(self):
        self.assertIn('b."策略_價量昨日_判斷"', SQL)
        self.assertIn('b."策略_價量五日_判斷"', SQL)
        self.assertIn('b."趨勢"', SQL)
        self.assertIn('b."策略_籌碼_判定"', SQL)


if __name__ == "__main__":
    unittest.main()
