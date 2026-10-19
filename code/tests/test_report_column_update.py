from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
TAIEX_SQL = (ROOT / "db_migrate" / "fix_taiex_report_requirements_20260817.sql").read_text(encoding="utf-8")
FUTURES_SQL = (ROOT / "db_migrate" / "fix_futures_report_requirements_20260817.sql").read_text(encoding="utf-8")


class ReportColumnUpdateTest(unittest.TestCase):
    def test_futures_day_type_matches_taiex(self):
        source = (ROOT / "module" / "futures_utils.py").read_text(encoding="utf-8")
        self.assertIn('if weekday == 0:\n        return "頭"', source)
        self.assertIn('if weekday in (1, 2, 3):\n        return "週間"', source)
        self.assertIn('if weekday == 4:\n        return "尾"', source)

    def test_taiex_ma_and_reordered_fields(self):
        self.assertIn('ADD COLUMN IF NOT EXISTS "120MA"', TAIEX_SQL)
        self.assertIn('ADD COLUMN IF NOT EXISTS "240MA"', TAIEX_SQL)
        self.assertIn("ROWS BETWEEN 119 PRECEDING AND CURRENT ROW", TAIEX_SQL)
        self.assertIn("ROWS BETWEEN 239 PRECEDING AND CURRENT ROW", TAIEX_SQL)
        self.assertIn("column_name = '60MA'", TAIEX_SQL)
        self.assertIn("column_name = '資金走向判定'", TAIEX_SQL)
        self.assertIn("s.\"位置\", s.\"策略_價量昨日_判斷\", s.\"策略_價量五日_判斷\"", TAIEX_SQL)
        self.assertIn("p120 <> p60 + 1 OR p240 <> p60 + 2", TAIEX_SQL)
        self.assertIn("pposition <> pfund + 1", TAIEX_SQL)

        stock_source = (ROOT / "module" / "stock_report_utils.py").read_text(encoding="utf-8")
        self.assertIn("def recalc_long_ma", stock_source)
        self.assertIn('SET "120MA" = c.ma120', stock_source)
        self.assertIn('"120MA",\n        "240MA",', stock_source)

    def test_futures_hidden_columns_and_new_fields(self):
        for column in (
            "昨日成交量", "夜10MA", "夜20MA", "夜60MA", "夜120MA",
            "日價漲跌", "日量增減_昨日", "日量增減_五日",
        ):
            self.assertIn(f"'{column}'", FUTURES_SQL)
        self.assertIn("ADD COLUMN IF NOT EXISTS \"日240MA\"", FUTURES_SQL)
        self.assertIn("ADD COLUMN IF NOT EXISTS \"K線型態\"", FUTURES_SQL)
        self.assertIn("r.\"K線型態\" AS \"K線型態\", s.\"跳空缺口\"", FUTURES_SQL)
        self.assertIn("r.\"日240MA\" AS \"日240MA\"", FUTURES_SQL)
        self.assertIn("hidden_count <> 0", FUTURES_SQL)
        self.assertIn("pk <> pgap - 1", FUTURES_SQL)
        self.assertIn("p240 <> p120 + 1", FUTURES_SQL)

        futures_source = (ROOT / "main" / "futures.py").read_text(encoding="utf-8")
        self.assertIn('日成交量, 夜成交量, "K線型態"', futures_source)
        self.assertIn('FU._classify_k_type(o, c, h, l)', futures_source)

    def test_predictor_uses_internal_complete_view(self):
        predictor = (ROOT / "main" / "futures_predict.py").read_text(encoding="utf-8")
        self.assertIn('VIEW_NAME = "v_futures_report_daily_model"', predictor)


if __name__ == "__main__":
    unittest.main()
