from pathlib import Path
import unittest


VIEW_PATH = (
    Path(__file__).resolve().parents[1]
    / "db_migrate"
    / "postgres_views.sql"
)
VIEW_SQL = VIEW_PATH.read_text(encoding="utf-8") if VIEW_PATH.exists() else ""


class TaiexReportViewTest(unittest.TestCase):
    @unittest.skipUnless(VIEW_SQL, "此程式包未附完整 postgres_views.sql")
    def test_new_columns_are_in_requested_order(self):
        view_sql = VIEW_SQL.split('-- View: v_taiex_report', 1)[1]
        final_select = view_sql.rsplit('FROM calc4;', 1)[0]

        self.assertLess(final_select.rindex('AS "60日乖離"'), final_select.rindex('AS 布林標準差'))
        self.assertLess(final_select.rindex('AS 布林標準差'), final_select.rindex('AS 布林上軌'))
        self.assertLess(final_select.rindex('AS 布林上軌'), final_select.rindex('AS 布林下軌'))
        self.assertLess(
            final_select.rindex('AS 法人買賣超_總成交_pct'),
            final_select.rindex('AS 散戶多空比_微台'),
        )

    @unittest.skipUnless(VIEW_SQL, "此程式包未附完整 postgres_views.sql")
    def test_bollinger_matches_futures_report_formula(self):
        self.assertIn('STDDEV_SAMP(收盤價)', VIEW_SQL)
        self.assertIn('MA20_calc + 2 * 布林標準差_calc', VIEW_SQL)
        self.assertIn('MA20_calc - 2 * 布林標準差_calc', VIEW_SQL)

    @unittest.skipUnless(VIEW_SQL, "此程式包未附完整 postgres_views.sql")
    def test_micro_retail_ratio_is_scaled_to_decimal(self):
        self.assertIn('FROM v_futures_report_daily f', VIEW_SQL)
        self.assertIn('WHERE f.日期 = calc4.日期', VIEW_SQL)
        self.assertIn(') / 100.0)::numeric, 4) AS 散戶多空比_微台', VIEW_SQL)


if __name__ == "__main__":
    unittest.main()
