import unittest
from datetime import datetime, date, timedelta
from module.futures_turning import calculate_bars, find_turning, session_results, COLUMNS
from common.report_columns import order_futures_report_columns


def bar(index, low, high, close, direction, session='position', day=date(2026,10,2)):
    time=datetime.combine(day,datetime.min.time())+timedelta(hours=8,minutes=45+5*index)
    return dict(time=time,last_time=time+timedelta(minutes=4),session_date=day,session=session,low=low,high=high,close=close,direction=direction)


class TurningTests(unittest.TestCase):
    def test_support_right(self):
        rows=[bar(0,10,20,15,-1),bar(1,11,21,16,0),bar(2,12,22,18,1)]
        self.assertEqual(find_turning(rows),18)

    def test_support_left_outside_session(self):
        self.assertEqual(find_turning([bar(0,10,20,17,1)],previous_direction=-1),17)

    def test_immediate_left_flat_does_not_qualify(self):
        rows=[bar(0,11,20,17,0),bar(1,10,20,18,1)]
        self.assertIsNone(find_turning(rows,previous_direction=-1))

    def test_second_low(self):
        rows=[bar(0,11,20,17,1),bar(1,10,21,18,1)]
        self.assertEqual(find_turning(rows,previous_direction=-1),17)

    def test_ties_earliest_qualifying(self):
        rows=[bar(0,10,20,17,1),bar(1,10,20,18,1)]
        self.assertEqual(find_turning(rows,previous_direction=-1),17)

    def test_tied_low_next_candle_before_next_price(self):
        rows=[bar(0,10,20,17,0),bar(1,11,20,18,-1),bar(2,10,20,19,1)]
        self.assertEqual(find_turning(rows),19)

    def test_resistance_mirror(self):
        rows=[bar(0,10,30,21,1),bar(1,11,25,20,0),bar(2,12,24,19,-1)]
        self.assertEqual(find_turning(rows,False),19)
        self.assertEqual(find_turning([bar(0,10,30,22,-1)],False,1),22)

    def test_no_right_outside_session(self):
        self.assertIsNone(find_turning([bar(0,10,20,17,-1)]))

    def test_ema_warmup_and_exact_resume(self):
        rows=[dict(close=100+i%9) for i in range(100)]
        all_bars=calculate_bars(rows)
        self.assertIsNone(all_bars[24]['dif'])
        self.assertIsNotNone(all_bars[25]['dif'])
        self.assertIsNone(all_bars[25]['direction'])
        resumed=calculate_bars(rows[40:],all_bars[39]['ema_state'])
        self.assertEqual([x['dif'] for x in resumed],[x['dif'] for x in all_bars[40:]])
        constant=calculate_bars([dict(close=100)]*40)
        self.assertEqual(constant[-1]['direction'],0)

    def test_incomplete_or_open_session_null(self):
        rows=[bar(0,10,20,17,1),bar(60,11,21,18,-1)]
        self.assertEqual(session_results(rows,datetime(2026,10,2,13,44),-1)[0][2:],(None,None))
        self.assertEqual(session_results(rows[:1],datetime(2026,10,3),-1)[0][2:],(None,None))
        self.assertEqual(session_results(rows,datetime(2026,10,2,14),-1)[0][2],17)

    def test_night_date_and_boundary(self):
        rows=[bar(0,10,20,17,1,session='after_market'),bar(1,11,21,18,-1,session='after_market')]
        rows[0]['time']=datetime(2026,10,2,15);rows[0]['last_time']=datetime(2026,10,2,15,4)
        rows[1]['time']=datetime(2026,10,3,4,55);rows[1]['last_time']=datetime(2026,10,3,4,59)
        result=session_results(rows,datetime(2026,10,3,5),-1)[0]
        self.assertEqual(result[:2],(date(2026,10,2),'after_market'))
        self.assertEqual(result[2],17)

    def test_refresh_persists_and_commits(self):
        import sys
        import types
        import pandas as pd
        from unittest.mock import patch
        from module.futures_turning import refresh_turning_levels
        records=[bar(i,90+i,110+i,100+i,1) for i in range(61)]
        source=pd.DataFrame(records)
        statements=[]
        class Connection:
            committed=False
            def execute(self, sql, params=None): statements.append(sql)
            def cursor(self): return self
            def executemany(self, sql, params): statements.append(sql)
            def commit(self): self.committed=True
            def __enter__(self): return self
            def __exit__(self,*args): pass
        connection=Connection()
        fake=types.ModuleType('common.db')
        fake.execute_sql=lambda sql: True
        fake.query_single_value=lambda sql: None
        fake.query_to_df=lambda sql,params: pd.DataFrame() if 'ema_state FROM' in sql else source
        fake.get_connection=lambda: connection
        with patch.dict(sys.modules,{'common.db':fake}):
            import common
            with patch.object(common,'db',fake,create=True), patch('module.futures_turning.export_turning_audit'):
                self.assertEqual(refresh_turning_levels(full=True),1)
        self.assertTrue(connection.committed)
        self.assertTrue(any('INSERT INTO report.futures_turning_levels' in sql for sql in statements))

    def test_column_order(self):
        expected=('日盤走勢',COLUMNS[0],COLUMNS[1],'夜盤走勢',COLUMNS[2],COLUMNS[3])
        cols=order_futures_report_columns(['日期','買賣權比_週五','其他',*reversed(expected)])
        self.assertEqual(cols,['日期','買賣權比_週五',*expected,'其他'])

    def test_view_sql_contains_session_trends_in_requested_order(self):
        from module.futures_turning import ensure_turning_views
        constants = ensure_turning_views.__code__.co_consts
        statement = next(value for value in constants if isinstance(value, str) and 'DO $turning$' in value)
        self.assertLess(statement.index('AS "日盤走勢"'), statement.index('t."日盤轉折支撐"'))
        self.assertLess(statement.index('AS "夜盤走勢"'), statement.index('t."夜盤轉折支撐"'))
        self.assertIn("THEN ''漲勢盤''", statement)
        self.assertIn("ELSE ''跌勢盤''", statement)
        self.assertNotIn('DROP VIEW report.v_futures_report_daily', statement)
        self.assertIn('RENAME TO v_futures_report_daily_without_session_trend', statement)

if __name__=='__main__':unittest.main()
