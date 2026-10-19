import unittest
from datetime import datetime, date, timedelta
from module.futures_turning import find_turning_match, session_results, turning_audit_rows

def bar(i,low=90,high=110,close=105,direction=1):
    t=datetime(2026,10,12,8,45)+timedelta(minutes=i*5)
    return dict(time=t,last_time=t+timedelta(minutes=4),session_date=t.date(),session='position',low=low,high=high,close=close,direction=direction,dif=10+i)

class BoundaryTests(unittest.TestCase):
    def test_priority(self):
        rows=[bar(0),bar(1,95,109,100,-1),bar(59,96,110,108,1)]
        m=find_turning_match(rows,True,1,100)
        self.assertEqual(m['value'],105);self.assertTrue(m['rule'].startswith('優先例外'))
        self.assertEqual(find_turning_match(rows,True,1)['value'],108)
    def test_pressure(self):
        m=find_turning_match([bar(0,90,120,100,-1)],False,-1,105)
        self.assertEqual(m['value'],100);self.assertTrue(m['rule'].startswith('優先例外'))
    def test_price_conditions(self):
        for close in [100,99]: self.assertIsNone(find_turning_match([bar(0,close=close)],True,1,100))
        for close in [100,101]: self.assertIsNone(find_turning_match([bar(0,close=close,direction=-1)],False,-1,100))
    def test_only_first_absolute_extreme(self):
        self.assertIsNone(find_turning_match([bar(0,low=95),bar(1,low=90)],True,1,100))
    def test_missing_and_flat_history(self):
        for prior in [None,0]: self.assertIsNone(find_turning_match([bar(0)],True,prior,100))
        self.assertIsNone(find_turning_match([bar(0)],True,1,None))
    def test_original_reversal(self):
        self.assertEqual(find_turning_match([bar(0)],True,-1,100)['value'],105)
    def test_no_right_future(self):
        rows=[bar(0,95,110,100,-1),bar(59,90,108,95,-1)]
        now=datetime(2026,10,13)
        before=session_results(rows,now=now)
        future=bar(60);future.update(time=datetime(2026,10,12,15),last_time=datetime(2026,10,12,15,4),session='after_market')
        after=session_results(rows+[future],now=now)
        self.assertIsNone(before[0][2]);self.assertEqual(before[0],after[0])
    def test_resume_csv_and_incomplete(self):
        prior=bar(-1,close=100)
        prior.update(time=datetime(2026,10,10,4,55),last_time=datetime(2026,10,10,4,59),session_date=date(2026,10,9),session='after_market')
        rows=[bar(0),bar(59,low=95)]
        now=datetime(2026,10,13)
        full=session_results([prior]+rows,now=now)[1]
        resumed=session_results(rows,now=now,previous_direction=1,previous_close=100)[0]
        self.assertEqual(full,resumed);self.assertEqual(full[2],105)
        audit=next(r for r in turning_audit_rows([prior]+rows,now=now) if r['日期']==date(2026,10,12) and r['欄位']=='日盤轉折支撐')
        self.assertEqual(audit['轉折值'],105);self.assertEqual(audit['是否首根極值例外'],'是');self.assertEqual(audit['左側K收盤'],100)
        self.assertIsNone(session_results(rows,now=datetime(2026,10,12,12),previous_direction=1,previous_close=100)[0][2])
if __name__=='__main__':unittest.main()
