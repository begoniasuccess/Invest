import unittest
from datetime import date, datetime, timedelta
from module.futures_turning import turning_audit_rows, session_results
class AuditTests(unittest.TestCase):
 def bars(self):
  start=datetime(2026,5,8,15)
  return [dict(time=t,last_time=t+timedelta(minutes=4),session_date=date(2026,5,8),session='after_market',high=20+i,low=10+i,close=15+i,dif=1+i,direction=[-1,0,1][i]) for i,t in enumerate([start,start+timedelta(minutes=5),datetime(2026,5,9,4,55)])]
 def test_provenance_and_value(self):
  b=self.bars();now=datetime(2026,5,10);r=turning_audit_rows(b,now)[0]
  self.assertEqual(r['轉折值'],session_results(b,now)[0][2]);self.assertEqual(r['候選K時間'],b[0]['time']);self.assertEqual(r['取價K時間'],b[2]['time']);self.assertEqual(r['日期'],date(2026,5,8))
 def test_incomplete(self):
  r=turning_audit_rows(self.bars()[:1],datetime(2026,5,10))[0];self.assertEqual(r['狀態'],'區間資料不完整');self.assertEqual(r['轉折值'],'')
 def test_left_outside(self):
  b=self.bars();b[0]['direction']=1
  prior=dict(b[0],time=datetime(2026,5,8,13,40),last_time=datetime(2026,5,8,13,44),session='position',direction=-1)
  r=turning_audit_rows([prior]+b,datetime(2026,5,10))[2]
  self.assertEqual(r['取價K時間'],b[0]['time']);self.assertEqual(r['左側K時間'],prior['time']);self.assertIn('規則2',r['規則'])
