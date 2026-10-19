import sys
import types
import unittest
from unittest.mock import patch
import pandas as pd
from dashboard import kline_service as service

class TimeFilterTests(unittest.TestCase):
    def load(self, start, end, interval='5m'):
        rows = pd.DataFrame({'時間': pd.date_range('2026-10-05 15:00', periods=40, freq='min'),
                             '開盤': range(40), '最高': range(40), '最低': range(40),
                             '收盤': range(40), '成交量': [1]*40, 'trading_session': ['after_market']*40})
        db = types.SimpleNamespace(query_to_df=lambda *args: pd.DataFrame([['2026-10-05','2026-10-05']]))
        common = types.ModuleType('common'); common.db = db
        with patch.dict(sys.modules, {'common': common}), patch.object(service, '_load_intraday_raw', return_value=rows):
            return service.load_kline_data('tx_full', interval, start, end)

    def test_minute_range_and_complete_candle(self):
        bars, meta = self.load('2026-10-05T15:20','2026-10-05T15:25')
        visible = bars[bars['日期'] >= pd.Timestamp(meta['display_start_date'])]
        self.assertEqual(list(visible['日期'].dt.strftime('%H:%M')), ['15:20','15:25'])
        self.assertEqual(visible.iloc[-1]['收盤'], 29)
        self.assertEqual(meta['display_end_date'], '2026-10-05T15:25')

    def test_legacy_date_end_includes_afternoon(self):
        bars, meta = self.load('2026-10-05','2026-10-05')
        self.assertEqual(meta['display_end_date'], '2026-10-05T23:59')
        self.assertEqual(len(bars), 8)

    def test_empty_minute_range(self):
        with self.assertRaisesRegex(ValueError, '沒有 K 線'):
            self.load('2026-10-05T16:00', '2026-10-05T16:05')

    def test_reversed_time(self):
        with self.assertRaisesRegex(ValueError, '開始日期'):
            self.load('2026-10-05T15:30','2026-10-05T15:20')

if __name__ == '__main__': unittest.main()
