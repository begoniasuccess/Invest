from datetime import datetime, timedelta
from unittest.mock import patch
import pandas as pd
import pytest
from dashboard import replay_service as r


def t(s): return datetime.fromisoformat(s)


def test_session_buckets_and_overnight():
    assert r.bucket(t('2026-10-28T08:49:59'), 'tx_day', 5) == t('2026-10-28T08:45:00')
    assert r.bucket(t('2026-10-28T09:44:59'), 'tx_day', 60) == t('2026-10-28T08:45:00')
    assert r.bucket(t('2026-10-29T00:02:00'), 'tx_night', 5) == t('2026-10-29T00:00:00')
    assert r.session_bounds('tx_night', t('2026-10-29T00:02:00'))[0] == t('2026-10-28T15:00:00')
    r.validate_range('tx_night', t('2026-10-28T23:55:00'), t('2026-10-29T05:00:00'))
    with pytest.raises(ValueError): r.validate_range('tx_day', t('2026-10-28T09:00:00'), t('2026-10-28T15:00:00'))


def test_history_only_complete_buckets_and_volume():
    rows=[(t('2026-10-28T08:45:00'),100,105,99,101,5),(t('2026-10-28T08:46:00'),101,103,97,98,3)]
    with patch.object(r,'_query',return_value=pd.DataFrame(rows)) as q:
        out=r.initialize('tx_day','TAIEX','202611',5,'2026-10-28T08:52:00','2026-10-28T09:15:00')
        assert q.call_args.args[1][-1]==t('2026-10-28T08:50:00')
        assert out['event_start']=='2026-10-28T08:50:00'
        assert out['history']==[dict(time=int(r.ms(rows[0][0])/1000),open=100.,high=105.,low=97.,close=98.,volume=8.)]


def test_tick_events_contract_order_and_volume():
    rows=[(t('2026-10-28T09:00:00.123456'),100,2,11),(t('2026-10-28T09:00:00.123456'),98,6,12)]
    with patch.object(r,'_query',return_value=pd.DataFrame(rows)) as q:
        out=r.events('tx_day','TAIEX','202611',5,'2026-10-28T09:00:00','2026-10-28T09:15:00')['events']
        assert q.call_args.args[1][0]=='202611'
        assert 'ORDER BY trade_datetime, source_row_no, id' in q.call_args.args[0]
        assert [x[5:] for x in out]==[[100.,1.,"11"],[98.,3.,"12"]]
        assert out[0][0]==out[1][0]
        assert out[0][0]%1000>123


def test_stock_minute_released_at_close_and_final_boundary():
    rows=[(t('2026-10-28T13:30:00'),100,105,99,102,0)]
    with patch.object(r,'_query',return_value=pd.DataFrame(rows)) as q:
        out=r.events('stock','TAIEX','',5,'2026-10-28T13:20:00','2026-10-28T13:31:00',True)['events']
        assert q.call_args.args[1][-1]==t('2026-10-28T13:30:00.000001')
        assert out[0][0]==r.ms(t('2026-10-28T13:31:00'))
        assert out[0][1]==int(r.ms(t('2026-10-28T13:30:00'))/1000)


def test_validation():
    with pytest.raises(ValueError): r.validate('tx_day','TAIEX','202611/202612',5)
    with pytest.raises(ValueError): r.validate('stock','TAIEX','',2)
    with pytest.raises(ValueError): r.parse_time('2026-10-28T09:00:00+08:00')
    with pytest.raises(ValueError): r.events('tx_day','TAIEX','202611',5,'2026-10-28T09:00:00','2026-10-28T09:16:00')
