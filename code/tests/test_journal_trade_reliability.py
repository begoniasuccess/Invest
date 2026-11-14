from datetime import date, datetime, timezone
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
import pytest
from dashboard import paper_trade_service as p, journal_service as j, journal_media as m
from dashboard.journal_api import Journal, Link
from dashboard.paper_trade_api import Action

NOW=datetime(2026,11,14,tzinfo=timezone.utc)
TICK={'id':1,'source_row_no':1,'trade_datetime':datetime(2026,11,6,9),'price':100}

@pytest.mark.parametrize('side,condition,price,expected',[
    ('LONG','ABOVE',101,True),('LONG','BELOW',99,True),('SHORT','ABOVE',101,True),('SHORT','BELOW',99,True),
    ('LONG','ABOVE',99,False),('LONG','BELOW',101,False),('SHORT','ABOVE',99,False),('SHORT','BELOW',101,False),
])
def test_independent_trigger(side,condition,price,expected):
    assert p.triggered('STOP',side,100,price,condition)==expected
    assert p.initial_order_decision('STOP',side,100,price,'IOC',condition)==('FILL' if expected else 'WAIT')

@pytest.mark.parametrize('side,price,stop,take,reason',[
    ('LONG',90,95,120,'STOP_LOSS'),('SHORT',110,105,80,'STOP_LOSS'),
    ('LONG',125,95,120,'TAKE_PROFIT'),('SHORT',75,105,80,'TAKE_PROFIT'),
    ('LONG',100,95,120,None),('SHORT',100,105,80,None),
])
def test_fill_rechecks_protection_same_tick(side,price,stop,take,reason):
    cur=Mock(); tick={**TICK,'price':price}
    order={'id':1,'run_id':1,'product':'TX','direction':side,'quantity':1,'position_effect':'OPEN','slippage':2,'stop_loss':D(stop),'take_profit':D(take)}
    pos={'trade_id':5,'quantity':1,'direction':side,'slippage':2,'stop_loss':D(stop),'take_profit':D(take)}
    with patch.object(p,'one',return_value=pos),patch.object(p,'close_trade') as close:
        p.fill(cur,order,tick,NOW,limit=True)
        if reason: close.assert_called_once_with(cur,pos,tick,NOW,reason)
        else: close.assert_not_called()
    assert 'FILLED' in cur.execute.call_args.args[0]


def test_group_link_from_open_trade_and_explicit_group_is_deduplicated():
    data=Journal(title='分批出場',body='心得',journal_date=date(2026,11,6),links=[Link(kind='trade',target_id=7),Link(kind='order',target_id=70)]).model_dump()
    cur=MagicMock(); conn=MagicMock();
    with patch.object(p,'connection') as c,patch.object(p,'cursor') as cc,patch.object(p,'one',side_effect=[{'id':1},{'order_id':70,'exit_time':None},{'id':70},{'id':70},{'id':1}]),patch.object(p,'many',return_value=[]):
        c.return_value.__enter__.return_value=conn;cc.return_value.__enter__.return_value=cur
        j.save(data)
    inserts=[call.args for call in cur.execute.call_args_list if 'INSERT INTO paper_trade.journal_links' in call.args[0]]
    assert len(inserts)==1 and 'entry_order_id' in inserts[0][0] and inserts[0][1]==(1,70)
    conn.commit.assert_called_once()


def test_trade_reverse_lookup_includes_stable_opening_order():
    with patch.object(p,'connection',return_value=MagicMock()),patch.object(p,'cursor',return_value=MagicMock()),patch.object(p,'many',return_value=[]) as m0:
        j.search(trade_id=20,link_kind='trade')
        sql,args=m0.call_args.args[1:]
        assert "l.kind='order'" in sql and 'SELECT order_id FROM paper_trade.trades WHERE id=%s' in sql
        assert args==(20,20,51,0)


def test_delete_rejects_stale_version():
    cur=MagicMock()
    with patch.object(p,'connection',return_value=MagicMock()),patch.object(p,'cursor') as cc,patch.object(p,'one',return_value={'updated_at':NOW}):
        cc.return_value.__enter__.return_value=cur
        with pytest.raises(ValueError,match='其他視窗'):j.delete(1,datetime(2026,11,13,tzinfo=timezone.utc))
    cur.execute.assert_not_called()


def test_media_markdown_reference_detection():
    body='![圖](/api/journal-media/files/%E5%9C%96.png)\n\n[影片][movie]\n\n[movie]: /api/journal-media/files/test.mp4\n\n`![x](/api/journal-media/files/ignored.png)`'
    assert m.media_names(body)=={'圖.png','test.mp4'}

