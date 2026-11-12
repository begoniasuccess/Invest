from datetime import datetime, timezone
from decimal import Decimal as D
from unittest.mock import Mock,patch
import pytest
from dashboard import paper_trade_service as p
from dashboard.paper_trade_api import Action,Note,Account
from pydantic import ValidationError


def t(seconds=0): return datetime(2026,11,6,8,45,seconds)
def tick(i, price, time=None): return dict(id=i,source_row_no=i,trade_datetime=time or t(),price=price)
def run(cursor=None):
    return dict(id=1,contract='202611',start_at=t(),end_at=t(59),cursor_id=None if cursor is None else cursor['id'],cursor_row=None if cursor is None else cursor['source_row_no'],cursor_time=None if cursor is None else cursor['trade_datetime'])


def test_costs_three_products_and_short():
    f=p.DEFAULT_FEES
    out=p.costs(38874,38974,'LONG',1,f)
    assert [out[x]['gross'] for x in p.PRODUCTS]==[D(20000),D(5000),D(1000)]
    assert [out[x]['tax'] for x in p.PRODUCTS]==[D(311),D(78),D(16)]
    assert [out[x]['net'] for x in p.PRODUCTS]==[D(19589),D(4862),D(954)]
    short=p.costs(38974,38874,'SHORT',2,f)
    assert all(short[x]['net']==2*out[x]['net'] for x in p.PRODUCTS)
    loss=p.costs(38874,38874,'LONG',1,f)
    assert all(loss[x]['net']<0 for x in p.PRODUCTS)


def test_slippage_and_trigger_direction():
    assert p.slipped(100,'LONG',2)==102 and p.slipped(100,'LONG',2,True)==98
    assert p.slipped(100,'SHORT',2)==98 and p.slipped(100,'SHORT',2,True)==102
    assert p.triggered('LIMIT','LONG',100,99) and not p.triggered('LIMIT','LONG',100,101)
    assert p.triggered('LIMIT','SHORT',100,101) and not p.triggered('LIMIT','SHORT',100,99)
    assert p.triggered('STOP','LONG',100,101) and not p.triggered('STOP','LONG',100,99)
    assert p.triggered('STOP','SHORT',100,99) and not p.triggered('STOP','SHORT',100,101)
    p.bracket_valid('LONG',100,90,110);p.bracket_valid('SHORT',100,110,90)
    with pytest.raises(ValueError): p.bracket_valid('LONG',100,101,None)
    with pytest.raises(ValueError): p.bracket_valid('SHORT',100,None,101)
    assert p.exit_trigger(dict(direction='LONG',stop_loss=95,take_profit=105),94)=='STOP_LOSS'
    assert p.exit_trigger(dict(direction='SHORT',stop_loss=105,take_profit=95),94)=='TAKE_PROFIT'


def test_cursor_rejects_rewind_same_timestamp():
    r=run(tick(2,100));c=Mock()
    with pytest.raises(ValueError,match='倒回'):p.advance(c,r,tick(1,99),datetime.now(timezone.utc))
    c.execute.assert_not_called()
    p.advance(c,r,tick(2,100),datetime.now(timezone.utc));c.execute.assert_not_called()


def test_fast_forward_pending_and_protective_every_tick():
    first=tick(1,100); second=tick(2,99); third=tick(3,94); fourth=tick(4,110)
    r=run(first); c=Mock(); wall=datetime.now(timezone.utc)
    o=dict(id=20,order_type='LIMIT',direction='LONG',target=D(99),stop_loss=D(95),take_profit=D(105))
    pos=dict(trade_id=30,direction='LONG',stop_loss=D(95),take_profit=D(105))
    with patch.object(p,'get_tick',return_value=first),patch.object(p,'many',side_effect=[[o],[],[second,third,fourth]]) as m,patch.object(p,'one',return_value=pos),patch.object(p,'fill') as fill,patch.object(p,'close_trade') as close:
        p.advance(c,r,fourth,wall)
        fill.assert_called_once_with(c,o,second,wall,True)
        close.assert_called_once_with(c,pos,third,wall,'STOP_LOSS')
        assert 'ORDER BY trade_datetime,source_row_no,id' in m.call_args.args[1]
    assert r['cursor_id']==4


def test_new_run_does_not_replay_before_start_cursor():
    r=run();c=Mock();w=datetime.now(timezone.utc)
    with patch.object(p,'many',side_effect=[[],[]]),patch.object(p,'fill') as f:
        p.advance(c,r,tick(4,110),w);f.assert_not_called()
    assert r['cursor_id']==4


def test_statistics_closed_only_and_comparison():
    common=dict(product='MTX',direction='LONG',quantity=1,entry_price=D(10000),fee_tx=D(50),fee_mtx=D(30),fee_tmf=D(15),tax_rate=p.TAX,slippage=0)
    rows=[p.enrich(dict(common,id=1,exit_time=t(1),exit_server_at=datetime(2026,11,12,10,tzinfo=timezone.utc),exit_price=D(10100))),p.enrich(dict(common,id=2,exit_time=t(2),exit_server_at=datetime(2026,11,12,11,tzinfo=timezone.utc),exit_price=D(9950))),p.enrich(dict(common,id=3,exit_time=None,exit_price=None))]
    s=p.statistics(rows,D(1000000)); tx=p.statistics(rows,D(1000000),'TX')
    assert s['count']==2 and s['wins']==1 and s['losses']==1 and s['win_rate']==50
    assert s['net']==sum(r['actual']['net'] for r in rows[:2]);assert s['max_drawdown']==-rows[1]['actual']['net']
    assert tx['net']!=s['net'] and s['equity']==D(1000000)+s['net']
    assert p.statistics([],D(1000))['count']==0


def test_api_validation_immutable_payload_and_product():
    base=dict(action='order',tick_id='9007199254740993',clock_ms=1793925900000,client_at='2026-11-12T22:00:00+08:00',request_id='12345678-1234-1234-1234-123456789abc')
    assert Action(**base).tick_id=='9007199254740993'
    for changes in ({'quantity':0},{'product':'fake'},{'tick_id':'invalid'},{'clock_ms':float('nan')},{'target':-1}):
        with pytest.raises(ValidationError): Action(**(base|changes))
    with pytest.raises(ValidationError): Note(note='test',entry_price=200)
    with pytest.raises(ValidationError): Account(name='test',fee_tx=-1)


def test_historical_and_wall_time_are_distinct():
    assert p.client_time('2026-11-12T22:00:00+08:00').utcoffset().total_seconds()==28800
    with pytest.raises(ValueError): p.client_time('2026-11-12T22:00:00')
