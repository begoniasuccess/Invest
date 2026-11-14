from datetime import datetime, timezone
from decimal import Decimal as D
from unittest.mock import Mock,patch
import pytest
from dashboard import paper_trade_service as p
from dashboard.paper_trade_api import Action,Account
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
    with pytest.raises(ValidationError): Action(**(base|{'entry_price':200}))
    with pytest.raises(ValidationError): Account(name='test',fee_tx=-1)


def test_historical_and_wall_time_are_distinct():
    assert p.client_time('2026-11-12T22:00:00+08:00').utcoffset().total_seconds()==28800
    with pytest.raises(ValueError): p.client_time('2026-11-12T22:00:00')


def test_position_effect_allocation_fifo_and_reversal():
    first={'quantity':2,'trade_id':1}; second={'quantity':3,'trade_id':2}
    assert p.allocation_plan([first,second],1,'OPEN')==([],1)
    assert p.allocation_plan([first,second],3,'CLOSE')==([(first,2),(second,1)],0)
    assert p.allocation_plan([first,second],6,'AUTO')==([(first,2),(second,3)],1)
    with pytest.raises(ValueError,match='不足'):p.allocation_plan([first],3,'CLOSE')


def test_partial_close_retains_open_lot_and_copies_entry():
    c=Mock();w=datetime.now(timezone.utc)
    trade=dict(trade_id=10,quantity=3,direction='LONG',slippage=1)
    with patch.object(p,'one',return_value={'id':11}) as insert:
        p.close_trade(c,trade,tick(2,110),w,'MANUAL',quantity=1,price=D(110),exit_order_id=20)
    assert 'entry_price' in insert.call_args.args[1]
    assert 'note' not in insert.call_args.args[1]
    assert insert.call_args.args[2]==(1,10)
    assert c.execute.call_args_list[0].args[1]==(2,10)
    assert c.execute.call_args_list[1].args[1][-2:]==(20,11)


def test_close_order_never_opens_and_auto_reverses():
    c=Mock();w=datetime.now(timezone.utc); t1=tick(2,110)
    o=dict(id=20,run_id=1,product='TX',direction='SHORT',quantity=3,slippage=2,position_effect='AUTO')
    pos=dict(trade_id=10,quantity=2,direction='LONG',slippage=0)
    with patch.object(p,'opposite_positions',return_value=[pos]),patch.object(p,'close_trade') as close:
        p.fill(c,o,t1,w)
        assert close.call_args.kwargs==dict(quantity=2,price=D(108),exit_order_id=20)
        assert c.execute.call_args_list[0].args[1][-1]==1
    c.reset_mock()
    with patch.object(p,'opposite_positions',return_value=[pos]),patch.object(p,'close_trade') as close:
        p.fill(c,{**o,'position_effect':'CLOSE'},t1,w)
        close.assert_not_called()
        assert "status='CANCELLED'" in c.execute.call_args.args[0]


def test_close_limit_uses_no_adverse_slippage():
    c=Mock();w=datetime.now(timezone.utc); t1=tick(2,110)
    o=dict(id=20,run_id=1,product='TX',direction='SHORT',quantity=1,slippage=2,position_effect='CLOSE')
    pos=dict(trade_id=10,quantity=2,direction='LONG',slippage=0)
    with patch.object(p,'opposite_positions',return_value=[pos]),patch.object(p,'close_trade') as close:
        p.fill(c,o,t1,w,limit=True)
        assert close.call_args.kwargs['price']==D(110)
        assert c.execute.call_count==1

@pytest.mark.parametrize('kind,side,target,price,tif,expected',[
    ('LIMIT','LONG',100,101,'ROD','WAIT'),
    ('LIMIT','LONG',100,101,'IOC','CANCEL'),
    ('LIMIT','LONG',100,101,'FOK','CANCEL'),
    ('LIMIT','LONG',100,99,'IOC','FILL'),
    ('LIMIT','SHORT',100,101,'FOK','FILL'),
    ('LIMIT','SHORT',100,99,'IOC','CANCEL'),
    ('MARKET','LONG',None,100,'IOC','FILL'),
    ('STOP','LONG',105,100,'IOC','WAIT'),
    ('STOP','SHORT',95,100,'FOK','WAIT'),
    ('STOP','LONG',105,106,'FOK','FILL'),
])
def test_initial_tif_decision(kind,side,target,price,tif,expected):
    assert p.initial_order_decision(kind,side,target,price,tif)==expected


def test_tif_compatibility():
    assert p.normalize_tif('LIMIT')=='ROD'
    assert p.normalize_tif('MARKET')=='IOC'
    assert p.normalize_tif('STOP')=='IOC'
    for kind in ('MARKET','STOP'):
        with pytest.raises(ValueError): p.normalize_tif(kind,'ROD')
    with pytest.raises(ValueError): p.normalize_tif('LIMIT','ROC')


def test_run_end_cancels_only_pending():
    c=Mock(); r=run(); wall=datetime.now(timezone.utc)
    p.expire_run_orders(c,r,t(58),wall); c.execute.assert_not_called()
    p.expire_run_orders(c,r,t(59),wall)
    sql,args=c.execute.call_args.args
    assert "status='PENDING'" in sql and 'run_id=%s' in sql
    assert args==(r['end_at'],wall,'回放時段結束',r['id'])
