from typing import Literal
from decimal import Decimal
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from dashboard import paper_trade_service as service

router=APIRouter(prefix='/api/paper-trade',tags=['Historical simulation'])

class Strict(BaseModel):
    model_config=ConfigDict(extra='forbid')

class Account(Strict):
    name:str=Field(min_length=1,max_length=80)
    initial_equity:Decimal=Field(default=1000000,gt=0,le=10**14,max_digits=18,decimal_places=2)
    fee_tx:Decimal=Field(default=50,ge=0,le=10000,decimal_places=2)
    fee_mtx:Decimal=Field(default=30,ge=0,le=10000,decimal_places=2)
    fee_tmf:Decimal=Field(default=15,ge=0,le=10000,decimal_places=2)
    slippage:int=Field(default=0,ge=0,le=1000)

class Run(Strict):
    account_id:int=Field(gt=0)
    market:Literal['tx_day','tx_night']
    contract:str=Field(pattern=r'^\d{6}$')
    start:str
    end:str

class Action(Strict):
    action:Literal['sync','order','cancel','close','close_all']
    tick_id:str=Field(pattern=r'^\d{1,19}$')
    client_at:str
    clock_ms:float=Field(ge=0,allow_inf_nan=False)
    request_id:str|None=None
    item_id:int|None=Field(default=None,gt=0)
    product:Literal['TX','MTX','TMF']='TX'
    direction:Literal['LONG','SHORT']='LONG'
    quantity:int=Field(default=1,ge=1,le=1000)
    order_type:Literal['MARKET','LIMIT','STOP']='MARKET'
    time_in_force:Literal['ROD','IOC','FOK']|None=None
    position_effect:Literal['AUTO','OPEN','CLOSE']='OPEN'
    target:Decimal|None=Field(default=None,gt=0,le=1000000,decimal_places=4)
    stop_loss:Decimal|None=Field(default=None,gt=0,le=1000000,decimal_places=4)
    take_profit:Decimal|None=Field(default=None,gt=0,le=1000000,decimal_places=4)

class Note(Strict):
    note:str=Field(max_length=10000)


def call(fn,*args):
    try: return fn(*args)
    except ValueError as e: raise HTTPException(400,str(e)) from e
    except Exception as e:
        state=getattr(e,'sqlstate',None)
        if state in ('3F000','42P01'): raise HTTPException(503,'尚未安裝模擬交易資料表，請執行 setup_paper_trade.py。') from e
        if state=='23505': raise HTTPException(409,'名稱或識別碼重複。') from e
        if state=='23514': raise HTTPException(400,'交易資料未通過資料庫檢核。') from e
        raise

@router.get('/accounts')
def get_accounts(): return call(service.accounts)
@router.post('/accounts')
def post_account(data:Account): return call(service.save_account,data.model_dump())
@router.put('/accounts/{account_id}')
def put_account(account_id:int,data:Account): return call(service.save_account,data.model_dump(),account_id)
@router.get('/runs')
def get_runs(account_id:int=Query(gt=0),market:str|None=None,contract:str|None=None,start:str|None=None,end:str|None=None):
    if market is not None and not all((contract,start,end)): raise HTTPException(400,'請提供完整回放條件')
    return call(service.run_list,account_id,market,contract,start,end)
@router.post('/runs')
def post_run(data:Run): return call(service.create_run,data.model_dump())
@router.post('/runs/{run_id}/action')
def post_action(run_id:int,data:Action):
    if data.action=='order' and not data.request_id: raise HTTPException(400,'缺少委託識別碼')
    if data.action in ('cancel','close') and data.item_id is None: raise HTTPException(400,'缺少委託／交易識別碼')
    return call(service.execute_action,run_id,data.model_dump())
@router.get('/performance')
def get_performance(account_id:int=Query(gt=0),run_id:int|None=None): return call(service.performance,account_id,run_id)
@router.patch('/trades/{trade_id}/note')
def patch_note(trade_id:int,data:Note): return call(service.note_trade,trade_id,data.note)
@router.delete('/trades/{trade_id}')
def delete_trade(trade_id:int): return call(service.delete_trade,trade_id)
