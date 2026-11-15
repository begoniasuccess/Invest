from datetime import date,datetime
from typing import Literal
from fastapi import APIRouter,Query,HTTPException
from pydantic import BaseModel,Field,model_validator,ConfigDict
from dashboard import journal_service as s
from dashboard.paper_trade_api import call
router=APIRouter(prefix='/api/paper-trade/journals',tags=['Trading journal'])
class Strict(BaseModel):
    model_config=ConfigDict(extra='forbid')
class Link(Strict):
    kind:Literal['account','run','trade','date','order']
    target_id:int|None=Field(default=None,gt=0)
    historical_date:date|None=None
    @model_validator(mode='after')
    def validate_target(self):
        if self.kind=='date':
            if self.historical_date is None or self.target_id is not None: raise ValueError('日期連結需要日期，不可指定 ID')
        elif self.target_id is None or self.historical_date is not None: raise ValueError('紀錄連結需要 ID，不可指定日期')
        return self
class Journal(Strict):
    title:str=Field(min_length=1,max_length=200)
    body:str=Field(default='',max_length=100000)
    journal_date:date
    tags:list[str]=Field(default_factory=list,max_length=30)
    links:list[Link]=Field(default_factory=list,max_length=500)
    expected_updated_at:datetime|None=None
class Preview(Strict):
    body:str=Field(max_length=100000)
def invoke(fn,*args):
    try: return call(fn,*args)
    except Exception as e:
        if getattr(e,'sqlstate',None)=='23503': raise HTTPException(400,'連結對象已不存在，請重新選擇') from e
        raise
@router.get('')
def list_journals(account_id:int|None=Query(default=None,gt=0),run_id:int|None=Query(default=None,gt=0),trade_id:int|None=Query(default=None,gt=0),history_date:date|None=None,start:date|None=None,end:date|None=None,keyword:str|None=Query(default=None,max_length=200),limit:int=Query(default=50,ge=1,le=100),offset:int=Query(default=0,ge=0),link_kind:Literal['account','run','trade','date','order']|None=None,tag:str|None=Query(default=None,max_length=50),sort:Literal['asc','desc']='desc'):
    return invoke(s.search,account_id,run_id,trade_id,history_date,start,end,keyword,limit,offset,link_kind,tag,sort)
@router.get('/tags')
def tags(): return {'items':invoke(s.list_tags)}
@router.post('/preview')
def preview(data:Preview):
    return {'html':s.render_markdown(data.body)}
@router.post('')
def create(data:Journal): return invoke(s.save,data.model_dump())
@router.get('/{journal_id}')
def get(journal_id:int): return invoke(s.get,journal_id)
@router.put('/{journal_id}')
def update(journal_id:int,data:Journal): return invoke(s.save,data.model_dump(),journal_id)
@router.delete('/{journal_id}')
def delete(journal_id:int,expected_updated_at:datetime): return invoke(s.delete,journal_id,expected_updated_at)
