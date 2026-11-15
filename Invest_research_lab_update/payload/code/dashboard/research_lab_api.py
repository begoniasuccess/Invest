"""Local research dashboard API; no database migration required."""
from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.encoders import jsonable_encoder
from urllib.parse import urlparse
from dashboard import research_lab_service as s
from dashboard.research_lab_review import review_text

router=APIRouter(prefix='/api/research-lab',tags=['research-lab'])

def invoke(fn,*args,**kwargs):
    try:return fn(*args,**kwargs)
    except s.Conflict as exc:raise HTTPException(409,str(exc)) from exc
    except FileNotFoundError as exc:raise HTTPException(404,str(exc)) from exc
    except (ValueError,TypeError,KeyError) as exc:raise HTTPException(400,str(exc)) from exc
    except OSError as exc:raise HTTPException(500,'檔案讀寫失敗，請檢查專案資料夾權限') from exc

def same_origin(request):
    origin=request.headers.get('origin')
    if origin:
        parsed=urlparse(origin)
        if parsed.netloc!=request.url.netloc or parsed.scheme!=request.url.scheme:raise HTTPException(403,'只允許本網站操作研究資料')

@router.get('/catalog')
def get_catalog():return invoke(s.listing)
@router.get('/experiments/{eid}')
def get_experiment(eid:str):return invoke(lambda:s.meta(s.experiment(eid)))
@router.put('/experiments/{eid}')
def update_experiment(request:Request,eid:str,data:dict=Body(...)):
    same_origin(request);return invoke(s.save_meta,eid,data)
@router.get('/experiments/{eid}/document')
def get_document(eid:str):return invoke(s.read_doc,eid)
@router.put('/experiments/{eid}/document')
def put_document(request:Request,eid:str,data:dict=Body(...)):
    same_origin(request);return invoke(s.save_doc,eid,data.get('body'),data.get('version'))
@router.post('/preview')
def preview(request:Request,data:dict=Body(...)):
    same_origin(request);body=data.get('body')
    if not isinstance(body,str) or len(body)>s.MAX_DOC:raise HTTPException(400,'Markdown 長度不合法')
    return {'html':s.render_markdown(body)}
@router.get('/experiments/{eid}/runs')
def runs(eid:str):return invoke(s.list_runs,eid)
@router.get('/experiments/{eid}/params')
def params(eid:str):return invoke(s.parameter_spec,eid)
@router.post('/experiments/{eid}/validate')
def validate(request:Request,eid:str,data:dict=Body(...)):
    same_origin(request);_,p,cfg,start,end,count=invoke(s.build_request,eid,data.get('parameters'),data.get('config_id'))
    return {'parameters':p,'config_id':cfg,'start':start,'end':end,'parameter_count':count,'database_write':False}
@router.post('/experiments/{eid}/execute')
def execute(request:Request,eid:str,data:dict=Body(...)):
    same_origin(request);return invoke(s.start_run,eid,data.get('parameters'),data.get('config_id'),data.get('note',''))
@router.get('/active')
def active():
    a=s.active_job()
    if not a:return {'active':None}
    return {'active':{k:v for k,v in a.items() if k!='token'}}
@router.post('/recover')
def recover(request:Request):same_origin(request);return invoke(s.recover_stale)
@router.get('/experiments/{eid}/runs/{rid}')
def run(eid:str,rid:str):return invoke(s.get_run,eid,rid)
@router.get('/experiments/{eid}/runs/{rid}/database-metadata')
def database_metadata(eid:str,rid:str):
    try:return invoke(s.database_metadata,eid,rid)
    except HTTPException:raise
    except Exception as exc:raise HTTPException(503,'資料庫查詢失敗或歷史表不存在；檔案結果仍可查看。') from exc
@router.post('/experiments/{eid}/runs/{rid}/cancel')
def cancel(request:Request,eid:str,rid:str):same_origin(request);return invoke(s.cancel_run,eid,rid)
@router.get('/experiments/{eid}/runs/{rid}/log')
def log(eid:str,rid:str):return {'text':invoke(s.log_tail,eid,rid)}
@router.get('/experiments/{eid}/runs/{rid}/review')
def get_review(eid:str,rid:str):return invoke(s.read_doc,eid,rid)
@router.put('/experiments/{eid}/runs/{rid}/review')
def put_review(request:Request,eid:str,rid:str,data:dict=Body(...)):
    same_origin(request);return invoke(s.save_doc,eid,data.get('body'),data.get('version'),rid)
@router.post('/experiments/{eid}/runs/{rid}/review-draft')
def draft(request:Request,eid:str,rid:str):
    same_origin(request);e=invoke(s.experiment,eid);g=invoke(s.get_run,eid,rid)
    if g['kind']=='new' and g['status'] not in ('completed','failed','cancelled','interrupted'):raise HTTPException(409,'回測仍在執行，請等結果完整後再產生摘要')
    return {'body':review_text(e,g)}
@router.get('/experiments/{eid}/runs/{rid}/files/{fid}/csv')
def csv_file(eid:str,rid:str,fid:str,offset:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=240),query:str=Query('',max_length=200),sort:str=Query('',max_length=5),descending:bool=False):
    f,p=invoke(s.get_file,eid,rid,fid)
    if f['type']!='csv':raise HTTPException(400,'不是 CSV 檔案')
    return invoke(s.csv_page,p,offset,limit,query,sort,descending)
@router.get('/experiments/{eid}/runs/{rid}/files/{fid}/json')
def json_file(eid:str,rid:str,fid:str):
    f,p=invoke(s.get_file,eid,rid,fid)
    if f['type']!='json':raise HTTPException(400,'不是 JSON 檔案')
    return invoke(s.read_json_artifact,f)
@router.get('/experiments/{eid}/runs/{rid}/files/{fid}/download')
def download(eid:str,rid:str,fid:str):
    f,p=invoke(s.get_file,eid,rid,fid);return FileResponse(p,filename=f['name'],media_type='application/octet-stream',headers={'X-Content-Type-Options':'nosniff'})
@router.get('/experiments/{eid}/runs/{rid}/files/{fid}/view')
def view(eid:str,rid:str,fid:str):
    f,p=invoke(s.get_file,eid,rid,fid)
    if f['type']=='html':
        # The existing chart gallery is self-contained. Never run arbitrary output HTML in dashboard origin.
        return FileResponse(p,media_type='text/html',headers={'Content-Security-Policy':"sandbox allow-scripts; default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; connect-src 'none'; form-action 'none'; base-uri 'none'",'X-Content-Type-Options':'nosniff'})
    if f['type'] in ('png','jpg','jpeg'):return FileResponse(p,headers={'X-Content-Type-Options':'nosniff'})
    raise HTTPException(400,'此格式請使用下載')
