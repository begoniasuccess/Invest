"""Research catalogue, read-only artifact browsing, and isolated local execution."""
from __future__ import annotations
import csv, hashlib, json, math, os, re, subprocess, sys, tempfile, threading, uuid
from datetime import date, datetime, timezone
from pathlib import Path
from itertools import islice

CODE = Path(__file__).resolve().parents[1]
PROJECT = CODE.parent
LAB = PROJECT / 'research_lab'
OUTPUT = PROJECT / 'output'
RUNS = OUTPUT / 'research_runs'
ALLOWED = {'.csv', '.json', '.html', '.txt', '.md', '.png', '.jpg', '.jpeg', '.svg', '.pdf'}
DATEPAIR = re.compile(r'(?<!\d)(\d{8})_(\d{8}|latest)(?!\d)')
ID_RE = re.compile(r'^(?:hist-[a-f0-9]{20}|run-[a-f0-9]{32})$')
LOCK = threading.RLock()
MAX_DOC = 500_000

class Conflict(ValueError): pass

def now(): return datetime.now(timezone.utc).isoformat()
def digest(data): return hashlib.sha256(data).hexdigest()
def json_read(path): return json.loads(path.read_text(encoding='utf-8-sig'),parse_constant=lambda _:None)
def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.research-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f: f.write(content)
        os.replace(name, path)
    finally:
        if os.path.exists(name): os.unlink(name)
def json_write(path, data): atomic_text(path, json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)+'\n')
def inside(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()): raise ValueError('檔案路徑超出允許範圍')
    return path

def catalog():
    data = json_read(LAB/'catalog.json')
    return {e['id']:e for e in data['experiments']}
def experiment(eid):
    e=catalog().get(eid)
    if e is None: raise FileNotFoundError('找不到實驗')
    return e

def meta_path(eid): return inside(LAB, f'experiments/{eid}/metadata.json')
def meta(e):
    p=meta_path(e['id']); saved=json_read(p) if p.is_file() else {}
    return {**e, **{k:v for k,v in saved.items() if k in ('title','category','status','favorite','tags')}}
def source_ok(e):
    p=inside(PROJECT,e['source_path'])
    return p.is_file() and digest(p.read_bytes())==e['source_sha256']

def save_meta(eid,data):
    e=experiment(eid); old=meta(e); out={k:old[k] for k in ('title','category','status','favorite','tags') if k in old}
    for k in ('title','category'):
        if k in data:
            v=str(data[k]).strip()
            if not v or len(v)>120: raise ValueError('標題／分類需為 1～120 字')
            out[k]=v
    if 'status' in data:
        if data['status'] not in ('unreviewed','researching','candidate','rejected','superseded','archived','completed'):raise ValueError('無效研究狀態')
        out['status']=data['status']
    if 'favorite' in data:
        if not isinstance(data['favorite'],bool):raise ValueError('無效收藏值')
        out['favorite']=data['favorite']
    if 'tags' in data:
        if not isinstance(data['tags'],list) or len(data['tags'])>20 or any(not isinstance(t,str) or len(t)>40 for t in data['tags']):raise ValueError('標籤格式錯誤')
        out['tags']=data['tags']
    with LOCK: json_write(meta_path(eid),out)
    return meta(e)

def date_text(s):
    try:return date(int(s[:4]),int(s[4:6]),int(s[6:8])).isoformat()
    except (ValueError,TypeError):return None

def period_for(name):
    m=DATEPAIR.search(name)
    return (m.group(0),date_text(m.group(1)),date_text(m.group(2)) if m.group(2)!='latest' else None) if m else ('undated',None,None)

def artifact(path):
    rel=path.relative_to(OUTPUT.resolve()).as_posix()
    return {'id':digest(rel.encode())[:24],'path':rel,'name':path.name,'size':path.stat().st_size,'modified_at':datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat(),'type':path.suffix.lower().lstrip('.')}

def history_groups():
    # Explicit mappings avoid substring collisions (e.g. foreign_flow vs streak, v1 vs v2).
    out={eid:[] for eid in catalog()}
    for eid,e in catalog().items():
        h=e['history']; base=inside(OUTPUT,h['root']+('/'+h['directory'] if h['directory'] else ''))
        if not base.is_dir():continue
        paths=base.rglob('*') if h['directory'] else base.iterdir()
        groups={}
        for p in paths:
            if p.is_symlink() or not p.is_file() or p.suffix.lower() not in ALLOWED or not p.name.startswith(h['prefix']):continue
            p=p.resolve()
            if not p.is_relative_to(base) or not p.is_relative_to(OUTPUT.resolve()):continue
            gallery=eid.endswith('_chart_gallery')
            if gallery and p.suffix.lower()!='.html':continue
            if not gallery and p.name=='retail_bounce_reentry_5m_gallery.html':continue
            key,start,end=period_for(p.name)
            # Older 2-1 files with the same date range are a separate format.
            variant=('directional' if '_directional_' in p.name else 'legacy') if eid=='futures_stop_trailing' else ''
            parent=p.parent.relative_to(OUTPUT.resolve()).as_posix()
            identity=f'{eid}|{parent}|{key}|{variant}'; rid='hist-'+digest(identity.encode())[:20]
            g=groups.setdefault(rid,{'id':rid,'experiment_id':eid,'kind':'historical','status':'historical','period_label':key,'start':start,'end':end,'folder':parent,'variant':variant,'files':[]})
            g['files'].append(artifact(p))
        for g in groups.values():
            g['files'].sort(key=lambda x:('manifest' not in x['name'], not any(t in x['name'] for t in ('summary','ranking','classifier','metrics','selection','overall')),x['name']))
            g['modified_at']=max(f['modified_at'] for f in g['files'])
        out[eid]=sorted(groups.values(),key=lambda g:(g['end'] or '',g['period_label'],g['folder'],g['variant']),reverse=True)
    return out

def run_dir(eid,rid):
    experiment(eid)
    if not re.fullmatch(r'run-[a-f0-9]{32}',rid):raise ValueError('無效執行編號')
    return inside(RUNS, f'{eid}/{rid}')
def new_runs(eid):
    base=inside(RUNS,eid); result=[]
    if base.is_dir():
        for p in base.glob('run-*/manifest.json'):
            if not p.resolve().is_relative_to(base) or not ID_RE.fullmatch(p.parent.name):continue
            try:
                m=json_read(p)
                if m.get('id')!=p.parent.name or m.get('experiment_id')!=eid:continue
                result.append({k:m.get(k) for k in ('id','experiment_id','kind','status','start','end','started_at','finished_at','created_at','note','error','progress','source_sha256')})
            except (OSError,ValueError):continue
    return sorted(result,key=lambda r:r.get('created_at') or '',reverse=True)
def list_runs(eid):
    experiment(eid)
    historical=[]
    for g in history_groups()[eid]:
        detail=historical_metadata(g)
        historical.append({k:detail.get(k) for k in ('id','experiment_id','kind','status','start','end','period_label','folder','variant','execution_time','time_source','modified_at','parameter_source','actual_data_end') }|{'file_count':len(g['files'])})
    return new_runs(eid)+historical
def listing():
    hs=history_groups(); result=[]
    for eid,e in catalog().items():
        ns=new_runs(eid); h=hs[eid]; x=meta(e)
        result.append({**{k:x.get(k) for k in ('id','title','category','kind','status','favorite','tags','source_path','related_experiment')},'history_count':len(h),'run_count':len(ns),'runner_enabled':bool(e['runner_enabled'] and source_ok(e)),'latest_new_run':ns[0] if ns else None,'latest_file_mtime':max((r['modified_at'] for r in h),default=None)})
    return result

def file_path(f):
    p=inside(OUTPUT,f['path'])
    if not p.is_file() or p.suffix.lower() not in ALLOWED:raise FileNotFoundError('結果檔案已不存在')
    return p

def read_json_artifact(f):
    p=file_path(f)
    if p.stat().st_size>8_000_000:raise ValueError('JSON 過大，請下載原檔')
    return json_read(p)
def open_csv(p):
    # Existing experiments export UTF-8 with or without BOM; fall back to Big5 only when needed.
    with p.open('rb') as test: raw=test.read(65536)
    try:raw.decode('utf-8-sig'); enc='utf-8-sig'
    except UnicodeDecodeError as ex:
        # A partial sample can end in the middle of a UTF-8 character.
        if ex.end==len(raw) and ex.reason=='unexpected end of data':enc='utf-8-sig'
        else:enc='cp950'
    return p.open(encoding=enc,newline='',errors='replace')

def historical_metadata(g):
    g=dict(g); g['execution_time']=None;g['time_source']='未記錄（下方檔案修改時間僅供參考）';g['parameters']=None;g['parameter_source']='未記錄';g['manifest_files']=[];g['manifest_run_id']=None
    for f in g['files']:
        if f['type']!='json' or 'manifest' not in f['name']:continue
        try:m=read_json_artifact(f)
        except (ValueError,OSError):continue
        if not isinstance(m,dict):continue
        g['manifest_files'].append(f['name']);g['manifest_run_id']=m.get('run_id')
        for k in ('generated_at','created_at','started_at','finished_at'):
            if m.get(k):g['execution_time']=m[k];g['time_source']=f'{f["name"]}：{k}';break
        cfg=m.get('settings') or m.get('config')
        if not isinstance(cfg,dict):
            cfg={k:m[k] for k in ('start','end','start_date','end_date','signal_column','futures_id','first_test_year','split_date','bucket_edges_pct','metric_version','strategy_version','features','targets','method','bucket_edges_pct','analysis_name') if k in m}
            if cfg:g['parameter_source']='manifest 部分欄位（不代表完整設定）'
        else:g['parameter_source']='manifest 設定快照（以檔案實際記載為限）'
        if cfg:g['parameters']=cfg
        if cfg:
            g['start']=cfg.get('start',cfg.get('start_date')) or g['start'];g['end']=cfg.get('end',cfg.get('end_date')) or g['end']
        g['actual_data_end']=m.get('actual_data_end')
    if g['parameters'] is None:
        for f in g['files']:
            if f['type']!='csv' or 'summary' not in f['name']:continue
            with open_csv(file_path(f)) as stream:
                reader=csv.DictReader(stream)
                if 'parameters' not in (reader.fieldnames or []):continue
                params=[]; total=0
                for row in reader:
                    total+=1
                    if len(params)>=200:continue
                    try:val=json.loads(row['parameters'])
                    except (ValueError,TypeError):continue
                    params.append({'parameter_id':row.get('parameter_id'),'direction':row.get('direction'),'parameters':val})
                if params:
                    g['parameters']={'summary_parameter_rows':params,'total_rows':total};g['parameter_source']='CSV parameters 欄（最多顯示 200 組；原始 CSV 保留完整）';break
    if g['parameters'] is None and g['experiment_id']=='futures_daily_amplitude_pattern':
        keys=('futures_id','rule_version','range_days','low_range_threshold','expanded_range_threshold','violent_range_threshold','extreme_range_threshold','biased_share_threshold','strong_share_threshold')
        f=next((x for x in g['files'] if x['type']=='csv'),None)
        if f:
            with open_csv(file_path(f)) as stream:
                rows=csv.DictReader(stream); vals={k:set() for k in keys if k in (rows.fieldnames or [])}
                for row in rows:
                    for k,v in vals.items():v.add(row.get(k,''))
            if vals:g['parameters']={k:sorted(v) for k,v in vals.items()};g['parameter_source']='CSV 逐列記錄的規則欄位（完整設定未保存）'
    g['date_source']='manifest／檔名期間（不是實際執行日期）' if g['manifest_files'] else '檔名期間（不是實際執行日期）'
    return g

def history_snapshot(g):
    p=LAB/'history_snapshot.json'
    if not p.is_file():return '未建立歷史輸出比對清單'
    entry=json_read(p).get(g['id'])
    if entry is None:return '新發現的歷史批次（尚未建立比對快照）'
    original={f['path']:f for f in entry};current={f['path']:f for f in g['files']}
    if set(original)!=set(current):return '檔案清單已變動；既有回顧可能不再對應'
    for path,f in current.items():
        old=original[path]
        if f['size']!=old['size']:return '檔案大小已變動；既有回顧可能不再對應'
        if old.get('sha256') and digest(file_path(f).read_bytes())!=old['sha256']:return '摘要／manifest 內容已變動；請重新核對回顧'
    return '檔案清單、大小及已記錄的摘要／manifest 雜湊相符（非全檔驗證）'

def get_run(eid,rid):
    if not ID_RE.fullmatch(rid):raise ValueError('無效批次編號')
    if rid.startswith('hist-'):
        experiment(eid)
        g=next((x for x in history_groups()[eid] if x['id']==rid),None)
        if g is None:raise FileNotFoundError('找不到歷史結果')
        result=historical_metadata(g);result['snapshot_status']=history_snapshot(g);return result
    d=run_dir(eid,rid); p=d/'manifest.json'
    if not p.is_file():raise FileNotFoundError('找不到執行批次')
    m=json_read(p);m['files']=[]
    for f in sorted((d/'artifacts').rglob('*')):
        if f.is_file() and not f.is_symlink() and f.suffix.lower() in ALLOWED and f.resolve().is_relative_to(d.resolve()):m['files'].append(artifact(f.resolve()))
    return m

def get_file(eid,rid,file_id):
    if not ID_RE.fullmatch(rid):raise ValueError('無效批次編號')
    if rid.startswith('hist-'):
        experiment(eid); group=next((x for x in history_groups()[eid] if x['id']==rid),None)
        if group is None:raise FileNotFoundError('找不到歷史結果')
        files=group['files']
    else:files=get_run(eid,rid)['files']
    f=next((x for x in files if x['id']==file_id),None)
    if f is None:raise FileNotFoundError('找不到結果檔案')
    return f,file_path(f)

def csv_page(p,offset=0,limit=100,query='',sort='',descending=False):
    if offset<0 or not 1<=limit<=240:raise ValueError('無效分頁範圍')
    size=p.stat().st_size
    with open_csv(p) as stream:
        reader=csv.reader(stream); columns=next(reader,[]); query=query.casefold().strip(); count=0; page=[]; more=False; total_known=True
        if sort:
            if not sort.isdigit() or int(sort)>=len(columns):raise ValueError('排序欄位不存在')
            if size>40*1024*1024:raise ValueError('超過 40 MiB 的 CSV 請使用分頁／搜尋或下載；暫不支援全檔排序')
            idx=int(sort)
            def key(row):
                v=row[idx] if idx<len(row) else ''
                try:
                    n=float(v)
                    if math.isfinite(n):return (0,n)
                except ValueError:pass
                return (1,v.casefold())
            rows=[r for r in reader if not query or any(query in x.casefold() for x in r)]
            rows.sort(key=key,reverse=descending);count=len(rows);page=rows[offset:offset+limit]
        else:
            for row in reader:
                if query and not any(query in x.casefold() for x in row):continue
                if offset<=count<offset+limit:page.append(row)
                count+=1
                if size>40*1024*1024 and count>offset+limit:
                    total_known=False;more=True;break
    return {'columns':columns,'rows':page,'total':count if total_known else None,'has_more':more or count>offset+limit,'offset':offset,'limit':limit}

def doc_path(eid,rid=None):
    e=experiment(eid)
    if rid is None:return inside(LAB,e['description_path'])
    get_run(eid,rid)
    return inside(LAB,f'reviews/{eid}/{rid}.md')
def render_markdown(body):
    from markdown_it import MarkdownIt
    return MarkdownIt('commonmark',{'html':False,'breaks':True}).enable(['table','strikethrough']).render(body)

def read_doc(eid,rid=None):
    p=doc_path(eid,rid)
    s=p.read_text(encoding='utf-8') if p.is_file() else ''
    return {'body':s,'version':digest(s.encode()),'html':render_markdown(s)}
def save_doc(eid,body,version,rid=None):
    if not isinstance(body,str) or len(body)>MAX_DOC:raise ValueError('Markdown 上限 50 萬字元')
    p=doc_path(eid,rid)
    with LOCK:
        old=p.read_text(encoding='utf-8') if p.is_file() else ''
        if version!=digest(old.encode()):raise Conflict('文件已被其他視窗修改，請重新載入後儲存')
        if p.is_file():
            backup=inside(LAB,f'revisions/{eid}/{rid or "description"}/{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")}-{digest(old.encode())[:8]}.md');atomic_text(backup,old)
        atomic_text(p,body)
    return read_doc(eid,rid)

# ----- Registered runner -----
LABELS={'start':'回測起日','end':'回測迄日','split_date':'探索／驗證切分日','futures_id':'期貨商品','cost_points':'來回成本（點）','shadow_threshold':'影線門檻（點）','alpha':'顯著水準','history_window':'歷史觀察窗','minimum_history':'最少歷史樣本','market':'市場','core_column':'核心訊號欄','foreign_days':'外資累計日數','first_test_year':'第一個測試年度'}
def parameter_spec(eid):
    e=experiment(eid);r=e['runner']; out={'mode':r['mode'],'enabled':bool(e['runner_enabled'] and source_ok(e)),'source_path':e['source_path'],'source_sha256':e['source_sha256']}
    if r['mode']=='config':
        configs=[]
        for rel in e['configs']:
            p=inside(PROJECT,rel);cfg=json_read(p);cfg.pop('output_dir',None);cfg.pop('write_database',None);cfg.pop('export_csv',None)
            configs.append({'id':rel,'label':p.name,'parameters':cfg})
        out.update(configs=configs,default_config=r['default_config'])
    else:out['fields']=[{**f,'label':LABELS.get(f['key'],f['key'].replace('_',' '))} for f in r['fields']]
    return out

def scalar(value,kind):
    if kind=='date':
        if not isinstance(value,str):raise ValueError('日期格式錯誤')
        return date.fromisoformat(value).isoformat()
    if kind=='int':
        if isinstance(value,bool):raise ValueError('需要整數')
        n=int(value)
        if float(value)!=n or abs(n)>10_000_000:raise ValueError('無效整數')
        return n
    if kind=='float':
        if isinstance(value,bool):raise ValueError('需要數字')
        n=float(value)
        if not math.isfinite(n) or abs(n)>1e12:raise ValueError('無效數值')
        return n
    if not isinstance(value,str) or not value.strip() or len(value)>250 or any(c in value for c in ('\n','\r','\x00')) or value.startswith('-'):raise ValueError('無效文字參數')
    return value

def validate_dates(p):
    start=p.get('start') or p.get('start_date');end=p.get('end') or p.get('end_date')
    if start:date.fromisoformat(start)
    if end:date.fromisoformat(end)
    if start and end and start>end:raise ValueError('起日不能晚於迄日')
    return start,end

def validate_tree(value,base,path=''):
    # Validate the existing config structure; arrays may change length, values must be typed.
    if isinstance(base,dict):
        if not isinstance(value,dict):raise ValueError(f'{path} 必須是物件')
        if set(value)!=set(base):raise ValueError(f'{path or "設定"} 欄位不符；請使用現有設定鍵值')
        return {k:validate_tree(value[k],v,path+'.'+k) for k,v in base.items()}
    if isinstance(base,list):
        if not isinstance(value,list) or len(value)>100 or (base and not value):raise ValueError(f'{path} 陣列長度不合法（最多100項）')
        if not base:return value
        # Some lists have nullable max bounds; accept a null only when at least one template allows it.
        result=[]
        for x in value:
            if isinstance(base[0],dict):
                candidate=next((v for v in base if all((v.get(k) is None)==(x.get(k) is None) for k in v) if isinstance(x,dict)),base[0]) if isinstance(x,dict) else base[0]
                result.append(validate_tree(x,candidate,path+'[]'))
            else:
                template=0.0 if all(isinstance(v,(int,float)) and not isinstance(v,bool) for v in base) and any(isinstance(v,float) for v in base) else base[0]
                result.append(validate_tree(x,template,path+'[]'))
        return result
    if base is None:
        if value is None:return None
        return scalar(value,'float')
    if isinstance(base,bool):
        if not isinstance(value,bool):raise ValueError(f'{path} 必須為布林')
        return value
    if isinstance(base,int):return scalar(value,'int')
    if isinstance(base,float):return scalar(value,'float')
    return scalar(value,'text')

def validate_grid(grid):
    from itertools import product
    fields=('gap_limit','trailing_activation','trailing_drawdown','stop_loss'); total=0; clean={}
    if not isinstance(grid,dict) or set(grid)!={'long','short'}:raise ValueError('多空網格必須包含 long 與 short')
    for side in ('long','short'):
        g=grid[side]
        if not isinstance(g,dict) or set(g)!=set(fields):raise ValueError('網格須包含四個門檻')
        clean[side]={}; count=1
        for field in fields:
            v=g[field]
            modes={'POINTS','PRICE_PERCENT'}|({'PROFIT_GIVEBACK_PERCENT'} if field=='trailing_drawdown' else set())
            if not isinstance(v,dict) or not v or not set(v)<=modes:raise ValueError(f'{field} 門檻模式不合法')
            cleaned={}
            for mode,vals in v.items():
                if not isinstance(vals,list) or not 1<=len(vals)<=100:raise ValueError('每個門檻最多100個值')
                ns=list(dict.fromkeys(scalar(x,'float') for x in vals))
                if any(n<=0 or (mode=='PROFIT_GIVEBACK_PERCENT' and n>100) for n in ns):raise ValueError('門檻必須為正數，浮盈回吐百分比不得大於100')
                cleaned[mode]=ns
            clean[side][field]=cleaned; count*=sum(map(len,cleaned.values()))
        total+=count
    if total>10000:raise ValueError(f'參數網格 {total} 組，超過網頁上限 10,000 組')
    return clean,total

def build_request(eid,params,config_id=None):
    e=experiment(eid)
    if not e['runner_enabled'] or not source_ok(e):raise Conflict('原始程式版本與註冊版本不符；為避免錯跑，暫停網頁執行')
    if not isinstance(params,dict):raise ValueError('參數必須是 JSON 物件')
    r=e['runner']; spec=parameter_spec(eid); count=None
    if r['mode']=='config':
        ref=next((x for x in spec['configs'] if x['id']==(config_id or spec['default_config'])),None)
        if ref is None:raise ValueError('不支援的設定檔')
        base=ref['parameters']; raw=dict(params); cleanbase=dict(base)
        if eid=='futures_stop_trailing':
            grid,count=validate_grid(raw.pop('parameter_grid',None)); cleanbase.pop('parameter_grid'); clean=validate_tree(raw,cleanbase);clean['parameter_grid']=grid
        else:clean=validate_tree(raw,cleanbase)
        for k in ('signal_column','core_column'):
            if k in clean and clean[k] not in ('策略_核心_五日價量','策略_核心_昨價量'):raise ValueError('不允許的核心訊號欄')
        if 'futures_id' in clean and not re.fullmatch(r'[A-Z0-9]{1,12}',clean['futures_id']):raise ValueError('期貨商品代碼格式錯誤')
        if eid=='futures_night_retrace':
            count=len(clean['x_bands'])*len(clean['y_retrace_pct_values'])*len(clean['z_stop_pct_values'])
            if count>10000:raise ValueError('網格超過 10,000 組')
        if 'write_database' in json_read(inside(PROJECT,ref['id'])):clean['write_database']=False
        if 'export_csv' in json_read(inside(PROJECT,ref['id'])):clean['export_csv']=True
    else:
        fields={f['key']:f for f in spec['fields']}
        if not set(params)<=set(fields):raise ValueError('存在未註冊的參數')
        clean={}
        for key,f in fields.items():
            value=params.get(key,f['default'])
            if value is None or value=='':
                if f['required']:raise ValueError(f'{key} 為必填')
                value=f['default']
                if value is None:clean[key]=None;continue
            v=scalar(value,f['type'])
            if f['choices'] and v not in f['choices']:raise ValueError(f'{key} 不在允許的選項中')
            if key=='futures_id' and not re.fullmatch(r'[A-Z0-9]{1,12}',v):raise ValueError('期貨商品代碼格式錯誤')
            if key=='core_column' and v not in ('策略_核心_五日價量','策略_核心_昨價量'):raise ValueError('不允許的核心訊號欄')
            clean[key]=v
    start,end=validate_dates(clean)
    return e,clean,config_id or r.get('default_config'),start,end,count

def command_for(e,params,d):
    r=e['runner']; args=[sys.executable,'-u','-m',e['module']]; out=d/'artifacts';out.mkdir(parents=True,exist_ok=True)
    if r['mode']=='config':
        cfg=dict(params);cfg['output_dir']=str(out.resolve());json_write(d/'config.json',cfg);args.extend(['--config',str((d/'config.json').resolve())])
    else:
        for f in r['fields']:
            v=params.get(f['key'])
            if v is not None:args.extend([f['flag'],str(v)])
        target=out/'retail_bounce_reentry_5m_gallery.html' if r['output_flag']=='--output' else out
        args.extend([r['output_flag'],str(target.resolve())]);json_write(d/'config.json',params)
    args.extend(r['force_flags']);return args

def lock_path():return RUNS/'.runner.lock'
def active_job():
    try:return json_read(lock_path())
    except FileNotFoundError:return None
    except (OSError,ValueError):return {'error':'執行鎖無法讀取；請檢查研究執行資料夾'}

def start_run(eid,params,config_id=None,note=''):
    e,clean,config_id,start,end,count=build_request(eid,params,config_id)
    if not isinstance(note,str) or len(note)>5000:raise ValueError('備註上限5000字')
    RUNS.mkdir(parents=True,exist_ok=True);rid='run-'+uuid.uuid4().hex;token=uuid.uuid4().hex;lock=lock_path()
    try:fd=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    except FileExistsError:raise Conflict('已有回測正在執行；請先等待完成或取消')
    with os.fdopen(fd,'w',encoding='utf-8') as f:json.dump({'experiment_id':eid,'id':rid,'token':token,'created_at':now()},f)
    d=run_dir(eid,rid); m={'id':rid,'experiment_id':eid,'kind':'new','status':'queued','created_at':now(),'started_at':None,'finished_at':None,'start':start,'end':end,'parameters':clean,'config_id':config_id,'parameter_count':count,'source_sha256':e['source_sha256'],'note':note,'database_write':False}
    try:
        d.mkdir(parents=True,exist_ok=False);args=command_for(e,clean,d);m['command']=args[1:];json_write(d/'manifest.json',m)
        # Worker is independent of HTTP and continues if the browser closes.
        kwargs={'cwd':str(CODE),'stdin':subprocess.DEVNULL,'stdout':subprocess.DEVNULL,'stderr':subprocess.DEVNULL,'env':{**os.environ,'PYTHONUTF8':'1'}}
        if os.name=='nt':kwargs['creationflags']=subprocess.CREATE_NEW_PROCESS_GROUP|subprocess.CREATE_NO_WINDOW
        else:kwargs['start_new_session']=True
        worker=subprocess.Popen([sys.executable,'-u','-m','dashboard.research_lab_worker',eid,rid,token],**kwargs)
        # Worker owns the manifest; parent never overwrites its status after launching.
        try:atomic_text(d/'worker.pid',str(worker.pid))
        except OSError:pass  # Worker publishes its own PID; never release a running job's lock.
    except Exception:
        m.update(status='failed',finished_at=now(),error='無法啟動回測工作程序')
        if d.is_dir():json_write(d/'manifest.json',m)
        release_lock(token);raise
    return m

def release_lock(token):
    a=active_job()
    if a and a.get('token')==token:
        try:lock_path().unlink()
        except FileNotFoundError:pass

def cancel_run(eid,rid):
    d=run_dir(eid,rid);m=get_run(eid,rid)
    if m['status'] not in ('queued','running'):raise Conflict('此批次已結束')
    atomic_text(d/'cancel.request','cancel\n');return {'requested':True}

def log_tail(eid,rid):
    d=run_dir(eid,rid);p=d/'execution.log'
    if not p.exists():return ''
    with p.open('rb') as f:
        f.seek(max(0,p.stat().st_size-65536));return f.read().decode('utf-8',errors='replace')


def database_metadata(eid,rid):
    g=get_run(eid,rid); raw=g.get('manifest_run_id')
    if not raw:return {'matched':False,'reason':'此批次沒有明確資料庫 run_id；不以相同日期猜測同一批次。'}
    try: key=uuid.UUID(str(raw))
    except ValueError:return {'matched':False,'reason':'manifest 未提供合法的 UUID'}
    from common import db
    from psycopg.rows import dict_row
    from fastapi.encoders import jsonable_encoder
    queries=[('research.analysis_run', 'SELECT run_id,analysis_name,futures_id,start_date,end_date,metric_version,status,started_at,finished_at,source_rows,valid_rows,excluded_rows FROM research.analysis_run WHERE run_id=%s'), ('research.night_retrace_backtest_run','SELECT run_id,strategy_name,strategy_version,futures_id,start_date,end_date,config,status,started_at,finished_at FROM research.night_retrace_backtest_run WHERE run_id=%s'),('backtest.strategy_runs','SELECT run_id,strategy_code,strategy_version,start_date,end_date,settings,created_at FROM backtest.strategy_runs WHERE run_id=%s')]
    # Exact UUID match only, no write or migration. Some experiments intentionally do not save DB runs.
    with db.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            for table,sql in queries:
                cur.execute(sql,(key,)); row=cur.fetchone()
                if row:return {'matched':True,'table':table,'record':jsonable_encoder(row)}
    return {'matched':False,'reason':'資料庫未找到相同 UUID；可能原本只輸出 CSV。'}


def pid_alive(pid):
    if not isinstance(pid,int) or pid<=0:return False
    if os.name=='nt':
        import ctypes
        from ctypes import wintypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];kernel.OpenProcess.restype=wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)];kernel.GetExitCodeProcess.restype=wintypes.BOOL
        kernel.CloseHandle.argtypes=[wintypes.HANDLE];kernel.CloseHandle.restype=wintypes.BOOL
        h=kernel.OpenProcess(0x1000,False,pid)
        if not h:return ctypes.get_last_error()!=87  # access denied is conservatively treated as alive
        try:
            code=wintypes.DWORD();ok=kernel.GetExitCodeProcess(h,ctypes.byref(code));return not ok or code.value==259
        finally:kernel.CloseHandle(h)
    try:
        stat=Path(f'/proc/{pid}/stat')
        if stat.is_file() and stat.read_text().rsplit(')',1)[1].split()[0]=='Z':return False
    except (OSError,IndexError):pass
    try:os.kill(pid,0);return True
    except ProcessLookupError:return False
    except PermissionError:return True

def recover_stale():
    a=active_job()
    if not a:return {'released':False,'message':'沒有執行鎖'}
    if not a.get('id') or not a.get('token'):raise Conflict('執行鎖內容異常，請先備份並人工確認')
    d=run_dir(a['experiment_id'],a['id']); m=json_read(d/'manifest.json')
    age=(datetime.now(timezone.utc)-datetime.fromisoformat(a['created_at'])).total_seconds()
    if age<60:raise Conflict('執行工作剛建立，請勿解除可能仍在啟動的鎖')
    pid=m.get('worker_pid')
    if not pid and (d/'worker.pid').is_file():pid=int((d/'worker.pid').read_text())
    if pid_alive(pid) or pid_alive(m.get('child_pid')):raise Conflict('工作程序或回測程序仍在執行，不允許解除鎖')
    # No automatic recovery: require explicit user action after checking the process status.
    if not pid:raise Conflict('無法確認原工作程序 PID，請人工檢查 .runner.lock 後再處理')
    if m['status'] in ('queued','running'):m.update(status='interrupted',finished_at=now(),error='工作程序已中止；原始輸出保留，請建立新批次');json_write(d/'manifest.json',m)
    release_lock(a['token']);return {'released':True,'message':'已確認工作程序不在執行，解除殘留鎖'}
