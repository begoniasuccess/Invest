"""Research lab regression tests; no market database required."""
import json, shutil, time, sys, os, hashlib
from pathlib import Path
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dashboard import research_lab_service as s
from dashboard.research_lab_api import router


def client():
    app=FastAPI();app.include_router(router);return TestClient(app)

@pytest.fixture
def sample(tmp_path,monkeypatch):
    root=tmp_path/'Invest';code=root/'code';dash=code/'dashboard';dash.mkdir(parents=True)
    lab=root/'research_lab';lab.mkdir();out=root/'output';out.mkdir()
    for name in ('research_lab_service.py','research_lab_worker.py','research_lab_review.py'):
        shutil.copy2(Path(s.__file__).with_name(name),dash/name)
    (dash/'__init__.py').touch();(code/'backtest').mkdir();(code/'backtest/__init__.py').touch()
    src=code/'backtest/demo.py';src.write_text('''import argparse, os, time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--output-dir');p.add_argument('--start');p.add_argument('--sleep',type=int,default=0);a=p.parse_args()
assert 'default_transaction_read_only=on' in os.environ['PGOPTIONS']
print('test run',flush=True);time.sleep(a.sleep)
d=Path(a.output_dir);d.mkdir(exist_ok=True);(d/'demo_summary.csv').write_text('direction,total_net_points,entry_count\\nLONG,25,2\\nSHORT,-5,1\\n')
''',encoding='utf-8')
    eid='demo';e={'id':eid,'title':'測試回測','category':'測試','kind':'backtest','status':'unreviewed','favorite':False,'tags':[],'module':'backtest.demo','source_path':'code/backtest/demo.py','source_sha256':hashlib.sha256(src.read_bytes()).hexdigest(),'description_path':'experiments/demo/README.md','configs':[],'runner_enabled':True,'history':{'root':'backtest','directory':'','prefix':'demo_'},'runner':{'mode':'cli','fields':[{'key':'start','flag':'--start','type':'date','default':'2020-01-01','required':False,'choices':None,'help':''},{'key':'sleep','flag':'--sleep','type':'int','default':0,'required':False,'choices':None,'help':''}],'output_flag':'--output-dir','force_flags':[]}}
    (lab/'catalog.json').write_text(json.dumps({'experiments':[e]}),encoding='utf-8');(lab/'experiments/demo').mkdir(parents=True);(lab/'experiments/demo/README.md').write_text('# 實驗',encoding='utf-8')
    hist=out/'backtest';hist.mkdir();(hist/'demo_summary_20200101_20200102.csv').write_text('方向,total_net_points,entry_count\n多,5,2\n空,-2,1\n多,10,3\n',encoding='utf-8-sig');(hist/'demo_manifest_20200101_20200102.json').write_text(json.dumps({'settings':{'start':'2020-01-01','end':'2020-01-02','cost':3},'generated_at':'2020-01-03T00:00:00+00:00'}));(hist/'demo_gallery_20200101_20200102.html').write_text('<script>console.log(1)</script>');(hist/'other_summary_20200101_20200102.csv').write_text('a\n1\n')
    for k,v in {'CODE':code,'PROJECT':root,'LAB':lab,'OUTPUT':out,'RUNS':out/'research_runs'}.items():monkeypatch.setattr(s,k,v)
    monkeypatch.delenv('POSTGRES_DSN',raising=False)
    return root,eid

def test_history_and_csv(sample):
    _,eid=sample;c=client();base='/api/research-lab/experiments/'+eid
    r=c.get('/api/research-lab/catalog');assert r.status_code==200 and r.json()[0]['history_count']==1
    rr=c.get(base+'/runs').json();rid=rr[0]['id'];path=base+'/runs/'+rid
    m=c.get(path).json();assert len(m['files'])==3 and m['parameters']['cost']==3 and m['execution_time'].startswith('2020-01-03')
    f=next(f for f in m['files'] if f['type']=='csv');p=path+'/files/'+f['id'];r=c.get(p+'/csv',params={'sort':'1','descending':'true','limit':1,'query':'多'});assert r.status_code==200 and r.json()['rows'][0][1]=='10' and r.json()['has_more']
    assert c.get(p+'/download').status_code==200
    assert c.get(path+'/files/not-a-real-file/download').status_code==404
    h=next(f for f in m['files'] if f['type']=='html');r=c.get(path+'/files/'+h['id']+'/view');assert 'sandbox' in r.headers['content-security-policy']
    assert c.post(path+'/review-draft').status_code==200

def test_edit_and_conflict(sample):
    root,eid=sample;c=client();base='/api/research-lab/experiments/'+eid
    d=c.get(base+'/document').json();r=c.put(base+'/document',json={'body':'# 新說明\n<script>alert(1)</script>','version':d['version']});assert r.status_code==200 and '<script>' not in r.json()['html']
    assert c.put(base+'/document',json={'body':'舊','version':d['version']}).status_code==409
    assert list((root/'research_lab/revisions').rglob('*.md'))
    assert c.put(base,json={'favorite':True}).json()['favorite']
    assert c.put(base,json={'title':'外站'},headers={'Origin':'https://evil.test'}).status_code==403
    assert c.get('/api/research-lab/experiments/nope').status_code==404

def test_registered_parameters(sample):
    _,eid=sample;c=client();base='/api/research-lab/experiments/'+eid
    assert c.post(base+'/validate',json={'parameters':{'sleep':1}}).status_code==200
    assert c.post(base+'/validate',json={'parameters':{'output_dir':'../../bad'}}).status_code==400
    assert c.post(base+'/validate',json={'parameters':{'sleep':1.5}}).status_code==400
    (s.CODE/'backtest/demo.py').write_text('changed');assert c.post(base+'/validate',json={'parameters':{}}).status_code==409

def wait_for_done(eid,rid,seconds=15):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        m=s.get_run(eid,rid)
        if m['status'] not in ('running','queued') and s.active_job() is None:return m
        time.sleep(.1)
    raise AssertionError('Worker timeout')

def test_worker_and_cancellation(sample):
    _,eid=sample;c=client();base='/api/research-lab/experiments/'+eid
    m=c.post(base+'/execute',json={'parameters':{'sleep':1}});assert m.status_code==200,m.text;rid=m.json()['id'];assert s.active_job()['id']==rid
    assert c.post(base+'/execute',json={'parameters':{}}).status_code==409
    m=wait_for_done(eid,rid);assert m['status']=='completed',m;assert len(m['files'])==1 and m['started_at'] and m['finished_at'];assert 'test run' in s.log_tail(eid,rid);assert '25' in s.read_doc(eid,rid)['body'];assert s.run_dir(eid,rid).joinpath('source_hashes.json').exists()
    second=s.start_run(eid,{'sleep':10});assert second['id']!=rid
    s.cancel_run(eid,second['id']);assert wait_for_done(eid,second['id'])['status']=='cancelled'
    assert s.get_run(eid,rid)['status']=='completed'

def test_grid_and_csv(tmp_path):
    fields={'gap_limit':{'POINTS':[300,500]},'trailing_activation':{'POINTS':[125]},'trailing_drawdown':{'POINTS':[100]},'stop_loss':{'POINTS':[100,125]}}
    _,n=s.validate_grid({'long':fields,'short':fields});assert n==8
    p=tmp_path/'large.csv';p.write_text('n\n1\n3\n2\n');assert s.csv_page(p,sort='0',descending=True)['rows']==[['3'],['2'],['1']]
    with pytest.raises(ValueError):s.csv_page(p,sort='9')
