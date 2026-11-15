"""Independent local job worker. No web-server process or browser lifetime dependency."""
from __future__ import annotations
import os, subprocess, sys, time, hashlib
from pathlib import Path
from dashboard import research_lab_service as s


def readonly_environment():
    env={**os.environ,'PYTHONUTF8':'1','PYTHONUNBUFFERED':'1'}
    # Preserve normal project credentials without placing them in log, manifest or command.
    from dotenv import dotenv_values
    local=dotenv_values(s.CODE/'.env')
    for k,v in local.items():
        if v is not None and k not in env:env[k]=v
    opts=env.get('PGOPTIONS','')+' -c default_transaction_read_only=on'
    env['PGOPTIONS']=opts.strip()
    # Connection-string options override PGOPTIONS, so explicitly append the same guard.
    if env.get('POSTGRES_DSN'):
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        info=conninfo_to_dict(env['POSTGRES_DSN'])
        info['options']=(info.get('options','')+' -c default_transaction_read_only=on').strip()
        env['POSTGRES_DSN']=make_conninfo(**info)
    return env


def worker(eid,rid,token):
    d=s.run_dir(eid,rid); manifest=d/'manifest.json'; m=s.json_read(manifest)
    if (s.active_job() or {}).get('token')!=token:return 2
    child=None; log=d/'execution.log'; cancel=d/'cancel.request'
    try:
        if cancel.exists():
            m.update(status='cancelled',finished_at=s.now(),error=None);s.json_write(manifest,m);return 0
        e=s.experiment(eid)
        if not s.source_ok(e):raise RuntimeError('程式已在排程後修改，為避免錯跑已停止')
        # Preserve the exact top-level source and code/config hashes, not credentials or data.
        source=s.inside(s.PROJECT,e['source_path']);snapshot=d/'source_snapshot';snapshot.mkdir(exist_ok=True)
        (snapshot/source.name).write_bytes(source.read_bytes())
        hashes={}
        for group in ('analysis','backtest','common','dashboard'):
            for p in (s.CODE/group).rglob('*.py'):
                if p.is_file() and not p.is_symlink():hashes[p.relative_to(s.CODE).as_posix()]=s.digest(p.read_bytes())
        s.json_write(d/'source_hashes.json',hashes)
        args=s.command_for(e,m['parameters'],d)
        m.update(status='running',started_at=s.now(),worker_pid=os.getpid(),command=args[1:]);s.json_write(manifest,m)
        env=readonly_environment()
        with log.open('wb') as f:
            f.write(('Research run '+rid+'\n固定註冊入口；PostgreSQL 預設唯讀；輸出隔離。\n').encode('utf-8'));f.flush()
            child=subprocess.Popen(args,cwd=s.CODE,env=env,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT)
            m['child_pid']=child.pid;s.json_write(manifest,m)
            while child.poll() is None:
                if cancel.exists():
                    child.terminate()
                    try:child.wait(timeout=10)
                    except subprocess.TimeoutExpired:child.kill();child.wait()
                    m.update(status='cancelled',exit_code=child.returncode,finished_at=s.now());break
                time.sleep(.5)
            else:
                # The process may have finished before a cancellation request arrived.
                m.update(status='completed' if child.returncode==0 else 'failed',exit_code=child.returncode,finished_at=s.now())
                if child.returncode!=0:m['error']=f'回測程序結束代碼 {child.returncode}，請查看執行日誌。'
        files=[x for x in (d/'artifacts').rglob('*') if x.is_file() and x.suffix.lower() in s.ALLOWED]
        if m['status']=='completed' and not files:m.update(status='failed',error='程式回傳成功，但沒有產生預期結果檔')
        m['output_files']=[p.relative_to(d).as_posix() for p in files]
        # Keep actual data period only if supplied by the strategy, not inferred from wall-clock.
        m['actual_data_period']=None
        for p in files:
            if p.suffix.lower()=='.json' and 'manifest' in p.name:
                try: native=s.json_read(p)
                except Exception:continue
                if isinstance(native,dict) and native.get('actual_data_end'):
                    m['actual_data_period']={'end':native['actual_data_end'],'source':p.name};break
        if m['status']=='completed':
            try:
                from dashboard.research_lab_review import review_text
                review=s.inside(s.LAB,f'reviews/{eid}/{rid}.md')
                if not review.exists():s.atomic_text(review,review_text(s.experiment(eid),{**m,'files':[s.artifact(p.resolve()) for p in files]}))
            except Exception as exc:
                with log.open('ab') as f:f.write(('\n自動摘要失敗（不影響回測成果）：'+str(exc)).encode('utf-8',errors='replace'))
        s.json_write(manifest,m)
        return 0 if m['status']=='completed' else 1
    except BaseException as exc:
        if child is not None and child.poll() is None:
            child.kill();child.wait()
        m.update(status='failed',finished_at=s.now(),error='執行器失敗；請查閱日誌')
        # Exception details can include connection strings, so do not log the DSN or arbitrary environment.
        with log.open('ab') as f:f.write(('\n執行器錯誤類型：'+type(exc).__name__+'\n').encode('utf-8'))
        s.json_write(manifest,m);return 1
    finally:s.release_lock(token)

if __name__=='__main__':
    if len(sys.argv)!=4:raise SystemExit(2)
    raise SystemExit(worker(*sys.argv[1:]))
