"""Preview-first incremental installer. Run after stopping Dashboard. No DB changes."""
from pathlib import Path
from datetime import datetime
import argparse, hashlib, json, shutil, os, tempfile, sys

HERE=Path(__file__).resolve().parent

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None

def safe(root,rel):
    p=root/rel
    if not p.resolve().is_relative_to(root.resolve()):raise ValueError(f'Unsafe path: {rel}')
    if p.is_symlink():raise ValueError(f'Symlink not allowed: {rel}')
    return p

def copy_atomic(src,dst):
    dst.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=dst.parent,prefix='.research-install-');os.close(fd)
    try:shutil.copyfile(src,tmp);os.replace(tmp,dst)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def write_json(p,value):
    p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')

def check(root):
    if not (root/'code/dashboard/app.py').is_file():raise ValueError('Please choose the Invest project root containing code/dashboard/app.py.')

def plan(root):
    entries=json.loads((HERE/'manifest.json').read_text(encoding='utf-8'))['files'];actions=[];conflicts=[]
    for e in entries:
        rel=e['path'];dst=safe(root,rel);src=safe(HERE/'payload',rel)
        if sha(src)!=e['new_sha256']:raise ValueError(f'Package integrity error: {rel}')
        if dst.exists() and not dst.is_file():conflicts.append(rel+' (not a file)');continue
        cur=sha(dst)
        if cur==e['new_sha256']:print('SKIP  ',rel);continue
        if cur is None:
            if e['kind']=='code' and e['old_sha256'] is not None:conflicts.append(rel+' (expected existing source missing)');continue
        elif cur!=e['old_sha256']:
            if e['kind']=='document':print('KEEP USER EDIT',rel);continue
            conflicts.append(rel+' (local version differs)');continue
        print('UPDATE' if cur else 'ADD   ',rel);actions.append((e,cur))
    return actions,conflicts

def restore(root,backup,items,force=False):
    # Rollback code only. User-authored research files remain in place.
    for e in reversed(items):
        rel=e['path'];dst=safe(root,rel)
        if e['kind']!='code' and not force:continue
        if not force and sha(dst)!=e['new_sha256']:raise ValueError('Rollback would overwrite later changes: '+rel)
        if e['previous_sha256'] is None:
            if dst.is_file():dst.unlink()
        else:
            src=safe(backup/'files',rel)
            if sha(src)!=e['previous_sha256']:raise ValueError('Backup checksum mismatch: '+rel)
            copy_atomic(src,dst)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--project',type=Path,default=HERE.parent);ap.add_argument('--apply',action='store_true');ap.add_argument('--rollback',type=Path);args=ap.parse_args();root=args.project.resolve();check(root)
    if args.rollback:
        backup=args.rollback.resolve();m=json.loads((backup/'manifest.json').read_text(encoding='utf-8'))
        if Path(m['project']).resolve()!=root:raise ValueError('Backup belongs to another project.')
        rows=[x for x in m['files'] if x['kind']=='code']
        for e in rows:
            if sha(safe(root,e['path']))!=e['new_sha256']:raise ValueError('Rollback conflict: '+e['path'])
            if e['previous_sha256'] and sha(safe(backup/'files',e['path']))!=e['previous_sha256']:raise ValueError('Backup checksum mismatch: '+e['path'])
        restore(root,backup,rows);print('Code rollback complete. Research documents and output retained.');return 0
    actions,conflicts=plan(root)
    if conflicts:
        print('\nCONFLICTS:');print('\n'.join(conflicts));print('No files changed. Resolve versions before installation.');return 2
    print(f'\nPlan: {len(actions)} file(s). No database or existing output changes.')
    if not args.apply:print('Preview OK. Re-run with --apply to install.');return 0
    if not actions:print('Already installed.');return 0
    backup=root/'archive/update_backups'/('research_lab_'+datetime.now().strftime('%Y%m%d_%H%M%S_%f'));backup.mkdir(parents=True,exist_ok=False)
    rows=[{**e,'previous_sha256':cur} for e,cur in actions]
    for e in rows:
        if e['previous_sha256'] is not None:
            dst=safe(root,e['path']);b=safe(backup/'files',e['path']);b.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(dst,b)
            if sha(b)!=e['previous_sha256']:raise ValueError('File changed during backup: '+e['path'])
    write_json(backup/'manifest.json',{'project':str(root),'files':rows})
    changed=[]
    try:
        for e in rows:
            dst=safe(root,e['path']);src=safe(HERE/'payload',e['path'])
            if sha(dst)!=e['previous_sha256']:raise ValueError('File changed during installation: '+e['path'])
            copy_atomic(src,dst);changed.append(e)
            if sha(dst)!=e['new_sha256']:raise ValueError('Installation checksum mismatch: '+e['path'])
    except Exception:
        restore(root,backup,changed,force=True);raise
    print('Installation completed. Backup:',backup);print('Restart Dashboard and press Ctrl+F5. No PostgreSQL migration required.');return 0
if __name__=='__main__':
    try:raise SystemExit(main())
    except (ValueError,OSError,KeyError) as e:print('ERROR:',e,file=sys.stderr);raise SystemExit(1)
