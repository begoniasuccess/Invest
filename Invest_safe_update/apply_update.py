"""Apply one small Invest update stage to the existing code folder.

Default is a read-only version check. --apply creates a source-file backup first.
No database migration, credential modification, data move, or full code replacement.
"""
from __future__ import annotations
import argparse
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sys
import tempfile
import os

BUNDLE = Path(__file__).resolve().parent


def fingerprint(path: Path) -> str:
    # User's Windows checkout and source export may use CRLF, BOM, or a final blank line.
    value = path.read_text(encoding='utf-8-sig').rstrip()
    return sha256(value.encode('utf-8')).hexdigest()


def safe(project: Path, relative: str) -> Path:
    p = Path(relative)
    if p.is_absolute() or '..' in p.parts:
        raise ValueError(f'Unsafe path: {relative}')
    result = project
    for part in p.parts:
        result = result / part
        if result.is_symlink() or (hasattr(result, 'is_junction') and result.is_junction()):
            raise ValueError(f'Symbolic link/junction refused: {result}')
    if not result.resolve().is_relative_to(project.resolve()):
        raise ValueError(f'Path escapes project: {result}')
    return result


def manifest(stage: str):
    return json.loads((BUNDLE/stage/'manifest.json').read_text(encoding='utf-8'))['files']


def plan(project: Path, stage: str):
    changes, errors = [], []
    for item in manifest(stage):
        relative = item['path']
        dest = safe(project, 'code/'+relative)
        src = safe(BUNDLE, stage+'/'+relative)
        if not src.is_file() or fingerprint(src) != item['new_sha256']:
            errors.append(f'PACKAGE ERROR: {relative} is missing or changed')
            continue
        if dest.exists() and not dest.is_file():
            errors.append(f'CONFLICT: not a regular file: {dest}')
            continue
        before = fingerprint(dest) if dest.exists() else None
        if before == item['new_sha256']:
            print(f'SAME {relative}')
        elif before in item['allowed_old_sha256']:
            changes.append({**item, 'before_sha256': before})
            print(f'PLAN {"ADD" if before is None else "UPDATE"} {relative}')
        else:
            errors.append(f'CONFLICT: {relative} differs from the provided version (or is missing). Do not overwrite.')
    return changes, errors


def atomic_copy(src: Path, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.invest-update-', suffix='.tmp', dir=dest.parent)
    os.close(fd)
    temp = Path(name)
    try:
        shutil.copy2(src, temp)
        os.replace(temp, dest)
    finally:
        temp.unlink(missing_ok=True)


def install(project: Path, stage: str, changes: list[dict]):
    if not changes:
        print('This stage is already installed.'); return None
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    backup = safe(project, f'archive/update_backups/{stamp}-{stage}')
    backup.mkdir(parents=True, exist_ok=False)
    log = {'stage':stage,'files':changes,'project':str(project)}
    # Back up every original before replacing even one file.
    for item in changes:
        dest = safe(project, 'code/'+item['path'])
        before = item['before_sha256']
        current = fingerprint(dest) if dest.exists() else None
        if before != current:
            raise RuntimeError(f'File changed after preview: {dest}')
        if before is not None:
            saved = backup/'files'/item['path']
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dest, saved)
            if fingerprint(saved) != before:
                raise RuntimeError(f'Backup verification failed: {saved}')
    (backup/'backup.json').write_text(json.dumps(log,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'BACKUP: {backup}')
    applied=[]
    try:
        for item in changes:
            dest = safe(project,'code/'+item['path'])
            before = item['before_sha256']
            if (fingerprint(dest) if dest.exists() else None) != before:
                raise RuntimeError(f'File changed during installation: {dest}')
            src = safe(BUNDLE,stage+'/'+item['path'])
            atomic_copy(src,dest); applied.append(item)
            if fingerprint(dest)!=item['new_sha256']:
                raise RuntimeError(f'Installed file verification failed: {dest}')
            print(f'UPDATED {item["path"]}')
    except BaseException:
        # Restore only files this process has replaced; don't touch files changed by others.
        for item in reversed(applied):
            dest=safe(project,'code/'+item['path'])
            if dest.is_file() and fingerprint(dest)==item['new_sha256']:
                if item['before_sha256'] is None: dest.unlink()
                else: atomic_copy(backup/'files'/item['path'],dest)
        raise
    return backup


def rollback(project: Path, backup: Path):
    backup=backup.resolve(); parent=safe(project,'archive/update_backups').resolve()
    if backup==parent or not backup.is_relative_to(parent):
        raise ValueError('Select the backup folder printed by apply_update.py under archive/update_backups.')
    log=json.loads((backup/'backup.json').read_text(encoding='utf-8'))
    errors=[];todo=[]
    for item in log['files']:
        dest=safe(project,'code/'+item['path']);before=item['before_sha256']
        cur=fingerprint(dest) if dest.exists() else None
        if cur==before:continue
        if cur!=item['new_sha256']:
            errors.append(f'CONFLICT (not overwritten): {dest}');continue
        if before is not None:
            src=backup/'files'/item['path']
            if not src.is_file() or fingerprint(src)!=before:
                errors.append(f'BACKUP ERROR: {src}');continue
        todo.append(item)
    if errors:
        for error in errors:print(error)
        return False
    for item in reversed(todo):
        dest=safe(project,'code/'+item['path'])
        if item['before_sha256'] is None:dest.unlink()
        else:atomic_copy(backup/'files'/item['path'],dest)
        print(f'RESTORED {item["path"]}')
    print('Code-only rollback complete. Database and data locations were NOT rolled back.')
    return True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage',nargs='?',choices=['features','paths'])
    parser.add_argument('--project',type=Path,default=BUNDLE.parent,help='Existing Invest root (defaults to the bundle parent)')
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--rollback',type=Path,help='Restore only source files from a printed update backup')
    args=parser.parse_args()
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8',errors='replace')
    try:
        project=args.project.resolve()
        if not (project/'code/dashboard/app.py').is_file() or not (project/'code/start_dashboard.py').is_file():
            raise ValueError(f'Existing Invest/code folder was not found: {project}. No new code folder will be created.')
        if args.rollback:
            if args.stage or args.apply:raise ValueError('Use --rollback without a stage or --apply.')
            return 0 if rollback(project,args.rollback) else 2
        if not args.stage:raise ValueError('Choose features or paths.')
        print('Project:',project)
        print('Stage:',args.stage)
        changes,errors=plan(project,args.stage)
        for error in errors:print(error)
        if errors:
            print('Version check failed. Nothing was changed. Share the conflict paths and current files for review.');return 2
        print(f'{len(changes)} files to update. .env, data, output, journal_media and start_dashboard are unchanged.')
        if not args.apply:
            print('Preview OK. Re-run with --apply to install.');return 0
        install(project,args.stage,changes)
        print('Code update completed. See README.md for the next step.')
        return 0
    except (OSError,ValueError,RuntimeError) as exc:
        print(f'ERROR: {exc}\nNo credentials, database records or media files were removed. Check the printed backup path.');return 1


if __name__=='__main__':
    raise SystemExit(main())
