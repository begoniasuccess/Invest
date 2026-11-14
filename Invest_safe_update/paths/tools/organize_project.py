"""Organize only explicitly reviewed Invest files, with a dry run and verified backup.

Run from any working directory. The script lives in Invest/code/tools.
No database, .env, media, raw legacy database, or arbitrary directory is moved.
"""
from __future__ import annotations
import argparse
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sys
from dataclasses import dataclass, asdict

PROJECT = Path(__file__).resolve().parents[2]
YU_NAMES = ("Yu點位資料整理_2025_2026.xlsx", "yu_futures_analysis_2026.xlsx", "yu_taiex_analysis_2025.csv", "yu_taiex_analysis_2026.csv")
FIN_NAMES = ("finMind.csv", "finMind_futures_final_settlement_price.csv", "finMind_futures_institutional_investors.csv", "finMind_futures_institutional_investors_after_hours.csv", "finMind_option_final_settlement_price.csv")

# Existing output/ files are the preferred FinMind snapshots. A differing copy in code/
# is retained under archive/organization_conflicts, never used to overwrite output/.
MAPPINGS = [
    *((f"code/{name}", f"data/yu/{name}", "strict") for name in YU_NAMES),
    *((f"output/{name}", f"output/finmind/{name}", "archive") for name in FIN_NAMES),
    *((f"code/{name}", f"output/finmind/{name}", "archive") for name in FIN_NAMES),
    ("code/APPLY_REPORT_COLUMNS_20260817.md", "code/docs/APPLY_REPORT_COLUMNS_20260817.md", "strict"),
    ("code/APPLY_TAIEX_REPORT_UPDATE.md", "code/docs/APPLY_TAIEX_REPORT_UPDATE.md", "strict"),
    ("code/taifex_night_fallback.sql", "code/db_migrate/taifex_night_fallback.sql", "strict"),
    ("code/all_python_code.txt", "archive/code_exports/code/all_python_code.txt", "archive"),
    ("all_python_code.txt", "archive/code_exports/project/all_python_code.txt", "archive"),
    ("postgres_schema.sql", "output/db_exports/legacy_project_postgres_schema.sql", "archive"),
    ("output/postgres_schema.sql", "output/db_exports/legacy_output_postgres_schema.sql", "archive"),
    ("code/tests/test_futures_open_to_night_close_convergence.py", "archive/code_legacy/tests/test_futures_open_to_night_close_convergence.py", "duplicate-test"),
    ("code/tests/backtest/futures_raw_signal_strength_scan.py", "archive/code_legacy/tests/backtest/futures_raw_signal_strength_scan.py", "archive"),
    ("code/tests/backtest/README_futures_raw_signal_strength_scan.md", "archive/code_legacy/tests/backtest/README_futures_raw_signal_strength_scan.md", "archive"),
    ("code/tests/backtest/tests/test_futures_raw_signal_strength_scan.py", "archive/code_legacy/tests/backtest/tests/test_futures_raw_signal_strength_scan.py", "archive"),
]

@dataclass(frozen=True)
class Move:
    source: str
    destination: str
    digest: str
    reason: str


def digest(path: Path) -> str:
    h = sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def safe_path(project: Path, relative: str) -> Path:
    """Reject reparse links (including junctions on supported Windows Python) and escapes."""
    part = Path(relative)
    if part.is_absolute() or '..' in part.parts:
        raise ValueError(f"Unsafe relative path: {relative}")
    path = project
    for component in part.parts:
        path = path / component
        if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
            raise ValueError(f"Symbolic link/junction: {path}")
    if not path.resolve().is_relative_to(project.resolve()):
        raise ValueError(f"Path escapes project: {path}")
    return path


def file_hash(project: Path, relative: str):
    path = safe_path(project, relative)
    if not path.exists():
        return None
    if not path.is_file():
        raise ValueError(f"Expected a file: {path}")
    return digest(path)


def normalized_text(path: Path) -> str:
    return path.read_text(encoding='utf-8-sig').rstrip()


def build_plan(project: Path):
    """Plan all destinations in order, including destinations not yet copied."""
    virtual: dict[str, str] = {}
    moves, notices, conflicts = [], [], []
    for source, target, policy in MAPPINGS:
        try:
            src = safe_path(project, source)
            source_hash = file_hash(project, source)
            if source_hash is None:
                continue
            if policy == 'duplicate-test':
                canonical = safe_path(project, 'code/backtest/tests/test_futures_open_to_night_close_convergence.py')
                if not canonical.is_file() or normalized_text(src) != normalized_text(canonical):
                    conflicts.append(f"KEEP {source}: canonical test differs or is missing")
                    continue
            existing = virtual.get(target)
            if existing is None:
                existing = file_hash(project, target)
            reason = 'identical destination; keep one active copy' if existing == source_hash else 'move'
            if existing is not None and existing != source_hash:
                if policy in ('strict', 'duplicate-test'):
                    conflicts.append(f"KEEP {source}: different existing {target}; choose manually")
                    continue
                original_target = target
                target = f"archive/organization_conflicts/{source}.{source_hash[:16]}"
                # A hash suffix is stable on reruns, but never overwrite even a conflicting archive.
                existing = virtual.get(target)
                if existing is None:
                    existing = file_hash(project, target)
                if existing is not None and existing != source_hash:
                    conflicts.append(f"KEEP {source}: different archive collision at {target}")
                    continue
                reason = f"different copy; preserve {original_target}, archive this version"
            virtual[target] = source_hash
            moves.append(Move(source, target, source_hash, reason))
        except (OSError, ValueError) as exc:
            conflicts.append(f"KEEP {source}: {exc}")
    for name in YU_NAMES:
        if not (project/'code'/name).is_file() and not (project/'data'/'yu'/name).is_file():
            notices.append(f"MISSING input (not created by this tool): {name}")
    return moves, notices, conflicts


def copy_verified(source: Path, destination: Path, expected: str):
    """Exclusive creation, byte verification, never overwrite a different file."""
    if digest(source) != expected:
        raise RuntimeError(f"Source changed during organization: {source}")
    if destination.exists():
        if destination.is_file() and digest(destination) == expected:
            return
        raise FileExistsError(f"Destination differs: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        with source.open('rb') as inf, destination.open('xb') as outf:
            created = True
            shutil.copyfileobj(inf, outf, 1024 * 1024)
        if digest(destination) != expected:
            raise IOError(f"Copy verification failed: {destination}")
        shutil.copystat(source, destination)
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise


def assert_path_update(project: Path):
    checks = {
        'code/common/paths.py': ('YU_DATA_ROOT', 'FINMIND_OUTPUT_ROOT'),
        'code/dashboard/yu_level_chart_service.py': ('yu_label_path(YU_ACTUAL_LABELS)',),
        'code/machine_learning/yu_strategy_model.py': ('yu_label_path(path)',),
        'code/machine_learning/yu_key_k_stage2.py': ('yu_label_path(path)',),
        'code/machine_learning/yu_key_level_rule_discovery.py': ('yu_label_path(path)',),
        'code/module/finMind.py': ('FINMIND_OUTPUT_ROOT / "finMind.csv"',),
        'code/common/utils.py': ('output_path.parent.mkdir',),
    }
    for relative, markers in checks.items():
        path = safe_path(project, relative)
        if not path.is_file() or not all(m in path.read_text(encoding='utf-8-sig') for m in markers):
            raise RuntimeError(f"Install the paths patch first; missing path support: {relative}")


def write_json(path: Path, data):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    temp.replace(path)


def apply_plan(project: Path, moves: list[Move]):
    assert_path_update(project)
    if not moves:
        print('No files to move.'); return None
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    backup = safe_path(project, f'archive/organization_backups/{stamp}')
    backup.mkdir(parents=True, exist_ok=False)
    log = {'project': str(project), 'moves': [asdict(m) for m in moves], 'completed': []}
    log_path = backup/'moves.json'
    write_json(log_path, log)
    print(f"Verified source backups: {backup}")
    for move in moves:
        src = safe_path(project, move.source)
        dst = safe_path(project, move.destination)
        saved = safe_path(project, f'archive/organization_backups/{stamp}/files/{move.source}')
        # First preserve the original even if the destination will be rewritten by later research.
        copy_verified(src, saved, move.digest)
        copy_verified(src, dst, move.digest)
        if digest(src) != move.digest or digest(saved) != move.digest or digest(dst) != move.digest:
            raise RuntimeError(f"Verification changed before removal: {src}")
        # No source is removed until both its backup and destination are byte-verified.
        src.unlink()
        log['completed'].append(move.source)
        write_json(log_path, log)
        print(f"MOVED {move.source} -> {move.destination}")
    # Remove only the known empty old test directories; caches and unknown files are not removed.
    for relative in ('code/tests/backtest/tests','code/tests/backtest'):
        folder = safe_path(project, relative)
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
    return backup


def restore(project: Path, backup: Path):
    backup = backup.resolve()
    parent = safe_path(project, 'archive/organization_backups').resolve()
    if not backup.is_relative_to(parent) or backup == parent:
        raise ValueError('Choose a backup folder under archive/organization_backups')
    log = json.loads((backup/'moves.json').read_text(encoding='utf-8'))
    errors = []
    for item in log['moves']:
        source = safe_path(project, item['source'])
        saved = backup/'files'/item['source']
        if not saved.is_file():
            continue  # operation did not reach this source
        try:
            copy_verified(saved, source, item['digest'])
            print(f"RESTORED {item['source']}")
        except (OSError, ValueError, RuntimeError) as exc:
            errors.append(str(exc))
    for e in errors:
        print('KEEP (not overwritten):', e)
    print('New-location files were left in place; no output or media was deleted.')
    return not errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Move reviewed files; default is dry-run')
    parser.add_argument('--restore', type=Path, help='Restore original locations from a verified organization backup')
    args = parser.parse_args()
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    try:
        if not (PROJECT/'code'/'dashboard'/'app.py').is_file():
            raise ValueError(f'Not the Invest project: {PROJECT}')
        if args.restore:
            if args.apply:
                raise ValueError('--restore and --apply cannot be combined')
            return 0 if restore(PROJECT, args.restore) else 2
        moves, notices, conflicts = build_plan(PROJECT)
        print(f'Project: {PROJECT}')
        for move in moves:
            print(f'PLAN {move.source} -> {move.destination}\n     {move.reason}')
        for notice in notices: print(notice)
        for conflict in conflicts: print(conflict)
        print(f'{len(moves)} files planned; {len(conflicts)} files need manual review.')
        print('Untouched: .env, journal_media, data_center.db-journal, tmp.json, unknown root files, deprecated, ai_skill, existing report/backtest output folders.')
        if not args.apply:
            print('Preview only. Run again with --apply after validating the paths patch.')
            return 0
        backup = apply_plan(PROJECT, moves)
        if backup:
            print(f'Organization complete. Restore from: {backup}')
        return 2 if conflicts else 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(f'ERROR: {exc}\nOriginal backups are retained. Do not overwrite conflicting files.')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
