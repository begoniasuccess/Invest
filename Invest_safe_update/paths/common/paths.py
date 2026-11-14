"""Stable locations; CODE_ROOT is the existing code folder, PROJECT_ROOT its parent."""
from pathlib import Path
CODE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = CODE_ROOT.parent
DATA_ROOT = PROJECT_ROOT / "data"
OUTPUT_ROOT = PROJECT_ROOT / "output"
YU_DATA_ROOT = DATA_ROOT / "yu"
FINMIND_OUTPUT_ROOT = OUTPUT_ROOT / "finmind"
DB_EXPORT_ROOT = OUTPUT_ROOT / "db_exports"
YU_FILES = {"Yu點位資料整理_2025_2026.xlsx", "yu_futures_analysis_2026.xlsx", "yu_taiex_analysis_2025.csv", "yu_taiex_analysis_2026.csv"}

def yu_label_path(value):
    """Explicit existing paths win; resolve the four former code-root input files."""
    path = Path(value)
    if path.exists() or path.name not in YU_FILES:
        return path
    if len(path.parts) == 1 or path.resolve() == (CODE_ROOT / path.name).resolve():
        old = CODE_ROOT / path.name
        return old if old.is_file() else YU_DATA_ROOT / path.name
    return path

def resolve_output(value):
    """Resolve known legacy ../output defaults independently of the current directory."""
    path = Path(value)
    if not path.is_absolute() and path.parts[:2] == ('..', 'output'):
        return OUTPUT_ROOT.joinpath(*path.parts[2:])
    return path
