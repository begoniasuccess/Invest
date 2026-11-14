from pathlib import Path
from tools import organize_project as org
from common import paths


def write(root, name, value):
    p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(value);return p


def test_plan_preserves_output_and_archives_different_code_csv(tmp_path):
    write(tmp_path,'output/finMind.csv',b'output')
    write(tmp_path,'code/finMind.csv',b'code')
    write(tmp_path,'code/finMind_futures_institutional_investors.csv',b'same')
    write(tmp_path,'output/finMind_futures_institutional_investors.csv',b'same')
    write(tmp_path,'code/.env',b'SECRET TEST ONLY')
    write(tmp_path,'data_center.db-journal',b'journal')
    write(tmp_path,'journal_media/test.png',b'media')
    moves,_,conflicts=org.build_plan(tmp_path)
    assert not conflicts
    assert len(moves)==4
    assert moves[0].source=='output/finMind.csv'
    assert moves[1].source=='output/finMind_futures_institutional_investors.csv'
    assert any(m.source=='code/finMind.csv' and m.destination.startswith('archive/organization_conflicts/') for m in moves)
    assert next(m for m in moves if m.source=='code/finMind_futures_institutional_investors.csv').destination=='output/finmind/finMind_futures_institutional_investors.csv'
    # Preview has not modified source files or created output folders.
    assert (tmp_path/'code/finMind.csv').read_bytes()==b'code'
    assert not (tmp_path/'output/finmind').exists()
    assert (tmp_path/'code/.env').read_bytes()==b'SECRET TEST ONLY'


def test_actual_move_and_restore(tmp_path,monkeypatch):
    monkeypatch.setattr(org,'assert_path_update',lambda _:None)
    write(tmp_path,'code/yu_futures_analysis_2026.xlsx',b'yu')
    write(tmp_path,'code/finMind.csv',b'older')
    write(tmp_path,'output/finMind.csv',b'current')
    write(tmp_path,'code/.env',b'SECRET TEST ONLY')
    write(tmp_path,'journal_media/chart.png',b'media')
    moves,_,errors=org.build_plan(tmp_path);assert not errors
    backup=org.apply_plan(tmp_path,moves)
    assert (tmp_path/'data/yu/yu_futures_analysis_2026.xlsx').read_bytes()==b'yu'
    assert (tmp_path/'output/finmind/finMind.csv').read_bytes()==b'current'
    assert not (tmp_path/'code/finMind.csv').exists()
    assert not (tmp_path/'code/yu_futures_analysis_2026.xlsx').exists()
    assert (tmp_path/'code/.env').read_bytes()==b'SECRET TEST ONLY'
    assert (tmp_path/'journal_media/chart.png').read_bytes()==b'media'
    assert org.build_plan(tmp_path)[0]==[]
    # A later output generation may change the new file. Rollback still has the old original.
    (tmp_path/'output/finmind/finMind.csv').write_bytes(b'new output')
    assert org.restore(tmp_path,backup)
    assert (tmp_path/'output/finMind.csv').read_bytes()==b'current'
    assert (tmp_path/'code/finMind.csv').read_bytes()==b'older'
    assert (tmp_path/'output/finmind/finMind.csv').read_bytes()==b'new output'


def test_yu_conflict_never_moves_or_changes_preferred_source(tmp_path):
    write(tmp_path,'code/yu_futures_analysis_2026.xlsx',b'old input')
    write(tmp_path,'data/yu/yu_futures_analysis_2026.xlsx',b'different input')
    moves,_,conflicts=org.build_plan(tmp_path)
    assert not moves and conflicts
    assert (tmp_path/'code/yu_futures_analysis_2026.xlsx').read_bytes()==b'old input'


def test_no_apply_without_installed_paths(tmp_path):
    import pytest
    write(tmp_path,'code/finMind.csv',b'example')
    moves,_,_=org.build_plan(tmp_path)
    with pytest.raises(RuntimeError,match='paths patch'):
        org.apply_plan(tmp_path,moves)
    assert (tmp_path/'code/finMind.csv').is_file()


def test_resolve_yu_before_and_after_moving(tmp_path,monkeypatch):
    code=tmp_path/'code';yu=tmp_path/'data/yu';name='yu_taiex_analysis_2025.csv'
    monkeypatch.setattr(paths,'CODE_ROOT',code);monkeypatch.setattr(paths,'YU_DATA_ROOT',yu)
    elsewhere=tmp_path/'cwd';elsewhere.mkdir();monkeypatch.chdir(elsewhere)
    old=write(tmp_path,'code/'+name,b'old')
    new=write(tmp_path,'data/yu/'+name,b'new')
    assert paths.yu_label_path(name)==old
    assert paths.yu_label_path(old)==old
    old.unlink()
    assert paths.yu_label_path(name)==new
    assert paths.yu_label_path(old)==new
    explicit=write(tmp_path,'custom/'+name,b'custom')
    assert paths.yu_label_path(explicit)==explicit
    assert paths.yu_label_path(Path('other')/name)==Path('other')/name


def test_backup_cannot_overwrite_changed_destination(tmp_path):
    import pytest
    source=write(tmp_path,'source',b'a');target=write(tmp_path,'target',b'b')
    with pytest.raises(FileExistsError):
        org.copy_verified(source,target,org.digest(source))
    assert target.read_bytes()==b'b' and source.read_bytes()==b'a'
