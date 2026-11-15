from datetime import date
from unittest.mock import MagicMock, patch
import pytest
from dashboard import journal_service as s, paper_trade_service as p
from dashboard.journal_api import Journal, router


def test_tags_normalized_and_deduplicated():
    assert s.normalize_tags([' MA20 ', 'ma20', '恐慌性殺盤', ' ', '支撐']) == ['MA20','恐慌性殺盤','支撐']
    for tags in [['x'*51], ['a\nb'], ['x']*31]:
        with pytest.raises(ValueError): s.normalize_tags(tags)


def test_save_tags_with_existing_atomic_write():
    data=Journal(title='回測',journal_date=date(2026,11,15),tags=['  DIF ', 'dif','轉折']).model_dump()
    conn=MagicMock(); cur=MagicMock()
    with patch.object(p,'connection') as c,patch.object(p,'cursor') as cc,patch.object(p,'one',side_effect=[{'id':1},{'id':1,'tags':['DIF','轉折']}]) as one,patch.object(p,'many',return_value=[]):
        c.return_value.__enter__.return_value=conn;cc.return_value.__enter__.return_value=cur
        assert s.save(data)['tags']==['DIF','轉折']
    sql,args=one.call_args_list[0].args[1:]
    assert 'journal_date,tags' in sql and args[-1]==['DIF','轉折']
    conn.commit.assert_called_once()


@pytest.mark.parametrize('sort,expected',[('asc','ASC'),('desc','DESC')])
def test_tag_filter_and_date_sort(sort,expected):
    with patch.object(p,'connection',return_value=MagicMock()),patch.object(p,'cursor',return_value=MagicMock()),patch.object(p,'many',return_value=[]) as many:
        s.search(tag="x' OR 1=1",sort=sort,keyword='DIF',limit=25,offset=25)
    sql,args=many.call_args.args[1:]
    assert 'lower(t.tag)=lower(%s)' in sql and "x' OR 1=1" not in sql
    assert f'ORDER BY j.journal_date {expected},j.id {expected}' in sql
    assert args==('DIF',"x' OR 1=1",26,25)


def test_tag_catalog_and_api_route_order():
    with patch.object(p,'connection',return_value=MagicMock()),patch.object(p,'cursor',return_value=MagicMock()),patch.object(p,'many',return_value=[{'tag':'DIF','count':2}]) as many:
        assert s.list_tags()==[{'tag':'DIF','count':2}]
        assert 'GROUP BY lower(t.tag)' in many.call_args.args[1]
    paths=[r.path for r in router.routes]
    assert paths.index('/api/paper-trade/journals/tags') < paths.index('/api/paper-trade/journals/{journal_id}')
    with pytest.raises(ValueError): s.search(sort='invalid')
