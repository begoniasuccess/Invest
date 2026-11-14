from datetime import date,datetime,timezone
from unittest.mock import MagicMock,patch
import pytest
from pydantic import ValidationError
from dashboard import journal_service as s
from dashboard.journal_api import Journal,Link
from dashboard import paper_trade_service as p

DAY=date(2026,11,6)
NOW=datetime(2026,11,13,tzinfo=timezone.utc)

def test_typed_links():
    assert Link(kind='trade',target_id=10).target_id==10
    assert Link(kind='date',historical_date=DAY).historical_date==DAY
    assert Link(kind='account',target_id=2).target_id==2
    assert Link(kind='run',target_id=4).target_id==4
    for payload in [dict(kind='date',target_id=2),dict(kind='run'),dict(kind='trade',target_id=2,historical_date=DAY),dict(kind='run',target_id=-1)]:
        with pytest.raises(ValidationError): Link(**payload)

def test_markdown_and_unsafe_content():
    html=s.render_markdown('# 研究\n\n**粗體**\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n\n<script>alert(1)</script>\n\n[x](javascript:alert(1))\n\n[安全](https://example.com)')
    assert '<h1>研究</h1>' in html and '<strong>粗體</strong>' in html and '<table>' in html
    assert '<script>' not in html and 'href="javascript:' not in html and 'href="https://example.com"' in html

def test_create_multiple_links_without_touching_executions():
    data=Journal(title='策略研究',body='## 研究',journal_date=DAY,links=[Link(kind='trade',target_id=7),Link(kind='trade',target_id=7),Link(kind='run',target_id=3),Link(kind='date',historical_date=DAY),Link(kind='account',target_id=1)]).model_dump()
    conn=MagicMock();cur=MagicMock()
    with patch.object(p,'connection') as c,patch.object(p,'cursor') as cc,patch.object(p,'one',side_effect=[{'id':42},{'order_id':70,'exit_time':NOW},{'order_id':70,'exit_time':NOW},{'id':42,'title':'策略研究'}]),patch.object(p,'many',return_value=[]):
        c.return_value.__enter__.return_value=conn;cc.return_value.__enter__.return_value=cur
        assert s.save(data)['id']==42
    stmts=[x.args for x in cur.execute.call_args_list]
    assert len(stmts)==6 and stmts[1][0].startswith('DELETE FROM paper_trade.journal_links')
    assert [x[1] for x in stmts[2:]]==[(42,7),(42,3),(42,DAY),(42,1)]
    conn.commit.assert_called_once()

def test_edit_concurrency_guard():
    data=Journal(title='新標題',journal_date=DAY,expected_updated_at=NOW).model_dump()
    cur=MagicMock()
    with patch.object(p,'connection',return_value=MagicMock()),patch.object(p,'cursor') as c,patch.object(p,'one',return_value={'id':1,'updated_at':datetime(2026,11,14,tzinfo=timezone.utc)}):
        c.return_value.__enter__.return_value=cur
        with pytest.raises(ValueError,match='其他視窗'): s.save(data,1)
    cur.execute.assert_called_once_with('LOCK TABLE paper_trade.journals IN ROW EXCLUSIVE MODE')

def test_search_parameterized_and_independent_date_link():
    with patch.object(p,'connection',return_value=MagicMock()),patch.object(p,'cursor',return_value=MagicMock()),patch.object(p,'many',side_effect=[[{'id':1},{'id':2}],[]]) as m:
        result=s.search(account_id=2,run_id=3,trade_id=4,history_date=DAY,keyword="x' OR 1=1",limit=1,link_kind='trade')
        sql,args=m.call_args_list[0].args[1:]
        assert 'l.account_id=%s' in sql and 'l.run_id=%s' in sql and 'l.trade_id=%s' in sql and "l.kind IN ('trade','order')" in sql
        assert 'd.historical_date=%s' in sql and "x' OR 1=1" not in sql
        assert args==(2,3,4,4,DAY,"x' OR 1=1",2,0) and result['has_more']

def test_delete_only_journal():
    conn=MagicMock();cur=MagicMock()
    with patch.object(p,'connection') as c,patch.object(p,'cursor') as cc,patch.object(p,'one',return_value={'id':1,'updated_at':NOW}) as one:
        c.return_value.__enter__.return_value=conn;cc.return_value.__enter__.return_value=cur
        assert s.delete(1,NOW)=={'deleted':1}
        assert 'FOR UPDATE' in one.call_args.args[1]
        cur.execute.assert_called_once_with('DELETE FROM paper_trade.journals WHERE id=%s',(1,))
        conn.commit.assert_called_once()
