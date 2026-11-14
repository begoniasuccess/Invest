"""Independent Markdown journals; execution records are never updated here."""
from dashboard import paper_trade_service as p

VIEW='paper_trade.journal_link_details'

def links(cur, journal_id):
    return p.many(cur,f'SELECT * FROM {VIEW} WHERE journal_id=%s ORDER BY id',(journal_id,))

def get_one(cur, journal_id):
    row=p.require(p.one(cur,'SELECT * FROM paper_trade.journals WHERE id=%s',(journal_id,)),'日記')
    return {**row,'links':links(cur,journal_id)}

def get(journal_id):
    with p.connection() as conn,p.cursor(conn) as cur:
        return get_one(cur,journal_id)

def search(account_id=None,run_id=None,trade_id=None,history_date=None,start=None,end=None,keyword=None,limit=50,offset=0,link_kind=None):
    if start and end and start>end: raise ValueError('起始日期不能晚於結束日期')
    sql='SELECT j.* FROM paper_trade.journals j WHERE TRUE'; args=[]
    conditions=[]; linkargs=[]
    for col,val in [('account_id',account_id),('run_id',run_id)]:
        if val is not None: conditions.append('l.'+col+'=%s');linkargs.append(val)
    if trade_id is not None:
        conditions.append("(l.trade_id=%s OR (l.kind='order' AND l.entry_order_id=(SELECT order_id FROM paper_trade.trades WHERE id=%s)))")
        linkargs.extend((trade_id,trade_id))
    if link_kind is not None:
        # Old trade backlinks asked for kind=trade; include the stable opening group too.
        if link_kind=='trade' and trade_id is not None: conditions.append("l.kind IN ('trade','order')")
        else: conditions.append('l.kind=%s');linkargs.append(link_kind)
    if conditions:
        sql+=f' AND EXISTS (SELECT 1 FROM {VIEW} l WHERE l.journal_id=j.id AND '+ ' AND '.join(conditions)+')';args+=linkargs
    if history_date: sql+=f' AND EXISTS (SELECT 1 FROM {VIEW} d WHERE d.journal_id=j.id AND d.historical_date=%s)';args.append(history_date)
    if start: sql+=' AND j.journal_date >= %s';args.append(start)
    if end: sql+=' AND j.journal_date <= %s';args.append(end)
    if keyword: sql+=' AND strpos(lower(j.title || chr(10) || j.body),lower(%s))>0';args.append(keyword)
    sql+=' ORDER BY j.journal_date DESC,j.id DESC LIMIT %s OFFSET %s';args.extend([limit+1,offset])
    with p.connection() as conn,p.cursor(conn) as cur:
        rows=p.many(cur,sql,tuple(args));return {'items':[{**r,'links':links(cur,r['id'])} for r in rows[:limit]],'has_more':len(rows)>limit}

def save(data,journal_id=None):
    title=data['title'].strip()
    if not title: raise ValueError('請輸入日記標題')
    with p.connection() as conn,p.cursor(conn) as cur:
        from dashboard.journal_media import media_names, resolve_file
        # The table lock conflicts with media deletion's SHARE ROW EXCLUSIVE lock.
        # An existing broken reference can be kept/removed; newly added references must exist.
        cur.execute('LOCK TABLE paper_trade.journals IN ROW EXCLUSIVE MODE')
        old_body=''
        if journal_id is None:
            for name in media_names(data['body']): resolve_file(name)
            row=p.one(cur,'INSERT INTO paper_trade.journals(title,body,journal_date) VALUES(%s,%s,%s) RETURNING id',(title,data['body'],data['journal_date']));journal_id=row['id']
        else:
            row=p.require(p.one(cur,'SELECT id,updated_at,body FROM paper_trade.journals WHERE id=%s FOR UPDATE',(journal_id,)),'日記')
            if data.get('expected_updated_at') != row['updated_at']: raise ValueError('日記已在其他視窗修改，請重新開啟後再儲存')
            for name in media_names(data['body'])-media_names(row.get('body','')): resolve_file(name)
            cur.execute('UPDATE paper_trade.journals SET title=%s,body=%s,journal_date=%s,updated_at=clock_timestamp() WHERE id=%s',(title,data['body'],data['journal_date'],journal_id))
        # All edits are atomic, including replacing links. FK validation prevents dangling targets.
        seen=set(); targets=[]
        for original in data['links']:
            l=dict(original)
            if l['kind']=='trade':
                # Open positions are linked by immutable order id so later partial closes stay related.
                trade=p.require(p.one(cur,'SELECT order_id,exit_time FROM paper_trade.trades WHERE id=%s',(l['target_id'],)),'交易')
                if trade['exit_time'] is None: l.update(kind='order',target_id=trade['order_id'])
            if l['kind']=='order':
                p.require(p.one(cur,"SELECT id FROM paper_trade.orders WHERE id=%s AND status='FILLED' AND (EXISTS (SELECT 1 FROM paper_trade.trades WHERE order_id=paper_trade.orders.id) OR EXISTS (SELECT 1 FROM paper_trade.journal_links WHERE entry_order_id=paper_trade.orders.id AND journal_id=%s))",(l['target_id'],journal_id)),'已成交開倉委託')
            key=(l['kind'],l.get('target_id'),l.get('historical_date'))
            if key in seen: continue
            seen.add(key)
            cols={'account':'account_id','run':'run_id','trade':'trade_id','date':'historical_date','order':'entry_order_id'}
            col=cols[l['kind']]; value=l['historical_date'] if l['kind']=='date' else l['target_id']
            targets.append((col,value))
        cur.execute('DELETE FROM paper_trade.journal_links WHERE journal_id=%s',(journal_id,))
        for col,value in targets:
            cur.execute(f'INSERT INTO paper_trade.journal_links(journal_id,{col}) VALUES(%s,%s)',(journal_id,value))
        result=get_one(cur,journal_id);conn.commit();return result

def delete(journal_id, expected_updated_at):
    with p.connection() as conn,p.cursor(conn) as cur:
        row=p.require(p.one(cur,'SELECT updated_at FROM paper_trade.journals WHERE id=%s FOR UPDATE',(journal_id,)),'日記')
        if expected_updated_at != row['updated_at']: raise ValueError('日記已在其他視窗修改，請重新開啟後再刪除')
        cur.execute('DELETE FROM paper_trade.journals WHERE id=%s',(journal_id,));conn.commit();return {'deleted':journal_id}

def render_markdown(body):
    # Raw HTML disabled, unsafe URL protocols rejected by markdown-it.
    from markdown_it import MarkdownIt
    from html import escape
    from dashboard.journal_media import local_video_url
    md=MarkdownIt('commonmark',{'html':False,'breaks':True}).enable(['table','strikethrough'])
    default_image=md.renderer.rules['image']
    def image_rule(tokens, idx, options, env):
        token=tokens[idx]; src=token.attrGet('src') or ''
        if local_video_url(src):
            safe=escape(src,quote=True)
            return f'<video controls preload="metadata" playsinline src="{safe}"></video>'
        return default_image(tokens,idx,options,env)
    md.renderer.rules['image']=image_rule
    return md.render(body)
