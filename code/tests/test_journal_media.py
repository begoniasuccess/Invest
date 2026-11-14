from fastapi import FastAPI
from fastapi.testclient import TestClient
from dashboard import journal_media as m
from dashboard.journal_service import render_markdown
import pytest
from io import BytesIO
from PIL import Image

def png():
    out=BytesIO(); Image.new("RGB",(2,2)).save(out,format="PNG"); return out.getvalue()
PNG=png()
VIDEO=b"\x00\x00\x00\x18ftypisom0123456789"

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'ROOT',tmp_path/'journal_media')
    app=FastAPI(); app.include_router(m.router)
    return TestClient(app)

def upload(client,name,data=PNG):
    return client.post('/api/journal-media/upload',params={'filename':name},content=data,headers={'Content-Type':'application/octet-stream'})

def test_upload_list_reuse_and_range(client):
    r=upload(client,'走勢圖.png'); assert r.status_code==200
    image=r.json(); assert image['kind']=='image' and image['display_name']=='走勢圖.png'
    assert client.get(image['url']).content==PNG
    assert '<img' in render_markdown(f'![圖]({image["url"]})')
    video=upload(client,'回放.mp4',VIDEO).json()
    assert '<video controls' in render_markdown(f'![影片]({video["url"]})')
    r=client.get(video['url'],headers={'Range':'bytes=2-4'}); assert r.status_code==206 and r.content==VIDEO[2:5]
    assert r.headers['x-content-type-options']=='nosniff'
    assert len(client.get('/api/journal-media',params={'kind':'video'}).json()['items'])==1
    assert len(client.get('/api/journal-media',params={'keyword':'走勢'}).json()['items'])==1
    duplicate=upload(client,'走勢圖.png').json(); assert duplicate['name']!=image['name']
    assert client.get(image['url']).content==PNG

def test_manual_file_download(client):
    root=m.media_root(); (root/'報告.pdf').write_bytes(b'pdf')
    data=client.get('/api/journal-media').json()['items'][0]; assert data['kind']=='file'
    assert 'attachment' in client.get(data['url']).headers['content-disposition']

def test_reject_unsafe_and_oversized(client,monkeypatch,tmp_path):
    for name in ['../x.png','a\\x.png','evil.svg','evil.html','x.exe']:
        assert upload(client,name).status_code==400
    assert upload(client,'empty.png',b'').status_code==400
    monkeypatch.setattr(m,'MAX_BYTES',3)
    assert upload(client,'big.mp4',b'1234').status_code==413
    assert list(m.media_root().iterdir())==[]
    outside=tmp_path/'secret.png'; outside.write_bytes(b'secret')
    (m.media_root()/'link.png').symlink_to(outside)
    assert client.get('/api/journal-media/files/link.png').status_code==404
    assert client.get('/api/journal-media').json()['items']==[]

def test_safe_markdown():
    assert '<video' not in render_markdown('![x](https://example.com/a.mp4)')
    assert '<video' not in render_markdown('![x](/api/journal-media/files/%2e%2e%2fx.mp4)')
    assert '<script>' not in render_markdown('<script>alert(1)</script>')


def test_reject_wrong_media_and_rename_without_changing_url(client):
    assert upload(client,'fake.png',b'not an image').status_code==400
    assert upload(client,'fake.mp4',b'not a video').status_code==400
    data=upload(client,'原名.png').json()
    r=client.put('/api/journal-media/rename/'+data['name'],json={'display_name':'新名稱.png'})
    assert r.status_code==200 and r.json()['url']==data['url']
    assert r.json()['display_name']=='新名稱.png'
    assert client.get(data['url']).content==PNG
    assert client.put('/api/journal-media/rename/'+data['name'],json={'display_name':'wrong.mp4'}).status_code==400
    assert client.get('/api/journal-media',params={'keyword':'新名稱'}).json()['items'][0]['name']==data['name']


def test_media_delete_blocks_referenced_files(client,monkeypatch):
    from dashboard import paper_trade_service as p
    from unittest.mock import MagicMock
    data=upload(client,'example.png').json()
    monkeypatch.setattr(p,'connection',lambda:MagicMock())
    monkeypatch.setattr(p,'cursor',lambda conn:MagicMock())
    monkeypatch.setattr(m,'references',lambda cur,name:[{'id':1,'title':'Research','journal_date':'2026-11-06'}])
    assert client.get('/api/journal-media/references/'+data['name']).json()['items'][0]['id']==1
    assert client.delete(data['url']).status_code==409
    assert client.get(data['url']).status_code==200
    monkeypatch.setattr(m,'references',lambda cur,name:[])
    assert client.delete(data['url']).status_code==200
    assert client.get(data['url']).status_code==404
