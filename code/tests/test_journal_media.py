from fastapi import FastAPI
from fastapi.testclient import TestClient
from dashboard import journal_media as m
from dashboard.journal_service import render_markdown
import pytest

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'ROOT',tmp_path/'journal_media')
    app=FastAPI(); app.include_router(m.router)
    return TestClient(app)

def upload(client,name,data=b'example'):
    return client.post('/api/journal-media/upload',params={'filename':name},content=data,headers={'Content-Type':'application/octet-stream'})

def test_upload_list_reuse_and_range(client):
    r=upload(client,'走勢圖.png'); assert r.status_code==200
    image=r.json(); assert image['kind']=='image' and image['display_name']=='走勢圖.png'
    assert client.get(image['url']).content==b'example'
    assert '<img' in render_markdown(f'![圖]({image["url"]})')
    video=upload(client,'回放.mp4',b'0123456789').json()
    assert '<video controls' in render_markdown(f'![影片]({video["url"]})')
    r=client.get(video['url'],headers={'Range':'bytes=2-4'}); assert r.status_code==206 and r.content==b'234'
    assert r.headers['x-content-type-options']=='nosniff'
    assert len(client.get('/api/journal-media',params={'kind':'video'}).json()['items'])==1
    assert len(client.get('/api/journal-media',params={'keyword':'走勢'}).json()['items'])==1
    duplicate=upload(client,'走勢圖.png').json(); assert duplicate['name']!=image['name']
    assert client.get(image['url']).content==b'example'

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
