"""Local project media library. Only explicit allowlisted files are served."""
from pathlib import Path
from uuid import uuid4
from urllib.parse import quote, unquote
import re
from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import FileResponse

router = APIRouter(prefix='/api/journal-media', tags=['Journal media'])
# dashboard/ -> code/ -> project root; separate from source and database.
ROOT = Path(__file__).resolve().parents[2] / 'journal_media'
TYPES = {'.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.gif':'image/gif',
         '.webp':'image/webp','.avif':'image/avif','.bmp':'image/bmp',
         '.mp4':'video/mp4','.webm':'video/webm','.ogg':'video/ogg','.ogv':'video/ogg',
         '.mov':'video/quicktime','.pdf':'application/pdf','.txt':'text/plain',
         '.csv':'text/csv','.zip':'application/zip'}
PREFIX = '/api/journal-media/files/'
MAX_BYTES = 500 * 1024 * 1024

def kind_for(name):
    mime = TYPES.get(Path(name).suffix.lower(), '')
    return 'image' if mime.startswith('image/') else 'video' if mime.startswith('video/') else 'file'

def valid_name(name):
    return (bool(name) and len(name)<=240 and name not in ('.','..') and not name.startswith('.')
            and not any(c in name for c in '/\\:\x00') and not any(ord(c)<32 for c in name)
            and Path(name).suffix.lower() in TYPES)

def media_root():
    ROOT.mkdir(parents=True, exist_ok=True)
    if ROOT.is_symlink(): raise HTTPException(500,'媒體資料夾不可為符號連結')
    return ROOT.resolve()

def resolve_file(name):
    if not valid_name(name): raise HTTPException(404,'找不到檔案')
    root=media_root(); path=root/name
    if path.is_symlink() or not path.is_file() or path.resolve().parent != root:
        raise HTTPException(404,'找不到檔案')
    return path

def file_info(path):
    name=path.name
    display=re.sub(r'^[0-9a-f]{32}_','',name)
    return {'name':name,'display_name':display,'kind':kind_for(name),'size':path.stat().st_size,
            'url':PREFIX+quote(name,safe='')}

@router.get('')
def list_files(keyword: str=Query('',max_length=200), kind: str=Query('all',pattern='^(all|image|video|file)$')):
    files=[]
    for path in media_root().iterdir():
        if not valid_name(path.name) or path.is_symlink() or not path.is_file(): continue
        info=file_info(path)
        if keyword.casefold() not in info['display_name'].casefold() or kind!='all' and kind!=info['kind']: continue
        files.append(info)
    return {'items':sorted(files,key=lambda x:x['display_name'].casefold())}

@router.post('/upload')
async def upload(request: Request, filename: str=Query(...,max_length=240)):
    # A raw request body avoids multipart dependencies and permits bounded streaming.
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/octet-stream":
        raise HTTPException(415,"請使用檔案上傳功能")
    if not valid_name(filename): raise HTTPException(400,'不支援的副檔名或檔名')
    root=media_root(); suffix=Path(filename).suffix.lower()
    # Avoid Windows reserved characters and overly long UTF-8 filenames.
    stem=re.sub(r'[^\w.\- ]','_',Path(filename).stem,flags=re.UNICODE).strip(' .')[:48] or 'file'
    name=uuid4().hex+'_'+stem+suffix; dest=root/name; temp=root/('.'+uuid4().hex+'.upload')
    size=0
    try:
        with temp.open('xb') as out:
            async for chunk in request.stream():
                size+=len(chunk)
                if size>MAX_BYTES: raise HTTPException(413,'單檔上限 500 MB')
                out.write(chunk)
        if not size: raise HTTPException(400,'檔案不可為空')
        temp.rename(dest)
    finally:
        temp.unlink(missing_ok=True)
    return file_info(dest)

@router.get('/files/{name}')
@router.head('/files/{name}')
def serve(name: str):
    path=resolve_file(name); mime=TYPES[path.suffix.lower()]
    # Browser-renderable media only; all other files download instead of executing.
    inline=kind_for(name) in ('image','video')
    return FileResponse(path,media_type=mime,filename=path.name,
                        content_disposition_type='inline' if inline else 'attachment',
                        headers={'X-Content-Type-Options':'nosniff','Cache-Control':'private, max-age=0'})

def local_video_url(src):
    if not src.startswith(PREFIX): return False
    name=unquote(src[len(PREFIX):])
    return valid_name(name) and kind_for(name)=='video'
