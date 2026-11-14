"""Read-only localhost checks after starting the Dashboard; uses only Python stdlib."""
from __future__ import annotations
import argparse
import json
import sys
from urllib.request import urlopen
from urllib.parse import urlsplit, urlencode
from urllib.error import HTTPError, URLError


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://127.0.0.1:8050')
    args=parser.parse_args()
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8',errors='replace')
    base=args.url.rstrip('/')
    if urlsplit(base).hostname not in ('127.0.0.1','localhost','::1'):
        parser.error('Use your localhost Dashboard URL only.')
    failures=[]
    def request(path,expected=None,parse=False):
        try:
            with urlopen(base+path,timeout=20) as res:
                raw=res.read()
                if expected and expected.encode() not in raw:raise ValueError(f'Missing expected feature: {expected}')
                value=json.loads(raw) if parse else None
                print(f'OK {res.status} {path}')
                return value
        except (HTTPError,URLError,TimeoutError,ValueError) as e:
            detail=''
            if isinstance(e,HTTPError):
                detail=e.read(1500).decode('utf-8',errors='replace')
            print(f'FAIL {path}: {e} {detail}');failures.append(path);return None
    for p,marker in [('/',None),('/replay','ptResync'),('/paper-trade','jTrades'),('/paper-journal','dEditor'),('/kline',None),('/yu-key-levels',None)]:request(p,marker)
    request('/api/health',parse=True)
    accounts=request('/api/paper-trade/accounts',parse=True)
    request('/api/paper-trade/journals?limit=1',parse=True)
    request('/api/journal-media?kind=all',parse=True)
    if isinstance(accounts,list) and accounts:
        aid=accounts[0]['id']
        request('/api/paper-trade/runs?'+urlencode({'account_id':aid}),parse=True)
        request('/api/paper-trade/performance?'+urlencode({'account_id':aid}),parse=True)
    else:print('SKIP account performance: no account available.')
    print('Read-only checks completed. No orders, journals, or market records were modified.')
    if failures:
        print('Failed:',', '.join(failures));return 1
    return 0

if __name__=='__main__':raise SystemExit(main())
