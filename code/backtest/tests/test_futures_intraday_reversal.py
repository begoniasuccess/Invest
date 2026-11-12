from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from backtest.futures_intraday_reversal import Settings, detect_events, run, wilson


def cfg():
    return Settings(start=date(2026,1,2),end=date(2026,1,2),output_dir=Path('/tmp/unused'),
                    points=(100.,),percents=(1.,),vol_multiples=(1.,),
                    reversal_targets=(50.,100.),continuation_targets=(50.,100.),
                    min_tick_coverage_minutes=0)


def make_ticks(prices):
    first=datetime(2026,1,2,8,45)
    times=[first+timedelta(minutes=i) for i in range(len(prices))]
    return pd.DataFrame({'at':times,'price':prices,'source_row_no':range(1,len(prices)+1)})


def test_down_reversal_first():
    e,h=detect_events(date(2026,1,2),'202601',make_ticks([1000,900,850,950,940]),cfg())
    event=[x for x in e if x['direction']=='DOWN' and x['threshold_type']=='POINT'][0]
    assert event['trigger_price']==900
    assert event['mfe_points']==50
    assert event['mae_points']==50
    pair=[x for x in h if pd.notna(x['continuation_target']) and x['threshold_type']=='POINT' and
          x['direction']=='DOWN' and x['reversal_target']==50 and x['continuation_target']==50][0]
    assert pair['first_hit']=='CONTINUATION_FIRST'


def test_up_reversal_first():
    e,h=detect_events(date(2026,1,2),'202601',make_ticks([1000,1100,1050,1150]),cfg())
    event=[x for x in e if x['direction']=='UP' and x['threshold_type']=='POINT'][0]
    assert event['mfe_points']==50
    pair=[x for x in h if pd.notna(x['continuation_target']) and x['direction']=='UP' and
          x['threshold_type']=='POINT' and x['reversal_target']==50 and x['continuation_target']==50][0]
    assert pair['first_hit']=='REVERSAL_FIRST'


def test_no_future_leak_or_repeat():
    e,_=detect_events(date(2026,1,2),'202601',make_ticks([1000,1100,1000,1100,1020]),cfg())
    assert len([x for x in e if x['direction']=='UP' and x['threshold_type']=='POINT'])==1
    assert all(x['trigger_time']!='2026-01-02T08:45:00' for x in e)


def test_wilson():
    assert wilson(0,10)[0]==0
    assert abs(wilson(10,10)[1]-1)<1e-12
