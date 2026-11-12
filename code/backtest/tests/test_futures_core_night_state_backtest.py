from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd

from backtest.futures_core_night_state_backtest import (
    Settings, bucket, candidate_states, simulate, summarize,
)


def test_bucket_boundaries():
    assert bucket(0.25) == '25～50%'
    assert bucket(0.50) == '50～75%'
    assert bucket(1.25) == '>=125%'


def test_no_future_and_both_sides():
    s = Settings(date(2018, 1, 1), date(2018, 1, 10))
    sample = pd.DataFrame([dict(signal_date='2018-01-08',trade_date='2018-01-09',
        core_direction='多',night_trend='跌勢盤', night_open=1000,
        night_close=900,previous_day_close=1010, average_range=200,
        day_open=910, contract_date='201801')])
    states,skip=candidate_states(sample,s)
    assert len(states)==2 and len(skip)==0
    assert set(states.trade_direction)=={'LONG','SHORT'}
    assert set(states.bucket)=={'50～75%'}


def test_ticks_chronological_stop_and_target():
    s = Settings(date(2018, 1, 1), date(2018, 1, 10),cost_points=3)
    def tick(n,p):
        return SimpleNamespace(at=datetime(2018,1,9,8,45,n),price=p)
    result=simulate([tick(0,1000),tick(1,1051),tick(2,900)],'LONG',100,s)
    assert result['exit_reason']=='TAKE_PROFIT'
    assert result['net_points']==47
    result=simulate([tick(0,1000),tick(1,974),tick(2,1100)],'LONG',100,s)
    assert result['exit_reason']=='STOP_LOSS'
    assert result['net_points']==-29


def test_summary():
    frame=pd.DataFrame([dict(strategy='x',trade_date='2018-01-01',entry_time='08:45',net_points=10),
        dict(strategy='x',trade_date='2018-01-02',entry_time='08:45',net_points=-5)])
    result=summarize(frame,['strategy']).iloc[0]
    assert result['trades']==2 and result['pf']==2
