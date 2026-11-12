"""核心方向 × 夜盤走勢 × 夜盤漲跌幅/30日均幅；日盤開盤進場研究。

獨立讀取 PostgreSQL，不修改任何既有表。所有訊號在交易日開盤前已知。
使用 Tick 先後順序處理固定停損停利（不把同分鐘高低點當成先後順序）。
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd

from backtest.futures_stop_trailing import (
    BacktestSettings, StrategyParameters, Threshold, ThresholdMode,
    load_day_ticks_bulk,
)

SOURCE = 'report.v_futures_report_daily_corrected_turning'
BUCKETS = (0.0, 0.25, 0.50, 0.75, 1.00, 1.25, float('inf'))


@dataclass(frozen=True)
class Settings:
    start: date
    end: date
    futures_id: str = 'TX'
    signal_column: str = '策略_核心_五日價量'
    average_range_days: int = 30
    sl_ratio: float = 0.25
    tp_ratio: float = 0.50
    cost_points: float = 0.0
    entry_time: time = time(8, 45)
    exit_time: time = time(13, 45)
    batch_days: int = 5
    gap_limit_points: float | None = None
    amplitude_mode: str = 'NIGHT_CLOSE_MINUS_OPEN'

    def __post_init__(self):
        if self.signal_column not in {'策略_核心_五日價量', '策略_核心_昨價量'}:
            raise ValueError('不支援的核心訊號欄位')
        if self.start > self.end or self.average_range_days < 2 or self.batch_days < 1:
            raise ValueError('日期 / 均幅天數 / 分批天數不正確')
        if min(self.sl_ratio, self.tp_ratio) <= 0 or self.cost_points < 0:
            raise ValueError('停損、停利必須正數，成本不可為負')
        if self.amplitude_mode not in {'NIGHT_CLOSE_MINUS_OPEN', 'NIGHT_CLOSE_MINUS_PREVIOUS_DAY_CLOSE'}:
            raise ValueError('不支援的夜盤漲跌度量')
        if self.gap_limit_points is not None and self.gap_limit_points < 0:
            raise ValueError('跳空限制不可負數')


def load_signals(s: Settings) -> pd.DataFrame:
    from common import db
    # signal_date 的核心/夜盤數值全部取完成時段的歷史列；LEAD 僅映射下一交易日。
    # avg range 在 signal_date 收盤已知，不使用 trade_date 任何行情。
    sql = f'''
    WITH ordered AS (
      SELECT r.futures_id, r."日期" AS signal_date,
        TRIM(r."{s.signal_column}"::text) AS core_direction,
        TRIM(r."夜盤走勢"::text) AS night_trend,
        r."夜盤開"::double precision AS night_open,
        r."夜盤收"::double precision AS night_close,
        r."日盤收"::double precision AS day_close,
        LAG(r."日盤收") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS previous_day_close,
        CASE WHEN COUNT(r."日盤高"-r."日盤低") OVER (
          PARTITION BY r.futures_id ORDER BY r."日期"
          ROWS BETWEEN {s.average_range_days - 1} PRECEDING AND CURRENT ROW
        ) = {s.average_range_days}
        THEN AVG(r."日盤高"-r."日盤低") OVER (
          PARTITION BY r.futures_id ORDER BY r."日期"
          ROWS BETWEEN {s.average_range_days - 1} PRECEDING AND CURRENT ROW
        ) END AS average_range,
        LEAD(r."日期") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS trade_date
      FROM {SOURCE} r WHERE r.futures_id = ?
    ), candidates AS (
      SELECT * FROM ordered WHERE trade_date BETWEEN ? AND ?
    ), opening AS (
      SELECT c.trade_date, k.contract_date, k.open AS day_open,
        ROW_NUMBER() OVER (
          PARTITION BY c.trade_date ORDER BY k.contract_date, k.bar_datetime, k.id
        ) AS rank_contract
      FROM candidates c
      JOIN raw_finmind.fm_taiwan_futures_kbar_1m k
        ON k.trading_date=c.trade_date AND k.futures_id=?
        AND k.trading_session='position' AND k.contract_date ~ '^[0-9]{{6}}$'
        AND k.bar_datetime::time >= ?
        AND k.bar_datetime::time < (? + INTERVAL '1 minute')
    )
    SELECT c.*, o.contract_date, o.day_open
    FROM candidates c LEFT JOIN opening o
      ON c.trade_date=o.trade_date AND o.rank_contract=1
    ORDER BY c.trade_date
    '''
    return db.sql_to_df(sql, params=(s.futures_id, s.start, s.end,
        s.futures_id, s.entry_time, s.entry_time),
        parse_dates=['signal_date', 'trade_date'])


def trend_normalize(raw) -> str | None:
    value = str(raw).strip()
    if value in {'漲勢', '漲勢盤'}:
        return '漲勢'
    if value in {'跌勢', '跌勢盤'}:
        return '跌勢'
    return None


def bucket(v: float) -> str:
    for i in range(len(BUCKETS)-1):
        lo, hi = BUCKETS[i], BUCKETS[i+1]
        if lo <= v < hi:
            return f'{lo*100:g}～{hi*100:g}%' if math.isfinite(hi) else f'>={lo*100:g}%'
    raise ValueError(v)


def candidate_states(frame: pd.DataFrame, s: Settings) -> tuple[pd.DataFrame, pd.DataFrame]:
    out, skips = [], []
    for r in frame.itertuples(index=False):
        trade_date = pd.Timestamp(r.trade_date).date()
        def skip(reason):
            skips.append({'trade_date': trade_date, 'reason': reason})
        core = str(r.core_direction).strip()
        trend = trend_normalize(r.night_trend)
        if core not in {'多', '空'}:
            skip('核心無有效多空'); continue
        if trend is None:
            skip('夜盤走勢缺失'); continue
        nums = [r.night_open, r.night_close, r.average_range, r.day_open]
        if not all(pd.notna(x) and math.isfinite(float(x)) for x in nums):
            skip('缺夜盤價格/均幅/日盤開盤'); continue
        avg = float(r.average_range)
        if avg <= 0:
            skip('均幅無效'); continue
        if pd.isna(r.contract_date):
            skip('無日盤契約'); continue
        if s.amplitude_mode == 'NIGHT_CLOSE_MINUS_PREVIOUS_DAY_CLOSE':
            ref = r.previous_day_close
            if pd.isna(ref) or not math.isfinite(float(ref)):
                skip('無前日日盤收'); continue
        else:
            ref = r.night_open
        move = float(r.night_close) - float(ref)
        ratio = abs(move)/avg
        gap = float(r.day_open) - float(r.night_close)
        # 與先前 2-1 一致：跳空限制只阻擋朝持倉方向的不利價格，非絕對價差。
        for side in ('LONG', 'SHORT'):
            if s.gap_limit_points is not None and (
                (side == 'LONG' and gap > s.gap_limit_points) or
                (side == 'SHORT' and gap < -s.gap_limit_points)
            ):
                skip('跳空篩選'); continue
            key = f'核心{core}_夜{trend}_{"多" if side == "LONG" else "空"}'
            out.append({
                'signal_date': pd.Timestamp(r.signal_date).date(),
                'trade_date': trade_date, 'contract_date': str(r.contract_date),
                'core_direction': core, 'night_trend': trend,
                'trade_direction': side, 'strategy': key,
                'night_reference': float(ref), 'night_close': float(r.night_close),
                'night_move_points': move, 'night_move_direction': '漲' if move > 0 else '跌' if move < 0 else '平',
                'night_move_ratio': ratio, 'bucket': bucket(ratio),
                'average_range': avg, 'day_open': float(r.day_open), 'open_gap': gap,
            })
    return pd.DataFrame(out), pd.DataFrame(skips)


def simulate(ticks, direction: str, avg: float, s: Settings) -> dict:
    if not ticks:
        return {'exit_reason': 'MISSING_TICKS'}
    sign = 1 if direction == 'LONG' else -1
    entry = ticks[0].price
    sl = entry - sign * avg * s.sl_ratio
    tp = entry + sign * avg * s.tp_ratio
    exit_tick, exit_price, reason = ticks[-1], ticks[-1].price, 'CLOSE_EXIT'
    # 直接跳價越過停損，採當筆價格；TP 預設以設定目標價成交，較保守。
    for t in ticks[1:]:
        if sign*(t.price-sl) <= 0:
            exit_tick, exit_price, reason = t, t.price, 'STOP_LOSS'; break
        if sign*(t.price-tp) >= 0:
            exit_tick, exit_price, reason = t, tp, 'TAKE_PROFIT'; break
    gross = sign*(exit_price-entry)
    return {
        'entry_time': ticks[0].at, 'entry_price': entry,
        'exit_time': exit_tick.at, 'exit_price': exit_price,
        'stop_price': sl, 'take_price': tp,
        'gross_points': gross, 'net_points': gross-s.cost_points,
        'exit_reason': reason,
    }


def max_drawdown(points: pd.Series) -> float:
    equity = points.cumsum()
    return float((equity-equity.cummax().clip(lower=0)).min()) if len(equity) else 0.0


def summarize(frame: pd.DataFrame, group: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=group+['trades','wins','win_rate_pct','total_points','avg_points','median_points','pf','max_drawdown'])
    ans=[]
    for keys,g in frame.groupby(group, dropna=False, sort=False):
        if not isinstance(keys, tuple): keys=(keys,)
        p=g.sort_values(['trade_date','entry_time'])['net_points']
        win=p[p>0].sum(); loss=-p[p<0].sum()
        ans.append({**dict(zip(group,keys)),
            'trades':len(g),'wins':int((p>0).sum()), 'win_rate_pct':round((p>0).mean()*100,2),
            'total_points':round(p.sum(),2), 'avg_points':round(p.mean(),2),
            'median_points':round(p.median(),2),
            'pf':round(win/loss,4) if loss else (float('inf') if win else float('nan')),
            'max_drawdown':round(max_drawdown(p),2),
        })
    return pd.DataFrame(ans)


def run(s: Settings, *, candidates: pd.DataFrame | None = None,
        ticks_loader=None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw = load_signals(s) if candidates is None else candidates
    states, skips = candidate_states(raw,s)
    if states.empty:
        return pd.DataFrame(), states, skips
    # 與現有回測共用 bulk loader，但保持固定 SL/TP 的獨立模擬器。
    adapter = BacktestSettings(start=s.start,end=s.end,
        signal_column=s.signal_column,futures_id=s.futures_id,
        entry_time=s.entry_time,forced_exit_time=s.exit_time,
        long_parameters=(StrategyParameters(
            gap_limit=Threshold(ThresholdMode.POINTS,1),
            trailing_activation=Threshold(ThresholdMode.POINTS,1),
            trailing_drawdown=Threshold(ThresholdMode.POINTS,1),
            stop_loss=Threshold(ThresholdMode.POINTS,1)),),
        short_parameters=(StrategyParameters(
            gap_limit=Threshold(ThresholdMode.POINTS,1),
            trailing_activation=Threshold(ThresholdMode.POINTS,1),
            trailing_drawdown=Threshold(ThresholdMode.POINTS,1),
            stop_loss=Threshold(ThresholdMode.POINTS,1)),),
    )
    unique = states[['trade_date','contract_date']].drop_duplicates()
    pairs = list(unique.itertuples(index=False,name=None))
    output=[]
    for pos in range(0,len(pairs),s.batch_days):
        subset=pairs[pos:pos+s.batch_days]
        tick_map = (ticks_loader or load_day_ticks_bulk)(adapter,subset)
        subset_keys=set(subset)
        sub=states[states.apply(lambda r:(r.trade_date,r.contract_date) in subset_keys,axis=1)]
        for r in sub.itertuples(index=False):
            result=simulate(tick_map.get((r.trade_date,r.contract_date),[]),r.trade_direction,r.average_range,s)
            output.append({**r._asdict(),**result})
        print(f'交易日 {min(pos+s.batch_days,len(pairs))}/{len(pairs)}',flush=True)
    return pd.DataFrame(output),states,skips


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--config',type=Path,default=Path(__file__).parent/'configs'/'futures_core_night_state_v1.json')
    a=ap.parse_args()
    cfg=json.loads(a.config.read_text(encoding='utf-8'))
    s=Settings(start=date.fromisoformat(cfg['start']),end=date.fromisoformat(cfg['end']),
        futures_id=cfg.get('futures_id','TX'),
        signal_column=cfg.get('signal_column','策略_核心_五日價量'),
        average_range_days=int(cfg.get('average_range_days',30)),
        sl_ratio=float(cfg.get('sl_ratio',.25)), tp_ratio=float(cfg.get('tp_ratio',.5)),
        cost_points=float(cfg.get('cost_points',0)),
        batch_days=int(cfg.get('batch_days',5)),
        gap_limit_points=cfg.get('gap_limit_points'),
        amplitude_mode=cfg.get('amplitude_mode','NIGHT_CLOSE_MINUS_OPEN'))
    detail,states,skips=run(s)
    out=Path(cfg.get('output_dir','../output/backtest'))
    if not out.is_absolute():out=(Path(__file__).resolve().parents[1]/out).resolve()
    out.mkdir(parents=True,exist_ok=True)
    tag=f'{s.start:%Y%m%d}_{s.end:%Y%m%d}'
    prefix='futures_core_night_state_'+tag
    def save(df,ending):
        path=out/f'{prefix}_{ending}.csv';df.to_csv(path,index=False,encoding='utf-8-sig');print(path)
    save(detail,'detail')
    save(states,'signals')
    save(skips,'skipped')
    eligible=detail[detail['exit_reason']!='MISSING_TICKS'].copy() if not detail.empty else detail
    if not eligible.empty:
        eligible['year']=pd.to_datetime(eligible.trade_date).dt.year
        save(summarize(eligible,['strategy']),'summary')
        save(summarize(eligible,['strategy','bucket']),'bucket_summary')
        save(summarize(eligible,['strategy','year']),'year_summary')
        save(summarize(eligible,['strategy','bucket','year']),'year_bucket_summary')
        save(summarize(eligible,['strategy','night_move_direction','bucket']),'direction_bucket_summary')
        save(summarize(eligible,['core_direction','trade_direction']),'core_baseline')
        save(summarize(eligible,['night_trend','trade_direction']),'night_baseline')
    else:
        print('沒有可確認成交的交易，請檢查契約或 Tick 資料')

if __name__=='__main__':main()
