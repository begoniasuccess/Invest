"""TX 日盤開盤漲跌後反轉事件研究（V1.0；不模擬成交）。

執行：python -m backtest.futures_intraday_reversal --config backtest/configs/futures_intraday_reversal_v1.json
讀取指定實際月契約之 tick；相同門檻/日期/方向僅取第一次觸及。
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Settings:
    start: date
    end: date
    output_dir: Path
    points: tuple[float, ...]
    percents: tuple[float, ...]
    vol_multiples: tuple[float, ...]
    reversal_targets: tuple[float, ...]
    continuation_targets: tuple[float, ...]
    min_tick_coverage_minutes: int = 295
    max_tick_gap_minutes: int = 15
    futures_id: str = "TX"
    split_date: date = date(2024, 1, 1)


def load_config(path: Path) -> Settings:
    raw = json.loads(path.read_text(encoding="utf-8"))
    th = raw["thresholds"]
    def numbers(values):
        ans = tuple(sorted(set(float(x) for x in values)))
        if not ans or any(not math.isfinite(x) or x <= 0 for x in ans):
            raise ValueError("門檻必須為正數且不得為空")
        return ans
    cfg = Settings(
        start=date.fromisoformat(raw["start"]), end=date.fromisoformat(raw["end"]),
        output_dir=Path(raw.get("output_dir", "../output/backtest/intraday_reversal")),
        points=numbers(th["points"]), percents=numbers(th["percents"]),
        vol_multiples=numbers(th["vol_multiples"]),
        reversal_targets=numbers(raw["reversal_targets"]),
        continuation_targets=numbers(raw["continuation_targets"]),
        min_tick_coverage_minutes=int(raw.get("min_tick_coverage_minutes", 295)),
        max_tick_gap_minutes=int(raw.get("max_tick_gap_minutes", 15)),
        futures_id=raw.get("futures_id", "TX"),
        split_date=date.fromisoformat(raw.get("split_date", "2024-01-01")),
    )
    if cfg.start > cfg.end:
        raise ValueError("起始日期必須不晚於結束日期")
    return cfg


def load_days(cfg: Settings) -> pd.DataFrame:
    """從日盤08:45第一根1分K鎖定實際月契約；只用之前20日振幅。"""
    from common import db
    sql = '''
    WITH d AS (
      SELECT "日期" AS trade_date, "日盤高" - "日盤低" AS day_range,
        COUNT("日盤高" - "日盤低") OVER (
          ORDER BY "日期" ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prev_count,
        AVG("日盤高" - "日盤低") OVER (
          ORDER BY "日期" ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prev20_range
      FROM core.futures_report_daily_raw
      WHERE futures_id = ?
    ), candidates AS (
      SELECT k.trading_date, k.contract_date, k.bar_datetime, k.open,
         ROW_NUMBER() OVER (PARTITION BY k.trading_date
           ORDER BY k.contract_date, k.bar_datetime) AS rn
      FROM raw_finmind.fm_taiwan_futures_kbar_1m k
      WHERE k.futures_id = ? AND k.trading_session = 'position'
        AND k.contract_date ~ '^[0-9]{6}$'
        AND k.trading_date BETWEEN ? AND ?
        AND k.bar_datetime::time >= TIME '08:45:00'
        AND k.bar_datetime::time < TIME '08:46:00'
    )
    SELECT c.trading_date AS trade_date, c.contract_date,
           c.open AS minute_open, d.prev20_range, d.prev_count
    FROM candidates c LEFT JOIN d ON d.trade_date = c.trading_date
    WHERE c.rn = 1 ORDER BY c.trading_date
    '''
    return db.sql_to_df(sql, params=(cfg.futures_id, cfg.futures_id, cfg.start, cfg.end),
                        parse_dates=["trade_date"])


def load_ticks(cfg: Settings, day: date, contract: str) -> pd.DataFrame:
    from common import db
    sql = '''
       SELECT trade_datetime AS at, price, source_row_no
       FROM raw_finmind.fm_taiwan_futures_tick
       WHERE futures_id = ? AND contract_date = ? AND source_date = ?
         AND trade_datetime >= ? AND trade_datetime <= ?
       ORDER BY trade_datetime, source_row_no
    '''
    return db.sql_to_df(sql, params=(
        cfg.futures_id, contract, day, datetime.combine(day, time(8, 45)),
        datetime.combine(day, time(13, 45))), parse_dates=["at"])


def _first_true(mask: np.ndarray) -> int | None:
    hits = np.flatnonzero(mask)
    return int(hits[0]) if len(hits) else None


def _clock_bucket(ts: pd.Timestamp) -> str:
    hhmm = ts.hour * 60 + ts.minute
    if hhmm < 570: return "08:45-09:29"
    if hhmm < 630: return "09:30-10:29"
    if hhmm < 690: return "10:30-11:29"
    if hhmm < 750: return "11:30-12:29"
    return "12:30-13:45"


def detect_events(day: date, contract: str, tick: pd.DataFrame, cfg: Settings,
                  prev20_range: float | None = None) -> tuple[list[dict], list[dict]]:
    """tick 事件；觸發 tick 自身不拿來推斷反轉和續跌，僅比較後續 tick。"""
    if tick.empty:
        return [], []
    prices = tick["price"].to_numpy(dtype=float)
    times = pd.to_datetime(tick["at"]).reset_index(drop=True)
    if not np.all(np.isfinite(prices)) or (prices <= 0).any():
        raise ValueError("tick 價格非正數或非有限值")
    day_open = float(prices[0]); close = float(prices[-1])
    thresholds = [("POINT", x, x) for x in cfg.points]
    thresholds += [("PCT", x, day_open*x/100) for x in cfg.percents]
    if prev20_range is not None and math.isfinite(prev20_range) and prev20_range > 0:
        thresholds += [("VOL", x, prev20_range*x) for x in cfg.vol_multiples]
    details, hits = [], []
    for direction in ("UP", "DOWN"):
        sign = 1 if direction == "UP" else -1
        change = sign*(prices-day_open)
        for kind, value, threshold in thresholds:
            first = _first_true(change >= threshold - 1e-9)
            if first is None:
                continue
            theoretical = day_open + sign*threshold
            # event 的反轉方向是觸發漲跌的反方向
            future = prices[first+1:]
            reversal = -sign*(future-theoretical)
            continuation = sign*(future-theoretical)
            mfe = max(0., float(np.max(reversal))) if len(future) else 0.
            mae = max(0., float(np.max(continuation))) if len(future) else 0.
            trigger_at = times.iloc[first]
            future_times = times.iloc[first+1:].reset_index(drop=True)
            gap = (datetime.combine(day, time(13,45)) - trigger_at.to_pydatetime()).total_seconds()/60
            base = dict(trade_date=day.isoformat(), year=day.year,
                        period="explore" if day < cfg.split_date else "validate",
                        direction=direction, threshold_type=kind, threshold_value=value,
                        threshold_points=round(threshold,6), contract_date=contract,
                        day_open=day_open, trigger_time=trigger_at.isoformat(),
                        trigger_price=round(theoretical,6), first_cross_price=float(prices[first]),
                        source_row_no=int(tick.iloc[first]["source_row_no"]),
                        minutes_remaining=round(gap,4), time_bucket=_clock_bucket(trigger_at),
                        day_close=close, future_high=float(np.max(future)) if len(future) else np.nan,
                        future_low=float(np.min(future)) if len(future) else np.nan,
                        mfe_points=round(mfe,6), mae_points=round(mae,6),
                        close_reversal_points=round(float(-sign*(close-theoretical)),6),
                        return_to_open=bool(-sign*(close-day_open)>=0),
                        previous20_range=prev20_range,
                        data_quality="TICK_OK")
            details.append(base)
            for target in cfg.reversal_targets:
                rev_idx = _first_true(reversal >= target - 1e-9)
                hits.append(dict(**{k:base[k] for k in (
                    "trade_date","year","period","direction","threshold_type",
                    "threshold_value","threshold_points","time_bucket","minutes_remaining")},
                    reversal_target=target, continuation_target=np.nan,
                    reversal_hit=rev_idx is not None,
                    continuation_hit=np.nan, first_hit="NOT_APPLICABLE",
                    reversal_minutes=(future_times.iloc[rev_idx]-trigger_at).total_seconds()/60
                        if rev_idx is not None else np.nan))
            for target in cfg.reversal_targets:
                r_idx = _first_true(reversal >= target - 1e-9)
                for adverse in cfg.continuation_targets:
                    c_idx = _first_true(continuation >= adverse - 1e-9)
                    if r_idx is None and c_idx is None: label = "NEITHER"
                    elif c_idx is None or (r_idx is not None and r_idx < c_idx): label = "REVERSAL_FIRST"
                    elif r_idx is None or c_idx < r_idx: label = "CONTINUATION_FIRST"
                    else: label = "SAME_TICK"
                    hits.append(dict(**{k:base[k] for k in (
                        "trade_date","year","period","direction","threshold_type",
                        "threshold_value","threshold_points","time_bucket","minutes_remaining")},
                        reversal_target=target, continuation_target=adverse,
                        reversal_hit=r_idx is not None, continuation_hit=c_idx is not None,
                        first_hit=label,
                        reversal_minutes=(future_times.iloc[r_idx]-trigger_at).total_seconds()/60
                            if r_idx is not None else np.nan))
    return details,hits


def wilson(success: int, total: int) -> tuple[float,float]:
    if not total: return (float('nan'),float('nan'))
    z = 1.959963984540054
    p = success/total; den=1+z*z/total
    mid=(p+z*z/(2*total))/den
    width=z*math.sqrt(p*(1-p)/total + z*z/(4*total*total))/den
    return max(0,mid-width),min(1,mid+width)


def probability_summary(hits: pd.DataFrame, grouping: list[str], first_hit: bool) -> pd.DataFrame:
    result=[]
    for key, group in hits.groupby(grouping, dropna=False, sort=True):
        if not isinstance(key,tuple):key=(key,)
        row=dict(zip(grouping,key)); n=len(group)
        if first_hit:
            success=int((group.first_hit=="REVERSAL_FIRST").sum())
            cont=int((group.first_hit=="CONTINUATION_FIRST").sum())
            neither=int((group.first_hit=="NEITHER").sum())
            tie=int((group.first_hit=="SAME_TICK").sum())
            row.update(continuation_first_count=cont, neither_count=neither,
                       ambiguous_count=tie,
                       decisive_reversal_rate=success/(success+cont) if success+cont else np.nan)
        else:
            success=int(group.reversal_hit.sum())
        lo,hi=wilson(success,n)
        row.update(sample_count=n, reversal_count=success, reversal_rate=success/n,
                   wilson95_low=lo,wilson95_high=hi, low_sample=n<30)
        result.append(row)
    return pd.DataFrame(result)


def event_summary(events: pd.DataFrame, grouping: list[str]) -> pd.DataFrame:
    records=[]
    for key,g in events.groupby(grouping,dropna=False,sort=True):
        if not isinstance(key,tuple):key=(key,)
        record=dict(zip(grouping,key));record["sample_count"]=len(g)
        for field in ("mfe_points","mae_points","close_reversal_points"):
            record.update({f"{field}_{k}":float(v) for k,v in zip(
                ("mean","p25","p50","p75","p90","p95"),
                (g[field].mean(),*g[field].quantile([.25,.5,.75,.9,.95]).values))})
        record["close_reversal_rate"]=float((g.close_reversal_points>0).mean())
        record["return_to_open_rate"]=float(g.return_to_open.mean())
        records.append(record)
    return pd.DataFrame(records)


def build_reports(events: pd.DataFrame, hits: pd.DataFrame) -> dict[str,pd.DataFrame]:
    base=["direction","threshold_type","threshold_value","threshold_points"]
    rev=hits[hits.continuation_target.isna()].copy()
    pair=hits[hits.continuation_target.notna()].copy()
    group=base+["reversal_target"]
    return {
        "event_detail":events,
        "first_hit_detail":pair,
        "reversal_probability":probability_summary(rev,group,False),
        "first_hit_probability":probability_summary(pair,group+["continuation_target"],True),
        "mfe_mae_summary":event_summary(events,base),
        "time_analysis":probability_summary(rev,group+["time_bucket"],False),
        "yearly_analysis":probability_summary(rev,group+["year"],False),
        "validation_summary":probability_summary(rev,group+["period"],False),
        "yearly_mfe_mae":event_summary(events,base+["year"]),
    }


def run(cfg: Settings, days: pd.DataFrame | None = None, tick_loader=None) -> dict[str,pd.DataFrame]:
    if days is None: days=load_days(cfg)
    if tick_loader is None: tick_loader=load_ticks
    event_rows=[]; hit_rows=[]; quality=[]
    for d in days.itertuples(index=False):
        day=pd.Timestamp(d.trade_date).date()
        contract=str(d.contract_date)
        t=tick_loader(cfg,day,contract)
        status="OK"
        if t.empty: status="NO_TICKS"
        else:
            t=t.sort_values(["at","source_row_no"],kind="stable").reset_index(drop=True)
            timestamps=pd.to_datetime(t["at"])
            if len(t)<2: status="TOO_FEW_TICKS"
            elif timestamps.iloc[0]>pd.Timestamp(datetime.combine(day,time(8,46))): status="MISSING_OPEN"
            elif timestamps.iloc[-1]<pd.Timestamp(datetime.combine(day,time(13,44))): status="MISSING_CLOSE"
            elif (timestamps.diff().dropna().dt.total_seconds().max()/60)>cfg.max_tick_gap_minutes: status="LARGE_TICK_GAP"
            elif (timestamps.iloc[-1]-timestamps.iloc[0]).total_seconds()/60<cfg.min_tick_coverage_minutes: status="INSUFFICIENT_COVERAGE"
            elif len(t["source_row_no"].unique())!=len(t): status="DUPLICATE_SOURCE_ROW"
        quality.append(dict(trade_date=day.isoformat(),contract_date=contract,status=status,
                            tick_count=len(t)))
        if status!="OK":continue
        prev_range=(float(d.prev20_range) if pd.notna(d.prev20_range) and
                    pd.notna(d.prev_count) and int(d.prev_count)==20 else None)
        e,h=detect_events(day,contract,t,cfg,prev_range)
        event_rows.extend(e);hit_rows.extend(h)
    events=pd.DataFrame(event_rows); hits=pd.DataFrame(hit_rows)
    if events.empty: raise RuntimeError("沒有有效觸發事件；請檢查資料日期、tick完整性與門檻")
    reports=build_reports(events,hits)
    q=pd.DataFrame(quality)
    reports["data_quality_detail"]=q
    reports["data_quality_summary"]=q.groupby("status",as_index=False).agg(days=("trade_date","count"))
    # 對每種門檻提供「有沒有發生」的交易日頻率，不會把未觸發日錯算到反轉分母。
    eligible=(q.status=="OK").sum()
    trig=events.groupby(["direction","threshold_type","threshold_value"],as_index=False).agg(trigger_days=("trade_date","nunique"))
    trig["eligible_days"]=eligible; trig["trigger_frequency"]=trig.trigger_days/eligible
    reports["trigger_frequency"]=trig
    return reports


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,default=Path("backtest/configs/futures_intraday_reversal_v1.json"))
    args=parser.parse_args(); cfg=load_config(args.config)
    reports=run(cfg)
    cfg.output_dir.mkdir(parents=True,exist_ok=True)
    for name,frame in reports.items():
        frame.to_csv(cfg.output_dir/f"{name}.csv",index=False,encoding="utf-8-sig")
        print(name,len(frame))
    manifest=dict(version="1.0",created_at=datetime.now().isoformat(),
                  config=json.loads(args.config.read_text(encoding="utf-8")),
                  files=list(reports),valid_days=int((reports["data_quality_detail"].status=="OK").sum()),
                  caveat="事件研究非實際交易績效；同日門檻高度相關；僅完整tick交易日")
    (cfg.output_dir/"manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    print("輸出完成：",cfg.output_dir.resolve())


if __name__=="__main__":main()
