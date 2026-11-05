from __future__ import annotations

import argparse, json, uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
import pandas as pd

from backtest.futures_combination_exit_backtest import Settings as ExitSettings, _number, load_contract_map, load_day_ticks, simulate_wait_entry_trade, summarize
from backtest.futures_combination_signal_backtest import Settings as SignalSettings, load_source, prepare_daily
from backtest.futures_raw_signal_strength_scan import _rolling_percentile

ANALYSIS_NAME = "futures_retail_extreme_expansion_backtest"
VERSION = "1.0.0"

@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    history_window: int = 252
    minimum_history: int = 60
    range_days: int = 30
    ma5_exclude_percentile: float = 80.0
    stop_loss_ratio: float = 1 / 4
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0

ENTRY_DISTANCE_SCENARIOS = (("A_ENTRY_1_4R",1/4),("A_ENTRY_1_3R",1/3),("A_ENTRY_5_12R",5/12),("A_BASE_ENTRY_1_2R",1/2))
RETAIL_THRESHOLD_SCENARIOS = (("B_RETAIL_Q10",10.0),("B_BASE_RETAIL_Q20",20.0),("B_RETAIL_Q25",25.0),("B_RETAIL_Q30",30.0))

def prepare_research_daily(source: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    ordered=source.sort_values("日期").drop_duplicates("日期",keep="last").copy()
    close=pd.to_numeric(ordered["日盤收"],errors="coerce")
    ma5=close.rolling(5,min_periods=5).mean()
    ordered["MA5乖離_pct"]=(close/ma5-1)*100
    ordered["MA5乖離_歷史分位_pct"]=_rolling_percentile(ordered["MA5乖離_pct"],settings.history_window,settings.minimum_history)
    day_range=pd.to_numeric(ordered["日盤高"],errors="coerce")-pd.to_numeric(ordered["日盤低"],errors="coerce")
    ordered["平均振幅30"]=day_range.rolling(settings.range_days,min_periods=settings.range_days).mean()
    ss=SignalSettings(start=settings.start,end=settings.end,futures_id=settings.futures_id,core_column="策略_核心_五日價量",history_window=settings.history_window,minimum_history=settings.minimum_history,round_trip_cost_points=settings.round_trip_cost_points)
    return prepare_daily(ordered,ss)

def build_candidates(daily: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    retail=pd.to_numeric(daily["散戶多空比_歷史分位_pct"],errors="coerce")
    ma5p=pd.to_numeric(daily["MA5乖離_歷史分位_pct"],errors="coerce")
    valid=(daily["核心方向"].eq("多") & retail.notna() & (ma5p<settings.ma5_exclude_percentile) & pd.to_numeric(daily["平均振幅30"],errors="coerce").notna() & pd.to_numeric(daily["夜盤收"],errors="coerce").notna())
    rows=[]
    for scenario,distance in ENTRY_DISTANCE_SCENARIOS:
        x=daily.loc[valid&(retail<=20)].copy(); x["study"]="A_ENTRY_DISTANCE"; x["scenario"]=scenario; x["entry_distance_ratio"]=distance; x["retail_threshold_pct"]=20.; rows.append(x)
    for scenario,threshold in RETAIL_THRESHOLD_SCENARIOS:
        x=daily.loc[valid&(retail<=threshold)].copy(); x["study"]="B_RETAIL_THRESHOLD"; x["scenario"]=scenario; x["entry_distance_ratio"]=.5; x["retail_threshold_pct"]=threshold; rows.append(x)
    out=pd.concat(rows,ignore_index=True)
    out["交易日"]=pd.to_datetime(out["隔日日期"]).dt.date; out["訊號日"]=pd.to_datetime(out["訊號日"]).dt.date
    return out.sort_values(["study","scenario","交易日"]).reset_index(drop=True)

def run_backtest(settings: Settings) -> pd.DataFrame:
    ss=SignalSettings(start=settings.start,end=settings.end,futures_id=settings.futures_id,core_column="策略_核心_五日價量",history_window=settings.history_window,minimum_history=settings.minimum_history,round_trip_cost_points=settings.round_trip_cost_points)
    candidates=build_candidates(prepare_research_daily(load_source(ss),settings),settings)
    es=ExitSettings(start=settings.start,end=settings.end,futures_id=settings.futures_id,range_days=settings.range_days,stop_loss_ratio=settings.stop_loss_ratio,round_trip_cost_points=settings.round_trip_cost_points,point_value_twd=settings.point_value_twd)
    contracts=load_contract_map(es); cache={}; rows=[]
    for item in candidates.itertuples(index=False):
        d=item.交易日; contract=contracts.get(d); avg=_number(item.平均振幅30); night=_number(item.夜盤收)
        base={"signal_date":item.訊號日,"trade_date":d,"signal_name":"核心多＋綜合散戶極度偏空＋排除正乖離過大","direction":"LONG","study":item.study,"scenario":item.scenario,"variant":item.scenario,"entry_distance_ratio":item.entry_distance_ratio,"retail_threshold_pct":item.retail_threshold_pct,"retail_percentile_pct":item.散戶多空比_歷史分位_pct,"ma5_bias_percentile_pct":item.MA5乖離_歷史分位_pct,"contract_date":contract}
        if not contract or avg is None or night is None:
            rows.append({**base,"entry_status":"MISSING_REFERENCE","exit_reason":"MISSING_DATA"}); continue
        key=(d,contract)
        if key not in cache: cache[key]=load_day_ticks(es,d,contract)
        result=simulate_wait_entry_trade(cache[key],"LONG",night,avg,entry_allowed_time=es.entry_allowed_time,entry_distance_ratio=item.entry_distance_ratio,stop_enabled=False,stop_loss_ratio=settings.stop_loss_ratio,activation_ratio=None,drawdown_ratio=None,round_trip_cost_points=settings.round_trip_cost_points)
        rows.append({**base,**result})
    detail=pd.DataFrame(rows)
    detail["year"]=pd.to_datetime(detail["trade_date"]).dt.year
    detail["net_twd"]=pd.to_numeric(detail.get("net_points"),errors="coerce")*settings.point_value_twd
    return detail

def export_outputs(detail: pd.DataFrame, settings: Settings, output_dir: Path):
    output_dir.mkdir(parents=True,exist_ok=True); end=pd.to_datetime(detail["trade_date"]).max().date(); suffix=f"{settings.start:%Y%m%d}_{end:%Y%m%d}"
    groups=["study","scenario","entry_distance_ratio","retail_threshold_pct"]
    summary=summarize(detail,groups); yearly=summarize(detail,groups+["year"]); status=detail.groupby(["study","scenario","entry_status"],dropna=False).size().rename("count").reset_index()
    paths={"detail":output_dir/f"retail_expansion_detail_{suffix}.csv","summary":output_dir/f"retail_expansion_summary_{suffix}.csv","yearly":output_dir/f"retail_expansion_yearly_{suffix}.csv","entry_status":output_dir/f"retail_expansion_entry_status_{suffix}.csv","manifest":output_dir/f"retail_expansion_manifest_{suffix}.json"}
    detail.to_csv(paths["detail"],index=False,encoding="utf-8-sig"); summary.to_csv(paths["summary"],index=False,encoding="utf-8-sig"); yearly.to_csv(paths["yearly"],index=False,encoding="utf-8-sig"); status.to_csv(paths["entry_status"],index=False,encoding="utf-8-sig")
    paths["manifest"].write_text(json.dumps({"run_id":str(uuid.uuid4()),"analysis":ANALYSIS_NAME,"version":VERSION,"settings":{k:v.isoformat() if isinstance(v,date) else v for k,v in asdict(settings).items()},"study_a":dict(ENTRY_DISTANCE_SCENARIOS),"study_b":dict(RETAIL_THRESHOLD_SCENARIOS),"baseline_check":"A_BASE_ENTRY_1_2R與B_BASE_RETAIL_Q20應相同，並接近前次187訊號、64筆進場"},ensure_ascii=False,indent=2),encoding="utf-8")
    return paths

def main():
    p=argparse.ArgumentParser(); p.add_argument("--start",type=date.fromisoformat,default=date(2018,1,1)); p.add_argument("--end",type=date.fromisoformat); p.add_argument("--cost-points",type=float,default=3.); p.add_argument("--output-dir",type=Path,default=Path("../output/backtest/retail_expansion")); a=p.parse_args()
    s=Settings(start=a.start,end=a.end,round_trip_cost_points=a.cost_points); detail=run_backtest(s); paths=export_outputs(detail,s,a.output_dir); print(summarize(detail,["study","scenario","entry_distance_ratio","retail_threshold_pct"]).to_string(index=False)); [print(f"[retail_expansion] {k}: {v.resolve()}") for k,v in paths.items()]

if __name__=="__main__": main()
