"""Deterministic numeric evidence, not an AI-generated trading recommendation."""
from __future__ import annotations
import csv, math, json
from collections import Counter
from itertools import islice
from dashboard import research_lab_service as s

IDENTITIES=['strategy','direction','signal_name','variant','scenario','study','night_gap_group_label','segment','parameter_id','x_label','y_retrace_pct','z_stop_pct','分類','基準','策略_核心_五日價量','組合訊號','交易方向','樣本狀態','研究目標','指標代碼','指標類別','規則名稱','條件名稱','價格反應期間','價格反應類型','方向定義','期間','模型','特徵','欄位','結果指標','檢定類型','分組方式','分組','夜盤幅度桶','日夜關係','分層類型','開盤距夜收桶','開盤相對夜收','開盤錨點位置','threshold_type','threshold_value','threshold_points','reversal_target','continuation_target','星期','預測時段','綜合方向判定','目標','評估期間','bucket_label','metric_label','metric_name','策略欄位','當沖結果','結果代碼','core_direction','night_trend','trade_direction','signal_direction','data_quality_status','環境','夜盤漲跌分組','夜盤走勢','核心多空']
METRICS=['entry_count','entered_count','trades','signal_count','sample_count','有效訊號數','樣本數','多樣本數','空樣本數','驗證樣本數','可判定日數','極端訊號數','訊號數','win_rate_pct','命中率_pct','方向命中率_pct','綜合命中率_pct','direction_hit_rate_pct','total_net_points','total_points','總淨點數','模擬開收盤總點數','average_net_points','avg_points','平均淨點數','模擬開收盤平均點數','profit_factor','pf','獲利因子','max_drawdown_points','max_drawdown','最大回撤點數','long_total_net_points','short_total_net_points','up_day_rate_pct','上漲機率_pct','平均漲跌幅_pct','mean_open_to_close_points','open_to_close_points_mean','mean_value','median_value','positive_rate_pct','正確率_pct','多數類基準_pct','相對多數類增減_百分點','全期_符合數','全期_命中率_pct','全期_事件率_pct','全期_母體事件率_pct','全期_參考組事件率_pct','全期_Lift','全期_相對參考組Lift','全期_相對其餘日Lift','全期_相對相反價格反應Lift','驗證期_符合數','驗證期_命中率_pct','驗證期_事件率_pct','驗證期_Lift','驗證期_相對參考組Lift','驗證期_相對其餘日Lift','驗證期_相對相反價格反應Lift','全期_Q1事件率_pct','全期_Q5事件率_pct','全期_Q5減Q1_百分點','驗證期_Q5減Q1_百分點','年度趨勢一致率','年度正向率','全期_Fisher_q值','FDR_q值','趨勢_q值','FDR顯著','校正後顯著','正式通過','第一階段候選','簡化候選','重複驗證通過','年平均MAE','年平均MAE改善_pct','年平均ROC_AUC','年平均Brier','年平均前20pct劇烈率Lift','驗證準確率_pct','驗證AUC','驗證平衡準確率_pct','Spearman_rho','效果值_Spearman_rho','Q1平均','Q5平均','Q5減Q1','多減空_pct','探索期_多減空_pct','驗證期_多減空_pct','效果值','驗證期_效果值','年度方向一致率','最強Spearman_rho','穩定方向證據數','隔日收盤在兩者之間_pct','隔日收在夜收外側_pct','隔日同時觸及兩端_pct','隔日日盤平均振幅_pct','反向機率_pct','同向機率_pct','反向減同向_百分點','收盤比開盤更接近夜收_pct','盤中收回至少100pct_機率','隔日日內振幅平均_pct','振幅至少2.0pct_機率','reversal_rate','decisive_reversal_rate','wilson95_low','wilson95_high','low_sample','train_entered_count','train_total_net_points','validation_total_net_points','test_total_net_points','overall_max_drawdown_points','MAE','ROC_AUC','Brier','Spearman','平均日內振幅_pct昨收','平均日內振幅_點','漲跌幅平均_pct','上漲比例_pct','next_day_close_up_rate_pct','觸及率_pct','實際樣本數']
POS_KEYS=['total_net_points','total_points','總淨點數','模擬開收盤總點數']
RULE_FLAGS=['正式通過','第一階段候選','簡化候選','重複驗證通過','FDR顯著','FDR_5pct顯著','校正後顯著','前後期同方向']

def number(x):
    try:
        v=float(x)
        return v if math.isfinite(v) else None
    except (ValueError,TypeError):return None

def fmt(v):
    n=number(v)
    if n is None:return str(v or '—')
    if n==0:return '0'
    if 0<abs(n)<.001:return f'{n:.3g}'
    if n.is_integer():return f'{int(n):,}'
    return f'{n:,.4f}'.rstrip('0').rstrip('.')

def safe(v):return fmt(v).replace('|','\\|').replace('\n',' ').replace('\r',' ')
def name_row(row,cols):return '／'.join(str(row[c]) for c in cols if row.get(c) not in (None,'')) or '該列'
def label_file(f):return f['name']

def table(rows,columns):
    return '\n'.join(['| '+' | '.join(c.replace('|','\\|') for c in columns)+' |','| '+' | '.join('---' for _ in columns)+' |']+['| '+' | '.join(safe(r.get(c,'')) for c in columns)+' |' for r in rows])

def pick_files(e,g):
    fs=[f for f in g['files'] if f['type']=='csv']
    if e['id']=='futures_daily_amplitude_pattern':return fs[:1]
    if e['id']=='futures_night_retrace':return [f for f in fs if '_selection_' in f['name'] or '_parameter_summary_' in f['name']]
    if e['id']=='futures_intraday_reversal':return [f for f in fs if f['name'] in ('data_quality_summary.csv','trigger_frequency.csv','reversal_probability.csv','first_hit_probability.csv','validation_summary.csv')]
    if e['id']=='futures_next_day_session_statistics':return [f for f in fs if 'by_core_' in f['name'] or 'by_night_trend_' in f['name'] or 'by_night_change_' in f['name']]
    if e['id']=='futures_open_to_night_close_convergence':return [f for f in fs if 'by_gap_' in f['name'] or '_tests_' in f['name']]
    if e['id']=='futures_signal_path_statistics':return [f for f in fs if any(t in f['name'] for t in ('baseline_summary','direction_summary','data_quality_summary','threshold_summary','trailing_summary'))]
    if e['id']=='futures_core_night_state_backtest':return [f for f in fs if re.search(r'_\d{8}_\d{8}_summary\.csv$',f['name']) or 'baseline' in f['name']]
    if e['id']=='futures_retail_night_gap_adaptive_entry_backtest':return [f for f in fs if any(t in f['name'] for t in ('policy_summary','matrix_summary','path_summary'))]
    if e['id']=='futures_night_bucket':return [f for f in fs if 'summary' in f['name']]
    if e['id']=='futures_weekday_settlement_analysis':return [f for f in fs if 'overall' in f['name'] or 'comparison' in f['name']]
    primary=[f for f in fs if any(t in f['name'] for t in ('summary','classifier','ranking','metrics','models','tests','shortlist','candidates','_by_bucket_')) and not any(t in f['name'] for t in ('yearly','year_','by_entry','by_trend','entry_status','detail','gap_bucket'))]
    return primary[:5] or fs[:1]

import re

def summary_file(e,f):
    p=s.file_path(f)
    if p.stat().st_size>25*1024*1024:return [f'此檔案較大（{p.stat().st_size/1024/1024:.1f} MiB），請使用分頁瀏覽；未把逐筆明細當作策略摘要。']
    with s.open_csv(p) as stream:
        reader=csv.DictReader(stream); cols=reader.fieldnames or [];rows=list(reader)
    if not rows:return ['此表沒有資料列；不據此判定策略無效。']
    if 'amplitude_label' in cols and 'valid_for_research' in cols:
        valid=[r for r in rows if r.get('valid_for_research','').lower() in ('true','1')];count=Counter(r['amplitude_label'] for r in valid)
        lines=[f'共有 {len(rows):,} 筆日盤資料；可供研究 {len(valid):,} 筆。以下為可研究日的實際振幅分類。','',table([{'振幅型態':k,'樣本數':v,'占有效日_pct':100*v/max(len(valid),1)} for k,v in count.most_common()],['振幅型態','樣本數','占有效日_pct']),'','這是目標標籤分布，不是盤前預測結果；後續特徵掃描必須只使用當時已知的資訊。']
        return lines
    selected=rows
    if '期間' in cols and any(r.get('期間')=='全期' for r in rows):selected=[r for r in rows if r['期間']=='全期']
    if '評估期間' in cols and any(r.get('評估期間')=='全部樣本外' for r in rows):selected=[r for r in rows if r['評估期間']=='全部樣本外']
    if 'segment' in cols and any(r.get('segment')=='OVERALL' for r in rows):selected=[r for r in rows if r['segment']=='OVERALL']
    ids=[k for k in IDENTITIES if k in cols]; metrics=[k for k in METRICS if k in cols]
    # Preserve actual labels even for custom tables with unfamiliar column names.
    if not ids:ids=cols[:min(3,len(cols))]
    if not metrics:metrics=[k for k in cols if k not in ids][:9]
    use=list(dict.fromkeys(ids+metrics))
    # Avoid overly wide tables; key parameter, sample, return and risk metrics take priority.
    use=use[:19]
    lines=[f'原始表共 {len(rows):,} 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。']
    money=next((x for x in POS_KEYS if x in cols),None)
    if money:
        subgroups={}
        # Separate directions and overlapping aggregate rows when possible.
        grouping=next((k for k in ('direction','交易方向','trade_direction') if k in cols),None)
        for r in selected:
            n=number(r.get(money))
            if n is None:continue
            subgroups.setdefault(r.get(grouping,'全部列') if grouping else '全部列',[]).append(r)
        show=[]
        for group,rs in subgroups.items():
            order=sorted(rs,key=lambda r:number(r[money]),reverse=True);best,worst=order[0],order[-1];positive=sum(number(r[money])>0 for r in rs)
            lines.append(f'- {group}：{positive}/{len(rs)} 組／列淨點數為正；最高 {fmt(best[money])}（{name_row(best,ids)}），最低 {fmt(worst[money])}（{name_row(worst,ids)}）。')
            show.extend(order if len(order)<=6 else order[:3]+order[-2:])
        if show:selected=show[:20]
        lines.append('上述是原始表內的描述性排名，不是樣本外選模。應同看交易數、平均每筆、PF、最大回撤、成本與年度表；尤其參數重疊不可合併為獨立績效。')
    elif '正確率_pct' in cols and '多數類基準_pct' in cols:
        vals=[r for r in selected if number(r.get('正確率_pct')) is not None and number(r.get('多數類基準_pct')) is not None]
        if vals:
            best=max(vals,key=lambda r:number(r['正確率_pct'])-number(r['多數類基準_pct'])); wins=sum(number(r['正確率_pct'])>number(r['多數類基準_pct']) for r in vals)
            lines.append(f'- 在所列期間的 {len(vals)} 種分類條件中，{wins} 種正確率高於多數類基準。最大差異為 {fmt(number(best["正確率_pct"])-number(best["多數類基準_pct"]))} 個百分點（{name_row(best,ids)}）。')
            lines.append('方向命中率不是獲利率；低於基準亦不能直接推論反向交易一定有效。')
        selected=vals or selected
    elif '全期_事件率_pct' in cols or '全期_命中率_pct' in cols:
        key='全期_事件率_pct' if '全期_事件率_pct' in cols else '全期_命中率_pct'
        ranked=[r for r in selected if number(r.get(key)) is not None]; ranked.sort(key=lambda r:number(r[key]),reverse=True)
        if ranked:
            best=ranked[0];n=best.get('全期_符合數','未知');lines.append(f'- 最高 {key} 為 {fmt(best[key])}%（{name_row(best,ids)}；符合樣本 {fmt(n)}）。')
            lifts=[k for k in ('全期_Lift','全期_相對參考組Lift','全期_相對其餘日Lift','全期_相對相反價格反應Lift') if k in cols]
            if lifts:
                lk=lifts[0];lv=[r for r in ranked if number(r.get(lk)) is not None];good=sum(number(r[lk])>1 for r in lv);lines.append(f'- {lk} 大於 1：{good}/{len(lv)} 列。須注意不同規則的比較組與母體可能不相同。')
            selected=ranked if len(ranked)<=12 else ranked[:6]+ranked[-3:]
        lines.append('事件率／命中率必須與母體、參考組、樣本數、探索／驗證期及檢定共同解讀；不等同交易報酬。')
    elif '年平均MAE' in cols or '驗證AUC' in cols:
        k='年平均MAE' if '年平均MAE' in cols else '驗證AUC';valid=[r for r in rows if number(r.get(k)) is not None];valid.sort(key=lambda r:number(r[k]),reverse=k=='驗證AUC')
        baseline=next((r for r in rows if '基準模型'==r.get('模型') or '基準｜' in r.get('模型','')),None)
        if valid:
            best=valid[0];lines.append(f'- 按 {k} 的樣本內表格排名，最佳列為 {name_row(best,ids)}，{fmt(best[k])}。')
        if baseline:lines.append(f'- 基準列：{name_row(baseline,ids)}；{k}={fmt(baseline.get(k))}。')
        selected=(([baseline] if baseline else [])+valid[:6])
        if '年平均ROC_AUC' in cols:
            auc=[r for r in rows if number(r.get('年平均ROC_AUC')) is not None];auc.sort(key=lambda r:number(r['年平均ROC_AUC']),reverse=True);selected+=auc[:3]
        lines.append('模型優劣要以相同測試期間與基準比較。掃描多個模型後挑最高值仍有選擇偏差，不代表未來一定有效。')
    elif 'reversal_rate' in cols or 'decisive_reversal_rate' in cols:
        # Representative original rows, not a best-of-threshold claim.
        selected=[r for r in selected if r.get('threshold_type') in ('POINTS','points') and r.get('threshold_value') in ('100','100.0','250','250.0','500','500.0') and r.get('reversal_target') in ('100','100.0')][:12] or selected[:12]
        lines.append('不同門檻在同一天可能重疊，且反轉比例不等於可成交的策略勝率；應優先核對樣本數、信賴區間與有效 Tick 品質。')
    elif '全期_Q5減Q1_百分點' in cols:
        good=[r for r in rows if r.get('第一階段候選','').lower()=='true'];lines.append(f'- 第一階段候選 {len(good)}/{len(rows)} 列；以下優先顯示候選及原始排序。');selected=(good or rows)[:12]
    elif 'train_total_net_points' in cols:
        selected=rows[:10];pos=sum((number(r.get('test_total_net_points')) or 0)>0 for r in rows);lines.append(f'- 原訓練期篩選清單中，測試期總淨點數為正的列數 {pos}/{len(rows)}；保留原本的訓練期選擇順序，不用測試期重新挑選。')
    else:
        selected=selected[:12]
    for flag in RULE_FLAGS:
        if flag in cols:
            values=[r[flag].strip().lower() for r in rows if r.get(flag,'').strip()];valid=[x for x in values if x in ('true','false','1','0','是','否')]
            if valid:lines.append(f'- {flag}：{sum(x in ("true","1","是") for x in valid)}/{len(valid)} 列成立（依 CSV 原有判定，不另行重算）。')
    if selected and use:
        # Unique rows and bounded table. Include all key metrics and source names.
        dedup=[];seen=set()
        for r in selected:
            key=tuple(r.get(c) for c in use)
            if key not in seen:seen.add(key);dedup.append(r)
        lines+=['',table(dedup[:20],use)]
    return lines

# Observations verified against the supplied 2026-11-14 output archive, not applied to future batches.
# Some experiments have multiple historical parameter grids; their numeric rankings are computed separately above.
CURATED={
'futures_core_next_day_hit':'方向命中率約51.25%（642次有效訊號），綜合命中率71.34%包含長影線追加命中，不能當成方向準確率或實際勝率。多單51.54%、空單50.68%，單獨方向優勢不明顯。',
'futures_core_five_day_next_day_statistics':'以昨日日收為基準，多、空訊號的隔日上漲機率分別55.38%、55.04%；以夜收為基準則53.40%、53.92%。核心標籤沒有明顯拉開隔日收盤方向，需另看路徑與條件篩選。',
'futures_core_night_state_backtest':'八種策略中，核心空＋夜漲勢＋做空總點數3,280.53、PF1.2218（408筆），核心多＋夜跌勢＋做多1,295.65、PF1.0654；核心空＋夜跌勢＋做空反而−6,202.60。夜盤狀態確實能區分環境，但須以基準表、幅度桶及分年資料驗證，不能把8種重疊情境相加。',
'futures_stop_trailing':'重點是多空分開判讀：不同日期區間與網格大小並不相同，不能直接比較最佳參數的總點數。最新2018～2026單組500/125/100/125點且成本為0的結果，多單+5,063點、空單−2,381點；其他歷史批次為網格探索，應從各自表格確認參數與樣本。',
'futures_turning_level_intraday':'v1、v2 使用不同策略組及成交/篩選口徑，不能直接把結果合併；v1最高正點數列是D多單+895，v2最高正點數列是B空單+450.77。整體需看ALL列、交易數與回撤，不宜只選局部多空贏家。',
'futures_session_trend_intraday':'2-2.10為+1,654.40點、PF1.1064；2-2.8僅+203.15點；2-2.7與2-2.9為負。支持走勢／選點組合需要分開測，不是所有轉折點都有優勢。',
'futures_session_trend_intraday_a':'加入出場與距離規則後，2-2-A.7為+471.18點、PF1.0366，2-2-A.10為+235.77；其餘兩組為負。移動保利提高勝率並不等同提高期望值，仍要檢查獲利幅度與回撤。',
'futures_session_trend_night':'2-2.11～2-2.14四組總淨點數均為負；2-2.13最接近損益兩平（−202.33點，PF0.9876），不是已證實可直接採用的夜盤策略。',
'futures_combination_signal_backtest':'最高總淨點數的核心空＋夜盤漲為4,195點（410樣本、PF1.1602）；排除負乖離過大後為3,350點、平均14.89點。這些列的正式通過旗標仍為False，不能因總績效正而忽略多重檢定與年度穩定性。',
'futures_night_retrace':'訓練篩選與測試期必須分開。selection 首列（x_00_05、y=1.0%、z=0.7%）訓練93筆+442.55點，驗證23筆+128.87點，測試8筆−375.38點，顯示優勢並非穩定延續；不要用TEST的最高點數反向挑參數。',
'futures_foreign_flow_direction_scan':'外資3日極端訊號順勢預測開收方向只有48.66%，低於52.18%多數類基準；極值先後走勢50.73%也只高於基準約0.66百分點。單靠外資極端買賣不足以建立方向策略。',
'futures_foreign_flow_streak_direction_scan':'連續2、3、5日同向及連3日極端的順勢分類，請對照全期多數類基準；連續2日開收47.93%低於52.18%，不是連買連賣越久越有延續性。',
'futures_foreign_flow_price_reaction_direction_scan':'1日價格同向確認的開收順勢正確率42.48%，低於52.18%基準；背離吸收的1日開收反向正確率49.21%仍未超過基準。個別「連續大買＋3日背離→跌勢盤」60.71%僅28樣本，驗證樣本更少，適合提出恐慌／追價收割假說而非直接定為策略。',
'futures_daily_amplitude_feature_scan':'近期5日日盤振幅／前30日比對「劇烈以上」的Q5−Q1為17.62個百分點，驗證期15.44，年度趨勢一致率1.0；夜盤振幅與夜成交量也有一致候選。方向性需另驗證。',
'futures_daily_amplitude_violent_combo_scan':'近期振幅、夜盤振幅、夜成交量皆Q5：104樣本、41個事件、事件率39.42%，母體12.37%、Lift3.19；雙因子近期＋夜盤振幅皆Q5：154樣本、33.77%、Lift2.73。三因子提高強度，但減少覆蓋；兩因子更適合作為高波動環境的簡化候選之一。',
'futures_daily_amplitude_foreign_flow_scan':'外資3日極端大賣單獨443樣本、劇烈率20.54%，加入雙高振幅主規則後68樣本、44.12%，相對主規則33.77%增加10.35百分點。大買Q5單獨6.20%，加入主規則18.18%但僅11樣本。結果更支持外資與波動環境的關聯，而非直接預測方向。',
'futures_main_position_direction_backtest':'驗證期多數類基準準確率51.30%，主力新建多單原值模型50.97%、AUC0.5249，未呈現強方向優勢。需要避免以原始係數或訓練期結果代替樣本外表現。',
'futures_volatility_strength_backtest':'全期分數0→1→2→3，平均隔日振幅約0.9975%、1.1768%、1.4461%、1.8080%；振幅至少2%的機率亦由3.42%升至27.33%。分數有助分層波動，但並非多空訊號，最高分組也要看樣本數。',
'futures_volatility_model_compare':'全部樣本外，簡單分數振幅MAE0.4452、Ridge 0.4522，較複雜的迴歸未必改善；1%振幅事件AUC由0.6327至0.6526，分類略有改善。應依不同目標選擇模型，不用同一指標混合判斷。',
'futures_margin_maintenance_scan':'維持率水準與隔日相對昨收振幅Spearman約−0.2005，但與前均幅比的相關只有−0.0344；單日急降與隔日振幅約+0.1342。絕對振幅關聯不等於超出近期波動的事件率，宜與近期振幅控制條件合用。',
'futures_volatility_combo_model':'128組模型以年度樣本外比較；基準MAE0.30690、AUC0.61456。含主力淨空及融資維持率三日急降的模型AUC0.64829，MAE0.30274；改善幅度有限且組合很多，需要留出額外驗證，避免把全表最佳列當成未來表現。',
'futures_signal_path_statistics':'資料品質表顯示2019～2020有466筆缺開盤契約，舊結果不可直接當作完整期間。方向、首次達標、回吐等是研究事件，不是成交後的停損／停利結果。',
'futures_intraday_reversal':'原manifest記錄2038個有效交易日（2018/01/01～2026/11/06）。反轉/續行為事件門檻，不是交易策略；同一天多個門檻高度重疊，判讀需看觸發頻率及品質表。',
'futures_retail_bounce_reentry_chart_gallery':'此HTML是反彈重進三方案的案例圖庫，不是獨立績效實驗；請與「散戶極空策略：跌破失效價後反彈重進」的CSV對照，不能從單張成功案例估計整體策略勝率。',
'futures_weekday_settlement_analysis':'整體表比較各星期而非精確的期貨結算日事件。週二平均日內振幅1.221553%高於週三1.123583%；需再檢查逐年穩定性，不應直接推論每週三或每週五具有固定交易優勢。',
}

def review_text(e,g,curated=False):
    lines=['# 結果回顧｜'+e['title'],'',f'批次：`{g["id"]}`  ',f'研究期間：{g.get("start") or "未知"} ～ {g.get("end") or "未知／latest"}  ',f'執行時間：{g.get("execution_time") or g.get("started_at") or "未記錄"}  ',f'參數來源：{g.get("parameter_source") or ("本次參數快照" if g.get("kind")=="new" else "未記錄")}','']
    lines+=['## 研究結果與判讀','']
    if curated and e['id'] in CURATED:lines += [CURATED[e['id']], '']
    if e['id'].endswith('_chart_gallery'):lines+=['此為圖庫輔助工具。請與反彈重進實驗的統計結果共同查閱；本頁沒有獨立交易績效。','']
    for f in pick_files(e,g):
        lines += ['### '+f['name'],'',f'來源：`output/{f["path"]}`','']
        try:lines += summary_file(e,f)+['']
        except Exception as exc:lines += ['此表無法自動摘要：'+type(exc).__name__+'。請查閱原始結果。','']
    lines+=['## 研究限制與後續','', '- 上述數字來自本批次原始輸出，未重新執行回測或更改成交、成本與樣本口徑。','- 同一實驗可能包含重疊日期、不同方向、網格參數或不同模型。不要直接將表格各列加總。','- 優先檢查樣本量、交易成本、資料品質、探索／驗證分割與逐年一致性；統計關聯或樣本內最佳列不構成可交易策略。','- 歷史批次沒有紀錄的時間、完整參數或程式版本維持未知。新批次可利用manifest重現參數，但資料庫日後更新可能改變結果。','', '## 我的研究心得／下一步','', '（可在 Dashboard 直接編輯、儲存。本段留給後續人工研究。）','']
    return '\n'.join(lines)
