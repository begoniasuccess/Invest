import os
import sys
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from module import twse, finMind
import stock_report_utils as SRU

from common import db  # 你的 DB 模組

# ======================================
# 常數定義
# ======================================
TABLE = "stock_report_daily"

# 完整欄位（對齊目前 DB schema）
COLUMNS = [
    "日期","股票代號",
    "開盤價","收盤價","收盤_開盤","最高價","最低價","日振幅","漲跌幅_pct","日振幅_昨收_pct",
    "成交量","量增率_pct",
    "5日均量","5日最大量_日期","5日最大量",
    "10日均量","10日最大量_日期","10日最大量",
    "20日均量","20日最大量_日期","20日最大量",
    "60日均量","60日最大量_日期","60日最大量",
    "實體_pct","上影_pct","下影_pct","K線型態","跳空缺口",
    "5日平均","10日平均","20日平均","60日平均",
    "5日上升幅度","10日上升幅度","20日上升幅度","60日上升幅度",
    "5日扣抵值","10日扣抵值","20日扣抵值","60日扣抵值",
    "5日扣抵影響_pct","10日扣抵影響_pct","20日扣抵影響_pct","60日扣抵影響_pct",
    "5日乖離","10日乖離","20日乖離","60日乖離",
    "總成交金額_億","法人總買超_億","買超_外資_億","買超_投信_億","買超_自營商_億","買超_融資_億",
    "資金走向","資金走向判讀",
    "is_complete",
]

today = datetime.now()

# ======================================
# 主流程
# ======================================
def taiex_daily_report(months: int = 4, eDt: datetime = today):
    sDt = eDt - relativedelta(months=months)
    return export("TAIEX", sDt, eDt)

def export(stock_id, sDt, eDt):
    # 這個還是給三大法人用，保留 +1 天（你原本寫法保留）
    end_next = (eDt + relativedelta(days=1)).strftime("%Y-%m-%d")

    # === 1) 抓日資料：finMind.get_tw_stock_daily_price（走本地快取） ===
    df = finMind.get_tw_stock_daily_price(stock_id=stock_id, start_date=sDt, end_date=eDt)
    if df is None or df.empty:
        print(f"[警告] {stock_id} {sDt} ~ {eDt} 無日資料，export 回傳空 DataFrame")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] <= pd.to_datetime(eDt.date())].reset_index(drop=True)

    # === 2) 合併法人 ===
    df3 = finMind.get_tw_institutional_total(start_date=sDt, end_date=eDt)
    if df3 is None or df3.empty:
        df3 = pd.DataFrame(columns=["buy", "sell", "date", "name"])

    df3 = df3.copy()
    df3["net"] = pd.to_numeric(df3.get("buy"), errors="coerce") - pd.to_numeric(df3.get("sell"), errors="coerce")
    df3["date"] = pd.to_datetime(df3["date"])
    df3 = df3.pivot(index="date", columns="name", values="net").reset_index()
    df = df.merge(df3, on="date", how="left")

    # === 3) 合併融資：改用 finMind.get_tw_margin_total ===
    # 目標：最後 df 要有「今日餘額」（仟元）給你後面融資計算
    df["今日餘額"] = np.nan
    df_m = finMind.get_tw_margin_total(start_date=sDt, end_date=eDt)

    if df_m is not None and not df_m.empty:
        df_m = df_m.copy()
        col_date = "date" if "date" in df_m.columns else ("日期" if "日期" in df_m.columns else None)
        col_name = "name" if "name" in df_m.columns else ("項目" if "項目" in df_m.columns else None)
        col_today = "TodayBalance" if "TodayBalance" in df_m.columns else ("今日餘額" if "今日餘額" in df_m.columns else None)

        if col_date and col_name and col_today:
            df_m[col_date] = pd.to_datetime(df_m[col_date])
            df_m[col_name] = df_m[col_name].astype(str)

            # 你舊邏輯是「融資金額(仟元)」=> FinMind total 用 MarginPurchaseMoney 的 TodayBalance（常見是元）
            cand = df_m[df_m[col_name] == "MarginPurchaseMoney"].copy()
            if not cand.empty:
                cand[col_today] = pd.to_numeric(cand[col_today], errors="coerce")

                # 單位 heuristic：太大（>1e9）視為「元」=> /1000 轉「仟元」
                med = cand[col_today].median(skipna=True)
                if pd.notna(med) and med > 1e9:
                    cand["今日餘額"] = cand[col_today] / 1000.0
                else:
                    cand["今日餘額"] = cand[col_today]

                cand = cand[[col_date, "今日餘額"]].rename(columns={col_date: "date"})
                df = df.merge(cand, on="date", how="left", suffixes=("", "_m"))

                # 若 merge 出現重名保險
                if "今日餘額_m" in df.columns:
                    df["今日餘額"] = df["今日餘額"].combine_first(df["今日餘額_m"])
                    df.drop(columns=["今日餘額_m"], inplace=True, errors="ignore")

    # === 4) 基本欄位 rename ===
    df.rename(
        columns={
            "date": "日期",
            "open": "開盤價",
            "close": "收盤價",
            "max": "最高價",
            "min": "最低價",
            "Trading_Volume": "成交量",
            "Trading_money": "總成交金額_億",
        },
        inplace=True,
    )

    # === 5) 價量衍生 ===
    df["收盤_開盤"] = df["收盤價"] - df["開盤價"]
    df["日振幅"] = df["最高價"] - df["最低價"]
    df["漲跌幅_pct"] = (df["收盤價"] - df["收盤價"].shift(1)) / df["收盤價"].shift(1)
    df["量增率_pct"] = (df["成交量"] - df["成交量"].shift(1)) / df["成交量"].shift(1)

    df["昨收_tmp"] = df["收盤價"].shift(1)
    base_range = df["日振幅"] / df["昨收_tmp"]
    sign = np.sign(df["收盤價"] - df["昨收_tmp"])
    df["日振幅_昨收_pct"] = base_range * sign
    df.drop(columns=["昨收_tmp"], inplace=True)

    # === 6) 均量 / 均價 / 扣抵 / 乖離 ===
    df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce")
    for n in [5, 10, 20, 60]:
        df[f"{n}日均量"] = df["成交量"].rolling(n).mean()
        df[f"{n}日平均"] = df["收盤價"].rolling(n).mean()
        df[f"{n}日上升幅度"] = df[f"{n}日平均"] - df[f"{n}日平均"].shift(1)
        df[f"{n}日扣抵值"] = df["收盤價"].shift(n - 1)
        df[f"{n}日扣抵影響_pct"] = (df["收盤價"] - df[f"{n}日扣抵值"]) / df["收盤價"]
        df[f"{n}日乖離"] = (df["收盤價"] - df[f"{n}日平均"]) / df[f"{n}日平均"]

    # === 7) 金額換算 / 法人（這段修掉你的 scalar/fillna 問題） ===
    df["總成交金額_億"] = pd.to_numeric(df["總成交金額_億"], errors="coerce") / 1e8

    # 外資 / 投信：欄名通常穩定，但仍做防呆
    foreign = pd.to_numeric(SRU._scol(df, "Foreign_Investor", default=np.nan), errors="coerce")
    itrust  = pd.to_numeric(SRU._scol(df, "Investment_Trust", default=np.nan), errors="coerce")

    # 自營商：FinMind 可能只有 Dealer，也可能拆 Dealer_self/Dealer_Hedging/Foreign_Dealer_Self
    dealer = SRU._sum_cols_like(df, keywords=["Dealer"], default=0.0)

    # 法人總買超：若有 total 用 total，沒有就用 外資+投信+自營商
    total_net = pd.to_numeric(SRU._scol(df, "total", default=np.nan), errors="coerce")
    if total_net.isna().all():
        total_net = pd.to_numeric(foreign, errors="coerce").fillna(0.0) + pd.to_numeric(itrust, errors="coerce").fillna(0.0) + pd.to_numeric(dealer, errors="coerce").fillna(0.0)

    df["法人總買超_億"] = total_net / 1e8
    df["買超_外資_億"] = foreign / 1e8
    df["買超_投信_億"] = itrust / 1e8
    df["買超_自營商_億"] = pd.to_numeric(dealer, errors="coerce").fillna(0.0) / 1e8

    # 融資（今日餘額是「仟元」）
    df["融資餘額_億"] = pd.to_numeric(SRU._scol(df, "今日餘額", default=np.nan), errors="coerce") * 1000 / 1e8
    df["買超_融資_億"] = df["融資餘額_億"] - df["融資餘額_億"].shift(1)

    # 資金走向
    df["資金走向"] = df["收盤_開盤"] - (df["法人總買超_億"] + df["買超_融資_億"])

    def _fund_flow_label(x):
        if pd.isna(x):
            return None
        if x > 0:
            return "偏重大型股"
        if x < 0:
            return "偏重小型股"
        return None

    df["資金走向判讀"] = df["資金走向"].apply(_fund_flow_label)

    # === 8) 實體 / 上影 / 下影 ===
    rng = (df["最高價"] - df["最低價"]).replace(0, np.nan)
    df["實體_pct"] = (df["收盤價"] - df["開盤價"]).abs() / rng
    df["上影_pct"] = (df["最高價"] - np.maximum(df["開盤價"], df["收盤價"])) / rng
    df["下影_pct"] = (np.minimum(df["開盤價"], df["收盤價"]) - df["最低價"]) / rng

    df["K線型態"] = df.apply(SRU.classify_k_type, axis=1)

    # === 10) 跳空缺口 ===
    df["昨高"] = df["最高價"].shift(1)
    df["昨低"] = df["最低價"].shift(1)

    conds = [df["最低價"] > df["昨高"], df["最高價"] < df["昨低"]]
    choices = ["上跳空", "下跳空"]
    df["跳空狀態"] = np.select(conds, choices, default="無跳空")

    is_red = df["收盤價"] > df["開盤價"]
    df["今上緣"] = np.where(is_red, df["收盤價"], df["開盤價"])
    df["今下緣"] = np.where(is_red, df["開盤價"], df["收盤價"])

    df["昨上緣"] = df["今上緣"].shift(1)
    df["昨下緣"] = df["今下緣"].shift(1)

    df["跳空缺口"] = np.select(
        [df["跳空狀態"] == "上跳空", df["跳空狀態"] == "下跳空"],
        [df["今下緣"] - df["昨上緣"], df["今上緣"] - df["昨下緣"]],
        default=None,
    )

    # === 11) 已刪除：均線排列 / 趨勢 7 欄位 ===

    # === 12) 量能最大量（5/10/20/60） ===
    vols = df["成交量"].to_numpy()
    dates = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d").to_numpy()

    for n in [5, 10, 20, 60]:
        vmax_list, vdate_list = [], []
        for i in range(len(df)):
            if i + 1 < n:
                vmax_list.append(np.nan)
                vdate_list.append(np.nan)
                continue
            window_vol = vols[i + 1 - n : i + 1]
            window_date = dates[i + 1 - n : i + 1]
            idx = int(np.argmax(window_vol))
            vmax_list.append(window_vol[idx])
            vdate_list.append(window_date[idx])

        df[f"{n}日最大量"] = vmax_list
        df[f"{n}日最大量_日期"] = vdate_list

    # === 13) upsert 到 DB + 後續輸出 ===
    SRU.upsert(df, stock_id)
    SRU.update_is_complete()

    sql = f"""
        SELECT * FROM stock_report_daily
        WHERE 股票代號 = '{stock_id}'
          AND 日期 BETWEEN '{sDt.strftime("%Y-%m-%d")}' AND '{eDt.strftime("%Y-%m-%d")}'
        ORDER BY 日期
    """
    output = db.query_to_df(sql)
    output.drop(columns=["id", "is_complete", "updated_at"], inplace=True, errors="ignore")

    # =========================================================
    # ✅ 輸出欄位控制：只輸出你指定的欄位（沒指定就不輸出）
    # - 但入庫仍是全欄位（因為 upsert(df, stock_id) 已先做完）
    # =========================================================
    out_cols = [
        "日期","股票代號","開盤價","收盤價","收盤_開盤","最高價","最低價","日振幅","漲跌幅_pct",
        "日振幅_昨收_pct","成交量","量增率_pct",
        # "5日均量","5日最大量_日期","5日最大量", "10日均量","10日最大量_日期","10日最大量",
        # "20日均量","20日最大量_日期","20日最大量","60日均量","60日最大量_日期","60日最大量",
        "實體_pct","上影_pct","下影_pct","K線型態",
        "跳空缺口","5日平均","10日平均","20日平均","60日平均",
        # "5日上升幅度","10日上升幅度","20日上升幅度","60日上升幅度",
        "5日扣抵值","5日扣抵影響_pct","10日扣抵值",
        "10日扣抵影響_pct","20日扣抵值","20日扣抵影響_pct","60日扣抵值","60日扣抵影響_pct",
        "5日乖離","10日乖離","20日乖離","60日乖離","總成交金額_億",
        "法人總買超_億","買超_外資_億","買超_投信_億","買超_自營商_億","買超_融資_億",
        "資金走向","資金走向判讀",
    ]

    # 白名單：只留存在於 output 的欄位，且順序依 out_cols
    final_cols = [c for c in out_cols if c in output.columns]
    output = output[final_cols].copy()

    output.to_csv("stock_report.csv", index=False, encoding="utf-8-sig")
    return output

# =========================================================
# 4) 主函式：修補指定期間、指定欄位，可選 force_renew
# =========================================================
def repair_stock_report_fields(
    db,
    finMind,
    stock_id,
    sDt,
    eDt,
    fields,
    force_renew=False,
    table="stock_report_daily",
    lookback_days=180,
):
    """
    fields: list[str] 要修補的欄位
    force_renew:
      - False: 只補缺值
      - True : 期間內全部重算覆蓋
    """

    # 1) 決定 lookback 起點（rolling 欄需要）
    sDt_lb = (sDt - relativedelta(days=lookback_days))
    # 注意：這裡用 sDt_lb ~ eDt 抓資料來算，最後只回寫 sDt~eDt
    base = SRU._build_base_df(finMind, stock_id, sDt_lb, eDt)
    if base is None or base.empty:
        print(f"[repair] base empty: {stock_id} {sDt_lb}~{eDt}")
        return 0

    # 2) 算出完整衍生欄（跟 export 同一套邏輯）
    calc = SRU._compute_derived(base)

    # 3) 只保留要回寫期間
    calc["日期"] = pd.to_datetime(calc["日期"])
    mask = (calc["日期"] >= pd.to_datetime(sDt.date())) & (calc["日期"] <= pd.to_datetime(eDt.date()))
    calc = calc.loc[mask].copy()

    # 4) 從 DB 撈出該期間現況（用來判斷缺值/是否覆蓋）
    sql_now = f"""
        SELECT 日期, 股票代號, {",".join([f'"{c}"' for c in fields])}
        FROM "{table}"
        WHERE 股票代號 = '{stock_id}'
          AND 日期 BETWEEN '{sDt.strftime("%Y-%m-%d")}' AND '{eDt.strftime("%Y-%m-%d")}'
        ORDER BY 日期
    """
    now = db.query_to_df(sql_now)
    if now is None or now.empty:
        # 如果 DB 內沒有這段資料，你應該先跑 export/upsert 建立底，再 repair
        print(f"[repair] DB has no rows in range: {stock_id} {sDt}~{eDt}")
        return 0

    now["日期"] = pd.to_datetime(now["日期"])
    calc["日期"] = pd.to_datetime(calc["日期"])

    # 5) merge：用日期對齊（股票代號固定）
    merged = now.merge(
        calc[["日期"] + [c for c in fields if c in calc.columns]],
        on="日期",
        how="left",
        suffixes=("_old", "_new"),
    )
    merged["股票代號"] = stock_id

    # 6) 產出 patch：依 force_renew 決定覆蓋策略
    patch_rows = []
    for _, r in merged.iterrows():
        out = {"日期": r["日期"], "股票代號": stock_id}
        need_any = False

        for c in fields:
            if c not in SRU.FIELD_REGISTRY:
                raise ValueError(f"[repair] FIELD_REGISTRY 沒有定義欄位：{c}")

            newv = r.get(f"{c}_new", None)
            oldv = r.get(f"{c}_old", None)

            if force_renew:
                # 全覆蓋
                out[c] = newv
                need_any = True
            else:
                # 只補缺值
                if SRU._is_missing(oldv):
                    out[c] = newv
                    need_any = True

        if need_any:
            patch_rows.append(out)

    if not patch_rows:
        print("[repair] nothing to patch")
        return 0

    df_patch = pd.DataFrame(patch_rows)

    # 7) 回寫 DB（只更新指定欄位）
    updated = SRU._update_fields_to_db(
        db=db,
        table=table,
        key_cols=["股票代號", "日期"],
        df_patch=df_patch,
        fields=fields,
    )

    print(f"[repair] updated rows: {updated}, fields: {fields}, force_renew={force_renew}")
    return updated


# python -m main.stock_report
if __name__ == "__main__":
    df = export("TAIEX", datetime(2000,1,1), datetime.now())
    # df = taiex_daily_report(60)
    print(df.tail(5))
    print("DONE")
