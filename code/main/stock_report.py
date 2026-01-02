import os
import sys
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from module import twse, finMind

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
    "均線得分","均線方向","均線排列","均線距離_pct","均線狀態",
    "趨勢強度說明","趨勢等級",
    "5日乖離","10日乖離","20日乖離","60日乖離",
    "總成交金額_億","法人總買超_億","買超_外資_億","買超_投信_億","買超_自營商_億","買超_融資_億",
    "資金走向","資金走向判讀",
    "is_complete",
]

today = datetime.now()

# ======================================
# UPSERT
# ======================================
def upsert(df: pd.DataFrame, stock_id: str):
    if df.empty:
        print("⚠ df empty, skip")
        return

    df = df.copy()
    df["股票代號"] = stock_id
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")

    # --- is_complete：先預設 0, 後面覆寫 ---
    df["is_complete"] = 0

    # 欄位對齊 SQLite 標準欄位
    df = df[[c for c in df.columns if c in COLUMNS]]
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNS]

    # 根據「所有數值欄位」是否都有值來決定 is_complete
    text_cols = [
        "日期","股票代號","K線型態","均線方向","均線排列",
        "均線狀態","趨勢強度說明","資金走向判讀"
    ]
    numeric_cols = [c for c in COLUMNS if c not in text_cols + ["is_complete"]]
    # 如果有任何數值欄位是 NaN，就視為未完成
    df["is_complete"] = (~df[numeric_cols].isna().any(axis=1)).astype(int)

    # SQL 組起來
    col_sql = ",".join([f'"{c}"' for c in COLUMNS])
    ph = ",".join(["?"] * len(COLUMNS))
    update_sql = ",".join([f'"{c}" = excluded."{c}"' for c in COLUMNS if c not in ("日期", "股票代號")])

    sql = f"""
    INSERT INTO "{TABLE}" ({col_sql})
    VALUES ({ph})
    ON CONFLICT("股票代號","日期") DO UPDATE SET
        {update_sql}
    WHERE "{TABLE}".is_complete = 0
       OR excluded.is_complete = 1;
    """

    rows = list(df.itertuples(index=False, name=None))

    ok = db.execute_sql(sql, rows)
    if ok:
        print(f"✔ DB 寫入成功: {len(df)} rows, stock={stock_id}")
    else:
        print("❌ DB 寫入失敗（請看上方 SQLite Error）")

# ======================================
# 主流程
# ======================================
def taiex_daily_report(months: int = 4):
    sDt = today - relativedelta(months=months)
    return export("TAIEX", sDt, today)

def export(stock_id, sDt, eDt):
    # 這個還是給三大法人用，保留 +1 天的寫法
    end_next = (eDt + relativedelta(days=1)).strftime("%Y-%m-%d")

    # === 1) 抓日資料：改成用 finMind.get_tw_stock_daily_price（走本地快取） ===
    df = finMind.get_tw_stock_daily_price(
        stock_id=stock_id,
        start_date=sDt,
        end_date=eDt,
    )
    if df is None or df.empty:
        print(f"[警告] {stock_id} {sDt} ~ {eDt} 無日資料，export 回傳空 DataFrame")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    # 理論上 get_tw_stock_daily_price 已經是區間內，但這行當作保險
    df = df[df["date"] <= pd.to_datetime(eDt.date())].reset_index(drop=True)

    # === 2) 合併法人 ===
    df3 = finMind.get_tw_institutional_total(
        start_date=sDt,
        end_date=eDt,
    )
    if df3 is None or df3.empty:
        df3 = pd.DataFrame(columns=["buy", "sell", "date", "name"])

    df3 = df3.copy()
    df3["net"] = df3["buy"] - df3["sell"]
    df3["date"] = pd.to_datetime(df3["date"])
    df3 = df3.pivot(index="date", columns="name", values="net").reset_index()
    df = df.merge(df3, on="date", how="left")

    # === 3) 合併融資 ===
    df_m = twse.get_margin_trading(sDt, eDt)
    if not df_m.empty:
        df_m = df_m[df_m["項目"] == "融資金額(仟元)"].copy()
        df_m["日期"] = pd.to_datetime(df_m["日期"], format="%Y%m%d")
        df = df.merge(
            df_m[["日期", "今日餘額"]],
            left_on="date",
            right_on="日期",
            how="left",
        )
        df.drop(columns=["日期"], inplace=True, errors="ignore")
    else:
        df["今日餘額"] = np.nan

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

    # 日振幅_昨收_pct（有方向）
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

    # === 7) 金額換算 / 法人 ===
    df["總成交金額_億"] = pd.to_numeric(df["總成交金額_億"], errors="coerce") / 1e8
    df["法人總買超_億"] = pd.to_numeric(df.get("total"), errors="coerce") / 1e8
    df["買超_外資_億"] = pd.to_numeric(df.get("Foreign_Investor"), errors="coerce") / 1e8
    df["買超_投信_億"] = pd.to_numeric(df.get("Investment_Trust"), errors="coerce") / 1e8
    df["買超_自營商_億"] = (
        pd.to_numeric(df.get("Dealer_self"), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("Dealer_Hedging"), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("Foreign_Dealer_Self"), errors="coerce").fillna(0)
    ) / 1e8

    # 融資
    df["融資餘額_億"] = pd.to_numeric(df.get("今日餘額"), errors="coerce") * 1000 / 1e8
    df["買超_融資_億"] = df["融資餘額_億"] - df["融資餘額_億"].shift(1)

    # 資金走向
    df["資金走向"] = df["收盤_開盤"] - (df["法人總買超_億"] + df["買超_融資_億"])

    def _fund_flow_label(x):
        if pd.isna(x):
            return None
        if x > 0:
            return "偏重大型股(多)"
        if x < 0:
            return "偏重小型股(空)"
        return None

    df["資金走向判讀"] = df["資金走向"].apply(_fund_flow_label)

    # === 8) 實體 / 上影 / 下影 ===
    rng = (df["最高價"] - df["最低價"]).replace(0, np.nan)
    df["實體_pct"] = (df["收盤價"] - df["開盤價"]).abs() / rng
    df["上影_pct"] = (df["最高價"] - np.maximum(df["開盤價"], df["收盤價"])) / rng
    df["下影_pct"] = (np.minimum(df["開盤價"], df["收盤價"]) - df["最低價"]) / rng

    # === 9) K 線型態 ===
    def classify_k_type(r):
        body = r["實體_pct"]
        upper = r["上影_pct"]
        lower = r["下影_pct"]
        open_p = r["開盤價"]
        close_p = r["收盤價"]

        if pd.isna(body) or pd.isna(upper) or pd.isna(lower):
            return None

        if abs(close_p - open_p) < 1e-6 or body < 0.05:
            return "⬜ 十字線"

        is_red = close_p > open_p
        color = "🟥" if is_red else "🟩"

        if lower > 0.5 and body < 0.3:
            return f"{color} 錘子線"
        if upper > 0.5 and body < 0.3:
            return f"{color} 流星線"

        if body > 0.6:
            return f"{color} 長紅K" if is_red else f"{color} 長黑K"

        return f"{color} 中實體K"

    df["K線型態"] = df.apply(classify_k_type, axis=1)

    # === 10) 跳空缺口（同個股） ===
    df["昨高"] = df["最高價"].shift(1)
    df["昨低"] = df["最低價"].shift(1)

    conds = [
        df["最低價"] > df["昨高"],
        df["最高價"] < df["昨低"],
    ]
    choices = ["上跳空", "下跳空"]
    df["跳空狀態"] = np.select(conds, choices, default="無跳空")

    is_red = df["收盤價"] > df["開盤價"]
    df["今上緣"] = np.where(is_red, df["收盤價"], df["開盤價"])
    df["今下緣"] = np.where(is_red, df["開盤價"], df["收盤價"])

    df["昨上緣"] = df["今上緣"].shift(1)
    df["昨下緣"] = df["今下緣"].shift(1)

    df["跳空缺口"] = np.select(
        [
            df["跳空狀態"] == "上跳空",
            df["跳空狀態"] == "下跳空",
        ],
        [
            df["今下緣"] - df["昨上緣"],
            df["今上緣"] - df["昨下緣"],
        ],
        default=None,
    )

    # === 11) 均線排列 / 趨勢（5/10/20） ===
    for n in [5, 10, 20]:
        df[f"{n}日斜率"] = df[f"{n}日平均"] - df[f"{n}日平均"].shift(1)

    def judge_ma_type(r):
        a, b, c = r["5日平均"], r["10日平均"], r["20日平均"]
        if pd.notna(a) and pd.notna(b) and pd.notna(c):
            if a > b > c:
                return "多頭排列"
            if a < b < c:
                return "空頭排列"
        return "糾結"

    df["均線排列"] = df.apply(judge_ma_type, axis=1)

    def ma_score(r):
        s = 0
        for n in [5, 10, 20]:
            v = r.get(f"{n}日斜率")
            if pd.isna(v):
                continue
            if v > 0:
                s += 1
            elif v < 0:
                s -= 1
        return s

    df["均線得分"] = df.apply(ma_score, axis=1)
    df["均線方向"] = df["均線得分"].apply(
        lambda s: "上揚" if s >= 2 else ("下彎" if s <= -2 else "糾結")
    )
    df["均線距離_pct"] = (
        (df[["5日平均", "10日平均", "20日平均"]].max(axis=1)
         - df[["5日平均", "10日平均", "20日平均"]].min(axis=1))
        / df[["5日平均", "10日平均", "20日平均"]].mean(axis=1)
        * 100
    )
    df["均線狀態"] = df["均線距離_pct"].apply(
        lambda x: "糾結" if x < 0.5 else ("發散" if x > 2 else "正常")
    )

    def trend_label(r):
        t, d, b = r["均線排列"], r["均線方向"], r["均線距離_pct"]
        if t == "多頭排列" and d == "上揚" and b > 2:
            return "🚀 強勢多頭"
        if t == "多頭排列" and d == "上揚":
            return "🌤️ 穩定多頭"
        if t == "多頭排列" and d == "下彎":
            return "⚠️ 多頭轉弱"
        if t == "空頭排列" and d == "下彎" and b > 2:
            return "💣 強勢空頭"
        if t == "空頭排列" and d == "下彎":
            return "☁️ 穩定空頭"
        if t == "空頭排列" and d == "上揚":
            return "⚠️ 空頭轉弱"
        if r["均線狀態"] == "糾結":
            return "🤝 盤整區間"
        return "❓ 趨勢不明"

    df["趨勢強度說明"] = df.apply(trend_label, axis=1)
    score_map = {
        "🚀 強勢多頭": 3,
        "🌤️ 穩定多頭": 2,
        "⚠️ 多頭轉弱": 1,
        "🤝 盤整區間": 0,
        "⚠️ 空頭轉弱": -1,
        "☁️ 穩定空頭": -2,
        "💣 強勢空頭": -3,
    }
    df["趨勢等級"] = df["趨勢強度說明"].map(score_map).fillna(0)

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
    upsert(df, stock_id)
    update_is_complete()

    sql = f"""
        SELECT * FROM stock_report_daily
        WHERE 股票代號 = '{stock_id}'
          AND 日期 BETWEEN '{sDt.strftime("%Y-%m-%d")}' AND '{eDt.strftime("%Y-%m-%d")}'
        ORDER BY 日期
    """
    output = db.query_to_df(sql)
    output.drop(columns=["id", "is_complete", "updated_at"], inplace=True)

    col_order = [
        "日期","股票代號","開盤價","收盤價","收盤_開盤","最高價","最低價","日振幅","漲跌幅_pct",
        "日振幅_昨收_pct","成交量","量增率_pct","5日均量","5日最大量_日期","5日最大量",
        "10日均量","10日最大量_日期","10日最大量","20日均量","20日最大量_日期","20日最大量",
        "60日均量","60日最大量_日期","60日最大量","實體_pct","上影_pct","下影_pct","K線型態",
        "跳空缺口","5日平均","10日平均","20日平均","60日平均","5日上升幅度","10日上升幅度",
        "20日上升幅度","60日上升幅度","5日扣抵值","5日扣抵影響_pct","10日扣抵值",
        "10日扣抵影響_pct","20日扣抵值","20日扣抵影響_pct","60日扣抵值","60日扣抵影響_pct",
        "均線得分","均線方向","均線排列","均線距離_pct","均線狀態","趨勢強度說明",
        "趨勢等級","5日乖離","10日乖離","20日乖離","60日乖離","總成交金額_億",
        "法人總買超_億","買超_外資_億","買超_投信_億","買超_自營商_億","買超_融資_億",
        "資金走向","資金走向判讀",
    ]

    existing_cols = [c for c in col_order if c in output.columns]
    remaining_cols = [c for c in output.columns if c not in existing_cols]
    output = output[existing_cols + remaining_cols].copy()

    output.to_csv("stock_report.csv", index=False, encoding="utf-8-sig")
    return output

# 
def update_is_complete():
    table = "stock_report_daily"

    # 預設不檢查這些欄位
    exclude_cols = {
        "is_complete",
        "id",
        "跳空缺口",
        "created_at",
        "updated_at"
    }

    # 取得所有欄位
    cols_df = db.query_to_df(f"PRAGMA table_info('{table}');")
    all_cols = cols_df["name"].tolist()

    # 過濾不檢查的欄位
    check_cols = [c for c in all_cols if c not in exclude_cols]

    if not check_cols:
        print("⚠ 沒有可檢查欄位，跳過更新。")
        return

    # 安全包裝欄位名成 "欄位"
    def q(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    # 建立 NULL 判斷條件
    null_conditions = " OR ".join([f"{q(c)} IS NULL" for c in check_cols])

    # 最終 SQL
    sql = f"""
    UPDATE {q(table)}
    SET {q("is_complete")} = CASE
        WHEN {null_conditions} THEN 0
        ELSE 1
    END;
    """

    print("執行 SQL 中 ...")
    ok = db.execute_sql(sql)
    print("更新完成 ✔" if ok else "更新失敗 ❌")

# python -m main.stock_report
if __name__ == "__main__":
    df = taiex_daily_report(60)
    print(df.tail(5))
    print("DONE")
