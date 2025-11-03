# taiEX_daily_report_final.py
import os
import sys
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta
from module import twse

# === 基本設定 ===
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMC0wNCAxMzoxMjo1NyIsInVzZXJfaWQiOiJueWN1bGFiNjE1IiwiaXAiOiI0Mi43My41NS4xMDYifQ.YMhmYo6sx7_Z0WZwPbNcjDi8gPvt-a6bIx6XHeax4LM"
api = DataLoader()
api.login_by_token(api_token=token)

stockId = "TAIEX"
anaMonths = 2
forceRerun = False
forceReAna = True
if forceRerun:
    forceReAna = True

# === 日期與路徑 ===
eDt = datetime.today()
sDt = eDt - relativedelta(months=anaMonths)
print(sDt.strftime("%Y%m%d"), eDt.strftime("%Y%m%d"))

anaRootDir = f"../Data/ana/anaKplot/{stockId}/{sDt.strftime('%Y%m')}"
os.makedirs(anaRootDir, exist_ok=True)

rawDir = f"../Data/finMind/taiwan_stock_daily_adj/{stockId}/{sDt.strftime('%Y%m')}"
os.makedirs(rawDir, exist_ok=True)
rawFile = f"{rawDir}/{sDt.strftime('%Y%m%d')}_{eDt.strftime('%Y%m%d')}.csv"

# === 匯入或抓取日資料 ===
if not forceRerun and os.path.exists(rawFile):
    print(f"每日價量資料已存在：{rawFile}")
    df = pd.read_csv(rawFile)
else:
    df = api.taiwan_stock_daily_adj(
        stock_id=stockId,
        start_date=sDt.strftime("%Y-%m-%d"),
        end_date=eDt.strftime("%Y-%m-%d"),
    )
    df.to_csv(rawFile, index=False, encoding="utf-8-sig")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

# === 三大法人 ===
if stockId in ["TAIEX", "TPEx"]:
    miiDir = f"../Data/finMind/taiwan_stock_institutional_investors/all/{sDt.strftime('%Y%m')}"
else:
    miiDir = f"../Data/finMind/taiwan_stock_institutional_investors/{stockId}/{sDt.strftime('%Y%m')}"
os.makedirs(miiDir, exist_ok=True)
miiFile = f"{miiDir}/{sDt.strftime('%Y%m%d')}_{eDt.strftime('%Y%m%d')}-3mii.csv"

if not forceRerun and os.path.exists(miiFile):
    df_3mii = pd.read_csv(miiFile)
else:
    if stockId in ["TAIEX", "TPEx"]:
        df_3mii = api.taiwan_stock_institutional_investors_total(
            start_date=sDt.strftime("%Y-%m-%d"),
            end_date=eDt.strftime("%Y-%m-%d"),
        )
    else:
        df_3mii = api.taiwan_stock_institutional_investors(
            stock_id=stockId,
            start_date=sDt.strftime("%Y-%m-%d"),
            end_date=eDt.strftime("%Y-%m-%d"),
        )
    df_3mii.to_csv(miiFile, index=False, encoding="utf-8-sig")

# === 融資餘額（twse 自製） ===
df_margin = twse.get_margin_trading(sDt, eDt)
df_margin = df_margin[df_margin["項目"] == "融資金額(仟元)"].copy()
df_margin["日期"] = pd.to_datetime(df_margin["日期"], format="%Y%m%d")

# === 刪掉不需要欄位並加基礎衍生 ===
df = df.drop(columns=["spread", "Trading_turnover"], errors="ignore")
df["close-open"] = df["close"] - df["open"]
df["max-min"] = df["max"] - df["min"]
df["Volume_Change"] = (df["Trading_Volume"] - df["Trading_Volume"].shift(1)) / df["Trading_Volume"].shift(1)

# 均量與均線、乖離
for n in [5, 10, 20]:
    df[f"{n}日均量"] = df["Trading_Volume"].rolling(window=n).mean()
    df[f"{n}MA"] = df["close"].rolling(window=n).mean()
    df[f"{n}_Devi"] = (df["close"] - df[f"{n}MA"]) / df[f"{n}MA"]

# === 合併法人 ===
if df_3mii.empty:
    df_3mii = pd.DataFrame(columns=["buy", "sell", "date", "name"])
df_3mii["net_buy"] = df_3mii["buy"] - df_3mii["sell"]
df_3mii["date"] = pd.to_datetime(df_3mii["date"])
map_name = {
    "Foreign_Investor": "買超-外資",
    "Investment_Trust": "買超-投信",
    "Dealer_self": "買超-自營商(自行買賣)",
    "Dealer_Hedging": "買超-自營商(避險)",
    "Foreign_Dealer_Self": "買超-外資自營商",
    "total": "法人總買超",
}
df_3mii["col_name"] = df_3mii["name"].map(map_name)
df_3mii_wide = df_3mii.pivot(index="date", columns="col_name", values="net_buy").reset_index()
df_merged = df.merge(df_3mii_wide, on="date", how="left")

# === 合併融資 ===
df_merged = df_merged.merge(df_margin[["日期", "今日餘額"]], left_on="date", right_on="日期", how="left")
df_merged.drop(columns=["日期"], inplace=True)
df_merged["融資餘額(億)"] = pd.to_numeric(df_merged["今日餘額"], errors="coerce") * 1000 / 1e8
df_merged["融資增減(億)"] = df_merged["融資餘額(億)"] - df_merged["融資餘額(億)"].shift(1)

# === 資金走向（仍用原本計算基礎） ===
df_merged["資金走向"] = df_merged["close-open"] - (
    (pd.to_numeric(df_merged.get("法人總買超"), errors="coerce") / 1e8) + df_merged["融資增減(億)"]
)
df_merged["資金走向判讀"] = df_merged["資金走向"].apply(
    lambda x: "偏重大型股(多)" if x > 0 else ("偏重小型股(空)" if x < 0 else None)
)

# === 轉中文欄位 ===
rename_dict = {
    "date": "日期",
    "stock_id": "股票代號",
    "Trading_Volume": "成交量",
    "Volume_Change": "量增率(%)",
    "Trading_money": "總成交金額(億)",
    "open": "開盤價",
    "max": "最高價",
    "min": "最低價",
    "close": "收盤價",
    "close-open": "收盤-開盤",
    "max-min": "日振幅",
    "5MA": "5日平均",
    "10MA": "10日平均",
    "20MA": "20日平均",
    "5_Devi": "5日乖離",
    "10_Devi": "10日乖離",
    "20_Devi": "20日乖離",
    "法人總買超": "法人總買超(億)",
    "買超-外資": "買超-外資(億)",
    "買超-投信": "買超-投信(億)",
    "融資增減(億)": "買超-融資(億)",
}
new_df = df_merged.rename(columns=rename_dict).copy()

# === 金額單位統一換算成「億」 ===
for col in ["總成交金額(億)", "法人總買超(億)", "買超-外資(億)", "買超-投信(億)",
            "買超-自營商(自行買賣)", "買超-自營商(避險)", "買超-外資自營商"]:
    if col in new_df.columns:
        new_df[col] = pd.to_numeric(new_df[col], errors="coerce") / 1e8

# 自營商(億) = 外資自營商 + 自營(自行買賣) + 自營(避險)
new_df["買超-自營商(億)"] = (
    pd.to_numeric(new_df.get("買超-外資自營商"), errors="coerce").fillna(0) +
    pd.to_numeric(new_df.get("買超-自營商(自行買賣)"), errors="coerce").fillna(0) +
    pd.to_numeric(new_df.get("買超-自營商(避險)"), errors="coerce").fillna(0)
)

# === 價/線型衍生 ===
new_df["漲跌幅(%)"] = (new_df["收盤價"] - new_df["收盤價"].shift(1)) / new_df["收盤價"].shift(1)
new_df["實體(漲跌率)"] = (new_df["收盤價"] - new_df["開盤價"]) / new_df["開盤價"]

# 上影&下影（依紅黑K）
is_red = new_df["收盤價"] >= new_df["開盤價"]
new_df["上影(%)"] = np.where(
    is_red,
    (new_df["最高價"] - new_df["收盤價"]) / new_df["最高價"],
    (new_df["最高價"] - new_df["開盤價"]) / new_df["最高價"]
)
new_df["下影(%)"] = np.where(
    is_red,
    (new_df["開盤價"] - new_df["最低價"]) / new_df["最低價"],
    (new_df["收盤價"] - new_df["最低價"]) / new_df["最低價"]
)
# 影線/實體
body_up = (new_df["收盤價"] - new_df["開盤價"]).replace(0, np.nan)
body_dn = (new_df["開盤價"] - new_df["收盤價"]).replace(0, np.nan)
new_df["上影/實體"] = np.where(is_red,
    (new_df["最高價"] - new_df["收盤價"]) / body_up,
    (new_df["最高價"] - new_df["開盤價"]) / body_dn
)
new_df["下影/實體"] = np.where(is_red,
    (new_df["開盤價"] - new_df["最低價"]) / body_up,
    (new_df["收盤價"] - new_df["最低價"]) / body_dn
)

# 跳空缺口（以K棒「實體」上/下緣計）
new_df["昨高"] = new_df["最高價"].shift(1)
new_df["昨低"] = new_df["最低價"].shift(1)
# 依實體範圍
today_top = np.where(is_red, new_df["收盤價"], new_df["開盤價"])
today_bot = np.where(is_red, new_df["開盤價"], new_df["收盤價"])
yest_is_red = (new_df["收盤價"].shift(1) >= new_df["開盤價"].shift(1))
yest_top = np.where(yest_is_red, new_df["收盤價"].shift(1), new_df["開盤價"].shift(1))
yest_bot = np.where(yest_is_red, new_df["開盤價"].shift(1), new_df["收盤價"].shift(1))

cond_up_gap = (new_df["最低價"] > new_df["昨高"])
cond_dn_gap = (new_df["最高價"] < new_df["昨低"])
new_df["跳空缺口"] = np.select(
    [cond_up_gap, cond_dn_gap],
    [today_bot - yest_top, today_top - yest_bot],
    default=np.nan
)

# === 均線中文欄位（從英名轉中文） ===
new_df["5日平均"] = df["5MA"]
new_df["10日平均"] = df["10MA"]
new_df["20日平均"] = df["20MA"]
new_df["5日乖離"] = df["5_Devi"]
new_df["10日乖離"] = df["10_Devi"]
new_df["20日乖離"] = df["20_Devi"]

# 均線上升幅度
for n in [5, 10, 20]:
    col = f"{n}日平均"
    new_df[f"{n}日上升幅度"] = new_df[col] - new_df[col].shift(1)

# 扣抵值與影響（明日是否易上揚的直覺指標）
new_df["5日扣抵值"] = new_df["收盤價"].shift(4)
new_df["10日扣抵值"] = new_df["收盤價"].shift(9)
new_df["20日扣抵值"] = new_df["收盤價"].shift(19)
for n in [5, 10, 20]:
    new_df[f"{n}日扣抵影響(%)"] = (new_df["收盤價"] - new_df[f"{n}日扣抵值"]) / new_df["收盤價"]

# 均線排列/方向/得分/距離/狀態
def judge_ma_type(r):
    a, b, c = r["5日平均"], r["10日平均"], r["20日平均"]
    if pd.notna(a) and pd.notna(b) and pd.notna(c):
        if a > b > c:  return "多頭排列"
        if a < b < c:  return "空頭排列"
    return "糾結"
new_df["均線排列"] = new_df.apply(judge_ma_type, axis=1)

for n in [5, 10, 20]:
    new_df[f"{n}日斜率"] = new_df[f"{n}日平均"] - new_df[f"{n}日平均"].shift(1)

def ma_score(r):
    s = 0
    for n in [5, 10, 20]:
        v = r[f"{n}日斜率"]
        if pd.isna(v): continue
        s += 1 if v > 0 else -1 if v < 0 else 0
    return s
new_df["均線得分"] = new_df.apply(ma_score, axis=1)
new_df["均線方向"] = new_df["均線得分"].apply(lambda s: "上揚" if s >= 2 else ("下彎" if s <= -2 else "糾結"))

new_df["均線距離(%)"] = (
    (new_df[["5日平均","10日平均","20日平均"]].max(axis=1) -
     new_df[["5日平均","10日平均","20日平均"]].min(axis=1)) /
    new_df[["5日平均","10日平均","20日平均"]].mean(axis=1) * 100
)
new_df["均線狀態"] = new_df["均線距離(%)"].apply(lambda x: "糾結" if x < 0.5 else ("發散" if x > 2 else "正常"))

def trend_label(r):
    t, d, b = r["均線排列"], r["均線方向"], r["均線距離(%)"]
    if t == "多頭排列" and d == "上揚" and b > 2: return "🚀 強勢多頭"
    if t == "多頭排列" and d == "上揚":           return "🌤️ 穩定多頭"
    if t == "多頭排列" and d == "下彎":           return "⚠️ 多頭轉弱"
    if t == "空頭排列" and d == "下彎" and b > 2: return "💣 強勢空頭"
    if t == "空頭排列" and d == "下彎":           return "☁️ 穩定空頭"
    if t == "空頭排列" and d == "上揚":           return "⚠️ 空頭轉弱"
    if r["均線狀態"] == "糾結":                    return "🤝 盤整區間"
    return "❓ 趨勢不明"
new_df["趨勢強度說明"] = new_df.apply(trend_label, axis=1)

score_map = {
    "🚀 強勢多頭": 3, "🌤️ 穩定多頭": 2, "⚠️ 多頭轉弱": 1,
    "🤝 盤整區間": 0, "⚠️ 空頭轉弱": -1, "☁️ 穩定空頭": -2, "💣 強勢空頭": -3
}
new_df["趨勢等級"] = new_df["趨勢強度說明"].map(score_map).fillna(0)

# === 5/10/20 日「最大量」與「日期」 ===
def rolling_max_with_date(vol_series: pd.Series, date_series: pd.Series, window: int):
    """
    回傳 (最大量, 對應日期)
    使用 pandas idxmax() 的 rolling.apply 回傳 index 位置，再用 date_series 取日期。
    """
    # 先找到每個視窗內最大量的 index
    max_index = vol_series.rolling(window).apply(lambda x: x.idxmax(), raw=False)
    # 對應日期
    max_date = max_index.map(lambda i: date_series.iloc[int(i)] if pd.notna(i) else pd.NaT)
    # 最大值
    max_val = vol_series.rolling(window).max()
    return max_val, max_date

new_df["成交量"] = pd.to_numeric(new_df["成交量"], errors="coerce")
for n in [5, 10, 20]:
    vmax, vdate = rolling_max_with_date(new_df["成交量"], new_df["日期"], n)
    new_df[f"{n}日最大量"] = vmax
    new_df[f"{n}日最大量_日期"] = pd.to_datetime(vdate).dt.strftime("%Y-%m-%d")

# === 欄位順序（強制補齊缺漏） ===
columns_order = [
    "日期","股票代號",
    "開盤價","收盤價","收盤-開盤","最高價","最低價","日振幅","漲跌幅(%)",
    "成交量","量增率(%)",
    "5日均量","5日最大量_日期","5日最大量",
    "10日均量","10日最大量_日期","10日最大量",
    "20日均量","20日最大量_日期","20日最大量",
    "實體(漲跌率)","上影(%)","上影/實體","下影(%)","下影/實體","跳空缺口",
    "5日平均","10日平均","20日平均",
    "5日上升幅度","10日上升幅度","20日上升幅度",
    "5日扣抵值","5日扣抵影響(%)",
    "10日扣抵值","10日扣抵影響(%)",
    "20日扣抵值","20日扣抵影響(%)",
    "均線得分","均線方向","均線排列","均線距離(%)","均線狀態","趨勢強度說明","趨勢等級",
    "5日乖離","10日乖離","20日乖離",
    "總成交金額(億)","法人總買超(億)","買超-外資(億)","買超-投信(億)","買超-自營商(億)","買超-融資(億)",
    "資金走向","資金走向判讀"
]
for c in columns_order:
    if c not in new_df.columns:
        new_df[c] = np.nan
new_df = new_df.reindex(columns=columns_order)

# === 儲存 ===
outFile = f"{anaRootDir}/{sDt.strftime('%Y%m%d')}_{eDt.strftime('%Y%m%d')}-final_daily_report.csv"
new_df.to_csv(outFile, index=False, encoding="utf-8-sig")
print(f"✅ 已完成報表：{outFile}")
