import os
import sys
import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from module import twse

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMC0wNCAxMzoxMjo1NyIsInVzZXJfaWQiOiJueWN1bGFiNjE1IiwiaXAiOiI0Mi43My41NS4xMDYifQ.YMhmYo6sx7_Z0WZwPbNcjDi8gPvt-a6bIx6XHeax4LM"
api = DataLoader()
api.login_by_token(api_token=token)

stockId = 'TAIEX'; # 加權指數
anaMonths = 2 # 近N個月的資料

forceRerun = False # 是否重跑所有資料?
forceReAna = True # 是否重跑分析?

# 重跑所有資料包含了分析也要重跑
if forceRerun:
    forceReAna = True

def reorder_df(df: pd.DataFrame, target_order: list[str]) -> pd.DataFrame:
    new_df = pd.DataFrame()
    for col in target_order:
        if col in df.columns:
            new_df[col] = df[col]
        else:
            new_df[col] = None  # 若欄位不存在，填 None
    return new_df

### 開始產出報告
eDt = datetime.today() # 今天
sDt = eDt - relativedelta(months=anaMonths)  # 當前月份的1日
print(sDt.strftime("%Y%m%d"), eDt.strftime("%Y%m%d"))

anaRootDir = f'../Data/ana/anaKplot/{stockId}/{sDt.strftime("%Y%m")}'
os.makedirs(anaRootDir, exist_ok=True)

outputRootDir = f'../Data/finMind/taiwan_stock_daily_adj/{stockId}/{sDt.strftime("%Y%m")}'
os.makedirs(outputRootDir, exist_ok=True)
outputFile = f'{outputRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}.csv'
if not forceRerun and os.path.exists(outputFile):
    print(f"每日價量資料已存在：{outputFile}")
    df = pd.read_csv(outputFile)
else:
    df = api.taiwan_stock_daily_adj(
        stock_id=stockId,
        start_date=sDt.strftime("%Y-%m-%d"),
        end_date=eDt.strftime("%Y-%m-%d")
    )
    df.to_csv(outputFile, index=False, encoding="utf-8-sig")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

def calc_oc_gap(row):
    day1_range = sorted([row["day1_open"], row["day1_close"]])
    day2_range = sorted([row["day2_open"], row["day2_close"]])

    if day2_range[0] > day1_range[1]:  # 上跳空
        gap_start = day1_range[1]
        gap_end = day2_range[0]
        gap_size = day2_range[0] - day1_range[1]   # 固定 day2 - day1
    elif day2_range[1] < day1_range[0]:  # 下跳空
        gap_start = day2_range[1]
        gap_end = day1_range[0]
        gap_size = day2_range[1] - day1_range[0]   # 固定 day2 - day1
    else:
        return pd.Series([None, None])

    return pd.Series([f"{gap_start:.2f}~{gap_end:.2f}", round(gap_size,2)])

### 計算跳空缺口
def cal_gaps(df: pd.DataFrame):
    # 1. 鄰近日組合
    df_shift = df.shift(-1)
    combined = pd.DataFrame({
        "day1": df["date"],
        "day2": df_shift["date"],
        "day1_open": df["open"],
        "day1_close": df["close"],
        "day1_min": df["min"],
        "day1_max": df["max"],
        "day2_open": df_shift["open"],
        "day2_close": df_shift["close"],
        "day2_min": df_shift["min"],
        "day2_max": df_shift["max"]
    })
    combined = combined[:-1]  # 去掉最後一筆沒有下一日的資料

    # 2. 篩選跳空缺口
    mask_gap = (combined["day2_min"] > combined["day1_max"]) | (combined["day2_max"] < combined["day1_min"])
    gap_df = combined.loc[mask_gap].copy()  # copy 避免警告

    # 3. 計算 oc_gap 與 oc_gap_size
    gap_df[["oc_gap","oc_gap_size"]] = gap_df.apply(calc_oc_gap, axis=1)

    # 4. 最終表格
    final_df = gap_df.loc[:, ["day1","day2","day1_open","day1_close","day2_open","day2_close","oc_gap","oc_gap_size"]].copy()
    final_df.loc[:, ["day1_open","day1_close","day2_open","day2_close"]] = final_df.loc[:, ["day1_open","day1_close","day2_open","day2_close"]].round(2)
    return final_df

# 存檔
df_gaps = cal_gaps(df)
outputFile = f'{anaRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-gaps.csv'
if not forceReAna and os.path.exists(outputFile):
    print(f"跳空缺口資料已存在：{outputFile}")
else:
    df_gaps.to_csv(outputFile, index=False, encoding="utf-8-sig")
del df_gaps

### 三大法人 (Three Major Institutional Investors)
# buy,date,name,sell
if stockId == 'TAIEX' or stockId == 'TPEx' :
    outputRootDir = f'../Data/finMind/taiwan_stock_institutional_investors/all/{sDt.strftime("%Y%m")}'
else:
    outputRootDir = f'../Data/finMind/taiwan_stock_institutional_investors/{stockId}/{sDt.strftime("%Y%m")}'
os.makedirs(outputRootDir, exist_ok=True)
outputFile = f'{outputRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-3mii.csv'
if not forceRerun and os.path.exists(outputFile):
    print(f"三大法人資料已存在：{outputFile}")
    df_3mii = pd.read_csv(outputFile)
else:
    if stockId == 'TAIEX' or stockId == 'TPEx' :
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
    df_3mii.to_csv(outputFile, index=False, encoding="utf-8-sig")

### 融資餘額
df_margin = twse.get_margin_trading(sDt, eDt)
df_margin = df_margin[df_margin["項目"] == "融資金額(仟元)"]
df_margin["日期"] = pd.to_datetime(df_margin["日期"], format="%Y%m%d")

### 開始製作合併資料
# date,stock_id,Trading_Volume,Volume_Change,Trading_money,open,max,min,close,close-open,近5日均量,近10日均量,近20日均量,5MA,10MA,20MA,5_Devi,10_Devi,20_Devi,法人總買超,買超-外資,買超-外資自營商,買超-投信,買超-自營商(自行買賣),買超-自營商(避險)
outputFile = f'{anaRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-daily_report.csv'
if not forceReAna and os.path.exists(outputFile):
    print(f"每日資料已存在：{outputFile}")
    df_merged = pd.read_csv(outputFile)
    print(df_merged.head())
else:
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by="date")

    ### 刪掉目前不需要的欄位
    df = df.drop(columns=["spread", "Trading_turnover"])

    ### 計算衍生欄位
    df['close-open'] = df['close'] - df['open']
    df['max-min'] = df['max'] - df['min']

    volume_change_data = (df["Trading_Volume"] - df["Trading_Volume"].shift(1)) / df["Trading_Volume"].shift(1)
    pos = df.columns.get_loc("Trading_Volume") + 1
    df.insert(pos, "Volume_Change", volume_change_data)
    
    # 近5/10/20日均量 (Trading_Volume 的 rolling mean)
    df["近5日均量"] = df["Trading_Volume"].rolling(window=5).mean()
    df["近10日均量"] = df["Trading_Volume"].rolling(window=10).mean()
    df["近20日均量"] = df["Trading_Volume"].rolling(window=20).mean()

    # 5/10/20 MA (收盤價 close 的 rolling mean)
    df["5MA"] = df["close"].rolling(window=5).mean()
    df["10MA"] = df["close"].rolling(window=10).mean()
    df["20MA"] = df["close"].rolling(window=20).mean()

    # 乖離率 (Devi) = (收盤價 - MA) / MA
    df["5_Devi"] = (df["close"] - df["5MA"]) / df["5MA"]
    df["10_Devi"] = (df["close"] - df["10MA"]) / df["10MA"]
    df["20_Devi"] = (df["close"] - df["20MA"]) / df["20MA"]

    df['date2'] = df['date']
    
    ### 合併三大法人的資料到價量那邊
    if df_3mii.empty:
        df_3mii = pd.DataFrame(columns=["buy", "date", "name", "sell"])
    
    df_3mii['net_buy'] = df_3mii['buy'] - df_3mii['sell']

    # 確保兩邊的 date 型態一致
    df_3mii['date'] = pd.to_datetime(df_3mii['date'])

    # 建立 name → 欄位名稱的對應表（加上 total）
    name_to_col = {
        "Foreign_Investor": "買超-外資", # 外資及陸資(不含外資自營)
        "Investment_Trust": "買超-投信", # 投信
        "Dealer_self": "買超-自營商(自行買賣)", # 自營商(自行買賣)
        "Dealer_Hedging": "買超-自營商(避險)", # 自營商(避險)
        "Foreign_Dealer_Self": "買超-外資自營商", # 外資自營商
        "total": "法人總買超" 
    }

    # 把 name 換成對應的欄位名稱
    df_3mii['col_name'] = df_3mii['name'].map(name_to_col)

    # 轉寬表：date 當索引，col_name 變欄位
    df_3mii_wide = df_3mii.pivot(index='date', columns='col_name', values='net_buy').reset_index()

    # merge 回 df
    df_merged = df.merge(df_3mii_wide, on='date', how='left')

    ### 合併融資餘額的資料
    # 先確保日期欄位型別一致
    df_margin["日期"] = pd.to_datetime(df_margin["日期"], format="%Y%m%d")
    df_merged["date"] = pd.to_datetime(df_merged["date"], format="%Y-%m-%d")
    df_merged = pd.merge(
        df_merged,
        df_margin[["日期", "今日餘額"]],
        how="left",              # 保留 df_merged 的全部列
        left_on="date",          # df_merged 的對應欄位
        right_on="日期"          # df_margin 的對應欄位
    )
    df_merged = df_merged.drop(columns=["日期"])
    # 原本的融資餘額是千元，這邊轉成億
    
    df_merged["今日餘額"] = pd.to_numeric(df_merged["今日餘額"], errors="coerce")
    df_merged["今日餘額"] = df_merged["今日餘額"] * 1000/100000000
    df_merged = df_merged.rename(columns={"今日餘額": "融資餘額(億)"})
    df_merged["融資增減(億)"] = (df_merged["融資餘額(億)"] - df_merged["融資餘額(億)"].shift(1))

    ### 資金走向
    df_merged["資金走向"] = df_merged["close-open"] - (df_merged["法人總買超"]/100000000 + df_merged["融資增減(億)"])
    df_merged["資金走向判讀"] = df_merged["資金走向"].apply(
        lambda x: "偏重大型股(多)" if x > 0 else ("偏重小型股(空)" if x < 0 else None)
    )
    
    df_merged.to_csv(outputFile, index=False, encoding="utf-8-sig")

### 開始製作新報表
rename_dict = {
	"date" : "日期",
	"stock_id" : "股票代號",
	"Trading_Volume" : "成交量",
	"Volume_Change" : "量增率(%)",
	"Trading_money" : "總成交金額(億)",
	"open" : "開盤價",
	"max" : "最高價",
	"min" : "最低價",
	"close" : "收盤價",
	"close-open" : "收盤-開盤",
	"max-min" : "日振幅",
	"近5日均量" : "5日均量",
	"近10日均量" : "10日均量",
	"近20日均量" : "20日均量",
	"5MA" : "5日平均",
	"10MA" : "10日平均",
	"20MA" : "20日平均",
	"5_Devi" : "5日乖離",
	"10_Devi" : "10日乖離",
	"20_Devi" : "20日乖離",
	"法人總買超" : "法人總買超(億)",
	"買超-外資" : "買超-外資(億)",
	"買超-投信" : "買超-投信(億)",
	"融資增減(億)" : "買超-融資(億)",
	"資金走向" : "資金走向",
	"資金走向判讀" : "資金走向判讀"
}
newcol_list = [
	"日期",
	"股票代號",
	"開盤價",
	"收盤價",
	"收盤-開盤",
	"最高價",
	"最低價",
	"日振幅",
	"成交量",
	"量增率(%)",
	"5日均量",
	"5日最大量_日期",
	"5日最大量",
	"10日均量",
	"10日最大量_日期",
	"10日最大量",
	"20日均量",
	"20日最大量_日期",
	"20日最大量",
	"實體(漲跌率)",
	"上影(%)",
	"上影/實體",
	"下影(%)",
	"下影/實體",
	"跳空缺口",
	"5日平均",
	"10日平均",
	"20日平均",
	"5日乖離",
	"10日乖離",
	"20日乖離",
	"總成交金額(億)",
	"法人總買超(億)",
	"買超-外資(億)",
	"買超-投信(億)",
	"買超-自營商(億)",
	"買超-融資(億)",
	"資金走向",
	"資金走向判讀"
]
outputFile = f'{anaRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-new_daily_report.csv'
if not forceReAna and os.path.exists(outputFile):
    print(f"每日新報表已存在：{outputFile}")
    new_df = pd.read_csv(outputFile)
    print(new_df.head())
else:
    # 先把欄位名稱換掉
    df_merged = df_merged.rename(columns=rename_dict)
    
    # 單位換算(億)
    df_merged["總成交金額(億)"] = df_merged["總成交金額(億)"]/100000000
    df_merged["法人總買超(億)"] = df_merged["法人總買超(億)"]/100000000
    df_merged["買超-外資(億)"] = df_merged["買超-外資(億)"]/100000000
    df_merged["買超-外資自營商"] = df_merged["買超-外資自營商"]/100000000
    df_merged["買超-投信(億)"] = df_merged["買超-投信(億)"]/100000000
    df_merged["買超-自營商(自行買賣)"] = df_merged["買超-自營商(自行買賣)"]/100000000
    df_merged["買超-自營商(避險)"] = df_merged["買超-自營商(避險)"]/100000000

    # 安排新報表的欄位順序
    new_df = pd.DataFrame()
    for newcol in newcol_list:
        if newcol in df_merged.columns:
            new_df[newcol] = df_merged[newcol]
        else:
            new_df[newcol] = None
    
    ### 寫入新欄位：
    # 5日最大量_日期, 5日最大量, 10日最大量_日期, 10日最大量, 20日最大量_日期, 20日最大量, 
    # 實體(漲跌率), 上影(%), 上影/實體, 下影(%), 下影/實體, 跳空缺口, 
    # 買超-自營商(億)

    # 最大交易量
    df_merged["成交量"] = pd.to_numeric(df_merged["成交量"], errors="coerce")
    day_counts = [5, 10, 20]
    for n in day_counts:
        # 1️⃣ 最大量數值
        new_df[f"{n}日最大量"] = df_merged["成交量"].rolling(window=n).max()

        # 2️⃣ 找出最大量的日期
        def max_date_in_window(x):
            idx = x.idxmax()
            return df_merged.loc[idx, "日期"]

        # 改用 apply + raw=False 讓 x 是 Series（包含 index）
        new_df[f"{n}日最大量_日期"] = (
            df_merged["成交量"]
            .rolling(window=n)
            .apply(lambda x: x.idxmax(), raw=False)  # 這裡回傳 index（數值）
            .apply(lambda idx: df_merged.loc[int(idx), "日期"] if not pd.isna(idx) else pd.NaT)
        )
    
    # 實體(漲跌率)
    new_df["實體(漲跌率)"] = (df_merged["收盤價"] - df_merged["開盤價"])/df_merged["開盤價"]

    for col in ["開盤價", "收盤價", "最高價", "最低價", "實體(漲跌率)"]:
        new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

    new_df["上影(%)"] = np.where(
        new_df["實體(漲跌率)"] > 0,
        (new_df["最高價"] - new_df["收盤價"]) / new_df["最高價"],
        np.where(
            new_df["實體(漲跌率)"] < 0,
            (new_df["最高價"] - new_df["開盤價"]) / new_df["最高價"],
            np.nan
        )
    )

    new_df["下影(%)"] = np.where(
        new_df["實體(漲跌率)"] > 0,
        (new_df["開盤價"] - new_df["最低價"]) / new_df["最低價"],
        np.where(
            new_df["實體(漲跌率)"] < 0,
            (new_df["收盤價"] - new_df["最低價"]) / new_df["最低價"],
            np.nan
        )
    )

    new_df["上影/實體"] = np.where(
        new_df["實體(漲跌率)"] >= 0,
        (new_df["最高價"] - new_df["收盤價"]) / (new_df["收盤價"] - new_df["開盤價"]).replace(0, np.nan),
        (new_df["最高價"] - new_df["開盤價"]) / (new_df["開盤價"] - new_df["收盤價"]).replace(0, np.nan)
    )

    new_df["下影/實體"] = np.where(
        new_df["實體(漲跌率)"] >= 0,
        (new_df["開盤價"] - new_df["最低價"]) / (new_df["收盤價"] - new_df["開盤價"]).replace(0, np.nan),
        (new_df["收盤價"] - new_df["最低價"]) / (new_df["開盤價"] - new_df["收盤價"]).replace(0, np.nan)
    )

    # 跳空缺口
    df_merged["昨高"] = df_merged["最高價"].shift(1)
    df_merged["昨低"] = df_merged["最低價"].shift(1)
    conditions = [
        df_merged["最低價"] > df_merged["昨高"],   # 上跳空
        df_merged["最高價"] < df_merged["昨低"]    # 下跳空
    ]
    choices = ["上跳空", "下跳空"]
    df_merged["跳空狀態"] = np.select(conditions, choices, default="無跳空")

    # 判斷紅黑K
    df_merged["is_red"] = df_merged["收盤價"] > df_merged["開盤價"]

    # 今上緣、今下緣
    df_merged["今上緣"] = np.where(df_merged["is_red"], df_merged["收盤價"], df_merged["開盤價"])
    df_merged["今下緣"] = np.where(df_merged["is_red"], df_merged["開盤價"], df_merged["收盤價"])

    # 昨上緣、昨下緣（shift 取前一天）
    df_merged["昨上緣"] = df_merged["今上緣"].shift(1)
    df_merged["昨下緣"] = df_merged["今下緣"].shift(1)

    # 計算跳空缺口
    df_merged["跳空缺口"] = np.select(
        [
            df_merged["跳空狀態"] == "上跳空",
            df_merged["跳空狀態"] == "下跳空"
        ],
        [
            df_merged["今下緣"] - df_merged["昨上緣"],   # 上跳空
            df_merged["今上緣"] - df_merged["昨下緣"]    # 下跳空
        ],
        default=None
    )
    new_df["跳空缺口"] = df_merged["跳空缺口"]

    # 買超-自營商(億)
    new_df["買超-自營商(億)"] = df_merged["買超-外資自營商"] + df_merged["買超-自營商(自行買賣)"] + df_merged["買超-自營商(避險)"]

    new_df.to_csv(outputFile, index=False, encoding="utf-8-sig")
