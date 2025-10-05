import os
import sys
import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMC0wNCAxMzoxMjo1NyIsInVzZXJfaWQiOiJueWN1bGFiNjE1IiwiaXAiOiI0Mi43My41NS4xMDYifQ.YMhmYo6sx7_Z0WZwPbNcjDi8gPvt-a6bIx6XHeax4LM"
api = DataLoader()
api.login_by_token(api_token=token)

anaMonths = 2 # 近N個月的資料
stockIdList = ["TAIEX", "TPEx", "0050", "00687B", "006201"]
#'TAIEX' # 加權指數
#'TPEx' # 櫃買指數

forceRerun = False # 是否呈重跑所有資料?
forceReAna = False # 是否重跑分析?

# 重跑所有資料包含了分析也要重跑
if forceRerun:
    forceReAna = True

### 開始產出報告
for stockId in stockIdList:
    eDt = datetime.today() # 今天
    sDt = eDt - relativedelta(months=anaMonths)  # 當前月份的1日
    # sDt = datetime(2025, 7, 4)

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

    ### 開始製作合併資料
    outputFile = f'{anaRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-daily_report.csv'
    if not forceReAna and os.path.exists(outputFile):
        print(f"每日資料已存在：{outputFile}")
        df_merged = pd.read_csv(outputFile)
    else:
        ### 刪掉目前不需要的欄位
        df = df.drop(columns=["spread", "Trading_turnover"])

        ### 計算衍生欄位
        df['close-open'] = df['close'] - df['open']
        
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

        df['date'] = pd.to_datetime(df['date'])
        
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
        
        df_merged.to_csv(outputFile, index=False, encoding="utf-8-sig")