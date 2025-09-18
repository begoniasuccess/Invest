import os
import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOS0xOCAxMTozMjoyOCIsInVzZXJfaWQiOiJueWN1bGFiNjE1IiwiaXAiOiI0Mi43My4xMDMuMjQ4In0.vidiMk4_ZU0shUvXklPfTe3MjmERnrIL4JvPiHJWtDQ"
api = DataLoader()
api.login_by_token(api_token=token)

anaMonths = 2 # 近N個月的資料

stockId = 'TAIEX' # 加權指數
eDt = datetime.today() # 今天
sDt = eDt - relativedelta(months=anaMonths)  # 當前月份的1日

outputRootDir = f'Data/finMind/taiwan_stock_daily_adj/{stockId}/{sDt.strftime("%Y%m")}'
os.makedirs(outputRootDir, exist_ok=True)
outputFile = f'{outputRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}.csv'
if os.path.exists(outputFile):
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
outputRootDir = f'Data/ana/gaps/{stockId}/{sDt.strftime("%Y%m")}'
os.makedirs(outputRootDir, exist_ok=True)
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
outputFile = f'{outputRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-gaps.csv'
if os.path.exists(outputFile):
    print(f"跳空缺口資料已存在：{outputFile}")
else:
    df_gaps.to_csv(outputFile, index=False, encoding="utf-8-sig")
del df_gaps


### 三大法人 (Three Major Institutional Investors)
outputRootDir = f'Data/finMind/taiwan_stock_institutional_investors/{stockId}/{sDt.strftime("%Y%m")}'
os.makedirs(outputRootDir, exist_ok=True)
outputFile = f'{outputRootDir}/{sDt.strftime("%Y%m%d")}_{eDt.strftime("%Y%m%d")}-3mii.csv'
if os.path.exists(outputFile):
    print(f"三大法人資料已存在：{outputFile}")
else:
    if stockId == 'TAIEX':
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

### 合併三大法人的資料到價量那邊
