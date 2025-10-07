import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import requests
import calendar
from db import query_to_df, execute_sql, get_connection, query_single
import json
import twse_api

twseUrl = "https://www.twse.com.tw/rwd/zh"
data_center = "../data/TwStockExchange"
common_params = "response=json"

# ======== 共用工具 ========
def _to_roc_date(dt: datetime) -> str:
    """將 datetime 轉成民國年月日格式（yyy.mm.dd）"""
    roc_year = dt.year - 1911
    return f"{roc_year:03d}.{dt.month:02d}.{dt.day:02d}"

def _roc_to_datetime(roc_date: str) -> datetime:
    """
    將民國年日期字串轉成 datetime
    例如 "114.04.11" → datetime(2025, 4, 11)
    """
    try:
        parts = roc_date.split(".")
        if len(parts) != 3:
            raise ValueError("格式錯誤，應為 yyy.mm.dd")
        year = int(parts[0]) + 1911  # 民國年轉西元年
        month = int(parts[1])
        day = int(parts[2])
        return datetime(year, month, day)
    except Exception as e:
        print(f"[Error] 轉換失敗: {roc_date} ({e})")
        return None
    
def _is_fully_in_range(sDt: datetime, eDt: datetime, minDt: datetime, maxDt: datetime) -> bool:
    """
    判斷區間 sDt~eDt 是否完全包含在 minDt~maxDt 內
    回傳布林值
    """
    return minDt <= sDt <= maxDt and minDt <= eDt <= maxDt

def _is_no_overlap(sDt: datetime, eDt: datetime, minDt: datetime, maxDt: datetime) -> bool:
    """
    判斷區間 sDt~eDt 是否與 minDt~maxDt 完全不重疊
    回傳布林值
    """
    return eDt < minDt or sDt > maxDt

def _overlap_period(sDt: datetime, eDt: datetime, minDt: datetime, maxDt: datetime):
    """
    判斷兩個時間區間是否重疊，並回傳重疊區間。

    參數：
        sDt, eDt : datetime
        minDt, maxDt : datetime

    回傳：
        若有重疊，回傳 (overlap_start, overlap_end)
        若無重疊，回傳 None
    """
    # 先確保時間順序正確
    if sDt > eDt or minDt > maxDt:
        raise ValueError("起訖時間錯誤：start 必須早於 end")

    # 計算重疊區間
    overlap_start = max(sDt, minDt)
    overlap_end = min(eDt, maxDt)

    if overlap_start <= overlap_end:
        return overlap_start, overlap_end
    else:
        return None


# ======== 注意股公告 ========
# 證券代號,證券名稱,累計次數,注意交易資訊,日期,收盤價,本益比
def get_notice(sDt: datetime | None = None, eDt: datetime | None = None):
    if (sDt > eDt):
        return None

    if eDt > datetime.today():
        eDt = datetime.today()

    table = "twse_announcement_notice"
    print("[sDt]=" , sDt, "[eDt]=", eDt)

    ### 先確認庫有資料
    sql = f"SELECT count(*) FROM {table}"
    dataCnt = query_single(sql)
    if (dataCnt < 1):
        maxDt = sDt - relativedelta(days=1)
        minDt = maxDt - relativedelta(days=1)        
    else:
        ### 先確認庫資料的上下界
        sql = f"SELECT min(日期), MAX(日期) FROM {table}"
        df_check = query_to_df(sql)
        minDt = _roc_to_datetime(df_check["min(日期)"].iloc[0])
        maxDt = _roc_to_datetime(df_check["MAX(日期)"].iloc[0])
        print(minDt, maxDt)

    ### 1.資料完全落在庫的範圍，直接搜庫
    if (_is_fully_in_range(sDt, eDt, minDt, maxDt)):
        print("*** 直接從庫提取資料")
        sql = f"SELECT * FROM {table}"
        sql += f" WHERE 日期 between '{_to_roc_date(sDt)}' AND '{_to_roc_date(eDt)}'"
        df = query_to_df(sql)
        return df
    
    ### 2.資料完全落在庫的範圍之外，fetch API
    if (_is_no_overlap(sDt, eDt, minDt, maxDt)):
        print("*** 從API提取資料")
        raw_data = twse_api.fetch_notice(sDt, eDt)
        data = raw_data["data"]

        ### 存到db裡
        values = []
        for r in data:
            try:
                pe = str(r[7]).strip()
                pe_value = float(pe) if pe.replace('.', '', 1).isdigit() else None
                if pe_value is None:
                    continue
                values.append((
                    r[1].strip(),
                    r[2].strip(),
                    int(r[3]),
                    r[4].strip(),
                    r[5].strip(),
                    float(r[6]),
                    pe_value
                ))
            except:
                continue

        sql = """
        INSERT OR REPLACE INTO twse_announcement_notice
        (證券代號, 證券名稱, 累計次數, 注意交易資訊, 日期, 收盤價, 本益比)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        execute_sql(sql, values)
        df = pd.DataFrame(raw_data.get("data", []), columns=raw_data.get("fields", []))
        return df
    
    ### 3.重疊的部分取庫，超出的部分用API，最後merge
    ## 找出重疊部分(From 庫)
    overlap_star, overlap_end = _overlap_period(sDt, eDt, minDt, maxDt)
    if overlap_star is not None:
        df_exist = get_notice(overlap_star, overlap_end)

    ## 找出需要獲取的部分
    if (sDt < minDt):
        df_new_left = get_notice(sDt, minDt - relativedelta(days=1))
    if (maxDt < eDt):
        df_new_right = get_notice(maxDt + relativedelta(days=1), eDt)
    
    ## 合併 DataFrames
    dfs = []
    if 'df_exist' in locals() and isinstance(df_exist, pd.DataFrame):
        dfs.append(df_exist)
    if 'df_new_left' in locals() and isinstance(df_new_left, pd.DataFrame):
        dfs.append(df_new_left)
    if 'df_new_right' in locals() and isinstance(df_new_right, pd.DataFrame):
        dfs.append(df_new_right)

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = None  # 如果都不存在，回傳空 DataFrame
    return df


# ======== 範例測試 ========
if __name__ == "__main__":
    year = 2023
    sDt = datetime(2020, 1, 1)
    eDt = datetime(2025, 12, 31)
    testData = get_notice(sDt, eDt)
    print(testData.head())
