import sys, os
sys.path.append(os.path.dirname(__file__))

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

data_center = "../data/TwStockExchange"

def _date_to_str(date: datetime = None, formate: str = None) -> str:
    """將 datetime 轉為 yyyymmdd 字串，若未指定則取今日"""
    if date is None:
        date = datetime.today()
    if formate is None:
        formate = "%Y%m%d"
    return date.strftime(formate)

def _save_to_csv(df: pd.DataFrame, apiEndpoint: str, filename: str):
    # 1. 檢查 data_center 是否存在
    if not os.path.exists(data_center):
        raise FileNotFoundError(f"❌ data_center 不存在：{data_center}")
    
    # 2. 確保目標資料夾存在
    dir_path = os.path.join(data_center, apiEndpoint)
    os.makedirs(dir_path, exist_ok=True)  # 如果不存在就建立
    
    path = os.path.join(dir_path, f"{filename}.csv")
    
    # 3. 儲存 CSV
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ 已儲存：{path}")

def _read_from_csv(apiEndpoint: str, filename: str) -> pd.DataFrame:
    # 1. 檢查 data 是否存在
    dir_path = os.path.join(data_center, apiEndpoint)    
    path = os.path.join(dir_path, f"{filename}.csv")

    if not os.path.exists(path):
        print(f"❌ CSV檔案 不存在：{path}")
        return None
    
    df = pd.read_csv(path)
    return df

def _cleanup_old_files(dir_path: str, stock_no: str, date_str: str, keep: str):
    """刪除同月份中除了 keep 的其他檔案"""
    for f in os.listdir(dir_path):
        if f.startswith(f"{stock_no}_{date_str}") and f.endswith(".csv") and f != keep:
            try:
                os.remove(os.path.join(dir_path, f))
                print(f"🧹 已刪除舊檔：{f}")
            except Exception as e:
                print(f"⚠️ 無法刪除 {f}: {e}")

def _to_roc_date(dt: datetime) -> str:
    """將 datetime 轉成民國年月日格式（yyy.mm.dd）"""
    roc_year = dt.year - 1911
    return f"{roc_year:03d}.{dt.month:02d}.{dt.day:02d}"

def _roc_to_datetime(roc_date: str, seperator: str= None) -> datetime:
    """
    將民國年日期字串轉成 datetime
    例如 "114.04.11" → datetime(2025, 4, 11)
    """
    
    if seperator is None:
        seperator = "."
    try:
        parts = roc_date.split(seperator)
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