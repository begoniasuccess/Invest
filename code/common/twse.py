import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import requests

twseUrl = "https://www.twse.com.tw/rwd/zh"

# ======== 共用工具 ========
def _date_to_str(date: datetime = None, formate: str = None) -> str:
    """將 datetime 轉為 yyyymmdd 字串，若未指定則取今日"""
    if date is None:
        date = datetime.today()
    if formate is None:
        formate = "%Y%m%d"
    return date.strftime(formate)

def _save_to_csv(df: pd.DataFrame, apiEndpoint: str, filename: str):
    data_center = "../data/TwStockExchange"
    
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

# ======== 1. 個股成交日資訊 ========
def get_stock_day(stock_no: str, date: datetime | None = None) -> pd.DataFrame:
    date_str = _date_to_str(date)
    apiEndpoint = "afterTrading/STOCK_DAY"
    apiParams = f"date={date_str}&stockNo={stock_no}&response=json"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", []))
    _save_to_csv(df, apiEndpoint, f"{stock_no}_{date_str}")
    return df

# ======== 2. 個股收盤價 ========
def get_stock_day_avg(stock_no: str, date: datetime | None = None) -> pd.DataFrame:
    date_str = _date_to_str(date)
    apiEndpoint = "afterTrading/STOCK_DAY_AVG"
    apiParams = f"date={date_str}&stockNo={stock_no}&response=json"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", []))
    _save_to_csv(df, apiEndpoint, f"{stock_no}_{date_str}")
    return df

# ======== 3. 三大法人 ========
def get_institutional_investors(date: datetime | None = None) -> pd.DataFrame:
    date_str = _date_to_str(date)
    apiEndpoint = "fund/BFI82U"
    apiParams = f"type=day&dayDate={date_str}&weekDate={date_str}&monthDate={date_str}&response=json"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", []))
    _save_to_csv(df, apiEndpoint, f"{date_str}")
    return df

# ======== 3.2 三大法人 區間版 ========
def get_institutional_investors_range(startDate: datetime, endDate: datetime) -> pd.DataFrame:
    apiEndpoint = "fund/BFI82U"
    all_data = []

    current = startDate
    while current <= endDate:
        date_str = _date_to_str(current)
        apiParams = f"type=day&dayDate={date_str}&weekDate={date_str}&monthDate={date_str}&response=json"
        apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

        res = requests.get(apiUrl)
        data = res.json()

        # 如果 API 回傳不是 OK 或沒有資料，跳過
        if data.get("stat") != "OK" or "data" not in data:
            print(f"⚠️ {date_str} 沒有資料，已跳過")
            current += timedelta(days=1)
            continue

        df_day = pd.DataFrame(data["data"], columns=data["fields"])
        df_day.insert(0, "日期", date_str)  # 加上日期欄位
        all_data.append(df_day)

        current += timedelta(days=1)

    # 合併所有日期的資料
    if not all_data:
        raise ValueError("❌ 區間內沒有任何有效資料")

    df_all = pd.concat(all_data, ignore_index=True)

    # 存成一個總檔案
    filename = f"{_date_to_str(startDate)}_{_date_to_str(endDate)}"
    _save_to_csv(df_all, apiEndpoint, filename)

    return df_all

# ======== 4. 融資融券餘額 ========
def get_margin_trading(date: datetime | None = None) -> pd.DataFrame:
    date_str = _date_to_str(date)
    apiEndpoint = "marginTrading/MI_MARGN"
    apiParams = f"date={date_str}&selectType=MS&response=json"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    # tables[0] 才有資料
    tables = data.get("tables", [])
    if not tables or "fields" not in tables[0] or "data" not in tables[0]:
        raise ValueError(f"❌ API 回傳格式異常: {data}")

    fields = tables[0]["fields"]
    rows = tables[0]["data"]

    df = pd.DataFrame(rows, columns=fields)

    _save_to_csv(df, apiEndpoint, f"{date_str}")
    return df

# ======== 4.2 融資融券餘額 區間版 ========
def get_margin_trading_range(startDate: datetime, endDate: datetime) -> pd.DataFrame:
    apiEndpoint = "marginTrading/MI_MARGN"
    all_data = []

    current = startDate
    while current <= endDate:
        date_str = _date_to_str(current)
        apiParams = f"date={date_str}&selectType=MS&response=json"
        apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

        res = requests.get(apiUrl)
        data = res.json()

        # 如果回傳只有 "stat" 且不是 "OK"，表示沒資料
        if data.get("stat") != "OK":
            print(f"⚠️ {date_str} 沒有資料，已跳過")
            current += timedelta(days=1)
            continue

        tables = data.get("tables", [])
        if tables and "fields" in tables[0] and "data" in tables[0]:
            fields = tables[0]["fields"]
            rows = tables[0]["data"]

            df_day = pd.DataFrame(rows, columns=fields)
            df_day.insert(0, "日期", date_str)  # 加上日期欄位
            all_data.append(df_day)
        else:
            print(f"⚠️ {date_str} API 格式異常，已跳過")

        current += timedelta(days=1)

    # 合併所有日期的資料
    if not all_data:
        raise ValueError("❌ 區間內沒有任何有效資料")

    df_all = pd.concat(all_data, ignore_index=True)

    # 存成一個總檔案
    filename = f"{_date_to_str(startDate)}_{_date_to_str(endDate)}"
    _save_to_csv(df_all, apiEndpoint, filename)

    return df_all

# ======== 5. 注意股公告 ========
def get_notice(start_date: datetime | None = None, end_date: datetime | None = None) -> pd.DataFrame:
    start_str = _date_to_str(start_date)
    end_str = _date_to_str(end_date)
    apiEndpoint = "announcement/notice"
    apiParams = f"querytype=1&stockNo=&selectType=&startDate={start_str}&endDate={end_str}&sortKind=STKNO&response=json"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", []))
    _save_to_csv(df, apiEndpoint, f"{start_str}_{end_str}")
    return df


# ======== 範例測試 ========
if __name__ == "__main__":
    test = datetime.today()
    # test = test - relativedelta(months=1)

    # # 測試下載各項資料
    # get_stock_day("2330", test)
    # get_stock_day_avg("0050", test)
    # get_institutional_investors(test)
    # get_margin_trading(test)
    # get_notice(datetime(2025, 10, 1), datetime(2025, 10, 4))
    get_margin_trading_range(datetime(2025, 10, 1), datetime(2025, 10, 4))
