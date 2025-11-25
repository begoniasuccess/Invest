import sys, os
sys.path.append(os.path.dirname(__file__))

import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from common import tools

twseUrl = "https://www.twse.com.tw/rwd/zh"
data_center = "../data/TwStockExchange"
common_params = "response=json"

# # ======== 1. 個股成交日資訊 (含快取、合併、清理機制) ========
# # 日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
# def get_stock_day(stock_no: str, date: datetime | None = None) -> pd.DataFrame:
#     today = datetime.today()
#     date = (date or today).replace(day=1)
#     date_str = date.strftime("%Y%m")
#     apiEndpoint = "afterTrading/STOCK_DAY"
#     dir_path = os.path.join(data_center, apiEndpoint)
#     os.makedirs(dir_path, exist_ok=True)

#     # 嘗試完整檔
#     df = tools._read_from_csv(apiEndpoint, f"{stock_no}_{date_str}")
#     if df is not None:
#         print(f"📂 檔案已存在：{stock_no}_{date_str}.csv")
#         return df

#     # 呼叫 API
#     apiUrl = f"{twseUrl}/{apiEndpoint}?date={date.strftime('%Y%m%d')}&stockNo={stock_no}&{common_params}"
#     res = requests.get(apiUrl).json()
#     df = pd.DataFrame(res.get("data", []), columns=res.get("fields", []))

#     if df.empty:
#         print("⚠️ API 回傳空資料")
#         return df

#     # 處理民國年日期（例如 '114/10/03' → '20241003'）
#     raw = df.iloc[-1, 0].replace("/", "")
#     if len(raw) == 7:  # 民國年格式
#         raw = str(int(raw[:3]) + 1911) + raw[3:]
#     last_date = datetime.strptime(raw, "%Y%m%d")

#     # 判斷是否為當月（尚未結束的月）
#     is_current_month = date.year == today.year and date.month == today.month
#     if not is_current_month:
#         # 過去月份一定存成完整月檔
#         filename = f"{stock_no}_{date_str}"
#     else:
#         # 本月尚未結束，可能不完整
#         days_in_month = calendar.monthrange(date.year, date.month)[1]
#         if last_date.day == days_in_month:
#             filename = f"{stock_no}_{date_str}"
#         else:
#             filename = f"{stock_no}_{date.strftime('%Y%m%d')}_{last_date.strftime('%Y%m%d')}"

#     tools._save_to_csv(df, apiEndpoint, filename)
#     tools._cleanup_old_files(dir_path, stock_no, date_str, keep=f"{filename}.csv")

#     return df

# # ======== 2. 個股收盤價 ========
# # 日期,收盤價
# def get_stock_day_avg(stock_no: str, date: datetime | None = None) -> pd.DataFrame:
#     today = datetime.today()
#     date = (date or today).replace(day=1)
#     date_str = date.strftime("%Y%m")
#     apiEndpoint = "afterTrading/STOCK_DAY_AVG"
#     dir_path = os.path.join(data_center, apiEndpoint)
#     os.makedirs(dir_path, exist_ok=True)

#     # 嘗試完整檔
#     df = tools._read_from_csv(apiEndpoint, f"{stock_no}_{date_str}")
#     if df is not None:
#         print(f"📂 檔案已存在：{stock_no}_{date_str}.csv")
#         return df

#     # 呼叫 API
#     apiUrl = f"{twseUrl}/{apiEndpoint}?date={date.strftime('%Y%m%d')}&stockNo={stock_no}&{common_params}"
#     res = requests.get(apiUrl).json()
#     df = pd.DataFrame(res.get("data", []), columns=res.get("fields", []))

#     if df.empty:
#         print("⚠️ API 回傳空資料")
#         return df

#     # 找最後一個合法日期（排除非日期列，例如月平均收盤價）
#     date_col = df.columns[0]  # 假設第一欄是日期
#     for d in reversed(df[date_col]):
#         raw = str(d).replace("/", "")
#         if raw.isdigit() and (len(raw) == 7 or len(raw) == 8):
#             # 處理民國年格式
#             if len(raw) == 7:
#                 raw = str(int(raw[:3]) + 1911) + raw[3:]
#             last_date = datetime.strptime(raw, "%Y%m%d")
#             break
#     else:
#         raise ValueError("找不到有效日期欄位")

#     # 判斷是否為當月（尚未結束的月）
#     is_current_month = date.year == today.year and date.month == today.month
#     if not is_current_month:
#         # 過去月份直接存完整月檔
#         filename = f"{stock_no}_{date_str}"
#     else:
#         # 本月尚未結束
#         days_in_month = calendar.monthrange(date.year, date.month)[1]
#         if last_date.day == days_in_month:
#             filename = f"{stock_no}_{date_str}"
#         else:
#             filename = f"{stock_no}_{date.strftime('%Y%m%d')}_{last_date.strftime('%Y%m%d')}"

#     # 儲存 CSV 並清理舊檔
#     tools._save_to_csv(df, apiEndpoint, filename)
#     tools._cleanup_old_files(dir_path, stock_no, date_str, keep=f"{filename}.csv")

#     return df

# # ======== 3. 三大法人 ========
# # 單位名稱,買進金額,賣出金額,買賣差額
# def get_institutional_investors(date: datetime | None = None) -> pd.DataFrame:
#     date_str = tools._date_to_str(date)
#     apiEndpoint = "fund/BFI82U"
#     apiParams = f"type=day&dayDate={date_str}&weekDate={date_str}&monthDate={date_str}&{common_params}"
#     apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

#     res = requests.get(apiUrl)
#     data = res.json()

#     df = pd.DataFrame(data.get("data", []), columns=data.get("fields", []))
#     tools._save_to_csv(df, apiEndpoint, f"{date_str}")
#     return df

# # ======== 3.2 三大法人 區間版 ========
# # 單位名稱,買進金額,賣出金額,買賣差額
# def get_institutional_investors_range(startDate: datetime, endDate: datetime) -> pd.DataFrame:
#     apiEndpoint = "fund/BFI82U"
#     all_data = []
#     current = startDate
#     while current <= endDate:
#         date_str = tools._date_to_str(current)
#         apiParams = f"type=day&dayDate={date_str}&weekDate={date_str}&monthDate={date_str}&{common_params}"
#         apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

#         res = requests.get(apiUrl)
#         data = res.json()

#         # 如果 API 回傳不是 OK 或沒有資料，跳過
#         if data.get("stat") != "OK" or "data" not in data:
#             print(f"⚠️ {date_str} 沒有資料，已跳過")
#             current += timedelta(days=1)
#             continue

#         df_day = pd.DataFrame(data["data"], columns=data["fields"])
#         df_day.insert(0, "日期", date_str)  # 加上日期欄位
#         all_data.append(df_day)

#         current += timedelta(days=1)

#     # 合併所有日期的資料
#     if not all_data:
#         raise ValueError("❌ 區間內沒有任何有效資料")

#     df_all = pd.concat(all_data, ignore_index=True)

#     # 存成一個總檔案
#     filename = f"{tools._date_to_str(startDate)}_{tools._date_to_str(endDate)}"
#     tools._save_to_csv(df_all, apiEndpoint, filename)

#     return df_all

# ======== 4. 融資融券餘額 ========
# 項目,買進,賣出,現金(券)償還,前日餘額,今日餘額
def fetch_margin_trading(date: datetime | None = None):
    date_str = date.strftime("%Y%m%d")
    apiEndpoint = "marginTrading/MI_MARGN"
    apiParams = f"date={date_str}&selectType=MS&{common_params}"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    try:
        res = requests.get(apiUrl, timeout=10)
        text = res.text.strip()

        # 1) 空白 → 假日 or TWSE 掛掉
        if text == "":
            print(f"[跳過] TWSE 回傳空白（假日?）: {date_str}")
            return None

        # 2) HTML → 被擋 or 維護
        if text.startswith("<") or text.startswith("<!--"):
            print(f"[跳過] TWSE 回傳 HTML（維護或被BAN）: {date_str}")
            return None

        # 3) 嘗試解析 JSON
        data = res.json()

    except Exception as e:
        print(f"[錯誤] JSON 解析失敗 {date_str}: {e}")
        return None

    # 4) TWSE 自己給的錯誤訊息
    if "stat" in data and ("無" in data["stat"] or "抱歉" in data["stat"]):
        print(f"[跳過] 無融資資料: {date_str}")
        return None

    # 5) tables 結構不完整
    tables = data.get("tables")
    if not tables or "fields" not in tables[0] or "data" not in tables[0]:
        print(f"[跳過] TWSE 回傳格式錯誤: {date_str}")
        return None

    return tables[0]

# # ======== 4.2 融資融券餘額 區間版 ========
# # 日期,項目,買進,賣出,現金_券_償還,前日餘額,今日餘額
def fetch_margin_trading_range(sDt: datetime, eDt: datetime):
    if sDt > eDt:
        return None
    
    data = []    
    current = sDt

    while current <= eDt:
        currentData = fetch_margin_trading(current)

        # 假日 or 無資料 or HTML → 跳過這一天
        if currentData is None:
            current += timedelta(days=1)
            continue
        
        rows = currentData.get("data", [])
        date_str = current.strftime("%Y%m%d")

        for aRow in rows:
            aRow.insert(0, date_str)

        data.extend(rows)
        current += timedelta(days=1)
        
    return {
        "fields": ['日期', '項目', '買進', '賣出', '現金_券_償還', '前日餘額', '今日餘額'],
        "data": data
    }


# ======== 5. 注意股公告 ========
# 編號,證券代號,證券名稱,累計次數,注意交易資訊,日期,收盤價,本益比
def fetch_notice(sDt: datetime | None = None, eDt: datetime | None = None):
    if (sDt > eDt):
        return None

    if eDt > datetime.today():
        eDt = datetime.today()

    start_str = sDt.strftime("%Y%m%d")
    end_str = eDt.strftime("%Y%m%d")

    sortKinds = ["STKNO", "DATE"]

    apiEndpoint = "announcement/notice"
    apiParams = f"querytype=1&{common_params}"
    apiParams += f"&stockNo=&selectType=&sortKind={sortKinds[1]}"
    apiParams += f"&startDate={start_str}&endDate={end_str}"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    raw_data = res.json()
    df = pd.DataFrame(raw_data.get("data", []), columns=raw_data.get("fields", []))
    tools._save_to_csv(df, apiEndpoint, f"{start_str}_{end_str}")
    del df
    return raw_data

# ======== 範例測試 ========
if __name__ == "__main__":
    # test = datetime.today()
    test = datetime(2025, 9, 3)
    print(fetch_margin_trading_range(test, test))