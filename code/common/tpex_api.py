import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import tools

tpexUrl = "https://www.tpex.org.tw/www/zh-tw"
data_center = "../Data/TaipeiExchange"
common_params = "response=json"

# ======== 注意股公告 ========
# 編號,證券代號,證券名稱,累計,注意交易資訊,公告日期,收盤價,本益比,link
def fetch_notice(sDt: datetime | None = None, eDt: datetime | None = None):
    if (sDt > eDt):
        return None

    if eDt > datetime.today():
        eDt = datetime.today()

    start_str = tools._date_to_str(sDt, "%Y/%m/%d")
    end_str = tools._date_to_str(eDt, "%Y/%m/%d")

    apiEndpoint = "bulletin/attention"
    apiUrl = f"{tpexUrl}/{apiEndpoint}"
    apiParams = {
        "startDate" : start_str,
        "endDate" : end_str,
        "code" : None,
        "cate" : None,
        "type" : "all",
        "order" : "date",
        "id" : None,
        "response" : "json"
    }

    # 發送 POST 請求
    response = requests.post(apiUrl, data=apiParams)
    response.raise_for_status()  # 檢查 HTTP 錯誤
    
    # 取得回應
    print("Status code:", response.status_code)
    # print("Response text:", response.text)

    # 嘗試把回傳內容轉成 JSON
    raw_data = response.json()

    return raw_data