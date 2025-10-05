from datetime import datetime
import dataHandler as db
import requests
import pandas as pd

twseUrl = "https://www.twse.com.tw/rwd/zh"
common_params = "response=json"

def convert_to_kebab_camel(s: str) -> str:
    """
    將字串用 / 分段，每段轉成首字母小寫駝峰，最後用 - 串起來
    """
    def to_camel(segment: str) -> str:
        parts = segment.lower().split("_")
        return parts[0] + ''.join(p.capitalize() for p in parts[1:])

    segments = s.split("/")
    segments_camel = [to_camel(seg) for seg in segments]
    return "_".join(segments_camel)

def get_margin_trading(date: datetime | None = None) -> pd.DataFrame:
    """
    使用 twse_db.py 的 auto_update_day 封裝單日抓取
    只回傳單日 DataFrame
    """
    date = date or datetime.today()
    date_str = date.strftime("%Y-%m-%d")

    # 封裝 fetch_api 函數
    def fetch_api(api_name, endpoint, date_str_api):
        apiUrl = f"{twseUrl}/{endpoint}?date={date_str_api}&selectType=MS&{common_params}"
        res = requests.get(apiUrl)
        data = res.json()

        tables = data.get("tables", [])
        if not tables or "fields" not in tables[0] or "data" not in tables[0]:
            return pd.DataFrame()

        fields = tables[0]["fields"]
        rows = tables[0]["data"]
        df = pd.DataFrame(rows, columns=fields)
        df["time"] = date.strftime("%Y-%m-%d")  # time 欄位方便 SQLite
        return df

    # 使用 twse_db.py 的 auto_update_day 來抓單日資料
    endpoint = "marginTrading/MI_MARGN"
    api_name = f"twse-{convert_to_kebab_camel(endpoint)}"    
    db.auto_update_day(
        api_name,
        endpoint,
        date=date,
        fetch_api=fetch_api
    )

    # 抓完就直接讀 SQLite    
    df = db.read_data(api_name, start=date_str, end=date_str)
    return df

if __name__ == "__main__":
    dt = datetime(2025, 10, 2)
    df = get_margin_trading(dt)
    print(df)