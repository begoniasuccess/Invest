import pandas as pd
import mplfinance as mpf
from FinMind.data import DataLoader

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0yOSAxNTozNjo1MCIsInVzZXJfaWQiOiJueWN1bGFiNjE1IiwiaXAiOiIxNDAuMTEzLjAuMjI5IiwiZXhwIjoxNzU3MDU3ODEwfQ.NF4Ok2t0ah1czrKpBHk8MvfxCFMPbks8MIcW8eD0Z5c"
api = DataLoader()
api.login_by_token(api_token=token)

sDt = '2025-06-01'
eDt = '2025-08-29'
df = api.taiwan_stock_daily_adj(
    stock_id='TAIEX',
    start_date=sDt,
    end_date=eDt
)

# 整理欄位
df.rename(columns={
    "date": "Date",
    "open": "Open",
    "max": "High",
    "min": "Low",
    "close": "Close",
    "Trading_Volume": "Volume"
}, inplace=True)

df["Date"] = pd.to_datetime(df["Date"])
df.set_index("Date", inplace=True)

outputFile = f'Data/twStockDailyAdj_{sDt}_{eDt}.csv'
df.to_csv(outputFile)

# 畫 K 線
mpf.plot(df, type="candle", mav=(5, 10, 20), volume=True,
        title="TAIEX Plot", style="yahoo")

