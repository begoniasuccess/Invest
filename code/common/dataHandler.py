import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time


# ---------- 設定 ----------
data_center = "../data_center"
DB_PATH = os.path.join(data_center, "data_center.db")
os.makedirs(data_center, exist_ok=True)

# ---------- 核心函數 ----------
def save_to_db(df: pd.DataFrame, table_name: str, time_col: str = "time"):
    """
    將資料存入 SQLite，若表格不存在自動建立
    自動去重 (以 time_col 為基準)
    """
    if df.empty:
        return

    conn = sqlite3.connect(DB_PATH)
    # 建表
    df.to_sql(table_name, conn, if_exists='append', index=False)
    # 去重
    conn.execute(f"""
        DELETE FROM {table_name}
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM {table_name}
            GROUP BY {time_col}
        )
    """)
    conn.commit()
    conn.close()
    print(f"✅ 已存入 {table_name} 表格")

def day_exists(table_name: str, date_str: str, time_col: str = "time") -> bool:
    """檢查某日資料是否存在"""
    if not os.path.exists(DB_PATH):
        return False
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT 1 FROM {table_name} WHERE {time_col} LIKE '{date_str}%' LIMIT 1;"
    exists = conn.execute(query).fetchone() is not None
    conn.close()
    return exists

def read_data(table_name: str, start: str = None, end: str = None) -> pd.DataFrame:
    """讀取表格資料，可指定日期範圍"""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    sql = f"SELECT * FROM {table_name}"
    if start and end:
        sql += f" WHERE time BETWEEN '{start}' AND '{end}'"
    elif start:
        sql += f" WHERE time >= '{start}'"
    elif end:
        sql += f" WHERE time <= '{end}'"
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# ---------- 自動更新 ----------
def auto_update_day(api_name, endpoint, date: datetime, fetch_api):
    """
    fetch_api(api_name, endpoint, date_str) -> DataFrame (需有 time 欄位)
    """
    date_str = date.strftime("%Y-%m-%d")
    if day_exists(api_name, date_str):
        print(f"📌 {date_str} 已存在，跳過")
        return

    df_new = fetch_api(api_name, endpoint, date.strftime("%Y%m%d"))
    if df_new.empty:
        print(f"⚠️ {date_str} 無資料")
        return

    save_to_db(df_new, api_name)
    print(f"✅ {date_str} 更新完成")

def auto_update_range(api_name, endpoint, start: datetime, end: datetime, fetch_api, sleep_sec=2):
    """批次更新區間資料"""
    cur = start
    while cur <= end:
        auto_update_day(api_name, endpoint, cur, fetch_api)
        cur += timedelta(days=1)
        time.sleep(sleep_sec)

# ---------- View 建立 ----------
def create_view(view_name: str, sql: str):
    """建立 SQLite View"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    conn.execute(f"CREATE VIEW {view_name} AS {sql}")
    conn.commit()
    conn.close()
    print(f"📌 已建立 View: {view_name}")