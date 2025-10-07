import sys, os
sys.path.append(os.path.dirname(__file__))

import sqlite3
import pandas as pd
from contextlib import contextmanager
from pathlib import Path

# 資料庫路徑
DB_PATH = Path("../data_center/data_center.db")

@contextmanager
def get_connection():
    """建立並自動關閉 SQLite 連線，增加 timeout 避免 locked"""
    conn = sqlite3.connect(DB_PATH, timeout=10)  # 最多等 10 秒
    try:
        yield conn
    finally:
        conn.close()

def query_to_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    """執行查詢並回傳 DataFrame"""
    with get_connection() as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    return df

def query_single(sql: str, params: tuple = ()):
    """
    執行查詢並回傳單一值。
    例如 SELECT COUNT(*) FROM table
    如果沒有結果，回傳 None
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None

def execute_sql(sql: str, params: tuple | list[tuple] = ()) -> bool:
    """
    執行 INSERT/UPDATE/DELETE
    支援單筆 tuple 或多筆 list[tuple]
    成功回傳 True，失敗回傳 False
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            if isinstance(params, list):
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
            conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"[SQLite Error] {e}")
        return False