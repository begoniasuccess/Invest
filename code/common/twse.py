import sys, os
sys.path.append(os.path.dirname(__file__))

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from db import query_to_df, execute_sql, get_connection, query_single
import twse_api
import tools

# sys.path.append(os.path.dirname(__file__))
# sys.path.append(os.path.dirname(os.path.dirname(__file__))) 

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
        minDt = tools._roc_to_datetime(df_check["min(日期)"].iloc[0])
        maxDt = tools._roc_to_datetime(df_check["MAX(日期)"].iloc[0])
        print(minDt, maxDt)

    ### 1.資料完全落在庫的範圍，直接搜庫
    if (tools._is_fully_in_range(sDt, eDt, minDt, maxDt)):
        print("*** 直接從庫提取資料")
        sql = f"SELECT * FROM {table}"
        sql += f" WHERE 日期 between '{tools._to_roc_date(sDt)}' AND '{tools._to_roc_date(eDt)}'"
        df = query_to_df(sql)
        return df
    
    ### 2.資料完全落在庫的範圍之外，fetch API
    if (tools._is_no_overlap(sDt, eDt, minDt, maxDt)):
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
                    None
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

        sql = f"""
        INSERT OR REPLACE INTO {table}
        (證券代號, 證券名稱, 累計次數, 注意交易資訊, 日期, 收盤價, 本益比)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        execute_sql(sql, values)
        df = pd.DataFrame(raw_data.get("data", []), columns=raw_data.get("fields", []))
        return df
    
    ### 3.重疊的部分取庫，超出的部分用API，最後merge
    ## 找出重疊部分(From 庫)
    overlap_star, overlap_end = tools._overlap_period(sDt, eDt, minDt, maxDt)
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


# 日期,項目,買進,賣出,現金_券_償還,前日餘額,今日餘額
def get_margin_trading(sDt: datetime, eDt: datetime):
    if (sDt > eDt):
        return None

    if eDt > datetime.today():
        eDt = datetime.today()

    table = "twse_marginTrading_miMargn"
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
        minDt = datetime.strptime(df_check["min(日期)"].iloc[0], "%Y%m%d")
        maxDt = datetime.strptime(df_check["MAX(日期)"].iloc[0], "%Y%m%d")
        print(minDt, maxDt)

    ### 1.資料完全落在庫的範圍，直接搜庫
    if (tools._is_fully_in_range(sDt, eDt, minDt, maxDt)):
        print("*** 直接從庫提取資料")
        sql = f"SELECT * FROM {table}"
        sql += f" WHERE 日期 between '{sDt.strftime('%Y%m%d')}' AND '{eDt.strftime('%Y%m%d')}'"
        df = query_to_df(sql)
        return df
    
    ### 2.資料完全落在庫的範圍之外，fetch API
    if (tools._is_no_overlap(sDt, eDt, minDt, maxDt)):
        print("*** 從API提取資料")
        raw_data = twse_api.fetch_margin_trading_range(sDt, eDt)
        if raw_data is None:
            print("fetch_margin_trading 回傳 None")
            return None
        data = raw_data["data"]
        print(data)

        ### 存到db裡
        values = []
        for r in data:
            try:
                values.append((
                    r[0].strip(),
                    r[1].strip(),
                    int(r[2].replace(',', '')),
                    int(r[3].replace(',', '')),
                    int(r[4].replace(',', '')),
                    int(r[5].replace(',', '')),
                    int(r[6].replace(',', ''))
                ))
            except:
                continue

        sql = f"""
        INSERT OR REPLACE INTO {table}
        (日期, 項目, 買進, 賣出, 現金_券_償還, 前日餘額, 今日餘額)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        if not execute_sql(sql, values):
            print("存庫失敗！")
        df = pd.DataFrame(raw_data.get("data", []), columns=raw_data.get("fields", []))
        return df
    
    ### 3.重疊的部分取庫，超出的部分用API，最後merge
    ## 找出重疊部分(From 庫)
    overlap_star, overlap_end = tools._overlap_period(sDt, eDt, minDt, maxDt)
    if overlap_star is not None:
        df_exist = get_margin_trading(overlap_star, overlap_end)

    ## 找出需要獲取的部分
    if (sDt < minDt):
        df_new_left = get_margin_trading(sDt, minDt - relativedelta(days=1))
    if (maxDt < eDt):
        df_new_right = get_margin_trading(maxDt + relativedelta(days=1), eDt)
    
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
    year = 2024
    sDt = datetime(2025, 7, 15)
    eDt = datetime.today()
    testData = get_margin_trading(sDt, eDt)
    print(testData.head(2))
