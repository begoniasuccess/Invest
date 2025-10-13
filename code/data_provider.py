import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from common import db
from common import tools
import api_getter

# ======== 注意股公告 ========
# 證券代號,證券名稱,累計,注意交易資訊,公告日期,收盤價,本益比,link
def get_notice(type: str, sDt: datetime, eDt: datetime):
    apiName = ""
    match type.lower():
        case "twse":
            apiName = "上市公布注意有價證券資訊"
            
        case "tpex":            
            apiName = "上櫃公布注意有價證券資訊"
        case _:
            return None
        
    return get_time_range_data(apiName, sDt, eDt)

# ======== 處置股公告 ========
# 證券代號,證券名稱,累計,注意交易資訊,公告日期,收盤價,本益比,link
def get_punish(type: str, sDt: datetime, eDt: datetime):
    match type.lower():
        case "twse":
            apiName = "上市公布處置有價證券"
            
        case "tpex":
            apiName = "上櫃處置有價證券資訊"
            
    return get_time_range_data(apiName, sDt, eDt)

### 以下為共用function
def get_time_range_data(apiName: str, sDt: datetime, eDt: datetime):
    if (sDt > eDt):
        return None

    if eDt > datetime.today():
        eDt = datetime.today()
    print("[sDt]=" , sDt, "[eDt]=", eDt)

    apiInfo = tools.get_api_info(apiName)
    table = apiInfo["storage_table"].iloc[0]    
    type = apiInfo["type"].iloc[0].lower()
    time_col = apiInfo["time_col"].iloc[0]

    ### 先確認庫資料的上下界
    # 先確認庫有資料
    sql = f"SELECT count(*) FROM {table}"
    dataCnt = int(db.query_single_value(sql))   
    # print([dataCnt])
    
    if (dataCnt < 1):
        maxDt = sDt - relativedelta(days=1)
        minDt = maxDt - relativedelta(days=1)        
    else:        
        sql = f"SELECT min({time_col}), MAX({time_col}) FROM {table}"
        df_check = db.query_to_df(sql)
        minDt = datetime.fromtimestamp(tools.roc_to_unix(df_check[f"min({time_col})"].iloc[0]))
        maxDt = datetime.fromtimestamp(tools.roc_to_unix(df_check[f"MAX({time_col})"].iloc[0]))
    print([minDt, maxDt])
    # sys.exit()

    ### 1.資料完全落在庫的範圍，直接搜庫
    if (tools._is_fully_in_range(sDt, eDt, minDt, maxDt)):
        print("*** 直接從庫提取資料")
        sql = f"SELECT * FROM {table}"
        sql += f" WHERE {apiInfo["time_col"].iloc[0]}_ts between {sDt.timestamp()} AND {eDt.timestamp()}"
        df = db.query_to_df(sql)
        return df
    
    ### 2.資料完全落在庫的範圍之外，fetch API
    if (tools._is_no_overlap(sDt, eDt, minDt, maxDt)):
        print("*** 從API提取資料")
        if "注意" in apiName:
            raw_data = api_getter.fetch_notice(type, sDt, eDt)
        elif "處置" in apiName:
            raw_data = api_getter.fetch_punish(type, sDt, eDt)
        else:
            return None    
        
        data = None
        try:
            # 確認 raw_data 是 dict
            if not isinstance(raw_data, dict):
                raise ValueError("API 回傳格式不是 dict")

            match type:
                case "twse":
                    data = raw_data.get("data")
                    if not data or not isinstance(data, list):
                        raise ValueError("API 回傳中沒有 data 或格式錯誤")
                    df = pd.DataFrame(data, columns=raw_data.get("fields"))
                    
                case "tpex":
                    tables = raw_data.get("tables")
                    if not tables or not isinstance(tables, list):
                        raise ValueError("API 回傳中沒有 tables 或格式錯誤")
                    fst_table = tables[0]
                    if not isinstance(fst_table, dict) or "data" not in fst_table:
                        raise ValueError("tables[0] 缺少 data 欄位")
                    data = fst_table["data"]
                    df = pd.DataFrame(raw_data["tables"][0].get("data", []), columns=raw_data["tables"][0].get("fields", []))
                    
                case _:
                    return None
        except Exception as e:
            print("⚠️ API 回傳格式異常:", e)
            print("原始回傳內容:", raw_data)
            return None

        ### 存到db裡
        writeResult = writein_db(apiName, data)
        if not writeResult:
            print("***資料庫寫入失敗！")       
        
        return df
    
    ### 3.重疊的部分取庫，超出的部分用API，最後merge
    ## 找出重疊部分(From 庫)
    overlap_star, overlap_end = tools._overlap_period(sDt, eDt, minDt, maxDt)
    if overlap_star is not None:
        df_exist = get_time_range_data(apiName, overlap_star, overlap_end)

    ## 找出需要獲取的部分
    if (sDt < minDt):
        df_new_left = get_time_range_data(apiName, sDt, minDt - relativedelta(days=1))
    if (maxDt < eDt):
        df_new_right = get_time_range_data(apiName, maxDt + relativedelta(days=1), eDt)
    
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
    
def writein_db(apiName, data) -> bool:
    apiInfo = tools.get_api_info(apiName)
    try:
        table = apiInfo["storage_table"].iloc[0]    
    except:
        return False
    
    values = []
    insert_cols = []
    match apiName:
        case "上市公布注意有價證券資訊":
            insert_cols = [
                "證券代號", 
                "證券名稱",
                "累計次數",
                "注意交易資訊", 
                "日期",
                "日期_ts",
                "收盤價",
                "本益比"
            ]
            for r in data:
                try:
                    # 嘗試轉換本益比
                    try:
                        pe_value = float(r[7])
                    except (ValueError, TypeError):
                        pe_value = None
                        
                    values.append((
                        r[1].strip(),
                        r[2].strip(),
                        int(r[3]),
                        r[4].strip(),
                        r[5].strip(),
                        tools.roc_to_unix(r[5].strip()),
                        float(r[6]),
                        pe_value
                    ))
                except:
                    print("Data write in error：", r)
                    continue

        case "上市公布處置有價證券":
            insert_cols = [
                "公布日期",
                "公布日期_ts",
                "證券代號",
                "證券名稱",
                "累計",
                "處置條件",
                "處置起迄時間",
                "處置措施",
                "處置內容",
                "備註"
            ]
            for r in data:
                try:
                    # 跳過當日未處置的資料
                    notice_content = r[7].strip()
                    if notice_content == "":
                        continue
                        
                    # 準備所有資料
                    values.append((
                        r[1].strip(), # 公布日期
                        tools.roc_to_unix(r[1].strip()), # 公布日期_ts
                        r[2].strip(), # 證券代號
                        r[3].strip(), # 證券名稱
                        int(r[4]), # 累計"
                        r[5].strip(), # 處置條件
                        r[6].strip(), # 處置起迄時間
                        r[7].strip(), # 處置措施
                        r[8].strip(), # 處置內容
                        r[9].strip(), # 備註
                    ))
                except:
                    print("Data write in error：", r)
                    continue
            
        case "上櫃公布注意有價證券資訊":
            insert_cols = [
                "證券代號",
                "證券名稱",
                "累計",
                "注意交易資訊",
                "公告日期",
                "公告日期_ts",
                "收盤價",
                "本益比",
                "link"
            ]
            for r in data:
                try:
                    # 跳過當日未處置的資料
                    notice_content = r[7].strip()
                    if notice_content == "":
                        continue
                    
                    # 嘗試轉換本益比
                    try:
                        pe_value = float(r[8])
                    except (ValueError, TypeError):
                        pe_value = None
                    
                    # 準備所有資料
                    values.append((
                        r[1].strip(), # 證券代號
                        r[2].strip(), # 證券名稱
                        int(r[3]), # 累計
                        r[4].strip(), # 注意交易資訊"
                        r[5].strip(), # 公告日期
                        tools.roc_to_unix(r[5].strip()), # 公告日期_ts
                        float(r[6]), # 收盤價
                        pe_value, # 本益比
                        r[8].strip(), # link
                    ))
                except:
                    print("Data write in error：", r)
                    continue
            
        case "上櫃處置有價證券資訊":
            insert_cols = [
                "公布日期", 
                "公布日期_ts",
                "證券代號", 
                "證券名稱", 
                "累計", 
                "處置起訖時間", 
                "處置原因", 
                "處置內容", 
                "收盤價", 
                "本益比", 
                "memo"
            ]
            for r in data:
                try:
                    # 跳過當日未處置的資料
                    punish_content = r[7].strip()
                    if punish_content == "" or punish_content == "本日無處置資料":
                        continue
                    
                    # 嘗試轉換本益比
                    try:
                        pe_value = float(r[9])
                    except (ValueError, TypeError):
                        pe_value = None
                                                
                    # 準備所有資料
                    values.append((
                        r[1].strip(), # 公布日期
                        tools.roc_to_unix(r[1].strip()), # 公布日期_ts
                        r[2].strip(), # 證券代號
                        r[3].strip(), # 證券名稱
                        int(r[4]), # 累計
                        r[5].strip(), # 處置起訖時間
                        r[6].strip(), # 處置原因
                        r[7].strip(), # 處置內容
                        float(r[8]), # 收盤價
                        pe_value, # 本益比
                        r[10].strip() # memo
                    ))
                except:
                    print("Data write in error：", r)
                    continue
        
        case _:
            return False
    
    if len(insert_cols) == 0:
        return False
    
    if len(values) == 0:
        return True
    
    sql = f"""
    INSERT OR REPLACE INTO {table} 
        ({",".join(insert_cols)})
    VALUES 
        ({", ".join(["?"] * len(insert_cols))})
    """
    return db.execute_sql(sql, values)


# ======== 範例測試 ========
if __name__ == "__main__":
    sDt = datetime(2020, 1, 1)
    eDt = datetime(2025, 10, 13)
    testData = get_punish("twse", sDt, eDt)
    print(testData.head())