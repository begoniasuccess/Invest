import sys, os
sys.path.append(os.path.dirname(__file__))
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import re
from common.constants import Panel
from common.constants import Iloc
import db

def nowTime():
    """取得當前時間 (yyyy/mm/dd hh:mm:ss)"""
    return datetime.now().strftime("%Y/%m/%d %H:%M:%S")

def ptMsg(msg, msg2=None):
    """打印時間與日誌 (yyyy/mm/dd hh:mm:ss)"""
    
    print(f"{nowTime()}：{msg}")
    if msg2 is not None:
        print(msg2)

def inTimeRange(targetDt: datetime, sDt: datetime , eDt: datetime) -> bool:
    return sDt <= targetDt <= eDt

def getSdtEdt(filePath: str) -> dict[str, datetime]:
    filename = Path(filePath).stem # 去除附檔名後的檔名

    # 找到兩組6位數字
    matches = re.findall(r'(\d{6})', filename)

    start_str = matches[0]
    end_str = matches[1]

    sDt = datetime.strptime(start_str + '01', "%Y%m%d") # Start Date
    eDt = datetime.strptime(end_str + '01', "%Y%m%d") # End Date

    eDt = eDt + pd.offsets.MonthEnd(0) # 時間推移到月底

    result = {
        "sDt": sDt,
        "eDt": eDt
    }
    return result

def getCloseDf(sYear: str , searchY: int) -> pd.DataFrame:
    base_dir = f'../data/analysis/summary/closePrice'
    sYear_int = int(sYear)
    
    # 產生三年的檔案清單
    years = [str(sYear_int + i) for i in range(searchY)]
    filenames = [f'closePrice_{y}.csv' for y in years]
    filepaths = [os.path.join(base_dir, fname) for fname in filenames]
    
    close_dfs = []
    for fp in filepaths:
        if os.path.exists(fp):
            try:
                df = pd.read_csv(fp, parse_dates=['date'], dtype={'stock_id': str})
                close_dfs.append(df)
                print(f"讀取檔案：{fp}，筆數：{len(df)}")
            except Exception as e:
                print(f"讀取檔案 {fp} 發生錯誤：{e}")
        else:
            print(f"檔案不存在：{fp}")

    # 合併或建立空 DataFrame
    if close_dfs:
        close_df = pd.concat(close_dfs, ignore_index=True)
    else:
        close_df = pd.DataFrame()

    print(f"closePrice：年份 {sYear} ~ {int(sYear) + searchY - 1} 合併資料筆數：{len(close_df)}")
    # print(close_df.head(3))
    ## for test
    close_df.to_csv(f"{base_dir}/closePrice_tmp.csv")
    return close_df
    
def delete_empty_csv_files(folder_path):
    deleted_files = []

    for filename in os.listdir(folder_path):
        if filename.lower().endswith('.csv'):
            filepath = os.path.join(folder_path, filename)
            try:
                with open(filepath, encoding="utf-8") as f:
                    lines = [line.strip() for line in f if line.strip()]
                    if not lines:
                        os.remove(filepath)
                        deleted_files.append(filename)
                        print(f"已刪除空檔案：{filename}")
            except Exception as e:
                print(f"讀取檔案時發生錯誤：{filename}，原因：{e}")

    print(f"\n總共刪除 {len(deleted_files)} 個空白CSV檔案。")
    return deleted_files
def is_really_empty_file(filepath):
    """強化版本：整份檔案去除空白、換行、BOM、制表符後，確認是否完全無內容"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
            cleaned_content = content.replace("\n", "").replace("\r", "").replace("\t", "").strip()
            return len(cleaned_content) == 0
    except Exception as e:
        print(f"檢查失敗：{filepath}，原因：{e}")
        return False

def delete_empty_csv_files_recursive(folder_path, size_threshold=2*1024):
    """檔案大小小且內容純空白，即刪除"""
    deleted_files = []
    checked_files = 0

    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            if filename.lower().endswith('.csv'):
                filepath = os.path.join(root, filename)
                checked_files += 1

                try:
                    # 檔案過小才進一步檢查內容
                    if os.path.getsize(filepath) <= size_threshold:
                        if is_really_empty_file(filepath):
                            os.remove(filepath)
                            deleted_files.append(filepath)
                            print(f"已刪除純空白檔案：{filepath}")
                except Exception as e:
                    print(f"處理失敗：{filepath}，原因：{e}")

                if checked_files % 100 == 0:
                    print(f"已檢查 {checked_files} 個檔案...")

    print(f"\n總共檢查 {checked_files} 個檔案，刪除 {len(deleted_files)} 個空白或純換行檔案。")
    return deleted_files

def getOutputCsvPath(target_folder, filePrefixIdx, csvName):        
    os.makedirs(target_folder, exist_ok=True) 
    outputPath = f'{target_folder}/{str(filePrefixIdx).zfill(2)}-{csvName}.csv'
    return outputPath

# 找出 觀察期-買入賣出日期 對應的資料列
def getOperiodDataRow(stock_id: str, closeDf: pd.DataFrame, baseDt: datetime, iloc: Iloc) -> pd.Series:
    dataRow = None
    candidates = closeDf[
        (closeDf['stock_id'] == stock_id)
        & (closeDf['date'].dt.year == baseDt.year)
        & (closeDf['date'].dt.month == baseDt.month)
    ]
    if not candidates.empty:
        dataRow = candidates.sort_values("date").iloc[iloc.value]
        
    if dataRow is None:
        return dataRow
    
    ### 確保 月初/月底 的資料要分別落在特定的日期內
    if (iloc == Iloc.Fst) and (dataRow["date"].day > 15):
        ptMsg(f'[{stock_id}]月初資料日期過大 => {dataRow["date"].strftime("%Y%m%d")}')
        return None
    
    if (iloc == Iloc.Last) and (dataRow["date"].day < 16):
        ptMsg(f'[{stock_id}]月底資料日期過小 => {dataRow["date"].strftime("%Y%m%d")}')
        return None
    
    return dataRow

# 找出 持有期-買入賣出日期 對應的資料列
def getHperiodDataRow(panelType: Panel, stock_id: str, closeDf: pd.DataFrame, baseDt: datetime, iloc: Iloc) -> pd.Series:
    ### Panel A
    if panelType == Panel.A:
        candidates = closeDf[
            (closeDf["stock_id"] == stock_id)
            & (closeDf["date_dt"].dt.year == baseDt.year)
            & (closeDf["date_dt"].dt.month == baseDt.month)
        ]
        if not candidates.empty:
            return candidates.sort_values("date_dt").iloc[iloc.value]

    ## Panel B
    if panelType == Panel.B:
        candidates = closeDf[
            (closeDf["stock_id"] == stock_id)
            & (closeDf["date_dt"] >= baseDt)
            & (closeDf["date_dt"] <= baseDt + timedelta(days=14))
        ]
        
        if not candidates.empty:
            return candidates.sort_values("date_dt").iloc[Iloc.Fst.value]
        
    return None

# 百分比排名 (0~100)
def scale_to_0_100(x):
    min_val = x.min()
    max_val = x.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series([None] * len(x), index=x.index)
    else:
        return (x - min_val) / (max_val - min_val) * 100
    
# 計算 RT_rank，注意：不先創欄位
def compute_rt_rank(group):
    mask = group["remark"] != "exclude"
    # 只針對非 exclude 算排名
    ranks = pd.Series(index=group.index, dtype="float")
    ranks.loc[mask] = group.loc[mask, "return"].rank(method="min", ascending=False)
    group["RT_rank"] = ranks
    return group

# 更新 remark: winner / loser
def mark_winner_loser(group):
    valid = group[group["remark"] != "exclude"]
    if valid.empty:
        return group

    n = len(valid)
    top_n = max(1, int(n * 0.1))
    bottom_n = max(1, int(n * 0.1))

    top_threshold = valid.nsmallest(top_n, "RT_rank")["RT_rank"].max()
    bottom_threshold = valid.nlargest(bottom_n, "RT_rank")["RT_rank"].min()

    # 只更新 valid 部分
    for idx in valid.index:
        rt_rank = group.loc[idx, "RT_rank"]
        if pd.isna(rt_rank):
            continue
        if rt_rank <= top_threshold:
            group.loc[idx, "remark"] = "winner"
        elif rt_rank >= bottom_threshold and rt_rank > top_threshold:
            group.loc[idx, "remark"] = "loser"

    return group

def parse_range_from_folder(folder_name):
    """ 解析資料夾名稱中的 yyyymm_yyyymm 為 datetime 區間 """
    match = re.match(r"(\d{6})_(\d{6})", folder_name)
    if not match:
        return None, None
    start_str, end_str = match.groups()
    start = datetime.strptime(start_str + '01', "%Y%m%d")
    # 將結束月份的最後一天作為結束日
    end = datetime.strptime(end_str + '01', "%Y%m%d")
    if end.month == 12:
        end = end.replace(month=1, year=end.year + 1)
    else:
        end = end.replace(month=end.month + 1)
    end = end.replace(day=1) - pd.Timedelta(days=1)
    return start, end

def findout_observerRTdata(output_path: str) -> bool:
    if os.path.exists(output_path):
        return True

    base_folder = os.path.dirname(output_path)
    base_folder = Path(base_folder)
    root_folder = base_folder.parent
    current_range_str = base_folder.name

    current_start, current_end = parse_range_from_folder(current_range_str)
    if current_start is None or current_end is None:
        print("⚠️ 資料夾名稱格式錯誤，應為 yyyymm_yyyymm")
        return False

    print(f"🔍 處理時間區間：{current_start.date()} ~ {current_end.date()}")

    combined_df = []

    for subfolder in root_folder.iterdir():
        if not subfolder.is_dir():
            continue
        sub_start, sub_end = parse_range_from_folder(subfolder.name)
        if sub_start is None or sub_end is None:
            continue

        # 檢查是否是涵蓋當前範圍的資料夾
        if sub_start <= current_start and sub_end >= current_end:
            csv_file = subfolder / "01-observerReturnList.csv"
            if csv_file.exists():
                print(f"✅ 找到符合範圍的檔案：{csv_file}")
                df = pd.read_csv(csv_file)

                # 過濾 start_date 與 end_date 在區間內的資料
                df['start_date'] = pd.to_datetime(df['start_date'])
                df['end_date'] = pd.to_datetime(df['end_date'])
                df = df[(df['start_date'] >= current_start) & (df['end_date'] <= current_end)]

                combined_df.append(df)
            else:
                print(f"❌ 找不到 01-observerReturnList.csv：{subfolder}")
    
    # 合併資料並寫出
    if combined_df:
        result_df = pd.concat(combined_df, ignore_index=True)
        result_df.to_csv(output_path, index=False)
        print(f"📄 寫入檔案：{output_path}")
        return os.path.exists(output_path)
    
    print("⚠️ 沒有找到任何符合條件的資料")
    return False
    
def roc_to_unix(roc_date: str) -> int:
    year = None 
    month = None 
    day = None    
    seperators = ["/", ".", "-"]
    for seperator in seperators:
        if seperator in roc_date:  
            year, month, day = map(int, roc_date.split(seperator))
    if year is None:
        return None
        
    gregorian_year = year + 1911 # 民國 → 西元（加 1911 年）
    dt = datetime(gregorian_year, month, day)
    return int(dt.timestamp())

def get_api_info(apiName: str) -> pd.DataFrame:
    sql = f"SELECT *, src_link || api_path AS url FROM data_source"
    sql += f" WHERE name = '{apiName}'"
    target = db.query_to_df(sql)
    return target

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

def chinese_to_int(s) -> int:
    # 如果是 int，直接返回
    if isinstance(s, int):
        return s

    # 如果是字串，先轉半形
    if isinstance(s, str):
        # 全形轉半形
        s = s.translate(str.maketrans(
            "０１２３４５６７８９",
            "0123456789"
        ))
        # 半形阿拉伯數字，直接轉 int
        if s.isdigit():
            return int(s)

    # 原本的中文數字處理
    num_map = {"零":0, "一":1,"二":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9}
    unit_map = {"十":10, "百":100, "千":1000}
    big_unit_map = {"萬":10000, "億":100000000}

    def section_to_number(section: str) -> int:
        total, num = 0, 0
        for ch in section:
            if ch in num_map:
                num = num_map[ch]
            elif ch in unit_map:
                total += (num or 1) * unit_map[ch]
                num = 0
            elif ch == "零":
                continue
        return total + num

    total, section = 0, ""
    for ch in reversed(s):
        if ch in big_unit_map:
            total += section_to_number(section) * big_unit_map[ch]
            section = ""
        else:
            section = ch + section
    total += section_to_number(section)
    return total
