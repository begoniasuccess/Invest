import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from common import db

# =========================================================
# 0) 小工具：判斷缺值、Series 取欄位、防呆加總
# =========================================================
def _is_missing(v):
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False

def _scol(df_, col, default=np.nan):
    if col in df_.columns:
        s = df_[col]
        if isinstance(s, pd.Series):
            return s
        return pd.Series([s] * len(df_), index=df_.index)
    return pd.Series([default] * len(df_), index=df_.index)

def _sum_cols(df_, cols, default=0.0):
    out = pd.Series([0.0] * len(df_), index=df_.index, dtype="float64")
    hit = False
    for c in cols:
        if c in df_.columns:
            out = out + pd.to_numeric(_scol(df_, c, default=0.0), errors="coerce").fillna(0.0)
            hit = True
    if not hit:
        return pd.Series([default] * len(df_), index=df_.index, dtype="float64")
    return out

def _sum_cols_like(df_, keywords, default=0.0):
    cols = []
    for c in df_.columns:
        cs = str(c)
        if any(k in cs for k in keywords):
            cols.append(c)
    if not cols:
        return pd.Series([default] * len(df_), index=df_.index, dtype="float64")
    return _sum_cols(df_, cols, default=default)


# =========================================================
# 1) 重新抓「可重建報告所需」的原始資料（價格/法人/融資）
#    - 這樣你就算 DB 某些原始欄也缺，仍可重建衍生欄
# =========================================================
def _build_base_df(finMind, stock_id, sDt, eDt):
    """
    回傳以「date(datetime)」為主鍵、包含價格 + 法人(淨額) + 融資(今日餘額) 的 base df
    """
    df = finMind.get_tw_stock_daily_price(stock_id=stock_id, start_date=sDt, end_date=eDt)
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] <= pd.to_datetime(eDt.date())].reset_index(drop=True)

    # 法人
    df3 = finMind.get_tw_institutional_total(start_date=sDt, end_date=eDt)
    if df3 is None or df3.empty:
        df3 = pd.DataFrame(columns=["buy", "sell", "date", "name"])
    else:
        df3 = df3.copy()
        df3["net"] = pd.to_numeric(df3.get("buy"), errors="coerce") - pd.to_numeric(df3.get("sell"), errors="coerce")
        df3["date"] = pd.to_datetime(df3["date"])
        df3 = df3.pivot(index="date", columns="name", values="net").reset_index()

    df = df.merge(df3, on="date", how="left")

    # 融資：今日餘額（仟元）
    df["今日餘額"] = np.nan
    df_m = finMind.get_tw_margin_total(start_date=sDt, end_date=eDt)

    if df_m is not None and not df_m.empty:
        df_m = df_m.copy()
        col_date = "date" if "date" in df_m.columns else ("日期" if "日期" in df_m.columns else None)
        col_name = "name" if "name" in df_m.columns else ("項目" if "項目" in df_m.columns else None)
        col_today = "TodayBalance" if "TodayBalance" in df_m.columns else ("今日餘額" if "今日餘額" in df_m.columns else None)

        if col_date and col_name and col_today:
            df_m[col_date] = pd.to_datetime(df_m[col_date])
            df_m[col_name] = df_m[col_name].astype(str)

            cand = df_m[df_m[col_name] == "MarginPurchaseMoney"].copy()
            if not cand.empty:
                cand[col_today] = pd.to_numeric(cand[col_today], errors="coerce")
                med = cand[col_today].median(skipna=True)

                # heuristic：太大視為元 -> /1000 轉仟元
                if pd.notna(med) and med > 1e9:
                    cand["今日餘額"] = cand[col_today] / 1000.0
                else:
                    cand["今日餘額"] = cand[col_today]

                cand = cand[[col_date, "今日餘額"]].rename(columns={col_date: "date"})
                df = df.merge(cand, on="date", how="left", suffixes=("", "_m"))
                if "今日餘額_m" in df.columns:
                    df["今日餘額"] = df["今日餘額"].combine_first(df["今日餘額_m"])
                    df.drop(columns=["今日餘額_m"], inplace=True, errors="ignore")

    return df


# =========================================================
# 2) 計算器（registry）：每個欄位一個 function（回寫到 df）
#    - 你可以持續擴充：新增 key + 對應計算函式即可
# =========================================================
def _classify_k_type_row(r):
    body = r.get("實體_pct", np.nan)
    upper = r.get("上影_pct", np.nan)
    lower = r.get("下影_pct", np.nan)
    open_p = r.get("開盤價", np.nan)
    close_p = r.get("收盤價", np.nan)

    if pd.isna(body) or pd.isna(upper) or pd.isna(lower):
        return None
    if pd.isna(open_p) or pd.isna(close_p):
        return None

    if abs(close_p - open_p) < 1e-6 or body < 0.05:
        return "⬜ 十字線"

    is_red = close_p > open_p
    color = "🟥" if is_red else "🟩"

    if lower > 0.5 and body < 0.3:
        return f"{color} 錘子線"
    if upper > 0.5 and body < 0.3:
        return f"{color} 流星線"
    if body > 0.6:
        return f"{color} 長紅K" if is_red else f"{color} 長黑K"
    return f"{color} 中實體K"


def _compute_derived(df):
    """
    只要 df 內有基本價量欄，就能生出大部分衍生欄
    """
    # rename 成你報告欄名
    df = df.copy()
    df.rename(
        columns={
            "date": "日期",
            "open": "開盤價",
            "close": "收盤價",
            "max": "最高價",
            "min": "最低價",
            "Trading_Volume": "成交量",
            "Trading_money": "總成交金額_億",
        },
        inplace=True,
    )

    # 價量衍生
    df["收盤_開盤"] = df["收盤價"] - df["開盤價"]
    df["日振幅"] = df["最高價"] - df["最低價"]
    df["漲跌幅_pct"] = (df["收盤價"] - df["收盤價"].shift(1)) / df["收盤價"].shift(1)
    df["量增率_pct"] = (df["成交量"] - df["成交量"].shift(1)) / df["成交量"].shift(1)

    df["昨收_tmp"] = df["收盤價"].shift(1)
    base_range = df["日振幅"] / df["昨收_tmp"]
    sign = np.sign(df["收盤價"] - df["昨收_tmp"])
    df["日振幅_昨收_pct"] = base_range * sign
    df.drop(columns=["昨收_tmp"], inplace=True)

    # 均量 / 均價 / 扣抵 / 乖離
    df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce")
    for n in [5, 10, 20, 60]:
        df[f"{n}日均量"] = df["成交量"].rolling(n).mean()
        df[f"{n}日平均"] = df["收盤價"].rolling(n).mean()
        df[f"{n}日上升幅度"] = df[f"{n}日平均"] - df[f"{n}日平均"].shift(1)
        df[f"{n}日扣抵值"] = df["收盤價"].shift(n - 1)
        df[f"{n}日扣抵影響_pct"] = (df["收盤價"] - df[f"{n}日扣抵值"]) / df["收盤價"]
        df[f"{n}日乖離"] = (df["收盤價"] - df[f"{n}日平均"]) / df[f"{n}日平均"]

    # 金額換算
    df["總成交金額_億"] = pd.to_numeric(df["總成交金額_億"], errors="coerce") / 1e8

    # 法人（防呆）
    foreign = pd.to_numeric(_scol(df, "Foreign_Investor", default=np.nan), errors="coerce")
    itrust  = pd.to_numeric(_scol(df, "Investment_Trust", default=np.nan), errors="coerce")
    dealer  = _sum_cols_like(df, keywords=["Dealer"], default=0.0)

    total_net = pd.to_numeric(_scol(df, "total", default=np.nan), errors="coerce")
    if total_net.isna().all():
        total_net = foreign.fillna(0.0) + itrust.fillna(0.0) + pd.to_numeric(dealer, errors="coerce").fillna(0.0)

    df["法人總買超_億"] = total_net / 1e8
    df["買超_外資_億"] = foreign / 1e8
    df["買超_投信_億"] = itrust / 1e8
    df["買超_自營商_億"] = pd.to_numeric(dealer, errors="coerce").fillna(0.0) / 1e8

    # 融資（今日餘額：仟元）
    df["融資餘額_億"] = pd.to_numeric(_scol(df, "今日餘額", default=np.nan), errors="coerce") * 1000 / 1e8
    df["買超_融資_億"] = df["融資餘額_億"] - df["融資餘額_億"].shift(1)

    # 資金走向
    df["資金走向"] = df["收盤_開盤"] - (df["法人總買超_億"] + df["買超_融資_億"])
    df["資金走向判讀"] = df["資金走向"].apply(lambda x: None if pd.isna(x) else ("偏重大型股" if x > 0 else ("偏重小型股" if x < 0 else None)))

    # 實體 / 上影 / 下影
    rng = (df["最高價"] - df["最低價"]).replace(0, np.nan)
    df["實體_pct"] = (df["收盤價"] - df["開盤價"]).abs() / rng
    df["上影_pct"] = (df["最高價"] - np.maximum(df["開盤價"], df["收盤價"])) / rng
    df["下影_pct"] = (np.minimum(df["開盤價"], df["收盤價"]) - df["最低價"]) / rng

    # K 線型態
    df["K線型態"] = df.apply(_classify_k_type_row, axis=1)

    # 跳空缺口
    df["昨高"] = df["最高價"].shift(1)
    df["昨低"] = df["最低價"].shift(1)
    df["跳空狀態"] = np.select(
        [df["最低價"] > df["昨高"], df["最高價"] < df["昨低"]],
        ["上跳空", "下跳空"],
        default="無跳空",
    )
    is_red = df["收盤價"] > df["開盤價"]
    df["今上緣"] = np.where(is_red, df["收盤價"], df["開盤價"])
    df["今下緣"] = np.where(is_red, df["開盤價"], df["收盤價"])
    df["昨上緣"] = df["今上緣"].shift(1)
    df["昨下緣"] = df["今下緣"].shift(1)

    df["跳空缺口"] = np.select(
        [df["跳空狀態"] == "上跳空", df["跳空狀態"] == "下跳空"],
        [df["今下緣"] - df["昨上緣"], df["今上緣"] - df["昨下緣"]],
        default=None,
    )

    # 量能最大量（5/10/20/60）
    vols = df["成交量"].to_numpy()
    dates = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d").to_numpy()
    for n in [5, 10, 20, 60]:
        vmax_list, vdate_list = [], []
        for i in range(len(df)):
            if i + 1 < n:
                vmax_list.append(np.nan)
                vdate_list.append(np.nan)
                continue
            window_vol = vols[i + 1 - n : i + 1]
            window_date = dates[i + 1 - n : i + 1]
            idx = int(np.argmax(window_vol))
            vmax_list.append(window_vol[idx])
            vdate_list.append(window_date[idx])
        df[f"{n}日最大量"] = vmax_list
        df[f"{n}日最大量_日期"] = vdate_list

    return df


# 這裡是「欄位 -> 如何從已算好的 df 取值」的 registry
# 你之後要補新欄位，只要加一個 key 即可
FIELD_REGISTRY = {
    # 價量
    "收盤_開盤": lambda d: d["收盤_開盤"],
    "日振幅": lambda d: d["日振幅"],
    "漲跌幅_pct": lambda d: d["漲跌幅_pct"],
    "量增率_pct": lambda d: d["量增率_pct"],
    "日振幅_昨收_pct": lambda d: d["日振幅_昨收_pct"],

    # 均量/均價/扣抵/乖離/上升幅度
    "5日均量": lambda d: d["5日均量"],
    "10日均量": lambda d: d["10日均量"],
    "20日均量": lambda d: d["20日均量"],
    "60日均量": lambda d: d["60日均量"],

    "5日平均": lambda d: d["5日平均"],
    "10日平均": lambda d: d["10日平均"],
    "20日平均": lambda d: d["20日平均"],
    "60日平均": lambda d: d["60日平均"],

    "5日上升幅度": lambda d: d["5日上升幅度"],
    "10日上升幅度": lambda d: d["10日上升幅度"],
    "20日上升幅度": lambda d: d["20日上升幅度"],
    "60日上升幅度": lambda d: d["60日上升幅度"],

    "5日扣抵值": lambda d: d["5日扣抵值"],
    "10日扣抵值": lambda d: d["10日扣抵值"],
    "20日扣抵值": lambda d: d["20日扣抵值"],
    "60日扣抵值": lambda d: d["60日扣抵值"],

    "5日扣抵影響_pct": lambda d: d["5日扣抵影響_pct"],
    "10日扣抵影響_pct": lambda d: d["10日扣抵影響_pct"],
    "20日扣抵影響_pct": lambda d: d["20日扣抵影響_pct"],
    "60日扣抵影響_pct": lambda d: d["60日扣抵影響_pct"],

    "5日乖離": lambda d: d["5日乖離"],
    "10日乖離": lambda d: d["10日乖離"],
    "20日乖離": lambda d: d["20日乖離"],
    "60日乖離": lambda d: d["60日乖離"],

    # 金額/法人/融資/資金走向
    "總成交金額_億": lambda d: d["總成交金額_億"],
    "法人總買超_億": lambda d: d["法人總買超_億"],
    "買超_外資_億": lambda d: d["買超_外資_億"],
    "買超_投信_億": lambda d: d["買超_投信_億"],
    "買超_自營商_億": lambda d: d["買超_自營商_億"],
    "買超_融資_億": lambda d: d["買超_融資_億"],
    "資金走向": lambda d: d["資金走向"],
    "資金走向判讀": lambda d: d["資金走向判讀"],

    # K 線/跳空/影線
    "實體_pct": lambda d: d["實體_pct"],
    "上影_pct": lambda d: d["上影_pct"],
    "下影_pct": lambda d: d["下影_pct"],
    "K線型態": lambda d: d["K線型態"],
    "跳空缺口": lambda d: d["跳空缺口"],

    # 量能最大
    "5日最大量": lambda d: d["5日最大量"],
    "5日最大量_日期": lambda d: d["5日最大量_日期"],
    "10日最大量": lambda d: d["10日最大量"],
    "10日最大量_日期": lambda d: d["10日最大量_日期"],
    "20日最大量": lambda d: d["20日最大量"],
    "20日最大量_日期": lambda d: d["20日最大量_日期"],
    "60日最大量": lambda d: d["60日最大量"],
    "60日最大量_日期": lambda d: d["60日最大量_日期"],
}


# =========================================================
# 3) DB 回寫：只更新指定欄位（不動其他欄）
#    - 你只要把 db.execute_sql / db.executemany 對上你的實作
# =========================================================
def _update_fields_to_db(db, table, key_cols, df_patch, fields):
    """
    df_patch: 至少包含 key_cols + fields
    key_cols 預設用 ["股票代號","日期"]（日期建議用 YYYY-MM-DD）
    """
    if df_patch.empty:
        return 0

    # 統一日期格式
    if "日期" in df_patch.columns:
        df_patch = df_patch.copy()
        df_patch["日期"] = pd.to_datetime(df_patch["日期"]).dt.strftime("%Y-%m-%d")

    set_clause = ", ".join([f'"{c}" = ?' for c in fields])
    where_clause = " AND ".join([f'"{k}" = ?' for k in key_cols])

    sql = f'UPDATE "{table}" SET {set_clause} WHERE {where_clause}'

    params = []
    for _, r in df_patch.iterrows():
        row_vals = [r.get(c, None) for c in fields] + [r.get(k, None) for k in key_cols]
        params.append(row_vals)

    # 你如果沒有 executemany，就 loop execute_sql 也行（慢一點）
    if hasattr(db, "executemany"):
        db.executemany(sql, params)
    else:
        for p in params:
            db.execute_sql(sql, p)

    return len(params)


# === helper: 取欄位成「整欄 Series」，不存在就回 0（或 NaN） ===
def _scol(df_, col, default=0.0):
    if col in df_.columns:
        s = df_[col]
        # 保險：有人把同名欄位 merge 壞掉變 scalar
        if isinstance(s, pd.Series):
            return s
        return pd.Series([s] * len(df_), index=df_.index)
    return pd.Series([default] * len(df_), index=df_.index)

# === helper: 多個候選欄位加總（存在才算） ===
def _sum_cols(df_, cols, default=0.0):
    out = pd.Series([0.0] * len(df_), index=df_.index, dtype="float64")
    hit = False
    for c in cols:
        if c in df_.columns:
            out = out + pd.to_numeric(_scol(df_, c, default=0.0), errors="coerce").fillna(0.0)
            hit = True
    if not hit:
        return pd.Series([default] * len(df_), index=df_.index, dtype="float64")
    return out

# === helper: 找出 df 內所有包含某些關鍵字的欄位並加總（for Dealer* 這種不穩定欄名） ===
def _sum_cols_like(df_, keywords, default=0.0):
    cols = []
    for c in df_.columns:
        cs = str(c)
        if any(k in cs for k in keywords):
            cols.append(c)
    if not cols:
        return pd.Series([default] * len(df_), index=df_.index, dtype="float64")
    return _sum_cols(df_, cols, default=default)

# === 9) K 線型態 ===
def classify_k_type(r):
    body = r["實體_pct"]
    upper = r["上影_pct"]
    lower = r["下影_pct"]
    open_p = r["開盤價"]
    close_p = r["收盤價"]

    if pd.isna(body) or pd.isna(upper) or pd.isna(lower):
        return None

    if abs(close_p - open_p) < 1e-6 or body < 0.05:
        return "⬜ 十字線"

    is_red = close_p > open_p
    color = "🟥" if is_red else "🟩"

    if lower > 0.5 and body < 0.3:
        return f"{color} 錘子線"
    if upper > 0.5 and body < 0.3:
        return f"{color} 流星線"
    if body > 0.6:
        return f"{color} 長紅K" if is_red else f"{color} 長黑K"
    return f"{color} 中實體K"

def update_is_complete():
    table = "stock_report_daily"

    # 預設不檢查這些欄位
    exclude_cols = {
        "is_complete",
        "id",
        "跳空缺口",
        "created_at",
        "updated_at"
    }

    # 取得所有欄位
    cols_df = db.query_to_df(f"PRAGMA table_info('{table}');")
    all_cols = cols_df["name"].tolist()

    # 過濾不檢查的欄位
    check_cols = [c for c in all_cols if c not in exclude_cols]

    if not check_cols:
        print("⚠ 沒有可檢查欄位，跳過更新。")
        return

    # 安全包裝欄位名成 "欄位"
    def q(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    # 建立 NULL 判斷條件
    null_conditions = " OR ".join([f"{q(c)} IS NULL" for c in check_cols])

    # 最終 SQL
    sql = f"""
    UPDATE {q(table)}
    SET {q("is_complete")} = CASE
        WHEN {null_conditions} THEN 0
        ELSE 1
    END;
    """

    print("執行 SQL 中 ...")
    ok = db.execute_sql(sql)
    print("更新完成 ✔" if ok else "更新失敗 ❌")
    

TABLE = "stock_report_daily"
COLUMNS = [
    "日期","股票代號",
    "開盤價","收盤價","收盤_開盤","最高價","最低價","日振幅","漲跌幅_pct","日振幅_昨收_pct",
    "成交量","量增率_pct",
    "5日均量","5日最大量_日期","5日最大量",
    "10日均量","10日最大量_日期","10日最大量",
    "20日均量","20日最大量_日期","20日最大量",
    "60日均量","60日最大量_日期","60日最大量",
    "實體_pct","上影_pct","下影_pct","K線型態","跳空缺口",
    "5日平均","10日平均","20日平均","60日平均",
    "5日上升幅度","10日上升幅度","20日上升幅度","60日上升幅度",
    "5日扣抵值","10日扣抵值","20日扣抵值","60日扣抵值",
    "5日扣抵影響_pct","10日扣抵影響_pct","20日扣抵影響_pct","60日扣抵影響_pct",
    "5日乖離","10日乖離","20日乖離","60日乖離",
    "總成交金額_億","法人總買超_億","買超_外資_億","買超_投信_億","買超_自營商_億","買超_融資_億",
    "資金走向","資金走向判讀",
    "is_complete",
]

# ======================================
# UPSERT
# ======================================
def upsert(df: pd.DataFrame, stock_id: str):
    if df.empty:
        print("⚠ df empty, skip")
        return

    df = df.copy()
    df["股票代號"] = stock_id
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")

    # --- is_complete：先預設 0, 後面覆寫 ---
    df["is_complete"] = 0

    # 欄位對齊 SQLite 標準欄位
    df = df[[c for c in df.columns if c in COLUMNS]]
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[COLUMNS]

    # 根據「所有數值欄位」是否都有值來決定 is_complete
    text_cols = [
        "日期","股票代號","K線型態","均線方向","均線排列",
        "均線狀態","趨勢強度說明","資金走向判讀"
    ]
    numeric_cols = [c for c in COLUMNS if c not in text_cols + ["is_complete"]]
    # 如果有任何數值欄位是 NaN，就視為未完成
    df["is_complete"] = (~df[numeric_cols].isna().any(axis=1)).astype(int)

    # SQL 組起來
    col_sql = ",".join([f'"{c}"' for c in COLUMNS])
    ph = ",".join(["?"] * len(COLUMNS))
    update_sql = ",".join([f'"{c}" = excluded."{c}"' for c in COLUMNS if c not in ("日期", "股票代號")])

    sql = f"""
    INSERT INTO "{TABLE}" ({col_sql})
    VALUES ({ph})
    ON CONFLICT("股票代號","日期") DO UPDATE SET
        {update_sql}
    WHERE "{TABLE}".is_complete = 0
       OR excluded.is_complete = 1;
    """

    rows = list(df.itertuples(index=False, name=None))

    ok = db.execute_sql(sql, rows)
    if ok:
        print(f"✔ DB 寫入成功: {len(df)} rows, stock={stock_id}")
    else:
        print("❌ DB 寫入失敗（請看上方 SQLite Error）")