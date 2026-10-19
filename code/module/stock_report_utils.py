# =========================
# module/stock_report_utils.py
# =========================
import numpy as np
import pandas as pd
from common import db

TABLE_SCHEMA = "core"
TABLE = "stock_report_daily"

def get_db_columns():
    cols_df = db.query_to_df(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        (TABLE_SCHEMA, TABLE),
    )

    if cols_df is None or cols_df.empty:
        raise RuntimeError(f"找不到資料表欄位：{TABLE_SCHEMA}.{TABLE}")

    return cols_df["column_name"].tolist()

def get_table_columns():
    """
    以 DB 真實欄位為準，不再手寫 COLUMNS。
    排除不應由 upsert 寫入的系統欄位。
    """
    skip_cols = {
        "id",
        "created_at",
        "updated_at",
    }

    return [
        c for c in get_db_columns()
        if c not in skip_cols
    ]

_FIELD_REGISTRY = None

def get_field_registry():
    global _FIELD_REGISTRY

    if _FIELD_REGISTRY is None:
        _FIELD_REGISTRY = set(get_table_columns())

    return _FIELD_REGISTRY


# =========================================================
# 0) 小工具
# =========================================================
def _is_missing(v):
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    return False


def _scol(df_, col, default=0.0):
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


def classify_k_type(r):
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


# =========================================================
# 1) 價量邏輯
# =========================================================
def _trend_regime(price_vs_ma):
    """
    依價格相對均線的位置，粗分多頭/空頭/中性
    > 0 : 多頭行情
    < 0 : 空頭行情
    """
    if pd.isna(price_vs_ma):
        return None
    if price_vs_ma > 0:
        return "多頭行情"
    if price_vs_ma < 0:
        return "空頭行情"
    return "中性"


def _pv_div_by_regime(regime, price_change, vol_change):
    if pd.isna(price_change) or pd.isna(vol_change) or regime is None:
        return None

    p_up = price_change > 0
    p_dn = price_change < 0
    v_up = vol_change > 0
    v_dn = vol_change < 0
    
    final_str = ""
    
    if p_up:
        final_str = final_str + "價漲"
    elif p_dn:
        final_str = final_str + "價跌"
    else:
        final_str = final_str + "價同"
        
    if v_up:
        final_str = final_str + "量增"
    elif v_dn:
        final_str = final_str + "量減"
    else:
        final_str = final_str + "量同"
        
    # if regime == "多頭行情":
    #     final_str = final_str + "(多頭)"
    # else:
    #     final_str = final_str + "(空頭)"
        
    """
    多頭行情：
      價漲量增 -> 多方
      價漲量減 -> 空方背離
      價跌量增 -> 多方背離
      價跌量減 -> 空方

    空頭行情：
      價漲量增 -> 空方背離
      價漲量減 -> 多方
      價跌量增 -> 多方背離
      價跌量減 -> 空方
    """

    # if regime == "多頭行情":
    #     if p_up and v_up:
    #         return "多方"
    #     if p_up and v_dn:
    #         return "空方背離"
    #     if p_dn and v_up:
    #         return "多方背離"
    #     if p_dn and v_dn:
    #         return "空方"

    # if regime == "空頭行情":
    #     if p_up and v_up:
    #         return "空方背離"
    #     if p_up and v_dn:
    #         return "多方"
    #     if p_dn and v_up:
    #         return "多方背離"
    #     if p_dn and v_dn:
    #         return "空方"

    # return None
    
    return final_str

def _fund_flow_text(price_bucket, fund_bucket):
    """
    規則：
      漲跌幅級距 == 法人級距 -> 中性
      法人級距 > 漲跌幅級距 -> 偏向大型股
      法人級距 < 漲跌幅級距 -> 偏向小型股
    """
    if pd.isna(price_bucket) or pd.isna(fund_bucket):
        return None, None

    price_bucket = int(price_bucket)
    fund_bucket = int(fund_bucket)

    if fund_bucket == price_bucket:
        label = "中性"
    elif fund_bucket > price_bucket:
        label = "偏向大型股"
    else:
        label = "偏向小型股"

    detail = f"{label}(價格:資金 = {price_bucket:+d}:{fund_bucket:+d})"
    return label, detail


# =========================================================
# 3) 重新抓可重建報告所需原始資料
# =========================================================
def _build_base_df(finMind, stock_id, sDt, eDt):
    """
    回傳以 date(datetime) 為主鍵、包含價格 + 法人(淨額) + 融資(今日餘額) 的 base df
    """
    df = finMind.get_tw_stock_daily_price(stock_id=stock_id, start_date=sDt, end_date=eDt)
    print(df.tail())
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

                # 太大視為元，轉成仟元
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
# 4) 計算衍生欄
# =========================================================
def _compute_derived(df):
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)

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

    # =========================
    # 基本數值轉型
    # =========================
    for c in ["開盤價", "收盤價", "最高價", "最低價", "成交量", "總成交金額_億"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    prev_close = df["收盤價"].shift(1)
    denom_open = df["開盤價"].replace(0, np.nan)

    # =========================
    # 基本價量
    # =========================
    df["收盤_開盤"] = df["收盤價"] - df["開盤價"]
    df["日振幅"] = df["最高價"] - df["最低價"]
    df["漲跌"] = df["收盤價"] - prev_close
    df["漲跌幅_pct"] = (df["收盤價"] - prev_close) / prev_close
    df["量增率_pct"] = (df["成交量"] - df["成交量"].shift(1)) / df["成交量"].shift(1)

    base_range = df["日振幅"] / prev_close.replace(0, np.nan)
    sign = np.sign(df["收盤價"] - prev_close)
    df["日振幅_昨收_pct"] = base_range * sign
    df["日振福_開盤_pct"] = (df["收盤價"] - df["開盤價"]) / denom_open
    df["日振幅/開盤"] = df["日振幅"] / denom_open

    # =========================
    # 年 / 月 / 星期 / 週次 / 天別
    # =========================
    dt = pd.to_datetime(df["日期"])
    df["年"] = dt.dt.year
    df["月"] = dt.dt.month

    wd_map = {
        0: "星期一", 1: "星期二", 2: "星期三", 3: "星期四",
        4: "星期五", 5: "星期六", 6: "星期日"
    }
    df["星期"] = dt.dt.weekday.map(wd_map)

    y = dt.dt.year
    jan1 = pd.to_datetime(y.astype(str) + "-01-01")
    jan1_wd = jan1.dt.weekday
    offset = (7 - jan1_wd) % 7
    firstMon = jan1 + pd.to_timedelta(offset, unit="D")
    week = ((dt - firstMon).dt.days // 7) + 1
    df["週次"] = week.clip(lower=1).astype(int)

    def _day_bucket(wd):
        if wd == 0:
            return "頭"
        if wd in (1, 2, 3):
            return "週間"
        if wd == 4:
            return "尾"
        return "週間"

    df["天別"] = dt.dt.weekday.apply(_day_bucket)

    # =========================
    # 均量 / 均價 / 扣抵 / 乖離 / 價量
    # =========================
    for n in [5, 10, 20, 60]:
        df[f"{n}日均量"] = df["成交量"].rolling(n).mean()
        df[f"{n}MA"] = df["收盤價"].rolling(n).mean()
        # df[f"{n}日上升幅度"] = df[f"{n}MA"] - df[f"{n}MA"].shift(1)
        # df[f"{n}日扣抵值"] = df["收盤價"].shift(n - 1)
        # df[f"{n}日扣抵影響_pct"] = (df["收盤價"] - df[f"{n}日扣抵值"]) / df["收盤價"].replace(0, np.nan)
        df[f"{n}日乖離"] = (df["收盤價"] - df[f"{n}MA"]) / df[f"{n}MA"].replace(0, np.nan)

        denom_volma = df[f"{n}日均量"].replace(0, np.nan)
        df[f"{n}日量增率_pct"] = (df["成交量"] - df[f"{n}日均量"]) / denom_volma

        df[f"{n}日行情"] = df[f"{n}日乖離"].apply(_trend_regime)

        df[f"{n}日價量"] = [
            _pv_div_by_regime(regime, pchg, vchg)
            for regime, pchg, vchg in zip(
                df[f"{n}日行情"],
                df[f"{n}日乖離"],
                df[f"{n}日量增率_pct"],
            )
        ]

    # =========================
    # 當日價量
    # =========================
    df["當日行情"] = ((df["收盤價"] - df["5MA"]) / df["5MA"].replace(0, np.nan)).apply(_trend_regime)

    df["當日價量"] = [
        _pv_div_by_regime(regime, pchg, vchg)
        for regime, pchg, vchg in zip(
            df["當日行情"],
            df["漲跌幅_pct"],
            df["量增率_pct"],
        )
    ]

    # =========================
    # 金額換算
    # =========================
    df["總成交金額_億"] = pd.to_numeric(df["總成交金額_億"], errors="coerce") / 1e8

    # =========================
    # 法人
    # =========================
    foreign = pd.to_numeric(_scol(df, "Foreign_Investor", default=np.nan), errors="coerce")
    itrust = pd.to_numeric(_scol(df, "Investment_Trust", default=np.nan), errors="coerce")
    dealer = _sum_cols_like(df, keywords=["Dealer"], default=0.0)

    total_net = pd.to_numeric(_scol(df, "total", default=np.nan), errors="coerce")
    if total_net.isna().all():
        total_net = (
            foreign.fillna(0.0) +
            itrust.fillna(0.0) +
            pd.to_numeric(dealer, errors="coerce").fillna(0.0)
        )

    df["法人總買超_億"] = total_net / 1e8
    df["買超_外資_億"] = foreign / 1e8
    df["買超_投信_億"] = itrust / 1e8
    df["買超_自營商_億"] = pd.to_numeric(dealer, errors="coerce").fillna(0.0) / 1e8

    # =========================
    # 融資（今日餘額：仟元）
    # =========================
    df["融資餘額_億"] = pd.to_numeric(_scol(df, "今日餘額", default=np.nan), errors="coerce") * 1000 / 1e8
    df["買超_融資_億"] = df["融資餘額_億"] - df["融資餘額_億"].shift(1)
    df["法人淨買賣超_扣融資_億"] = df["法人總買超_億"] - df["買超_融資_億"]

    # =========================
    # 法人買賣超 / 總成交 比例（扣融資後）
    # =========================
    denom = df["總成交金額_億"].replace(0, np.nan)
    df["法人買賣超_總成交_pct"] = (
        (df["法人總買超_億"] - df["買超_融資_億"]) / denom
    )

    # =========================
    # K 棒影線
    # =========================
    rng = (df["最高價"] - df["最低價"]).replace(0, np.nan)
    df["實體_pct"] = (df["收盤價"] - df["開盤價"]).abs() / rng
    df["上影_pct"] = (df["最高價"] - np.maximum(df["開盤價"], df["收盤價"])) / rng
    df["下影_pct"] = (np.minimum(df["開盤價"], df["收盤價"]) - df["最低價"]) / rng
    df["K線型態"] = df.apply(classify_k_type, axis=1)

    # =========================
    # 跳空缺口 / 跳空缺口_昨收_pct
    # =========================
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
        default=np.nan,
    )

    df["跳空缺口_昨收_pct"] = df["跳空缺口"] / prev_close.replace(0, np.nan)

    # =========================
    # 最大量
    # =========================
    vols = pd.to_numeric(df["成交量"], errors="coerce").fillna(0).to_numpy()
    dates_arr = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d").to_numpy()

    for n in [5, 10, 20, 60]:
        vmax_list, vdate_list = [], []
        for i in range(len(df)):
            if i + 1 < n:
                vmax_list.append(np.nan)
                vdate_list.append(np.nan)
                continue

            window_vol = vols[i + 1 - n:i + 1]
            window_date = dates_arr[i + 1 - n:i + 1]
            idx = int(np.argmax(window_vol))
            vmax_list.append(window_vol[idx])
            vdate_list.append(window_date[idx])

        df[f"{n}日最大量"] = vmax_list
        df[f"{n}日最大量_日期"] = vdate_list

    # ===== [補] 價格漲跌幅 =====
    df["漲跌幅_pct"] = np.where(
        df["收盤價"].shift(1) > 0,
        (df["收盤價"] - df["收盤價"].shift(1)) / df["收盤價"].shift(1) * 100,
        np.nan
    )
    
    # =========================
    # 🔥 百分位 / 級距（修正後）
    # 規則：
    # - 不往前多抓
    # - 不夠一年跨度就留 NULL
    # - 正負分開
    # =========================
    df["漲跌幅百分位數"] = _calc_signed_percentile_by_date(
        dates=df["日期"],
        series=df["漲跌幅_pct"],
        window_days=365,
        min_samples=20,
    )

    df["三大法人買賣超百分位數"] = _calc_signed_percentile_by_date(
        dates=df["日期"],
        series=df["法人淨買賣超_扣融資_億"],   # ← 改這行
        window_days=365,
        min_samples=20,
    )
    
    

    df["漲跌幅級距"] = df["漲跌幅百分位數"].apply(_to_bucket)
    df["三大法人買賣超級距"] = df["三大法人買賣超百分位數"].apply(_to_bucket)
    
    # =========================
    # 🔥 資金走向判定（核心）
    # =========================
    fund_labels = [
        _fund_flow_text(p, f)[0]
        for p, f in zip(df["漲跌幅級距"], df["三大法人買賣超級距"])
    ]

    df["資金走向判定"] = fund_labels

    # =========================
    # 不存中間計算欄
    # =========================
    df.drop(
        columns=[
            "當日行情",
            "5日行情", "10日行情", "20日行情", "60日行情",
            "融資餘額_億",
            "昨高", "昨低", "跳空狀態", "今上緣", "今下緣", "昨上緣", "昨下緣",
        ],
        inplace=True,
        errors="ignore",
    )

    return df

# =========================================================
# 5) update_is_complete
# =========================================================
def update_is_complete():
    exclude_cols = {
        "is_complete",
        "id",
        "created_at",
        "updated_at",
        # 長期均線前 119/239 筆依法為 NULL，不能因此判定原始資料不完整。
        "120MA",
        "240MA",
    }

    all_cols = get_db_columns()
    check_cols = [c for c in all_cols if c not in exclude_cols]

    if not check_cols:
        print("⚠ 沒有可檢查欄位，跳過更新。")
        return

    def q(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    null_conditions = " OR ".join([f"{q(c)} IS NULL" for c in check_cols])

    sql = f"""
    UPDATE {q(TABLE)}
    SET {q("is_complete")} = CASE
        WHEN {null_conditions} THEN 0
        ELSE 1
    END;
    """
    print("執行 SQL 中 ...")
    ok = db.execute_sql(sql)
    print("更新完成 ✔" if ok else "更新失敗 ❌")


def recalc_long_ma(stock_id: str = "TAIEX"):
    """用資料庫完整歷史重算 120MA／240MA，避免只抓近月資料造成均線斷裂。"""
    sql = """
    WITH calculated AS (
        SELECT
            id,
            CASE
                WHEN COUNT("收盤價") OVER (
                    PARTITION BY "股票代號" ORDER BY "日期"
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) = 120
                THEN AVG("收盤價") OVER (
                    PARTITION BY "股票代號" ORDER BY "日期"
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                )
            END AS ma120,
            CASE
                WHEN COUNT("收盤價") OVER (
                    PARTITION BY "股票代號" ORDER BY "日期"
                    ROWS BETWEEN 239 PRECEDING AND CURRENT ROW
                ) = 240
                THEN AVG("收盤價") OVER (
                    PARTITION BY "股票代號" ORDER BY "日期"
                    ROWS BETWEEN 239 PRECEDING AND CURRENT ROW
                )
            END AS ma240
        FROM stock_report_daily
        WHERE "股票代號" = ?
    )
    UPDATE stock_report_daily r
    SET "120MA" = c.ma120,
        "240MA" = c.ma240
    FROM calculated c
    WHERE c.id = r.id
      AND (r."120MA" IS DISTINCT FROM c.ma120
           OR r."240MA" IS DISTINCT FROM c.ma240)
    """
    return db.execute_sql(sql, (stock_id,))


# =========================================================
# 6) UPSERT
# =========================================================
def upsert(df: pd.DataFrame, stock_id: str):
    if df is None or df.empty:
        print("⚠ df empty, skip")
        return

    df = df.copy()
    df["股票代號"] = stock_id
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")

    db_columns = get_db_columns()
    
    skip_cols = {"id", "created_at", "updated_at"}

    # 只寫 df 真的有的欄位，不要把 DB 欄位全部補 NaN
    write_columns = [
        c for c in db_columns
        if c not in skip_cols and c in df.columns
    ]

    # 確保主鍵一定在
    for c in ["股票代號", "日期"]:
        if c not in write_columns:
            write_columns.insert(0, c)

    # 不在這裡算完整度，交給 update_is_complete()
    if "is_complete" in db_columns and "is_complete" not in write_columns:
        df["is_complete"] = 0
        write_columns.append("is_complete")

    df = df[write_columns]

    print("準備寫入日期範圍：", df["日期"].min(), "~", df["日期"].max())
    print(df[["日期", "股票代號", "收盤價"]].tail())

    col_sql = ",".join([f'"{c}"' for c in write_columns])
    ph = ",".join(["?"] * len(write_columns))

    update_cols = [
        c for c in write_columns
        if c not in ("日期", "股票代號")
    ]

    update_sql = ",".join([
        f'"{c}" = excluded."{c}"'
        for c in update_cols
    ])

    sql = f"""
    INSERT INTO "{TABLE}" ({col_sql})
    VALUES ({ph})
    ON CONFLICT("股票代號","日期") DO UPDATE SET
        {update_sql};
    """

    rows = list(df.itertuples(index=False, name=None))

    ok = db.execute_sql(sql, rows)

    if ok:
        print(f"✔ DB 寫入成功: {len(df)} rows, stock={stock_id}")

        check = db.query_to_df(f"""
            SELECT MAX(日期) AS max_date
            FROM "{TABLE}"
            WHERE 股票代號 = '{stock_id}'
        """)
        print("DB 最新日期：")
        print(check)
    else:
        print("❌ DB 寫入失敗（請看上方 PostgreSQL Error）")


# =========================================================
# 7) repair 用：只更新指定欄位
# =========================================================
def _to_sqlite_value(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, np.generic):
        return v.item()
    return v


def _update_fields_to_db(key_cols, df_patch, fields):
    if df_patch is None or df_patch.empty:
        return 0

    df_patch = df_patch.copy()
    if "日期" in df_patch.columns:
        df_patch["日期"] = pd.to_datetime(df_patch["日期"]).dt.strftime("%Y-%m-%d")

    set_clause = ", ".join([f'"{c}" = ?' for c in fields])
    where_clause = " AND ".join([f'"{k}" = ?' for k in key_cols])
    sql = f'UPDATE "{TABLE}" SET {set_clause} WHERE {where_clause}'

    params = []
    for _, r in df_patch.iterrows():
        row_vals = (
            [_to_sqlite_value(r.get(c, None)) for c in fields] +
            [_to_sqlite_value(r.get(k, None)) for k in key_cols]
        )
        params.append(tuple(row_vals))

    if hasattr(db, "executemany") and callable(getattr(db, "executemany")):
        db.executemany(sql, params)
    else:
        for p in params:
            db.execute_sql(sql, p)

    return len(params)


# =========================================================
# 🔥 百分位計算（核心）
# =========================================================

def _calc_signed_percentile_by_date(dates, series, window_days=365, min_samples=20):
    """
    規則：
    1. 只用目前資料集內，且在「當前日期之前」的資料
    2. 需要至少滿 365 天歷史跨度，否則直接 NaN
    3. 正負分開算
    4. 同方向樣本數 < min_samples -> NaN
    5. 不夠資料時，x==0 也不特判，照樣 NaN
    """
    dt = pd.to_datetime(dates)
    s = pd.to_numeric(series, errors="coerce")

    out = np.full(len(s), np.nan, dtype="float64")

    for i in range(len(s)):
        x = s.iloc[i]
        if pd.isna(x):
            continue

        cur_dt = dt.iloc[i]

        # ❗一定要滿365天歷史跨度，不夠就不要算
        earliest_allowed = cur_dt - pd.Timedelta(days=window_days)
        if dt.iloc[0] > earliest_allowed:
            continue

        # 只取目前資料內、當前日期之前、且落在前365天內的資料
        hist_mask = (dt >= earliest_allowed) & (dt < cur_dt)
        hist = s.loc[hist_mask].dropna()

        # 不把 0 混進正負樣本
        hist = hist[hist != 0]

        if x > 0:
            sample = hist[hist > 0]
        elif x < 0:
            sample = hist[hist < 0]
        else:
            # 不夠資料就不要硬給0
            continue

        if len(sample) < min_samples:
            continue

        rank = (sample < x).sum() + 1
        pct = rank / len(sample)

        out[i] = pct if x > 0 else -pct

    return pd.Series(out, index=series.index)


def _to_bucket(x):
    if pd.isna(x):
        return np.nan

    if x >= 0.9:
        return 3
    elif x >= 0.7:
        return 2
    elif x >= 0.3:
        return 1
    elif x > -0.3:
        return 0
    elif x > -0.7:
        return -1
    elif x > -0.9:
        return -2
    else:
        return -3
