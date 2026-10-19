# =========================
# main/stock_report_daily.py
# =========================
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from module import finMind as fm
from module import stock_report_utils as SRU
from common import db  # DB 模組
from common.report_columns import order_taiex_report_columns


today = datetime.now()


# ======================================
# 主流程
# ======================================
def taiex_daily_report(months: int = 4, eDt: datetime = None):
    if eDt is None:
        eDt = datetime.now()
    sDt = eDt - relativedelta(months=months)
    return export("TAIEX", sDt, eDt)


def export(stock_id, sDt, eDt):
    where_condition = """
        AND target_table LIKE 'fm_%' 
        AND target_table LIKE '%_stock_%'
    """
    db._limit_date_span(where_condition=where_condition)
    
    # 建 base df
    df = SRU._build_base_df(fm, stock_id, sDt, eDt)
    if df is None or df.empty:
        print(f"[警告] {stock_id} {sDt} ~ {eDt} 無日資料，export 回傳空 DataFrame")
        return pd.DataFrame()

    # 計算衍生欄位
    df = SRU._compute_derived(df)

    # 寫入 DB
    SRU.upsert(df, stock_id)
    SRU.recalc_long_ma(stock_id)
    SRU.update_is_complete()

    # 輸出view表
    sql = f"""
        SELECT * FROM v_taiex_report ORDER BY 日期
    """
    output = db.query_to_df(sql)

    # 對外報表顯示兩套明確核心欄位；舊「策略_核心」仍留在 View
    # 並固定等同五日價量版，供既有回測與模型相容使用。
    output = output.loc[:, order_taiex_report_columns(list(output.columns))]

    output.to_csv("C:/Users/USER/Desktop/Han/學習/金融策略/Invest/output/stock_report.csv", index=False, encoding="utf-8-sig")
    return output


# =========================================================
# 修補指定期間、指定欄位，可選 force_renew
# =========================================================
def repair_stock_report_fields(
    stock_id,
    sDt,
    eDt,
    fields,
    force_renew=False,
    lookback_days=180,
):
    """
    fields: list[str] 要修補的欄位
    force_renew:
      - False: 只補缺值
      - True : 期間內全部重算覆蓋
    """
    sDt_lb = sDt - relativedelta(days=lookback_days)

    base = SRU._build_base_df(fm, stock_id, sDt_lb, eDt)
    if base is None or base.empty:
        print(f"[repair] base empty: {stock_id} {sDt_lb}~{eDt}")
        return 0

    calc = SRU._compute_derived(base)

    calc["日期"] = pd.to_datetime(calc["日期"])
    mask = (
        (calc["日期"] >= pd.to_datetime(sDt.date())) &
        (calc["日期"] <= pd.to_datetime(eDt.date()))
    )
    calc = calc.loc[mask].copy()

    sql_now = f"""
        SELECT 日期, 股票代號, {",".join([f'"{c}"' for c in fields])}
        FROM "{SRU.TABLE}"
        WHERE 股票代號 = '{stock_id}'
          AND 日期 BETWEEN '{sDt.strftime("%Y-%m-%d")}' AND '{eDt.strftime("%Y-%m-%d")}'
        ORDER BY 日期
    """
    now = db.query_to_df(sql_now)
    if now is None or now.empty:
        print(f"[repair] DB has no rows in range: {stock_id} {sDt}~{eDt}")
        return 0

    now["日期"] = pd.to_datetime(now["日期"])
    calc["日期"] = pd.to_datetime(calc["日期"])

    now2 = now.rename(columns={c: f"{c}_old" for c in fields})
    calc2 = calc.rename(columns={c: f"{c}_new" for c in fields if c in calc.columns})

    merged = now2.merge(
        calc2[["日期"] + [f"{c}_new" for c in fields if f"{c}_new" in calc2.columns]],
        on="日期",
        how="left",
    )
    merged["股票代號"] = stock_id

    patch_rows = []
    for _, r in merged.iterrows():
        out = {"日期": r["日期"], "股票代號": stock_id}
        need_any = False

        for c in fields:
            if c not in SRU.get_field_registry():
                raise ValueError(f"[repair] FIELD_REGISTRY 沒有定義欄位：{c}")

            newv = r.get(f"{c}_new", None)
            oldv = r.get(f"{c}_old", None)

            if SRU._is_missing(newv):
                continue

            if force_renew:
                out[c] = newv
                need_any = True
            else:
                if SRU._is_missing(oldv):
                    out[c] = newv
                    need_any = True

        if need_any:
            patch_rows.append(out)

    if not patch_rows:
        SRU.update_is_complete()
        print("[repair] nothing to patch")
        return 0

    df_patch = pd.DataFrame(patch_rows)

    updated = SRU._update_fields_to_db(
        key_cols=["股票代號", "日期"],
        df_patch=df_patch,
        fields=fields,
    )

    SRU.update_is_complete()
    print(f"[repair] updated rows: {updated}, fields: {fields}, force_renew={force_renew}")
    return updated

# =========================================================
# 🔥 重算百分位 / 級距（專用）
# =========================================================
def rebuild_percentile_fields(
    stock_id,
    sDt,
    eDt,
    force_renew=True,
):
    """
    重算：
    - 漲跌幅百分位數
    - 漲跌幅級距
    - 三大法人買賣超百分位數
    - 三大法人買賣超級距
    - 資金走向判定

    規則：
    - 不往前抓
    - 不夠資料 -> NULL
    - 支援：
        force_renew=False → 只補 NULL
        force_renew=True  → 全覆蓋
    """

    fields = [
        "漲跌幅百分位數",
        "漲跌幅級距",
        "三大法人買賣超百分位數",
        "三大法人買賣超級距",
        "資金走向判定",
    ]

    # ===== build =====
    base = SRU._build_base_df(fm, stock_id, sDt, eDt)
    if base is None or base.empty:
        print(f"[rebuild_pct] base empty: {stock_id}")
        return 0

    calc = SRU._compute_derived(base)

    # ===== 切期間 =====
    calc["日期"] = pd.to_datetime(calc["日期"])

    mask = (
        (calc["日期"] >= pd.to_datetime(sDt.date())) &
        (calc["日期"] <= pd.to_datetime(eDt.date()))
    )
    calc = calc.loc[mask].copy()
    print(calc[[
        "日期",
        "漲跌幅百分位數",
        "漲跌幅級距",
        "三大法人買賣超百分位數",
        "三大法人買賣超級距",
        "資金走向判定",
    ]].tail(30))

    print("calc 非空筆數：")
    for c in [
        "漲跌幅百分位數",
        "漲跌幅級距",
        "三大法人買賣超百分位數",
        "三大法人買賣超級距",
        "資金走向判定",
    ]:
        print(c, calc[c].notna().sum())

    if calc.empty:
        print(f"[rebuild_pct] no calc rows")
        return 0

    # ===== 讀 DB 現有 =====
    sql_now = f"""
        SELECT 日期, 股票代號, {",".join([f'"{c}"' for c in fields])}
        FROM "{SRU.TABLE}"
        WHERE 股票代號 = '{stock_id}'
          AND 日期 BETWEEN '{sDt.strftime("%Y-%m-%d")}' AND '{eDt.strftime("%Y-%m-%d")}'
    """
    now = db.query_to_df(sql_now)

    if now is None or now.empty:
        print("[rebuild_pct] DB empty")
        return 0

    now["日期"] = pd.to_datetime(now["日期"])

    calc["股票代號"] = stock_id

    merged = now.merge(
        calc[["日期"] + fields],
        on="日期",
        how="left",
        suffixes=("_old", "_new")
    )

    patch_rows = []

    for _, r in merged.iterrows():
        out = {"日期": r["日期"], "股票代號": stock_id}
        need = False

        for c in fields:
            newv = r.get(f"{c}_new")
            oldv = r.get(f"{c}_old")

            if SRU._is_missing(newv):
                continue

            if force_renew:
                out[c] = newv
                need = True
            else:
                if SRU._is_missing(oldv):
                    out[c] = newv
                    need = True

        if need:
            patch_rows.append(out)

    if not patch_rows:
        print("[rebuild_pct] nothing to patch")
        return 0

    df_patch = pd.DataFrame(patch_rows)

    updated = SRU._update_fields_to_db(
        key_cols=["股票代號", "日期"],
        df_patch=df_patch,
        fields=fields,
    )

    SRU.update_is_complete()

    print(f"[rebuild_pct] updated rows: {updated}, force={force_renew}")
    return updated



def run_report_pipeline():
    """只更新加權指數日報表，不下載分 K。"""
    from module.core_strategy import ensure_core_strategy_rule

    if ensure_core_strategy_rule():
        print("[加權報表] 核心策略新判定已套用。")

    sDt = datetime(2024, 1, 1)
    eDt = datetime.now()
    stock_id = "TAIEX"
    print(eDt.strftime("%Y-%m-%d"))

    df = export(stock_id, sDt, eDt)
    print(df.tail(5))
    return df


def run_daily_pipeline():
    """執行加權指數日報表與一分 K 完整流程。"""
    df = run_report_pipeline()
    from main.intraday_pipeline import update_taiex_kbar_1m

    update_taiex_kbar_1m(datetime.now())
    return df


# python -m main.stock_report_taiex
if __name__ == "__main__":
    # db._limit_date_span()
    run_daily_pipeline()
    
    ###########
    # rebuild_price_volume_fields(
    #     stock_id="TAIEX",
    #     sDt=datetime(2024, 1, 1),
    #     eDt=datetime(2026, 4, 19),
    # )
    
    ########
    # rebuild_percentile_fields(
    #     stock_id="TAIEX",
    #     sDt=sDt,
    #     eDt=eDt
    # )
