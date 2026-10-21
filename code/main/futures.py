from module import finMind as fm, futures_utils as FU
from common import db, utils
from datetime import datetime, timedelta
from time import perf_counter

import pandas as pd
import traceback


class SourceDataPendingError(RuntimeError):
    """外部資料源尚未發布資料，不代表報表更新程序故障。"""

    is_data_pending = True


# =========================================================
# 抓取期貨、選擇權近期資料
# =========================================================
def run_tx_txo_all(
    start_date="2020-01-01",
    end_date=None,
):
    """
    抓 TX + TXO 全資料
    """

    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)

    if end_date is None:
        end_date = utils._taipei_today()
    elif isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)

    futures_id = "TX"
    option_id = "TXO"

    results = {}

    jobs = [
        {
            "name": "futures_daily",
            "func": lambda: fm.get_tw_futures_daily(
                futures_id=futures_id,
                start_date=start_date,
                end_date=end_date,
            ),
        },
        {
            "name": "futures_final_settlement_price",
            "func": lambda: fm.get_tw_futures_final_settlement_price(
                futures_id=futures_id,
                start_date=start_date,
                end_date=end_date,
            ),
        },
        {
            "name": "futures_institutional_investors_after_hours",
            "func": lambda: fm.get_tw_futures_institutional_investors_after_hours(
                futures_id=futures_id,
                start_date=start_date,
                end_date=end_date,
            ),
        },
        {
            "name": "option_final_settlement_price",
            "func": lambda: fm.get_tw_option_final_settlement_price(
                option_id=option_id,
                start_date=start_date,
                end_date=end_date,
            ),
        },
    ]

    print("======================================")
    print("開始抓取 FinMind TX / TXO 資料")
    print(f"區間: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print("======================================")

    for job in jobs:
        job_name = job["name"]
        print(f"\n>>> 開始執行：{job_name}")

        try:
            df = job["func"]()

            if df is None:
                df = pd.DataFrame()

            results[job_name] = df
            print(f"<<< 完成：{job_name}，筆數 = {len(df)}")

        except Exception as e:
            print(f"!!! 失敗：{job_name}")
            print(f"錯誤訊息：{e}")
            traceback.print_exc()
            results[job_name] = pd.DataFrame()

    print("\n======================================")
    print("全部任務執行完畢，結果摘要：")
    print("======================================")
    for k, v in results.items():
        print(f"{k}: {len(v)} 筆")

    return results

# =========================================================
# 主函式
# =========================================================
def build_futures_report_daily_raw(
    start_date,
    end_date=None,
    futures_id="TX",
    recent_rebuild_days=0,
    force_update=False,
) -> pd.DataFrame:

    target_table = "futures_report_daily_raw"
    idx_key = str(futures_id)

    start_ts = FU._to_ts(start_date)
    end_ts = FU._to_ts(end_date if end_date is not None else utils._taipei_today())

    if start_ts > end_ts:
        raise ValueError("start_date 不可大於 end_date")

    # === 更新來源資料 ===
    night_source_end = FU._get_night_source_end(end_ts, futures_id)
    print(
        f"[夜盤來源範圍] 報表截止={FU._dstr(end_ts)}，"
        f"來源截止={FU._dstr(night_source_end)}"
    )
    fm.get_tw_futures_daily(
        futures_id=futures_id,
        start_date=start_ts,
        end_date=night_source_end,
    )
    # 官方資料與 FinMind 分開保存；FinMind 夜盤缺檔時，來源 helper 會自動改採官方行情。
    try:
        from module.futures_datas import sync_taifex_futures_daily

        sync_taifex_futures_daily(
            futures_id,
            start_ts,
            night_source_end,
        )
    except Exception as exc:
        # 官方來源暫時無法連線時仍保留原本 FinMind 更新流程。
        print(f"[期交所備援] 本次同步失敗，暫以既有資料繼續：{exc}")
    fm.get_tw_futures_final_settlement_price(
        futures_id=futures_id,
        start_date=start_ts,
        end_date=end_ts,
    )
    fm.get_tw_futures_institutional_investors(
        futures_id=futures_id,
        start_date=start_ts,
        end_date=end_ts,
    )
    fm.get_tw_futures_institutional_investors_after_hours(
        futures_id=futures_id,
        start_date=start_ts,
        # 夜盤法人使用報表交易日本身的日期，不套用夜盤行情的
        # 「下一交易日」歸屬規則。
        end_date=end_ts,
        recent_refresh_days=14,
    )
    
    # === 建立價格對照表 ===
    weighted_close_map, tx_close_map, tx_settlement_map = FU._build_market_price_maps(
        start_ts=start_ts,
        end_ts=end_ts,
        futures_id=futures_id,
    )

    # === 查快取 ===
    span_row = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
        """,
        (target_table, idx_key),
    )

    mem_s = (
        pd.Timestamp(span_row.loc[0, "start_date"]).normalize()
        if not span_row.empty and pd.notna(span_row.loc[0, "start_date"])
        else None
    )
    mem_e = (
        pd.Timestamp(span_row.loc[0, "end_date"]).normalize()
        if not span_row.empty and pd.notna(span_row.loc[0, "end_date"])
        else None
    )

    actual_cache_row = db.query_to_df(
        """
        SELECT MIN(日期) AS min_date, MAX(日期) AS max_date
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        """,
        (idx_key,),
    )

    actual_mem_s = (
        pd.Timestamp(actual_cache_row.loc[0, "min_date"]).normalize()
        if not actual_cache_row.empty and pd.notna(actual_cache_row.loc[0, "min_date"])
        else None
    )
    actual_mem_e = (
        pd.Timestamp(actual_cache_row.loc[0, "max_date"]).normalize()
        if not actual_cache_row.empty and pd.notna(actual_cache_row.loc[0, "max_date"])
        else None
    )

    if mem_s is not None and actual_mem_s is not None:
        mem_s = max(mem_s, actual_mem_s)

    if mem_e is not None and actual_mem_e is not None:
        mem_e = min(mem_e, actual_mem_e)

    fetch_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    if force_update:
        fetch_ranges = [(start_ts, end_ts)]
    else:
        if mem_s is None or mem_e is None:
            fetch_ranges = [(start_ts, end_ts)]
        else:
            if start_ts < mem_s:
                fetch_ranges.append((start_ts, mem_s - pd.Timedelta(days=1)))
            if end_ts > mem_e:
                fetch_ranges.append((mem_e + pd.Timedelta(days=1), end_ts))

    print(
        f"[build] {idx_key} "
        f"req=({FU._dstr(start_ts)}~{FU._dstr(end_ts)}) "
        f"cache=({FU._dstr(mem_s) if mem_s is not None else None}~{FU._dstr(mem_e) if mem_e is not None else None}) "
        f"ranges={[( FU._dstr(s), FU._dstr(e) ) for s, e in fetch_ranges]}"
    )

    upsert_sql = """
    INSERT INTO futures_report_daily_raw (
        futures_id,
        日期, 年, 月, 星期, 週次, 天別,
        日盤開, 日盤收, 日盤高, 日盤低,
        日漲跌, 日振幅, 日振幅開盤比, 日振幅昨收比, 日收減日開, 日收開昨收比,
        夜盤開, 夜盤收, 夜盤高, 夜盤低,
        日法人多倉, 日法人空倉, 夜法人多單量, 夜法人空單量,
        日成交量, 夜成交量, "K線型態",
        "日5MA", "日10MA", "日20MA", "日60MA", "日120MA",
        "夜5MA", "夜10MA", "夜20MA", "夜60MA", "夜120MA",
        加權收盤價, 大台收盤價, 大台結算價
    )
    VALUES (
        ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?
    )
    ON CONFLICT(futures_id, 日期) DO UPDATE SET
        年 = excluded.年,
        月 = excluded.月,
        星期 = excluded.星期,
        週次 = excluded.週次,
        天別 = excluded.天別,
        日盤開 = excluded.日盤開,
        日盤收 = excluded.日盤收,
        日盤高 = excluded.日盤高,
        日盤低 = excluded.日盤低,
        日漲跌 = excluded.日漲跌,
        日振幅 = excluded.日振幅,
        日振幅開盤比 = excluded.日振幅開盤比,
        日振幅昨收比 = excluded.日振幅昨收比,
        日收減日開 = excluded.日收減日開,
        日收開昨收比 = excluded.日收開昨收比,
        夜盤開 = excluded.夜盤開,
        夜盤收 = excluded.夜盤收,
        夜盤高 = excluded.夜盤高,
        夜盤低 = excluded.夜盤低,
        日法人多倉 = excluded.日法人多倉,
        日法人空倉 = excluded.日法人空倉,
        夜法人多單量 = excluded.夜法人多單量,
        夜法人空單量 = excluded.夜法人空單量,
        日成交量 = excluded.日成交量,
        夜成交量 = excluded.夜成交量,
        "K線型態" = excluded."K線型態",
        加權收盤價 = excluded.加權收盤價,
        大台收盤價 = excluded.大台收盤價,
        大台結算價 = excluded.大台結算價
    """

    rows = []
    dates_to_rebuild = set()

    for fs, fe in fetch_ranges:
        df_dates = db.query_to_df(
            """
            SELECT DISTINCT date
            FROM fm_taiwan_futures_daily
            WHERE futures_id = ?
              AND trading_session = 'position'
              AND date >= ?
              AND date <= ?
            ORDER BY date
            """,
            (idx_key, FU._dstr(fs), FU._dstr(fe)),
        )

        if df_dates is not None and not df_dates.empty:
            dates_to_rebuild.update(
                pd.to_datetime(df_dates["date"]).dt.normalize().tolist()
            )

    if not force_update and recent_rebuild_days > 0:
        dates_to_rebuild.update(
            FU._get_recent_trading_dates(recent_rebuild_days, futures_id)
        )

    if not force_update:
        dates_to_rebuild.update(
            FU._get_incomplete_night_dates(start_ts, end_ts, futures_id)
        )

    dates_to_rebuild = sorted(dates_to_rebuild)

    print(
        f"[dates_to_rebuild] count={len(dates_to_rebuild)}, "
        f"tail={[ FU._dstr(x) for x in dates_to_rebuild[-10:] ] if dates_to_rebuild else []}"
    )

    for d in dates_to_rebuild:
        day_row = FU._get_month_contract_row(
            d,
            "position",
            FU._get_front_contract_month(d),
            futures_id,
        )
        if day_row is None:
            print(f"[skip] {idx_key} {FU._dstr(d)} day_row is None")
            continue

        next_d = FU._get_next_after_market_date(d, futures_id)
        night_row = None
        if next_d is not None:
            night_row = FU._get_month_contract_row(
                next_d,
                "after_market",
                FU._get_front_contract_month(next_d),
                futures_id,
            )

        o = float(day_row["open"]) if pd.notna(day_row["open"]) else None
        c = float(day_row["close"]) if pd.notna(day_row["close"]) else None
        h = float(day_row["max"]) if pd.notna(day_row["max"]) else None
        l = float(day_row["min"]) if pd.notna(day_row["min"]) else None

        day_vol = (
            float(day_row["volume"])
            if pd.notna(day_row["volume"])
            else None
        )
        night_vol = (
            float(night_row["volume"])
            if night_row is not None and pd.notna(night_row["volume"])
            else None
        )

        prev_df = db.query_to_df(
            """
            SELECT 日盤收
            FROM futures_report_daily_raw
            WHERE futures_id = ?
              AND 日期 < ?
            ORDER BY 日期 DESC
            LIMIT 1
            """,
            (idx_key, FU._dstr(d)),
        )

        prev_close = (
            float(prev_df.loc[0, "日盤收"])
            if not prev_df.empty and pd.notna(prev_df.loc[0, "日盤收"])
            else None
        )

        # === 日漲跌：以昨收為基準 ===
        diff = (
            (c - prev_close)
            if (c is not None and prev_close is not None)
            else None
        )
        amp = (h - l) if (h is not None and l is not None) else None
        amp_open = (amp / o) if (amp is not None and o is not None and o != 0) else None
        amp_prev = (
            (amp / prev_close)
            if (amp is not None and prev_close is not None and prev_close != 0)
            else None
        )

        # 日收減日開 / 日收開昨收比 仍維持收開邏輯
        close_minus_open = (
            (c - o)
            if (c is not None and o is not None)
            else None
        )
        close_open_vs_prev = (
            (close_minus_open / prev_close)
            if (
                close_minus_open is not None
                and prev_close is not None
                and prev_close != 0
            )
            else None
        )

        # === 日法人未平倉 ===
        inst_day_df = db.query_to_df(
            """
            SELECT
                SUM(COALESCE(long_open_interest_balance_volume, 0)) AS long_oi,
                SUM(COALESCE(short_open_interest_balance_volume, 0)) AS short_oi
            FROM fm_taiwan_futures_institutional_investors
            WHERE data_id = ?
              AND date = ?
            """,
            (idx_key, FU._dstr(d)),
        )

        day_inst_long = (
            float(inst_day_df.loc[0, "long_oi"])
            if not inst_day_df.empty and pd.notna(inst_day_df.loc[0, "long_oi"])
            else None
        )
        day_inst_short = (
            float(inst_day_df.loc[0, "short_oi"])
            if not inst_day_df.empty and pd.notna(inst_day_df.loc[0, "short_oi"])
            else None
        )

        # === 夜法人欄位：使用報表交易日本身的日期 ===
        night_inst_long = None
        night_inst_short = None

        inst_night_df = db.query_to_df(
            """
            SELECT
                SUM(COALESCE(long_deal_volume, 0)) AS long_vol,
                SUM(COALESCE(short_deal_volume, 0)) AS short_vol
            FROM fm_taiwan_futures_institutional_investors_after_hours
            WHERE futures_id = ?
              AND date = ?
            """,
            (idx_key, FU._dstr(d)),
        )

        night_inst_long = (
            float(inst_night_df.loc[0, "long_vol"])
            if not inst_night_df.empty and pd.notna(inst_night_df.loc[0, "long_vol"])
            else None
        )
        night_inst_short = (
            float(inst_night_df.loc[0, "short_vol"])
            if not inst_night_df.empty and pd.notna(inst_night_df.loc[0, "short_vol"])
            else None
        )

        rows.append(
            (
                idx_key,
                FU._dstr(d),
                d.year,
                d.month,
                FU._weekday_text(d),
                FU._week_no(d),
                FU._day_type_text(d),

                o, c, h, l,

                diff,
                amp,
                amp_open,
                amp_prev,
                close_minus_open,
                close_open_vs_prev,

                float(night_row["open"]) if night_row is not None and pd.notna(night_row["open"]) else None,
                float(night_row["close"]) if night_row is not None and pd.notna(night_row["close"]) else None,
                float(night_row["max"]) if night_row is not None and pd.notna(night_row["max"]) else None,
                float(night_row["min"]) if night_row is not None and pd.notna(night_row["min"]) else None,

                day_inst_long,
                day_inst_short,
                night_inst_long,
                night_inst_short,

                day_vol,
                night_vol,
                FU._classify_k_type(o, c, h, l),

                None, None, None, None, None,
                None, None, None, None, None,
                
                weighted_close_map.get(FU._dstr(d)),
                tx_close_map.get(FU._dstr(d)),
                tx_settlement_map.get(FU._dstr(d)),
            )
        )

    print(f"[rows_to_upsert] count={len(rows)}")
    if rows:
        print(f"[rows_to_upsert] tail_dates={[r[1] for r in rows[-5:]]}")

        ok = db.execute_sql(upsert_sql, rows)

        if not ok:
            raise RuntimeError(
                "futures_report_daily_raw 寫入失敗，"
                "停止後續報表計算，請查看上方 PostgreSQL Error"
            )

    # === date_span 更新 ===
    span_df = db.query_to_df(
        """
        SELECT MIN(日期) AS min_date, MAX(日期) AS max_date
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        """,
        (idx_key,),
    )

    if not span_df.empty and pd.notna(span_df.loc[0, "max_date"]):
        actual_s = pd.Timestamp(span_df.loc[0, "min_date"]).normalize()
        actual_e = pd.Timestamp(span_df.loc[0, "max_date"]).normalize()

        safe_e = actual_e - pd.Timedelta(days=1)

        if safe_e >= actual_s:
            db.execute_sql(
                """
                INSERT INTO date_span (target_table, idx_key, start_date, end_date, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(target_table, idx_key) DO UPDATE SET
                    start_date = excluded.start_date,
                    end_date   = excluded.end_date,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (target_table, idx_key, FU._dstr(actual_s), FU._dstr(safe_e)),
            )

    # === 重算 MA ===
    FU._recalc_futures_report_daily_raw_ma(futures_id)
    
    # === 重算 價量 ===
    FU._recalc_futures_report_daily_raw_pv(futures_id)

    # === 重算夜盤衍生欄位（實體寫入 raw，避免報表 View 即時計算）===
    recalc_futures_report_daily_raw_night_metrics(
        futures_id=futures_id,
        target_dates=dates_to_rebuild,
    )
    
    # === 重算 月選 Call / Put total ===
    FU._recalc_futures_report_daily_raw_month_option(
        futures_id=futures_id,
        option_id="TXO",
        target_dates=dates_to_rebuild,
    )
    
    # === 重算 週三選 Call / Put total ===
    FU._recalc_futures_report_daily_raw_weekly_wed_option(
        futures_id=futures_id,
        option_id="TXO",
        target_dates=dates_to_rebuild,
    )
    
    # === 重算 週五選 Call / Put total ===
    FU._recalc_futures_report_daily_raw_weekly_friday_option(
        futures_id=futures_id,
        option_id="TXO",
        target_dates=dates_to_rebuild,
    )

    # === 回填布林通道 ===
    df = db.query_to_df(
        """
        SELECT 日期, 日盤收
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (idx_key,),
    )

    if df is not None and not df.empty:
        df["日期"] = pd.to_datetime(df["日期"])
        df = df.sort_values("日期").copy()

        df["ma20"] = df["日盤收"].rolling(window=20, min_periods=20).mean()
        df["std"] = df["日盤收"].rolling(window=20, min_periods=20).std()
        df["upper"] = df["ma20"] + 2 * df["std"]
        df["lower"] = df["ma20"] - 2 * df["std"]

        update_rows = []
        for _, row in df.iterrows():
            update_rows.append(
                (
                    float(row["std"]) if pd.notna(row["std"]) else None,
                    float(row["upper"]) if pd.notna(row["upper"]) else None,
                    float(row["lower"]) if pd.notna(row["lower"]) else None,
                    idx_key,
                    row["日期"].strftime("%Y-%m-%d"),
                )
            )

        db.execute_sql(
            """
            UPDATE futures_report_daily_raw
            SET
                日布林標準差 = ?,
                日布林上軌 = ?,
                日布林下軌 = ?
            WHERE futures_id = ?
              AND 日期 = ?
            """,
            update_rows,
        )
        
    backfill_MTX_TMF_retail_long_short_ratio(start_date, end_date)

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 >= ?
          AND 日期 <= ?
        ORDER BY 日期
        """,
        (idx_key, FU._dstr(start_ts), FU._dstr(end_ts)),
    )


def recalc_futures_report_daily_raw_night_metrics(
    futures_id="TX",
    target_dates=None,
) -> pd.DataFrame:
    """計算夜盤衍生欄位並批次寫回 futures_report_daily_raw。

    計算只讀取 raw 與 fm_taiwan_futures_daily，不查詢正式報表 View。
    target_dates 有值時，會更新各目標日到其後 19 個交易日；這可涵蓋
    昨日比較與夜布林 20 筆視窗受到歷史修正的影響。
    """
    idx_key = str(futures_id)

    df = db.query_to_df(
        """
        WITH settlement_ranked AS (
            SELECT
                f.date AS 日期,
                f.futures_id,
                CASE
                    WHEN f.settlement_price IS NULL OR f.settlement_price = 0
                        THEN f.close
                    ELSE f.settlement_price
                END AS 全日收,
                ROW_NUMBER() OVER (
                    PARTITION BY f.date, f.futures_id
                    ORDER BY f.contract_date
                ) AS rn
            FROM fm_taiwan_futures_daily f
            WHERE f.futures_id = ?
              AND f.trading_session = 'position'
              AND LENGTH(f.contract_date) = 6
        )
        SELECT
            r.futures_id,
            r.日期,
            s.全日收,
            r.夜盤開,
            r.夜盤收,
            r.夜盤高,
            r.夜盤低,
            r.夜成交量,
            r."夜5MA",
            r."夜10MA",
            r."夜20MA",
            r."夜60MA",
            r."夜指標已計算"
        FROM futures_report_daily_raw r
        LEFT JOIN settlement_ranked s
          ON s.futures_id = r.futures_id
         AND s.日期 = r.日期
         AND s.rn = 1
        WHERE r.futures_id = ?
        ORDER BY r.日期
        """,
        (idx_key, idx_key),
    )

    if df is None or df.empty:
        print(f"[night_metrics] futures_id={idx_key}, 查無資料")
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"]).dt.normalize()
    numeric_columns = [
        "全日收", "夜盤開", "夜盤收", "夜盤高", "夜盤低", "夜成交量",
        "夜5MA", "夜10MA", "夜20MA", "夜60MA",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.sort_values("日期").reset_index(drop=True)
    previous_full_close = df["全日收"].shift(1)
    previous_night_volume = df["夜成交量"].shift(1)

    df["夜漲跌"] = df["夜盤收"] - previous_full_close
    df["夜價漲跌_pct"] = df["夜漲跌"] / previous_full_close.mask(
        previous_full_close == 0
    )

    def classify_price_volume(row):
        values = (
            row["夜盤收"], row["昨日全日收"],
            row["夜成交量"], row["昨日夜成交量"],
        )
        if any(pd.isna(value) for value in values):
            return None

        if row["夜盤收"] > row["昨日全日收"]:
            price_text = "價漲"
        elif row["夜盤收"] < row["昨日全日收"]:
            price_text = "價跌"
        else:
            price_text = "價同"

        if row["夜成交量"] > row["昨日夜成交量"]:
            volume_text = "量增"
        elif row["夜成交量"] < row["昨日夜成交量"]:
            volume_text = "量減"
        else:
            volume_text = "量同"

        return price_text + volume_text

    df["昨日全日收"] = previous_full_close
    df["昨日夜成交量"] = previous_night_volume
    df["夜價量結構"] = df.apply(classify_price_volume, axis=1)
    df["夜K線型態"] = df.apply(
        lambda row: FU._classify_k_type(
            row["夜盤開"], row["夜盤收"], row["夜盤高"], row["夜盤低"]
        ),
        axis=1,
    )

    for days in (5, 10, 20, 60):
        ma_column = f"夜{days}MA"
        df[f"夜{days}MA乖離率"] = (
            (df["夜盤收"] - df[ma_column]) / df[ma_column].mask(
                df[ma_column] == 0
            )
        )

    prior_full_close = df["全日收"].shift(1)
    prior_count = prior_full_close.rolling(window=19, min_periods=19).count()
    prior_sum = prior_full_close.rolling(window=19, min_periods=19).sum()
    prior_sum_sq = (prior_full_close ** 2).rolling(window=19, min_periods=19).sum()
    combined_sum = prior_sum + df["夜盤收"]
    estimated_ma20 = combined_sum / 20
    variance = (
        prior_sum_sq
        + (df["夜盤收"] ** 2)
        - (combined_sum ** 2) / 20
    ) / 19
    valid_bollinger = (prior_count == 19) & df["夜盤收"].notna()
    estimated_std = variance.clip(lower=0).pow(0.5).where(valid_bollinger)

    df["夜布林標準差"] = estimated_std
    df["夜布林上軌"] = estimated_ma20 + 2 * estimated_std
    df["夜布林下軌"] = estimated_ma20 - 2 * estimated_std

    initial_backfill_mask = ~df["夜指標已計算"].fillna(False).astype(bool)

    if target_dates is not None:
        target_set = {
            pd.Timestamp(value).normalize()
            for value in target_dates
        }
        update_mask = pd.Series(False, index=df.index)
        for row_index in df.index[df["日期"].isin(target_set)]:
            update_mask.loc[row_index:min(row_index + 19, len(df) - 1)] = True
        update_mask |= initial_backfill_mask
        update_df = df.loc[update_mask].copy()
    else:
        update_df = df.copy()

    if update_df.empty:
        print(f"[night_metrics] futures_id={idx_key}, 無受影響日期")
        return update_df

    def db_value(value, digits=None):
        if value is None or pd.isna(value):
            return None
        number = float(value)
        return round(number, digits) if digits is not None else number

    update_rows = []
    for _, row in update_df.iterrows():
        update_rows.append(
            (
                db_value(row["夜漲跌"], 2),
                db_value(row["夜價漲跌_pct"], 4),
                row["夜價量結構"],
                row["夜K線型態"],
                db_value(row["夜5MA乖離率"], 4),
                db_value(row["夜10MA乖離率"], 4),
                db_value(row["夜20MA乖離率"], 4),
                db_value(row["夜60MA乖離率"], 4),
                db_value(row["夜布林標準差"], 2),
                db_value(row["夜布林上軌"], 2),
                db_value(row["夜布林下軌"], 2),
                idx_key,
                row["日期"].strftime("%Y-%m-%d"),
            )
        )

    ok = db.execute_sql(
        """
        UPDATE futures_report_daily_raw
        SET
            "夜漲跌" = ?,
            "夜價漲跌_pct" = ?,
            "夜價量結構" = ?,
            "夜K線型態" = ?,
            "夜5MA乖離率" = ?,
            "夜10MA乖離率" = ?,
            "夜20MA乖離率" = ?,
            "夜60MA乖離率" = ?,
            "夜布林標準差" = ?,
            "夜布林上軌" = ?,
            "夜布林下軌" = ?,
            "夜指標已計算" = true
        WHERE futures_id = ?
          AND 日期 = ?
        """,
        update_rows,
    )
    if not ok:
        raise RuntimeError("夜盤衍生欄位寫入 futures_report_daily_raw 失敗")

    print(
        f"[night_metrics] futures_id={idx_key}, updated={len(update_rows)}, "
        f"range=({update_df['日期'].min().date()}~{update_df['日期'].max().date()})"
    )
    return update_df


def sync_report_csv_to_view(
    *,
    refresh_cache: bool = True,
    export_csv: bool = True,
    return_preview: bool = True,
):
    """依需求刷新期貨報表快取、匯出完整 CSV。

    注意：這兩件事都很慢，尤其完整 CSV 匯出與 materialized view refresh。
    Dashboard 每日更新不應呼叫這個函式；只有手動完整重建、回測前備份、
    或真的需要輸出 futures_report_daily.csv 時才使用。
    """
    from module.futures_turning import refresh_turning_levels
    refresh_turning_levels()
    if refresh_cache:
        from module.report_cache import refresh_futures_report_cache

        print("[報表快取] 開始刷新，Dashboard 期間仍可讀取上一版資料…")
        refresh_result = refresh_futures_report_cache(concurrently=True)
        print(f"[報表快取] {refresh_result.message}")
    else:
        print("[報表快取] 略過完整刷新。")

    if export_csv:
        columns_df = db.query_to_df(
            """
            SELECT attribute.attname AS column_name
            FROM pg_attribute attribute
            WHERE attribute.attrelid = 'report.v_futures_report_daily_corrected_turning'::regclass
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND attribute.attname NOT IN ('id', 'futures_id')
            ORDER BY attribute.attnum
            """
        )
        if columns_df.empty:
            raise RuntimeError("無法取得 v_futures_report_daily 欄位")

        def quote_identifier(value: str) -> str:
            return '"' + str(value).replace('"', '""') + '"'

        from common.report_columns import order_futures_report_columns

        ordered_columns = order_futures_report_columns(
            columns_df["column_name"].astype(str).tolist()
        )
        selected_columns = ", ".join(quote_identifier(value) for value in ordered_columns)
        export_query = (
            f"SELECT {selected_columns} "
            'FROM report.v_futures_report_daily_corrected_turning ORDER BY "日期"'
        )
        output_path = (
            "C:/Users/USER/Desktop/Han/學習/金融策略/Invest/"
            "output/futures_report_daily.csv"
        )
        started = perf_counter()
        db.copy_query_to_csv(export_query, output_path)
        elapsed = perf_counter() - started
        print(f"[CSV] 串流匯出完成：{output_path}，耗時 {elapsed:.2f} 秒")
    else:
        print("[CSV] 略過完整 futures_report_daily.csv 匯出。")

    if not return_preview:
        return {
            "ok": True,
            "refresh_cache": refresh_cache,
            "export_csv": export_csv,
        }

    return db.query_to_df(
        """
        SELECT *
        FROM report.v_futures_report_daily_corrected_turning
        ORDER BY "日期" DESC
        LIMIT 5
        """
    )


def sync_reopurt_csv_to_view(**kwargs):
    """相容既有拼字的別名，新程式請改用 sync_report_csv_to_view。"""
    return sync_report_csv_to_view(**kwargs)


def rebuild_futures_report_cache_and_csv():
    """手動完整刷新期貨報表快取並匯出完整 CSV。

    這個函式會重算整張 materialized view、再匯出 futures_report_daily.csv，
    可能耗時數分鐘；不要放在 Dashboard 每日更新。
    """
    return sync_report_csv_to_view(
        refresh_cache=True,
        export_csv=True,
        return_preview=True,
    )
    
    
def output_csv(
    df,
    csv_dir,
    drop_cols
) -> pd.DataFrame:
    
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
    
    if csv_dir == "" or csv_dir is None:
        csv_dir = "output.csv"
    
    df.to_csv(csv_dir, index=False, encoding="utf-8-sig")
    return df

def routine_output():
    where_condition = """
        AND target_table LIKE 'fm_%' 
        AND (target_table LIKE '%_futures_%' OR target_table LIKE '%_option_%')
    """
    db._limit_date_span(where_condition=where_condition)
    sDt = datetime(2024, 1, 1)
    eDt = utils._taipei_today()

    df = build_futures_report_daily_raw(
        start_date=sDt,
        end_date=eDt,
        futures_id="TX",
        recent_rebuild_days = 5
    )

# =========================================================
# 欄位回填函式
# =========================================================
def backfill_futures_report_daily_raw_volume(
    start_date=None,
    end_date=None,
    futures_id="TX",
    only_null=True,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw 的：
    - 日成交量
    - 夜成交量

    規則：
    - 日成交量：取當天 D 的 position session、近月/月合約 row 的 volume
    - 夜成交量：取下一交易日 N 的 after_market session、近月/月合約 row 的 volume，
      但寫回 D 這一天

    參數：
    - start_date, end_date:
        可指定只補某段日期；若都不給，則補整張 futures_report_daily_raw
    - futures_id:
        預設 TX
    - only_null:
        True  -> 只補 日成交量/夜成交量 任一為 NULL 的列
        False -> 指定區間全部重算覆蓋

    回傳：
    - 補完後的該區間 futures_report_daily_raw 資料
    """

    idx_key = str(futures_id)

    # =========================================================
    # 1. 決定要補哪些日期
    # =========================================================
    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append("(日成交量 IS NULL OR 夜成交量 IS NULL)")

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_futures_report_daily_raw_volume] 無需回填資料")
        if start_ts is None or end_ts is None:
            return db.query_to_df(
                """
                SELECT *
                FROM futures_report_daily_raw
                WHERE futures_id = ?
                ORDER BY 日期
                """,
                (idx_key,),
            )
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
              AND 日期 >= ?
              AND 日期 <= ?
            ORDER BY 日期
            """,
            (idx_key, FU._dstr(start_ts), FU._dstr(end_ts)),
        )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()

    print(
        f"[backfill_futures_report_daily_raw_volume] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({dates_to_fix[0].strftime('%Y-%m-%d')}~{dates_to_fix[-1].strftime('%Y-%m-%d')})"
    )

    # =========================================================
    # 2. 先補來源資料（避免缺資料）
    # =========================================================
    src_start = dates_to_fix[0]
    src_end = dates_to_fix[-1]

    # 夜盤只抓到報表截止日後的第一個交易日，避免重複延長查詢區間。
    night_source_end = FU._get_night_source_end(src_end, futures_id)
    fm.get_tw_futures_daily(
        futures_id=futures_id,
        start_date=src_start,
        end_date=night_source_end,
    )
    fm.get_tw_futures_final_settlement_price(
        futures_id=futures_id,
        start_date=src_start,
        end_date=src_end + pd.Timedelta(days=7),
    )

    # =========================================================
    # 3. 回填 volume
    # =========================================================
    update_rows = []

    for d in dates_to_fix:
        day_row = FU._get_month_contract_row(
            d,
            "position",
            FU._get_front_contract_month(d),
            futures_id,
        )

        if day_row is None:
            print(f"[skip] {idx_key} {FU._dstr(d)} day_row is None")
            continue

        next_d = FU._get_next_after_market_date(d, futures_id)

        night_row = None
        if next_d is not None:
            night_row = FU._get_month_contract_row(
                next_d,
                "after_market",
                FU._get_front_contract_month(next_d),
                futures_id,
            )

        day_vol = (
            float(day_row["volume"])
            if pd.notna(day_row["volume"])
            else None
        )

        night_vol = (
            float(night_row["volume"])
            if night_row is not None and pd.notna(night_row["volume"])
            else None
        )

        update_rows.append(
            (
                day_vol,
                night_vol,
                idx_key,
                FU._dstr(d),
            )
        )

    if not update_rows:
        print("[backfill_futures_report_daily_raw_volume] 沒有可更新列")
    else:
        db.execute_sql(
            """
            UPDATE futures_report_daily_raw
            SET
                日成交量 = ?,
                夜成交量 = ?
            WHERE futures_id = ?
              AND 日期 = ?
            """,
            update_rows,
        )
        print(f"[backfill_futures_report_daily_raw_volume] updated={len(update_rows)}")

    # =========================================================
    # 4. 回傳結果
    # =========================================================
    if start_ts is None or end_ts is None:
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 >= ?
          AND 日期 <= ?
        ORDER BY 日期
        """,
        (idx_key, FU._dstr(start_ts), FU._dstr(end_ts)),
    )

def backfill_futures_report_daily_raw_bollinger(
    start_date=None,
    end_date=None,
    futures_id="TX",
    only_null=True,
    window=20,
    std_mult=2.0,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw 的布林通道欄位：
    - 日布林標準差
    - 日布林上軌
    - 日布林下軌

    規則：
    - 以 日盤收 為基礎
    - 中軌 = window 日均線（這裡不寫回表，直接用 rolling mean）
    - 標準差 = window 日 rolling std
    - 上軌 = 中軌 + std_mult * 標準差
    - 下軌 = 中軌 - std_mult * 標準差

    參數：
    - start_date, end_date:
        可指定只補某段資料；若都不給，則補整張表
    - futures_id:
        預設 TX
    - only_null:
        True  -> 只補任一布林欄位為 NULL 的列
        False -> 指定區間全部重算覆蓋
    - window:
        布林通道視窗，預設 20
    - std_mult:
        標準差倍數，預設 2.0

    回傳：
    - 補完後的該 futures_id / 區間資料
    """

    idx_key = str(futures_id)

    if window <= 1:
        raise ValueError("window 必須大於 1")

    # =========================================================
    # 1. 找出目標日期
    # =========================================================
    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append(
            "("
            "日布林標準差 IS NULL OR "
            "日布林上軌 IS NULL OR "
            "日布林下軌 IS NULL"
            ")"
        )

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_futures_report_daily_raw_bollinger] 無需回填資料")
        if start_ts is None and end_ts is None:
            return db.query_to_df(
                """
                SELECT *
                FROM futures_report_daily_raw
                WHERE futures_id = ?
                ORDER BY 日期
                """,
                (idx_key,),
            )
        else:
            q_where = ["futures_id = ?"]
            q_params = [idx_key]
            if start_ts is not None:
                q_where.append("日期 >= ?")
                q_params.append(FU._dstr(start_ts))
            if end_ts is not None:
                q_where.append("日期 <= ?")
                q_params.append(FU._dstr(end_ts))

            return db.query_to_df(
                f"""
                SELECT *
                FROM futures_report_daily_raw
                WHERE {' AND '.join(q_where)}
                ORDER BY 日期
                """,
                tuple(q_params),
            )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()
    min_fix_date = min(dates_to_fix)
    max_fix_date = max(dates_to_fix)

    print(
        f"[backfill_futures_report_daily_raw_bollinger] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({FU._dstr(min_fix_date)}~{FU._dstr(max_fix_date)}), "
        f"only_null={only_null}, window={window}, std_mult={std_mult}"
    )

    # =========================================================
    # 2. 為了計算 rolling，往前多抓 window-1 天
    # =========================================================
    hist_start_df = db.query_to_df(
        """
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 < ?
        ORDER BY 日期 DESC
        LIMIT ?
        """,
        (idx_key, FU._dstr(min_fix_date), window - 1),
    )

    history_dates = []
    if hist_start_df is not None and not hist_start_df.empty:
        history_dates = pd.to_datetime(hist_start_df["日期"]).dt.normalize().tolist()

    calc_start = min(history_dates) if history_dates else min_fix_date
    calc_end = max_fix_date

    # =========================================================
    # 3. 讀取計算區間資料
    # =========================================================
    calc_df = db.query_to_df(
        """
        SELECT 日期, 日盤收
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 >= ?
          AND 日期 <= ?
        ORDER BY 日期
        """,
        (idx_key, FU._dstr(calc_start), FU._dstr(calc_end)),
    )

    if calc_df is None or calc_df.empty:
        print("[backfill_futures_report_daily_raw_bollinger] 查無可計算資料")
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    calc_df["日期"] = pd.to_datetime(calc_df["日期"])
    calc_df = calc_df.sort_values("日期").copy()

    # =========================================================
    # 4. 計算布林通道
    # =========================================================
    calc_df["bb_ma"] = calc_df["日盤收"].rolling(window=window, min_periods=window).mean()
    calc_df["bb_std"] = calc_df["日盤收"].rolling(window=window, min_periods=window).std()
    calc_df["bb_upper"] = calc_df["bb_ma"] + std_mult * calc_df["bb_std"]
    calc_df["bb_lower"] = calc_df["bb_ma"] - std_mult * calc_df["bb_std"]

    fix_date_set = {pd.Timestamp(x).normalize() for x in dates_to_fix}
    update_rows = []

    for _, row in calc_df.iterrows():
        d = pd.Timestamp(row["日期"]).normalize()
        if d not in fix_date_set:
            continue

        update_rows.append(
            (
                float(row["bb_std"]) if pd.notna(row["bb_std"]) else None,
                float(row["bb_upper"]) if pd.notna(row["bb_upper"]) else None,
                float(row["bb_lower"]) if pd.notna(row["bb_lower"]) else None,
                idx_key,
                d.strftime("%Y-%m-%d"),
            )
        )

    # =========================================================
    # 5. 寫回 DB
    # =========================================================
    if not update_rows:
        print("[backfill_futures_report_daily_raw_bollinger] 沒有可更新列")
    else:
        db.execute_sql(
            """
            UPDATE futures_report_daily_raw
            SET
                日布林標準差 = ?,
                日布林上軌 = ?,
                日布林下軌 = ?
            WHERE futures_id = ?
              AND 日期 = ?
            """,
            update_rows,
        )
        print(f"[backfill_futures_report_daily_raw_bollinger] updated={len(update_rows)}")

    # =========================================================
    # 6. 回傳結果
    # =========================================================
    result_where = ["futures_id = ?"]
    result_params = [idx_key]

    if start_ts is not None:
        result_where.append("日期 >= ?")
        result_params.append(FU._dstr(start_ts))

    if end_ts is not None:
        result_where.append("日期 <= ?")
        result_params.append(FU._dstr(end_ts))

    return db.query_to_df(
        f"""
        SELECT *
        FROM futures_report_daily_raw
        WHERE {' AND '.join(result_where)}
        ORDER BY 日期
        """,
        tuple(result_params),
    )

def backfill_futures_report_daily_raw_pv(
    start_date=None,
    end_date=None,
    futures_id="TX",
    only_null=True,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw 的：
    - 日當日價量
    - 日5日價量
    - 日10日價量
    - 日20日價量
    - 日60日價量

    規則：
    - 使用 FU._recalc_futures_report_daily_raw_pv 的邏輯
    - 會自動往前抓足夠資料（60日 window）

    參數：
    - only_null:
        True  -> 只補 NULL
        False -> 全覆蓋

    """

    idx_key = str(futures_id)

    # =========================================================
    # 1. 找目標列
    # =========================================================
    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append("""
        (
            日當日價量 IS NULL OR
            日5日價量 IS NULL OR
            日10日價量 IS NULL OR
            日20日價量 IS NULL OR
            日60日價量 IS NULL
        )
        """)

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_pv] 無需回填")
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()

    print(
        f"[backfill_pv] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({dates_to_fix[0].strftime('%Y-%m-%d')}~{dates_to_fix[-1].strftime('%Y-%m-%d')})"
    )

    # =========================================================
    # 2. 往前補資料（為了 rolling）
    # =========================================================
    hist_df = db.query_to_df(
        """
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 < ?
        ORDER BY 日期 DESC
        LIMIT 60
        """,
        (idx_key, FU._dstr(dates_to_fix[0])),
    )

    history_dates = []
    if hist_df is not None and not hist_df.empty:
        history_dates = pd.to_datetime(hist_df["日期"]).dt.normalize().tolist()

    calc_start = min(history_dates) if history_dates else dates_to_fix[0]
    calc_end = dates_to_fix[-1]

    # =========================================================
    # 3. 抓資料（整段）
    # =========================================================
    df = db.query_to_df(
        """
        SELECT
            id,
            日期,
            日盤收,
            日成交量
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 >= ?
          AND 日期 <= ?
        ORDER BY 日期
        """,
        (idx_key, FU._dstr(calc_start), FU._dstr(calc_end)),
    )

    if df is None or df.empty:
        print("[backfill_pv] 無資料")
        return

    df["日期"] = pd.to_datetime(df["日期"])
    df = df.sort_values("日期").copy()

    # =========================================================
    # 4. 套用你原本計算邏輯
    # =========================================================
    df["日盤收"] = pd.to_numeric(df["日盤收"], errors="coerce")
    df["日成交量"] = pd.to_numeric(df["日成交量"], errors="coerce")

    df["昨收"] = df["日盤收"].shift(1)
    df["昨量"] = df["日成交量"].shift(1)

    df["漲跌幅_pct"] = (df["日盤收"] - df["昨收"]) / df["昨收"].replace(0, pd.NA)
    df["量增率_pct"] = (df["日成交量"] - df["昨量"]) / df["昨量"].replace(0, pd.NA)

    # === 當日 ===
    df["5MA"] = df["日盤收"].rolling(5).mean()
    df["當日乖離"] = (df["日盤收"] - df["5MA"]) / df["5MA"].replace(0, pd.NA)
    df["當日行情"] = df["當日乖離"].apply(FU._trend_regime)

    df["日當日價量"] = [
        FU._pv_div_by_regime(r, p, v)
        for r, p, v in zip(df["當日行情"], df["漲跌幅_pct"], df["量增率_pct"])
    ]

    # === N日 ===
    for n in [5, 10, 20, 60]:
        df[f"{n}日均量"] = df["日成交量"].shift(1).rolling(n).mean()
        df[f"{n}MA"] = df["日盤收"].shift(1).rolling(n).mean()

        df[f"{n}日乖離"] = (df["日盤收"] - df[f"{n}MA"]) / df[f"{n}MA"].replace(0, pd.NA)

        df[f"{n}日量增率_pct"] = (
            (df["日成交量"] - df[f"{n}日均量"]) / df[f"{n}日均量"].replace(0, pd.NA)
        )

        df[f"{n}日行情"] = df[f"{n}日乖離"].apply(FU._trend_regime)

        df[f"日{n}日價量"] = [
            FU._pv_div_by_regime(r, p, v)
            for r, p, v in zip(
                df[f"{n}日行情"],
                df[f"{n}日乖離"],
                df[f"{n}日量增率_pct"],
            )
        ]

    # =========================================================
    # 5. 只更新 target 範圍
    # =========================================================
    fix_set = {pd.Timestamp(x).normalize() for x in dates_to_fix}

    update_rows = []
    for _, row in df.iterrows():
        d = pd.Timestamp(row["日期"]).normalize()
        if d not in fix_set:
            continue

        update_rows.append(
            (
                row["日當日價量"],
                row["日5日價量"],
                row["日10日價量"],
                row["日20日價量"],
                row["日60日價量"],
                idx_key,
                d.strftime("%Y-%m-%d"),
            )
        )

    if not update_rows:
        print("[backfill_pv] 無更新")
    else:
        db.execute_sql(
            """
            UPDATE futures_report_daily_raw
            SET
                日當日價量 = ?,
                日5日價量 = ?,
                日10日價量 = ?,
                日20日價量 = ?,
                日60日價量 = ?
            WHERE futures_id = ?
              AND 日期 = ?
            """,
            update_rows,
        )
        print(f"[backfill_pv] updated={len(update_rows)}")

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (idx_key,),
    )

def backfill_futures_report_daily_raw_market_prices(
    start_date=None,
    end_date=None,
    futures_id="TX",
    only_null=True,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw 的：
    - 加權收盤價
    - 大台收盤價
    - 大台結算價
    """

    idx_key = str(futures_id)

    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append("""
        (
            加權收盤價 IS NULL OR
            大台收盤價 IS NULL OR
            大台結算價 IS NULL
        )
        """)

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_market_prices] 無需回填")
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()

    min_d = min(dates_to_fix)
    max_d = max(dates_to_fix)

    print(
        f"[backfill_market_prices] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({FU._dstr(min_d)}~{FU._dstr(max_d)}), "
        f"only_null={only_null}"
    )

    weighted_close_map, tx_close_map, tx_settlement_map = FU._build_market_price_maps(
        start_ts=min_d,
        end_ts=max_d,
        futures_id=futures_id,
    )

    update_rows = []

    for d in dates_to_fix:
        d_str = FU._dstr(d)

        update_rows.append(
            (
                weighted_close_map.get(d_str),
                tx_close_map.get(d_str),
                tx_settlement_map.get(d_str),
                idx_key,
                d_str,
            )
        )

    if update_rows:
        db.execute_sql(
            """
            UPDATE futures_report_daily_raw
            SET
                加權收盤價 = ?,
                大台收盤價 = ?,
                大台結算價 = ?
            WHERE futures_id = ?
              AND 日期 = ?
            """,
            update_rows,
        )

        print(f"[backfill_market_prices] updated={len(update_rows)}")
    else:
        print("[backfill_market_prices] 無更新資料")

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (idx_key,),
    )

def backfill_futures_report_daily_raw_month_option(
    start_date=None,
    end_date=None,
    futures_id="TX",
    option_id="TXO",
    only_null=True,
    force_source_refresh=False,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw：
    - call_月選
    - put_月選

    規則：
    - 來源資料：fm_taiwan_option_daily
    - trading_session 固定 position
    - call_月選 使用 call
    - put_月選 使用 put
    - contract_date：
        第三個週三之前：當月 yyyymm
        第三個週三含當天之後：下一個月 yyyymm
    """

    idx_key = str(futures_id)

    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append(
            """
            (
                call_月選 IS NULL OR
                put_月選 IS NULL
            )
            """
        )

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_month_option] 無需回填")
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()

    print(
        f"[backfill_month_option] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({FU._dstr(dates_to_fix[0])}~{FU._dstr(dates_to_fix[-1])}), "
        f"only_null={only_null}"
    )

    # 如果你有這個 FinMind wrapper，就打開這段
    # 沒有的話先註解掉，因為你的 DB 已經有 fm_taiwan_option_daily
    if hasattr(fm, "get_tw_option_daily"):
        fm.get_tw_option_daily(
            option_id=option_id,
            start_date=dates_to_fix[0],
            end_date=dates_to_fix[-1],
            force_refresh=force_source_refresh,
        )

    FU._recalc_futures_report_daily_raw_month_option(
        futures_id=futures_id,
        option_id=option_id,
        target_dates=dates_to_fix,
    )

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (idx_key,),
    )

def backfill_futures_report_daily_raw_weekly_wed_option(
    start_date=None,
    end_date=None,
    futures_id="TX",
    option_id="TXO",
    only_null=True,
    force_source_refresh=False,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw：
    - call_週三
    - put_週三

    規則：
    - 來源資料：fm_taiwan_option_daily
    - trading_session 固定 position
    - call_週三 使用 call
    - put_週三 使用 put
    - contract_date：
        從當天所有 LIKE '%W%' 的合約中，
        找第一個「週三到期日 > 交易日」的 contract_date
    - 計算基準：
        加權收盤價
        大台結算價
    """

    idx_key = str(futures_id)

    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append(
            """
            (
                call_週三 IS NULL OR
                put_週三 IS NULL
            )
            """
        )

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_weekly_wed_option] 無需回填")
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()

    print(
        f"[backfill_weekly_wed_option] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({FU._dstr(dates_to_fix[0])}~{FU._dstr(dates_to_fix[-1])}), "
        f"only_null={only_null}"
    )

    if hasattr(fm, "get_tw_option_daily"):
        fm.get_tw_option_daily(
            option_id=option_id,
            start_date=dates_to_fix[0],
            end_date=dates_to_fix[-1],
            force_refresh=force_source_refresh,
        )

    FU._recalc_futures_report_daily_raw_weekly_wed_option(
        futures_id=futures_id,
        option_id=option_id,
        target_dates=dates_to_fix,
    )

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (idx_key,),
    )

def backfill_futures_report_daily_raw_weekly_friday_option(
    start_date=None,
    end_date=None,
    futures_id="TX",
    option_id="TXO",
    only_null=True,
    force_source_refresh=False,
) -> pd.DataFrame:
    """
    回填 futures_report_daily_raw：
    - call_週五
    - put_週五
    """

    idx_key = str(futures_id)

    where_sql = ["futures_id = ?"]
    params = [idx_key]

    if start_date is not None:
        start_ts = FU._to_ts(start_date)
        where_sql.append("日期 >= ?")
        params.append(FU._dstr(start_ts))
    else:
        start_ts = None

    if end_date is not None:
        end_ts = FU._to_ts(end_date)
        where_sql.append("日期 <= ?")
        params.append(FU._dstr(end_ts))
    else:
        end_ts = None

    if only_null:
        where_sql.append(
            """
            (
                call_週五 IS NULL OR
                put_週五 IS NULL
            )
            """
        )

    target_df = db.query_to_df(
        f"""
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE {' AND '.join(where_sql)}
        ORDER BY 日期
        """,
        tuple(params),
    )

    if target_df is None or target_df.empty:
        print("[backfill_weekly_friday_option] 無需回填")
        return db.query_to_df(
            """
            SELECT *
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (idx_key,),
        )

    dates_to_fix = pd.to_datetime(target_df["日期"]).dt.normalize().tolist()

    print(
        f"[backfill_weekly_friday_option] futures_id={idx_key}, "
        f"count={len(dates_to_fix)}, "
        f"range=({FU._dstr(dates_to_fix[0])}~{FU._dstr(dates_to_fix[-1])}), "
        f"only_null={only_null}"
    )

    if hasattr(fm, "get_tw_option_daily"):
        fm.get_tw_option_daily(
            option_id=option_id,
            start_date=dates_to_fix[0],
            end_date=dates_to_fix[-1],
            force_refresh=force_source_refresh,
        )

    FU._recalc_futures_report_daily_raw_weekly_friday_option(
        futures_id=futures_id,
        option_id=option_id,
        target_dates=dates_to_fix,
    )

    return db.query_to_df(
        """
        SELECT *
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (idx_key,),
    )


# 期貨 小台、微台多空比 => 用view算的，補資料就好
def backfill_MTX_TMF_retail_long_short_ratio(
    start_date:datetime,
    end_date:datetime=None
):
    if end_date == None:
        end_date = datetime.now()
    
    fm.get_tw_futures_daily(
        futures_id=["MTX", "TMF"],
        start_date=start_date,
        end_date=end_date,
    )
    
    fm.get_tw_futures_institutional_investors(
        futures_id=["MTX", "TMF"],
        start_date=start_date,
        end_date=end_date,
    )


def run_report_pipeline(
    recent_rebuild_days: int = 7,
    *,
    refresh_cache: bool = False,
    export_csv: bool = False,
    sync_intraday: bool = True,
):
    """更新台指期日報表及其必要的夜盤來源。

    Dashboard 日常快速更新模式：
    - 月選只補空值，不覆蓋歷史資料。
    - 週三選 / 週五選只重算最近 recent_rebuild_days 天。
    - 預設不刷新整張歷史物化快取，也不匯出完整 CSV。
    - 正式 View 採物化歷史＋小型尾端快取，新交易日不依賴全量刷新。

    若修改週選邏輯或需要正式回測前重新統一歷史資料，請另外執行
    rebuild_futures_weekly_option_history()。
    若還需要輸出完整 CSV，請手動執行 rebuild_futures_report_cache_and_csv()。
    """
    # 一次性把正式 View 改為「物化歷史＋小型尾端快取」。
    from module.report_cache import (
        ensure_futures_report_live_view,
        refresh_futures_dashboard_cache,
        refresh_futures_report_tail_cache,
    )
    from module.core_strategy import ensure_core_strategy_rule

    core_rule_changed = ensure_core_strategy_rule()
    if core_rule_changed:
        print("[期貨報表] 核心策略新判定已套用。")

    if ensure_futures_report_live_view():
        print("[期貨報表] 正式 View 已切換為歷史＋尾端快取。")

    # 轉折欄位依賴完整夜盤一分 K。必須先補「交易日晚間＋隔日凌晨」
    # Tick 並重建 K 棒，再建立日報表及刷新 Dashboard 快取；舊順序在
    # 週一會先把週五空值寫進快取，直到第二次執行才可能補上。
    if sync_intraday:
        from main.intraday_pipeline import update_tx_kbar_1m

        print("[期貨報表] 先同步夜盤 Tick／一分 K（含跨午夜檔）…")
        update_tx_kbar_1m(utils._taipei_today(), refresh_turning=False)

    routine_output()

    taipei_today = utils._taipei_today()
    today = taipei_today.strftime("%Y-%m-%d")
    recent_start = (taipei_today - timedelta(days=recent_rebuild_days)).strftime("%Y-%m-%d")

    backfill_futures_report_daily_raw_month_option(
        start_date=recent_start,
        end_date=today,
        futures_id="TX",
        option_id="TXO",
        only_null=True,
        force_source_refresh=True,
    )

    backfill_futures_report_daily_raw_weekly_wed_option(
        start_date=recent_start,
        end_date=today,
        futures_id="TX",
        option_id="TXO",
        only_null=False,
        force_source_refresh=True,
    )

    backfill_futures_report_daily_raw_weekly_friday_option(
        start_date=recent_start,
        end_date=today,
        futures_id="TX",
        option_id="TXO",
        only_null=False,
        force_source_refresh=True,
    )

    from module.futures_turning import refresh_turning_levels
    turning_sessions = refresh_turning_levels()
    print(f"[期貨報表] 5分K轉折更新：{turning_sessions} 個時段")
    tail_rows = refresh_futures_report_tail_cache()
    print(f"[期貨報表] 尾端快取刷新完成：{tail_rows} 筆。")

    dashboard_rows = refresh_futures_dashboard_cache(
        recent_days=14,
        force_full=core_rule_changed,
    )
    print(f"[期貨報表] Dashboard 快取刷新完成：{dashboard_rows} 筆。")

    from module.futures_turning import ensure_turning_views
    ensure_turning_views()
    audit_latest_completed_night()
    if refresh_cache or export_csv:
        return sync_report_csv_to_view(
            refresh_cache=refresh_cache,
            export_csv=export_csv,
            return_preview=True,
        )

    print("[期貨報表] 快速更新完成；已略過完整報表快取刷新與 CSV 匯出。")
    return {
        "ok": True,
        "mode": "fast",
        "recent_rebuild_days": recent_rebuild_days,
        "refresh_cache": False,
        "export_csv": False,
    }


def audit_latest_completed_night(futures_id: str = "TX") -> dict:
    """稽核最近已完成交易日的跨午夜來源，禁止靜默留下空白法人欄位。"""
    today = utils._taipei_today()
    report_date = db.query_single_value(
        """
        SELECT MAX(日期)
          FROM futures_report_daily_raw
         WHERE futures_id = ? AND 日期 < ?
        """,
        (futures_id, FU._dstr(today)),
    )
    if report_date is None:
        return {"status": "no_completed_report_date"}

    report_date = pd.Timestamp(report_date).normalize()
    price_source_date = FU._get_next_after_market_date(report_date, futures_id)
    institutional_source_date = report_date
    source_rows = int(
        db.query_single_value(
            """
            SELECT COUNT(*)
              FROM fm_taiwan_futures_institutional_investors_after_hours
             WHERE futures_id = ? AND date = ?
            """,
            (futures_id, FU._dstr(institutional_source_date)),
        )
        or 0
    )

    values = db.query_to_df(
        """
        SELECT r.夜法人多單量, r.夜法人空單量,
               t."夜盤轉折支撐", t."夜盤轉折壓力"
          FROM futures_report_daily_raw r
          LEFT JOIN report.futures_turning_levels t
            ON t.futures_id = r.futures_id AND t."日期" = r.日期
         WHERE r.futures_id = ? AND r.日期 = ?
        """,
        (futures_id, FU._dstr(report_date)),
    )
    row = values.iloc[0] if not values.empty else pd.Series(dtype=object)
    result = {
        "report_date": FU._dstr(report_date),
        "price_source_date": FU._dstr(price_source_date) if price_source_date is not None else None,
        "institutional_source_date": FU._dstr(institutional_source_date),
        "institutional_source_rows": source_rows,
        "夜法人多單量": row.get("夜法人多單量"),
        "夜法人空單量": row.get("夜法人空單量"),
        "夜盤轉折支撐": row.get("夜盤轉折支撐"),
        "夜盤轉折壓力": row.get("夜盤轉折壓力"),
    }
    print(f"[跨日稽核] {result}")

    required_fields = (
        "夜法人多單量",
        "夜法人空單量",
        "夜盤轉折支撐",
        "夜盤轉折壓力",
    )
    missing_fields = [name for name in required_fields if pd.isna(result[name])]
    if source_rows == 0:
        raise SourceDataPendingError(
            "最新夜盤法人資料尚未發布，請稍後再更新。"
        )
    if missing_fields:
        raise RuntimeError(
            "跨日稽核失敗：最近完成交易日的必要夜盤欄位仍不完整；"
            f"報表日={result['report_date']}、行情來源日={result['price_source_date']}、"
            f"法人來源日={result['institutional_source_date']}、"
            f"法人來源筆數={source_rows}、缺少欄位={missing_fields}。"
            "請勿使用這次報表做盤前規劃。"
        )
    return result


def rebuild_futures_weekly_option_history():
    """手動完整重建週選歷史資料。

    只在以下情況使用：
    - 修改週三選 / 週五選計算邏輯
    - 發現歷史週選資料錯誤或缺漏
    - 正式回測前需要全部歷史資料統一邏輯
    """
    today = datetime.now().strftime("%Y-%m-%d")

    backfill_futures_report_daily_raw_weekly_wed_option(
        start_date="2026-05-05",
        end_date=today,
        futures_id="TX",
        option_id="TXO",
        only_null=False,
    )

    friday_first_df = db.query_to_df(
        """
        SELECT MIN(date) AS first_date
        FROM fm_taiwan_option_daily
        WHERE option_id = 'TXO'
          AND trading_session = 'position'
          AND contract_date LIKE '%F%'
        """
    )
    friday_first_date = None
    if friday_first_df is not None and not friday_first_df.empty:
        first_value = friday_first_df.loc[0, "first_date"]
        if pd.notna(first_value):
            friday_first_date = FU._dstr(first_value)

    if friday_first_date is None:
        print("[weekly_friday_option] 來源表查無 F 合約，略過歷史重建")
    else:
        backfill_futures_report_daily_raw_weekly_friday_option(
            start_date=friday_first_date,
            end_date=today,
            futures_id="TX",
            option_id="TXO",
            only_null=False,
        )


def repair_futures_report_missing_fields(
    start_date="2024-01-01",
    end_date=None,
    futures_id="TX",
    *,
    refresh_cache=True,
):
    """重新抓取並回填期貨報表常見缺漏欄位。

    會處理法人總買超、月選、週三選與週五選。週五選的適用起日會從
    fm_taiwan_option_daily 實際存在的第一筆 F 合約推導，不使用固定日期。
    個別日期的來源重新抓取後仍不存在時保留 NULL，不製造假資料。
    refresh_cache=True 會在修補後刷新一次正式報表快取，讓 Dashboard
    立即讀到修補結果；這是維護操作，不放進每日快速流程。
    """
    idx_key = str(futures_id)
    start_ts = FU._to_ts(start_date)
    end_ts = FU._to_ts(end_date if end_date is not None else datetime.now())
    if start_ts > end_ts:
        raise ValueError("start_date 不可大於 end_date")

    gaps = db.query_to_df(
        """
        WITH friday_option_range AS (
            SELECT MIN(date) AS first_date
            FROM fm_taiwan_option_daily
            WHERE option_id = 'TXO'
              AND trading_session = 'position'
              AND contract_date LIKE '%F%'
        )
        SELECT
            v.日期,
            (
                SELECT COUNT(*)
                FROM fm_taiwan_stock_institutional_total src
                WHERE src.date = v.日期
            ) AS 來源法人筆數,
            (
                SELECT COUNT(*)
                FROM fm_taiwan_option_daily src
                WHERE src.option_id = 'TXO' AND src.date = v.日期
            ) AS 來源選擇權筆數,
            (
                SELECT COUNT(*)
                FROM fm_taiwan_option_daily src
                WHERE src.option_id = 'TXO'
                  AND src.date = v.日期
                  AND src.trading_session = 'position'
                  AND src.contract_date LIKE '%F%'
            ) AS 來源週五選筆數,
            (v.法人總買超_億 IS NULL) AS 缺法人總買超,
            (
                v.call_月選 IS NULL OR v.put_月選 IS NULL
                OR v.買賣權比_月選 IS NULL
            ) AS 缺月選,
            (
                v.call_週三 IS NULL OR v.put_週三 IS NULL
                OR v.買賣權比_週三 IS NULL
            ) AS 缺週三選,
            (
                v.日期 >= friday_option_range.first_date
                AND (
                    v.call_週五 IS NULL OR v.put_週五 IS NULL
                    OR v.買賣權比_週五 IS NULL
                )
            ) AS 缺週五選
        FROM report.v_futures_report_daily_corrected v
        CROSS JOIN friday_option_range
        WHERE v.futures_id = ?
          AND v.日期 BETWEEN ? AND ?
          AND (
              v.法人總買超_億 IS NULL
              OR v.call_月選 IS NULL OR v.put_月選 IS NULL
              OR v.買賣權比_月選 IS NULL
              OR v.call_週三 IS NULL OR v.put_週三 IS NULL
              OR v.買賣權比_週三 IS NULL
              OR (
                  v.日期 >= friday_option_range.first_date
                  AND (
                      v.call_週五 IS NULL OR v.put_週五 IS NULL
                      OR v.買賣權比_週五 IS NULL
                  )
              )
          )
        ORDER BY v.日期
        """,
        (idx_key, FU._dstr(start_ts), FU._dstr(end_ts)),
    )

    if gaps is None or gaps.empty:
        print("[repair_report_gaps] 指定區間沒有常見缺漏欄位")
        repaired_dates = []
    else:
        gaps["日期"] = pd.to_datetime(gaps["日期"]).dt.normalize()
        repaired_dates = gaps["日期"].tolist()
        print("[repair_report_gaps] 修補前缺漏日期與類型：")
        print(gaps.to_string(index=False))

        def grouped_ranges(values):
            ordered = sorted({pd.Timestamp(value).normalize() for value in values})
            if not ordered:
                return []
            groups = []
            group_start = group_end = ordered[0]
            for current in ordered[1:]:
                if current <= group_end + pd.Timedelta(days=1):
                    group_end = current
                else:
                    groups.append((group_start, group_end))
                    group_start = group_end = current
            groups.append((group_start, group_end))
            return groups

        # 選擇權金額需要加權收盤價與大台收盤／結算價作為履約價篩選基準。
        # 先強制補這些基準來源，再回寫 futures_report_daily_raw。
        for range_start, range_end in grouped_ranges(repaired_dates):
            print(
                f"[repair_report_gaps] 強制重抓計算基準："
                f"{FU._dstr(range_start)}~{FU._dstr(range_end)}"
            )
            fm.get_tw_stock_daily_price(
                stock_id="TAIEX",
                start_date=range_start,
                end_date=range_end,
                force_refresh=True,
            )
            fm.get_tw_futures_daily(
                futures_id=idx_key,
                start_date=range_start,
                end_date=range_end,
                force_refresh=True,
            )
            backfill_futures_report_daily_raw_market_prices(
                start_date=range_start,
                end_date=range_end,
                futures_id=idx_key,
                only_null=True,
            )

        legal_dates = gaps.loc[gaps["缺法人總買超"].astype(bool), "日期"]
        if not legal_dates.empty:
            from main.stock_report_taiex import export as export_taiex_report

            for range_start, range_end in grouped_ranges(legal_dates):
                print(
                    f"[repair_report_gaps] 強制重抓加權法人資料："
                    f"{FU._dstr(range_start)}~{FU._dstr(range_end)}"
                )
                fm.get_tw_institutional_total(
                    start_date=range_start,
                    end_date=range_end,
                    force_refresh=True,
                )
                export_taiex_report(
                    "TAIEX",
                    range_start - pd.Timedelta(days=10),
                    range_end,
                )

        option_mask = (
            gaps["缺月選"].astype(bool)
            | gaps["缺週三選"].astype(bool)
            | gaps["缺週五選"].astype(bool)
        )
        option_dates = gaps.loc[option_mask, "日期"]
        if not option_dates.empty:
            for range_start, range_end in grouped_ranges(option_dates):
                print(
                    f"[repair_report_gaps] 強制重抓 TXO："
                    f"{FU._dstr(range_start)}~{FU._dstr(range_end)}"
                )
                fm.get_tw_option_daily(
                    option_id="TXO",
                    start_date=range_start,
                    end_date=range_end,
                    force_refresh=True,
                )

        month_dates = gaps.loc[gaps["缺月選"].astype(bool), "日期"].tolist()
        wed_dates = gaps.loc[gaps["缺週三選"].astype(bool), "日期"].tolist()
        friday_dates = gaps.loc[gaps["缺週五選"].astype(bool), "日期"].tolist()
        if month_dates:
            FU._recalc_futures_report_daily_raw_month_option(
                futures_id=idx_key, option_id="TXO", target_dates=month_dates
            )
        if wed_dates:
            FU._recalc_futures_report_daily_raw_weekly_wed_option(
                futures_id=idx_key, option_id="TXO", target_dates=wed_dates
            )
        if friday_dates:
            FU._recalc_futures_report_daily_raw_weekly_friday_option(
                futures_id=idx_key, option_id="TXO", target_dates=friday_dates
            )

    successfully_changed_dates = []
    if repaired_dates:
        date_sql = ", ".join(
            f"DATE '{pd.Timestamp(value).strftime('%Y-%m-%d')}'"
            for value in sorted(set(repaired_dates))
        )
        source_after = db.query_to_df(
            f"""
            SELECT
                r.日期,
                s.法人總買超_億,
                r.call_月選, r.put_月選,
                r.call_週三, r.put_週三,
                r.call_週五, r.put_週五
            FROM futures_report_daily_raw r
            LEFT JOIN stock_report_daily s
              ON s.股票代號 = 'TAIEX' AND s.日期 = r.日期
            WHERE r.futures_id = ? AND r.日期 IN ({date_sql})
            """,
            (idx_key,),
        )
        source_after["日期"] = pd.to_datetime(source_after["日期"]).dt.normalize()
        after_by_date = source_after.set_index("日期")
        for _, gap_row in gaps.iterrows():
            date_value = gap_row["日期"]
            if date_value not in after_by_date.index:
                continue
            after = after_by_date.loc[date_value]
            repaired_groups = []
            if bool(gap_row["缺法人總買超"]):
                repaired_groups.append(pd.notna(after["法人總買超_億"]))
            if bool(gap_row["缺月選"]):
                repaired_groups.append(
                    pd.notna(after["call_月選"])
                    and pd.notna(after["put_月選"])
                    and float(after["put_月選"]) != 0
                )
            if bool(gap_row["缺週三選"]):
                repaired_groups.append(
                    pd.notna(after["call_週三"])
                    and pd.notna(after["put_週三"])
                    and float(after["put_週三"]) != 0
                )
            if bool(gap_row["缺週五選"]):
                repaired_groups.append(
                    pd.notna(after["call_週五"])
                    and pd.notna(after["put_週五"])
                    and float(after["put_週五"]) != 0
                )
            if any(repaired_groups):
                successfully_changed_dates.append(date_value)

    if refresh_cache and successfully_changed_dates:
        print("[repair_report_gaps] 刷新正式快取與缺漏日期的 Dashboard 快取…")
        from module.report_cache import (
            refresh_futures_dashboard_cache_dates,
            refresh_futures_report_cache,
            refresh_futures_report_tail_cache,
        )

        result = refresh_futures_report_cache(concurrently=True)
        print(f"[repair_report_gaps] {result.message}")
        refresh_futures_report_tail_cache()
        dashboard_count = refresh_futures_dashboard_cache_dates(successfully_changed_dates)
        print(f"[repair_report_gaps] Dashboard 指定日期刷新完成：{dashboard_count} 日")
    elif refresh_cache and repaired_dates:
        print(
            "[repair_report_gaps] API 重新抓取後仍沒有可回填的來源值；"
            "略過耗時的正式快取全量刷新。"
        )

    if not repaired_dates:
        return pd.DataFrame(
            columns=[
                "日期", "法人總買超_億",
                "call_月選", "put_月選", "買賣權比_月選",
                "call_週三", "put_週三", "買賣權比_週三",
                "call_週五", "put_週五", "買賣權比_週五", "修補狀態",
            ]
        )

    repaired_date_sql = ", ".join(
        f"DATE '{pd.Timestamp(value).strftime('%Y-%m-%d')}'"
        for value in sorted(set(repaired_dates))
    )
    result = db.query_to_df(
        f"""
        SELECT
            日期,
            法人總買超_億,
            call_月選, put_月選, 買賣權比_月選,
            call_週三, put_週三, 買賣權比_週三,
            call_週五, put_週五, 買賣權比_週五,
            CASE
                WHEN 法人總買超_億 IS NULL
                  OR call_月選 IS NULL OR put_月選 IS NULL OR 買賣權比_月選 IS NULL
                  OR call_週三 IS NULL OR put_週三 IS NULL OR 買賣權比_週三 IS NULL
                THEN '仍有缺漏（API 來源可能未提供）'
                WHEN 日期 >= (
                    SELECT MIN(date)
                    FROM fm_taiwan_option_daily
                    WHERE option_id = 'TXO'
                      AND trading_session = 'position'
                      AND contract_date LIKE '%F%'
                )
                  AND (call_週五 IS NULL OR put_週五 IS NULL OR 買賣權比_週五 IS NULL)
                THEN '仍有缺漏（週五選來源可能未提供）'
                WHEN 日期 < (
                    SELECT MIN(date)
                    FROM fm_taiwan_option_daily
                    WHERE option_id = 'TXO'
                      AND trading_session = 'position'
                      AND contract_date LIKE '%F%'
                )
                  AND (call_週五 IS NULL OR put_週五 IS NULL OR 買賣權比_週五 IS NULL)
                THEN '已補齊（週五選尚未上線，不適用）'
                ELSE '已補齊'
            END AS 修補狀態
        FROM report.v_futures_report_daily_corrected
        WHERE futures_id = ?
          AND 日期 IN ({repaired_date_sql})
        ORDER BY 日期
        """,
        (idx_key,),
    )
    print("[repair_report_gaps] 修補後驗證：")
    print(result.to_string(index=False))
    return result


def run_daily_pipeline():
    """執行台指期日報表、Tick 與一分 K 完整流程。"""
    return run_report_pipeline(sync_intraday=True)


# =========================================================
# main
# =========================================================
# python -m main.futures
if __name__ == "__main__":
    run_daily_pipeline()
