from datetime import datetime
import pandas as pd
from common import db
from module import finMind as fm

# =========================================================
# 共用工具
# =========================================================
def _to_ts(x) -> pd.Timestamp:
    return pd.Timestamp(x).normalize()


def _dstr(x) -> str:
    return _to_ts(x).strftime("%Y-%m-%d")


def _weekday_text(ts: pd.Timestamp) -> str:
    mapping = {
        0: "星期一",
        1: "星期二",
        2: "星期三",
        3: "星期四",
        4: "星期五",
        5: "星期六",
        6: "星期日",
    }
    return mapping[ts.weekday()]


def _day_type_text(ts: pd.Timestamp) -> str:
    weekday = ts.weekday()
    if weekday == 0:
        return "頭"
    if weekday in (1, 2, 3):
        return "週間"
    if weekday == 4:
        return "尾"
    return "週間"


def _classify_k_type(open_price, close_price, high_price, low_price):
    values = (open_price, close_price, high_price, low_price)
    if any(v is None or pd.isna(v) for v in values):
        return None

    price_range = high_price - low_price
    if price_range == 0:
        return None

    body = abs(close_price - open_price) / price_range
    upper = (high_price - max(open_price, close_price)) / price_range
    lower = (min(open_price, close_price) - low_price) / price_range

    if abs(close_price - open_price) < 1e-6 or body < 0.05:
        return "⬜ 十字線"

    color = "🟥" if close_price > open_price else "🟩"
    if lower > 0.5 and body < 0.3:
        return f"{color} 錘子線"
    if upper > 0.5 and body < 0.3:
        return f"{color} 流星線"
    if body > 0.6:
        return f"{color} 長紅K" if close_price > open_price else f"{color} 長黑K"
    return f"{color} 中實體K"


def _week_no(ts: pd.Timestamp) -> int:
    return int(ts.isocalendar().week)


def _sync_date_span_from_table(
    target_table: str,
    idx_key: str,
    idx_field: str,
    date_field: str,
):
    span_df = db.query_to_df(
        f"""
        SELECT MIN({date_field}) AS min_date, MAX({date_field}) AS max_date
        FROM {target_table}
        WHERE {idx_field} = ?
        """,
        (str(idx_key),),
    )

    actual_s = (
        pd.Timestamp(span_df.loc[0, "min_date"]).normalize()
        if not span_df.empty and pd.notna(span_df.loc[0, "min_date"])
        else None
    )
    actual_e = (
        pd.Timestamp(span_df.loc[0, "max_date"]).normalize()
        if not span_df.empty and pd.notna(span_df.loc[0, "max_date"])
        else None
    )

    print(
        f"[sync_date_span] table={target_table}, idx_key={idx_key}, "
        f"actual_s={_dstr(actual_s) if actual_s is not None else None}, "
        f"actual_e={_dstr(actual_e) if actual_e is not None else None}"
    )

    if actual_s is None or actual_e is None:
        return

    db.execute_sql(
        """
        INSERT INTO date_span (target_table, idx_key, start_date, end_date, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(target_table, idx_key) DO UPDATE SET
          start_date = excluded.start_date,
          end_date   = excluded.end_date,
          updated_at = CURRENT_TIMESTAMP
        """,
        (target_table, str(idx_key), _dstr(actual_s), _dstr(actual_e)),
    )

# =========================================================
# 來源資料 helper
# =========================================================
def _get_trading_dates(start_date, end_date, futures_id="TX") -> list[pd.Timestamp]:
    df = db.query_to_df(
        """
        SELECT DISTINCT date
        FROM fm_taiwan_futures_daily
        WHERE futures_id = ?
          AND trading_session = 'position'
          AND date >= ?
          AND date <= ?
        ORDER BY date
        """,
        (futures_id, _dstr(start_date), _dstr(end_date)),
    )
    if df.empty:
        return []
    return [pd.Timestamp(x).normalize() for x in df["date"].tolist()]


def _get_recent_trading_dates(limit=5, futures_id="TX") -> list[pd.Timestamp]:
    df = db.query_to_df(
        """
        SELECT DISTINCT trading_date
        FROM (
            SELECT date AS trading_date
            FROM fm_taiwan_futures_daily
            WHERE futures_id = ?
              AND trading_session = 'position'
            UNION
            SELECT trade_date AS trading_date
            FROM taifex_futures_daily
            WHERE futures_id = ?
              AND trading_session = 'position'
        ) source_dates
        ORDER BY trading_date DESC
        LIMIT ?
        """,
        (futures_id, futures_id, int(limit)),
    )
    if df.empty:
        return []
    return sorted(pd.Timestamp(x).normalize() for x in df["trading_date"].tolist())


def _get_next_trading_date(trading_date, futures_id="TX") -> pd.Timestamp | None:
    df = db.query_to_df(
        """
        SELECT MIN(date) AS next_date
        FROM fm_taiwan_futures_daily
        WHERE futures_id = ?
          AND trading_session = 'position'
          AND date > ?
        """,
        (futures_id, _dstr(trading_date)),
    )
    if df.empty or pd.isna(df.loc[0, "next_date"]):
        return None
    return pd.Timestamp(df.loc[0, "next_date"]).normalize()


def _get_night_source_end(
    report_end_date,
    futures_id="TX",
    max_lookahead_days: int = 14,
) -> pd.Timestamp:
    """取得報表截止日所需的最後一個夜盤來源日期。

    夜盤在來源端歸屬於下一個交易日，因此只需抓到報表截止日後的
    第一個交易日，不應在不同呼叫層重複累加日曆日。
    """
    report_end = _to_ts(report_end_date)
    lookahead_end = report_end + pd.Timedelta(days=max(1, int(max_lookahead_days)))
    frame = db.query_to_df(
        """
        SELECT MIN(source_date) AS source_end
        FROM (
            SELECT date AS source_date
            FROM fm_taiwan_stock_trading_date
            WHERE date > ? AND date <= ?
            UNION ALL
            SELECT date AS source_date
            FROM fm_taiwan_futures_daily
            WHERE futures_id = ?
              AND trading_session = 'after_market'
              AND date > ? AND date <= ?
            UNION ALL
            SELECT trade_date AS source_date
            FROM taifex_futures_daily
            WHERE futures_id = ?
              AND trading_session = 'after_market'
              AND trade_date > ? AND trade_date <= ?
        ) source_dates
        """,
        (
            _dstr(report_end), _dstr(lookahead_end),
            futures_id, _dstr(report_end), _dstr(lookahead_end),
            futures_id, _dstr(report_end), _dstr(lookahead_end),
        ),
    )
    if not frame.empty and pd.notna(frame.loc[0, "source_end"]):
        return pd.Timestamp(frame.loc[0, "source_end"]).normalize()

    # 交易日表尚未更新時保留安全上限，但只在這個中央函式回退一次。
    return lookahead_end

def _get_next_after_market_date(trading_date, futures_id="TX") -> pd.Timestamp | None:
    df = db.query_to_df(
        """
        SELECT MIN(next_date) AS next_date
        FROM (
            SELECT date AS next_date
            FROM fm_taiwan_futures_daily
            WHERE futures_id = ? AND trading_session = 'after_market' AND date > ?
            UNION ALL
            SELECT trade_date AS next_date
            FROM taifex_futures_daily
            WHERE futures_id = ? AND trading_session = 'after_market' AND trade_date > ?
        ) source_dates
        """,
        (futures_id, _dstr(trading_date), futures_id, _dstr(trading_date)),
    )
    if df.empty or pd.isna(df.loc[0, "next_date"]):
        return None
    return pd.Timestamp(df.loc[0, "next_date"]).normalize()

def _get_front_contract_month(trading_date, futures_id="TX") -> str | None:
    """
    用實際最後結算日判斷近月：
    找出結算日 >= trading_date 的第一個月合約
    """
    df = db.query_to_df(
        """
        SELECT date, contract_month
        FROM fm_taiwan_futures_final_settlement_price
        WHERE futures_id = ?
          AND futures_type = 'index'
          AND contract_month NOT LIKE '%W%'
          AND LENGTH(contract_month) = 6
          AND date >= ?
        ORDER BY date ASC
        LIMIT 1
        """,
        (futures_id, _dstr(trading_date)),
    )
    if df.empty:
        return None
    return str(df.loc[0, "contract_month"])


def _get_month_contract_row(
    trading_date,
    trading_session: str,
    contract_month: str | None,
    futures_id="TX",
) -> pd.Series | None:
    """
    先抓指定 contract_month。
    若抓不到，備援用當天該 session 成交量最大的純月合約。
    """
    if contract_month is not None:
        df = db.query_to_df(
            """
            SELECT date, futures_id, contract_date, open, max, min, close, volume, trading_session
            FROM fm_taiwan_futures_daily
            WHERE futures_id = ?
              AND date = ?
              AND trading_session = ?
              AND contract_date = ?
            LIMIT 1
            """,
            (futures_id, _dstr(trading_date), trading_session, contract_month),
        )
        if not df.empty:
            return df.iloc[0]

    df_fb = db.query_to_df(
        """
        SELECT date, futures_id, contract_date, open, max, min, close, volume, trading_session
        FROM fm_taiwan_futures_daily
        WHERE futures_id = ?
          AND date = ?
          AND trading_session = ?
          AND contract_date NOT LIKE '%/%'
          AND contract_date NOT LIKE '%W%'
          AND LENGTH(contract_date) = 6
        ORDER BY volume DESC, contract_date ASC
        LIMIT 1
        """,
        (futures_id, _dstr(trading_date), trading_session),
    )
    if df_fb.empty:
        official = db.query_to_df(
            """
            SELECT trade_date AS date, futures_id, contract_month AS contract_date,
                   open, high AS max, low AS min, close, volume, trading_session
            FROM taifex_futures_daily
            WHERE futures_id = ?
              AND trade_date = ?
              AND trading_session = ?
              AND contract_month ~ '^[0-9]{6}$'
              AND open IS NOT NULL AND high IS NOT NULL
              AND low IS NOT NULL AND close IS NOT NULL
            ORDER BY
              CASE WHEN contract_month = ? THEN 0 ELSE 1 END,
              volume DESC NULLS LAST,
              contract_month ASC
            LIMIT 1
            """,
            (futures_id, _dstr(trading_date), trading_session, contract_month),
        )
        if not official.empty:
            return official.iloc[0]
    if df_fb.empty:
        return None
    return df_fb.iloc[0]



def _get_incomplete_night_dates(
    start_date,
    end_date,
    futures_id="TX",
) -> list[pd.Timestamp]:
    df = db.query_to_df(
        """
        SELECT 日期
        FROM futures_report_daily_raw
        WHERE futures_id = ?
          AND 日期 >= ?
          AND 日期 <= ?
          AND (
                夜盤開 IS NULL
             OR 夜盤收 IS NULL
             OR 夜盤高 IS NULL
             OR 夜盤低 IS NULL
          )
        ORDER BY 日期
        """,
        (futures_id, _dstr(start_date), _dstr(end_date)),
    )
    if df.empty:
        return []
    return [pd.Timestamp(x).normalize() for x in df["日期"].tolist()]


# =========================================================
# MA 重算
# =========================================================
def _recalc_futures_report_daily_raw_ma(futures_id="TX"):
    """
    只重算指定 futures_id 的 futures_report_daily_raw MA 欄位
    """
    fid = str(futures_id)

    df = db.query_to_df(
        """
        SELECT
            id,
            日期,
            日盤收,
            夜盤收
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (fid,),
    )

    if df.empty:
        return

    df = df.copy()
    df["日盤收"] = pd.to_numeric(df["日盤收"], errors="coerce")
    df["夜盤收"] = pd.to_numeric(df["夜盤收"], errors="coerce")

    for win in [5, 10, 20, 60, 120, 240]:
        df[f"日{win}MA"] = df["日盤收"].rolling(win, min_periods=win).mean()

        # 對外報表不再需要夜10MA～夜120MA，也未要求夜240MA。
        if win in (5, 10, 20, 60, 120):
            df[f"夜{win}MA"] = df["夜盤收"].rolling(win, min_periods=win).mean()

    params = list(
        df[
            [
                "日5MA", "日10MA", "日20MA", "日60MA", "日120MA", "日240MA",
                "夜5MA", "夜10MA", "夜20MA", "夜60MA", "夜120MA",
                "id",
            ]
        ].itertuples(index=False, name=None)
    )

    if not params:
        return

    sql = """
    UPDATE futures_report_daily_raw
    SET
        "日5MA"   = ?,
        "日10MA"  = ?,
        "日20MA"  = ?,
        "日60MA"  = ?,
        "日120MA" = ?,
        "日240MA" = ?,
        "夜5MA"   = ?,
        "夜10MA"  = ?,
        "夜20MA"  = ?,
        "夜60MA"  = ?,
        "夜120MA" = ?
    WHERE id = ?
    """
    db.execute_sql(sql, params)



def _trend_regime(price_vs_ma):
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
        final_str += "價漲"
    elif p_dn:
        final_str += "價跌"
    else:
        final_str += "價同"

    if v_up:
        final_str += "量增"
    elif v_dn:
        final_str += "量減"
    else:
        final_str += "量同"

    return final_str

def _recalc_futures_report_daily_raw_pv(futures_id="TX"):
    """
    回填：
    日當日價量 / 日5日價量 / 日10日價量 / 日20日價量 / 日60日價量
    """

    fid = str(futures_id)

    df = db.query_to_df(
        """
        SELECT
            id,
            日期,
            日盤收,
            日成交量
        FROM futures_report_daily_raw
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (fid,),
    )

    if df.empty:
        return

    df = df.copy()

    # === 基本欄位 ===
    df["日盤收"] = pd.to_numeric(df["日盤收"], errors="coerce")
    df["日成交量"] = pd.to_numeric(df["日成交量"], errors="coerce")

    # === 昨收 / 昨量 ===
    df["昨收"] = df["日盤收"].shift(1)
    df["昨量"] = df["日成交量"].shift(1)

    df["漲跌幅_pct"] = (df["日盤收"] - df["昨收"]) / df["昨收"].replace(0, pd.NA)
    df["量增率_pct"] = (df["日成交量"] - df["昨量"]) / df["昨量"].replace(0, pd.NA)

    # =========================
    # 當日價量（用5MA判斷行情）
    # =========================
    df["5MA"] = df["日盤收"].shift(1).rolling(5).mean()
    df["當日乖離"] = (df["日盤收"] - df["5MA"]) / df["5MA"].replace(0, pd.NA)

    df["當日行情"] = df["當日乖離"].apply(_trend_regime)

    df["日當日價量"] = [
        _pv_div_by_regime(regime, pchg, vchg)
        for regime, pchg, vchg in zip(
            df["當日行情"],
            df["漲跌幅_pct"],
            df["量增率_pct"],
        )
    ]

    # =========================
    # N日價量
    # =========================
    for n in [5, 10, 20, 60]:
        df[f"{n}日均量"] = df["日成交量"].shift(1).rolling(n).mean()
        df[f"{n}MA"] = df["日盤收"].shift(1).rolling(n).mean()

        df[f"{n}日乖離"] = (df["日盤收"] - df[f"{n}MA"]) / df[f"{n}MA"].replace(0, pd.NA)

        df[f"{n}日量增率_pct"] = (
            (df["日成交量"] - df[f"{n}日均量"]) / df[f"{n}日均量"].replace(0, pd.NA)
        )

        df[f"{n}日行情"] = df[f"{n}日乖離"].apply(_trend_regime)

        df[f"日{n}日價量"] = [
            _pv_div_by_regime(regime, pchg, vchg)
            for regime, pchg, vchg in zip(
                df[f"{n}日行情"],
                df[f"{n}日乖離"],
                df[f"{n}日量增率_pct"],
            )
        ]

    # =========================
    # 寫回 DB
    # =========================
    update_rows = list(
        df[
            [
                "日當日價量",
                "日5日價量",
                "日10日價量",
                "日20日價量",
                "日60日價量",
                "id",
            ]
        ].itertuples(index=False, name=None)
    )

    if not update_rows:
        return

    db.execute_sql(
        """
        UPDATE futures_report_daily_raw
        SET
            日當日價量 = ?,
            日5日價量 = ?,
            日10日價量 = ?,
            日20日價量 = ?,
            日60日價量 = ?
        WHERE id = ?
        """,
        update_rows,
    )

def _safe_float(x):
    if pd.isna(x):
        return None
    return float(x)


def _build_market_price_maps(
    start_ts,
    end_ts,
    futures_id="TX",
):
    """
    建立三個日期對照表：
    - 加權收盤價：TAIEX close
    - 大台收盤價：TX position 近月 close
    - 大台結算價：TX position 近月 settlement_price

    規則：
    - 原則上取 contract_date 最小的月合約
    - 但若該筆 settlement_price = 0，代表可能是結算日已結算合約
    - 則改取下一個 contract_date 較大的月合約
    """

    # =====================================================
    # 1. 加權收盤價：TAIEX close
    # =====================================================
    df_taiex = fm.get_tw_stock_daily_price(
        stock_id="TAIEX",
        start_date=start_ts,
        end_date=end_ts,
    )

    weighted_close_map = {}

    if df_taiex is not None and not df_taiex.empty:
        df_taiex = df_taiex.copy()
        df_taiex["date"] = pd.to_datetime(df_taiex["date"]).dt.strftime("%Y-%m-%d")

        weighted_close_map = {
            row["date"]: _safe_float(row["close"])
            for _, row in df_taiex.iterrows()
        }

    # =====================================================
    # 2. 大台收盤價 / 大台結算價
    # =====================================================
    df_tx = fm.get_tw_futures_daily(
        futures_id=futures_id,
        start_date=start_ts,
        end_date=end_ts,
    )

    tx_close_map = {}
    tx_settlement_map = {}

    if df_tx is not None and not df_tx.empty:
        df_tx = df_tx.copy()
        df_tx["date"] = pd.to_datetime(df_tx["date"]).dt.strftime("%Y-%m-%d")
        df_tx["contract_date"] = df_tx["contract_date"].astype(str)
        df_tx["trading_session"] = df_tx["trading_session"].astype(str)

        df_tx["close"] = pd.to_numeric(df_tx["close"], errors="coerce")
        df_tx["settlement_price"] = pd.to_numeric(df_tx["settlement_price"], errors="coerce")

        df_tx = df_tx[
            (df_tx["trading_session"] == "position")
            & (df_tx["contract_date"].str.len() == 6)
        ].copy()

        if not df_tx.empty:
            df_tx = df_tx.sort_values(["date", "contract_date"]).copy()

            selected_rows = []

            for date_str, g in df_tx.groupby("date", sort=True):
                g = g.sort_values("contract_date").copy()

                valid_g = g[
                    g["settlement_price"].notna()
                    & (g["settlement_price"] != 0)
                ]

                if not valid_g.empty:
                    selected_rows.append(valid_g.iloc[0])
                else:
                    selected_rows.append(g.iloc[0])

            df_front = pd.DataFrame(selected_rows)

            tx_close_map = {
                row["date"]: _safe_float(row["close"])
                for _, row in df_front.iterrows()
            }

            tx_settlement_map = {
                row["date"]: _safe_float(row["settlement_price"])
                for _, row in df_front.iterrows()
            }

    return weighted_close_map, tx_close_map, tx_settlement_map

# =========================================================
# 月選 Call / Put total 計算
# =========================================================
def _third_wednesday(year: int, month: int) -> pd.Timestamp:
    """
    回傳該年月的第三個星期三
    """
    first_day = pd.Timestamp(year=year, month=month, day=1)

    # weekday: Monday=0, Wednesday=2
    days_to_wed = (2 - first_day.weekday()) % 7
    first_wed = first_day + pd.Timedelta(days=days_to_wed)

    third_wed = first_wed + pd.Timedelta(days=14)
    return third_wed.normalize()


def _yyyymm_add_month(ts: pd.Timestamp, months: int = 1) -> str:
    """
    yyyy-mm-dd + N 個月後，回傳 yyyymm
    """
    ts = pd.Timestamp(ts).normalize()
    y = ts.year
    m = ts.month + months

    while m > 12:
        y += 1
        m -= 12

    while m < 1:
        y -= 1
        m += 12

    return f"{y}{m:02d}"


def _get_month_option_contract_month(trading_date) -> str:
    """
    月選 contract_date 規則：
    - 第三個週三之前：當月 yyyymm
    - 第三個週三含當天之後：下一個月 yyyymm
    """
    d = pd.Timestamp(trading_date).normalize()
    third_wed = _third_wednesday(d.year, d.month)

    if d >= third_wed:
        return _yyyymm_add_month(d, 1)

    return f"{d.year}{d.month:02d}"


def _calc_month_option_row_value(
    call_put: str,
    strike_price,
    settlement_price,
    open_interest,
    weighted_close,
    tx_close,
):
    """
    單一履約價的月選價值。

    依照你的 Google Sheet 公式：

    Call:
        if 履約價 < 加權收盤價:
            結算價 - (大台收盤價 - 履約價)
        else:
            結算價

    Put:
        if 履約價 > 加權收盤價:
            結算價 + (大台收盤價 - 履約價)
        else:
            結算價
    """

    if pd.isna(strike_price) or pd.isna(settlement_price):
        return None

    if pd.isna(weighted_close) or pd.isna(tx_close):
        return None

    if pd.isna(open_interest):
        open_interest = 0

    cp = str(call_put).strip().lower()

    strike_price = float(strike_price)
    settlement_price = float(settlement_price)
    open_interest = float(open_interest)
    weighted_close = float(weighted_close)
    tx_close = float(tx_close)

    if cp == "call":
        if strike_price < weighted_close:
            value = settlement_price - (tx_close - strike_price)
        else:
            value = settlement_price

    elif cp == "put":
        if strike_price > weighted_close:
            value = settlement_price + (tx_close - strike_price)
        else:
            value = settlement_price
    else:
        return None
    
    if value < 0:
        value = 0
    return value


def _calc_month_option_total_for_day(
    trading_date,
    call_put: str,
    option_id="TXO",
):
    """
    計算單日 call_月選 或 put_月選 total。

    資料來源：
    - fm_taiwan_option_daily
    - futures_report_daily_raw

    必要條件：
    - futures_report_daily_raw 當天要已經有：
        加權收盤價
        大台收盤價
    """

    d = pd.Timestamp(trading_date).normalize()
    d_str = _dstr(d)

    cp = str(call_put).strip().lower()
    if cp not in ("call", "put"):
        raise ValueError("call_put 只能是 call 或 put")

    contract_month = _get_month_option_contract_month(d)

    # =====================================================
    # 1. 讀取當天加權收盤價 / 大台收盤價
    # =====================================================
    price_df = db.query_to_df(
        """
        SELECT
            加權收盤價,
            大台收盤價
        FROM futures_report_daily_raw
        WHERE futures_id = 'TX'
          AND 日期 = ?
        LIMIT 1
        """,
        (d_str,),
    )

    if price_df is None or price_df.empty:
        print(f"[month_option][skip] {d_str} futures_report_daily_raw 無價格資料")
        return None

    weighted_close = price_df.loc[0, "加權收盤價"]
    tx_close = price_df.loc[0, "大台收盤價"]

    if pd.isna(weighted_close) or pd.isna(tx_close):
        print(f"[month_option][skip] {d_str} 加權收盤價 / 大台收盤價 為 NULL")
        return None

    # =====================================================
    # 2. 讀取 TXO 月選資料
    # =====================================================
    opt_df = db.query_to_df(
        """
        SELECT
            option_id,
            contract_date,
            strike_price,
            call_put,
            settlement_price,
            open_interest
        FROM fm_taiwan_option_daily
        WHERE date = ?
          AND trading_session = 'position'
          AND LOWER(call_put) = ?
          AND contract_date = ?
        ORDER BY CAST(strike_price AS REAL)
        """,
        (d_str, cp, contract_month),
    )

    if opt_df is None or opt_df.empty:
        print(
            f"[month_option][skip] {d_str} "
            f"cp={cp}, contract_month={contract_month}, 查無 TXO 月選資料"
        )
        return None

    # =====================================================
    # 3. 逐筆計算後加總
    # =====================================================
    total = 0.0
    has_value = False

    for _, row in opt_df.iterrows():
        row_value = _calc_month_option_row_value(
            call_put=row["call_put"],
            strike_price=row["strike_price"],
            settlement_price=row["settlement_price"],
            open_interest=row["open_interest"],
            weighted_close=weighted_close,
            tx_close=tx_close,
        )

        if row_value is None or pd.isna(row_value):
            continue

        total += float(row_value)
        has_value = True

    if not has_value:
        return None

    return total


def _recalc_futures_report_daily_raw_month_option(
    futures_id="TX",
    option_id="TXO",
    target_dates=None,
):
    """
    回填 futures_report_daily_raw：
    - call_月選
    - put_月選

    若 target_dates=None，則重算整張 futures_report_daily_raw。
    """

    fid = str(futures_id)

    if target_dates is None:
        df_dates = db.query_to_df(
            """
            SELECT 日期
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (fid,),
        )

        if df_dates is None or df_dates.empty:
            return

        dates = pd.to_datetime(df_dates["日期"]).dt.normalize().tolist()

    else:
        dates = [
            pd.Timestamp(x).normalize()
            for x in target_dates
            if x is not None
        ]

    if not dates:
        return

    update_rows = []

    for d in dates:
        d_str = _dstr(d)

        call_total = _calc_month_option_total_for_day(
            trading_date=d,
            call_put="call",
            option_id=option_id,
        )

        put_total = _calc_month_option_total_for_day(
            trading_date=d,
            call_put="put",
            option_id=option_id,
        )

        update_rows.append(
            (
                call_total,
                put_total,
                fid,
                d_str,
            )
        )

    if not update_rows:
        return

    db.execute_sql(
        """
        UPDATE futures_report_daily_raw
        SET
            call_月選 = ?,
            put_月選 = ?
        WHERE futures_id = ?
          AND 日期 = ?
        """,
        update_rows,
    )

    print(f"[month_option] updated={len(update_rows)}")

# =========================================================
# 週三選 Call / Put total 計算
# =========================================================
def _nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> pd.Timestamp:
    """
    回傳指定年月第 nth 個 weekday 的日期。

    weekday:
        Monday=0
        Tuesday=1
        Wednesday=2
        Thursday=3
        Friday=4
    """
    first_day = pd.Timestamp(year=year, month=month, day=1)

    days_to_target = (weekday - first_day.weekday()) % 7
    first_target = first_day + pd.Timedelta(days=days_to_target)

    target_day = first_target + pd.Timedelta(days=7 * (nth - 1))
    return target_day.normalize()


def _parse_weekly_option_contract_date(contract_date: str) -> pd.Timestamp | None:
    """
    解析週選 contract_date。

    e.g.
        202604W5 -> 2026年04月第5個週三
        202605W1 -> 2026年05月第1個週三
    """
    if contract_date is None:
        return None

    s = str(contract_date).strip().upper()

    if "W" not in s:
        return None

    try:
        ym, w_part = s.split("W", 1)

        if len(ym) != 6:
            return None

        year = int(ym[:4])
        month = int(ym[4:6])
        nth = int(w_part)

        return _nth_weekday_of_month(
            year=year,
            month=month,
            weekday=2,  # Wednesday
            nth=nth,
        )

    except Exception:
        return None


def _get_weekly_wed_option_contract_date(
    trading_date,
    call_put: str = "call",
) -> str | None:
    """
    找出交易日當天要使用的週三選 contract_date。

    規則：
    1. 先查當天 fm_taiwan_option_daily 內所有 W 合約
    2. 將 contract_date 轉成實際週三到期日
    3. 找第一個 到期日 > trading_date 的 contract_date
    """
    d = pd.Timestamp(trading_date).normalize()
    d_str = _dstr(d)

    cp = str(call_put).strip().lower()

    df = db.query_to_df(
        """
        SELECT DISTINCT contract_date
        FROM fm_taiwan_option_daily
        WHERE date = ?
          AND trading_session = 'position'
          AND LOWER(call_put) = ?
          AND contract_date LIKE '%W%'
        ORDER BY contract_date
        """,
        (d_str, cp),
    )

    if df is None or df.empty:
        return None

    candidates = []

    for contract_date in df["contract_date"].tolist():
        expiry_date = _parse_weekly_option_contract_date(contract_date)

        if expiry_date is None:
            continue

        if expiry_date > d:
            candidates.append(
                (
                    expiry_date,
                    str(contract_date),
                )
            )

    if not candidates:
        return None

    candidates = sorted(candidates, key=lambda x: (x[0], x[1]))
    return candidates[0][1]


def _calc_option_row_value(
    call_put: str,
    strike_price,
    settlement_price,
    weighted_close,
    tx_base_price,
):
    """
    單一選擇權履約價價值。

    Call:
        if 履約價 < 加權收盤價:
            結算價 - (大台基準價 - 履約價)
        else:
            結算價

    Put:
        if 履約價 > 加權收盤價:
            結算價 + (大台基準價 - 履約價)
        else:
            結算價

    規則：
        - 不乘 open_interest
        - 小於 0 的值改算 0
    """

    if pd.isna(strike_price) or pd.isna(settlement_price):
        return None

    if pd.isna(weighted_close) or pd.isna(tx_base_price):
        return None

    cp = str(call_put).strip().lower()

    strike_price = float(strike_price)
    settlement_price = float(settlement_price)
    weighted_close = float(weighted_close)
    tx_base_price = float(tx_base_price)

    if cp == "call":
        if strike_price < weighted_close:
            value = settlement_price - (tx_base_price - strike_price)
        else:
            value = settlement_price

    elif cp == "put":
        if strike_price > weighted_close:
            value = settlement_price + (tx_base_price - strike_price)
        else:
            value = settlement_price

    else:
        return None

    if value < 0:
        value = 0

    return value

def _calc_weekly_wed_option_total_for_day(
    trading_date,
    call_put: str,
    option_id="TXO",
):
    """
    計算單日：
    - call_週三
    - put_週三

    使用：
    - 加權收盤價
    - 大台結算價
    - fm_taiwan_option_daily 的週選 W 合約
    """

    d = pd.Timestamp(trading_date).normalize()
    d_str = _dstr(d)

    cp = str(call_put).strip().lower()
    if cp not in ("call", "put"):
        raise ValueError("call_put 只能是 call 或 put")

    contract_date = _get_weekly_wed_option_contract_date(
        trading_date=d,
        call_put=cp,
    )

    if contract_date is None:
        print(f"[weekly_wed_option][skip] {d_str} cp={cp} 找不到週三選 contract_date")
        return None

    # =====================================================
    # 1. 讀取加權收盤價 / 大台結算價
    # =====================================================
    price_df = db.query_to_df(
        """
        SELECT
            加權收盤價,
            大台結算價
        FROM futures_report_daily_raw
        WHERE futures_id = 'TX'
          AND 日期 = ?
        LIMIT 1
        """,
        (d_str,),
    )

    if price_df is None or price_df.empty:
        print(f"[weekly_wed_option][skip] {d_str} futures_report_daily_raw 無價格資料")
        return None

    weighted_close = price_df.loc[0, "加權收盤價"]
    tx_settlement = price_df.loc[0, "大台結算價"]

    if pd.isna(weighted_close) or pd.isna(tx_settlement):
        print(f"[weekly_wed_option][skip] {d_str} 加權收盤價 / 大台結算價 為 NULL")
        return None

    # =====================================================
    # 2. 讀取週三選資料
    # =====================================================
    opt_df = db.query_to_df(
        """
        SELECT
            option_id,
            contract_date,
            strike_price,
            call_put,
            settlement_price,
            open_interest
        FROM fm_taiwan_option_daily
        WHERE date = ?
          AND trading_session = 'position'
          AND LOWER(call_put) = ?
          AND contract_date = ?
        ORDER BY contract_date, CAST(strike_price AS REAL)
        """,
        (d_str, cp, contract_date),
    )

    if opt_df is None or opt_df.empty:
        print(
            f"[weekly_wed_option][skip] {d_str} "
            f"cp={cp}, contract_date={contract_date}, 查無資料"
        )
        return None

    # =====================================================
    # 3. 逐筆計算，排除負值後加總
    # =====================================================
    total = 0.0
    has_value = False

    for _, row in opt_df.iterrows():
        row_value = _calc_option_row_value(
            call_put=row["call_put"],
            strike_price=row["strike_price"],
            settlement_price=row["settlement_price"],
            weighted_close=weighted_close,
            tx_base_price=tx_settlement,
        )

        if row_value is None or pd.isna(row_value):
            continue

        total += float(row_value)
        has_value = True

    if not has_value:
        return None

    return total


def _recalc_futures_report_daily_raw_weekly_wed_option(
    futures_id="TX",
    option_id="TXO",
    target_dates=None,
):
    """
    回填 futures_report_daily_raw：
    - call_週三
    - put_週三

    若 target_dates=None，則重算整張 futures_report_daily_raw。
    """

    fid = str(futures_id)

    if target_dates is None:
        df_dates = db.query_to_df(
            """
            SELECT 日期
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (fid,),
        )

        if df_dates is None or df_dates.empty:
            return

        dates = pd.to_datetime(df_dates["日期"]).dt.normalize().tolist()

    else:
        dates = [
            pd.Timestamp(x).normalize()
            for x in target_dates
            if x is not None
        ]

    if not dates:
        return

    update_rows = []

    for d in dates:
        d_str = _dstr(d)

        call_total = _calc_weekly_wed_option_total_for_day(
            trading_date=d,
            call_put="call",
            option_id=option_id,
        )

        put_total = _calc_weekly_wed_option_total_for_day(
            trading_date=d,
            call_put="put",
            option_id=option_id,
        )

        update_rows.append(
            (
                call_total,
                put_total,
                fid,
                d_str,
            )
        )

    if not update_rows:
        return

    db.execute_sql(
        """
        UPDATE futures_report_daily_raw
        SET
            call_週三 = ?,
            put_週三 = ?
        WHERE futures_id = ?
          AND 日期 = ?
        """,
        update_rows,
    )

    print(f"[weekly_wed_option] updated={len(update_rows)}")

# =========================================================
# 週五選 Call / Put total 計算
# =========================================================
def _parse_weekly_friday_option_contract_date(contract_date: str) -> pd.Timestamp | None:
    """
    解析週五選 contract_date。

    e.g.
        202605F1 -> 2026年05月第1個週五
        202605F2 -> 2026年05月第2個週五
    """
    if contract_date is None:
        return None

    s = str(contract_date).strip().upper()

    if "F" not in s:
        return None

    try:
        ym, f_part = s.split("F", 1)

        if len(ym) != 6:
            return None

        year = int(ym[:4])
        month = int(ym[4:6])
        nth = int(f_part)

        return _nth_weekday_of_month(
            year=year,
            month=month,
            weekday=4,  # Friday
            nth=nth,
        )

    except Exception:
        return None


def _get_weekly_friday_option_contract_date(
    trading_date,
    call_put: str = "call",
) -> str | None:
    """
    找出交易日當天要使用的週五選 contract_date。

    規則：
    1. 查當天 fm_taiwan_option_daily 內所有 F 合約
    2. 將 contract_date 轉成實際週五到期日
    3. 找第一個 到期日 > trading_date 的 contract_date
    """
    d = pd.Timestamp(trading_date).normalize()
    d_str = _dstr(d)

    cp = str(call_put).strip().lower()

    df = db.query_to_df(
        """
        SELECT DISTINCT contract_date
        FROM fm_taiwan_option_daily
        WHERE date = ?
          AND trading_session = 'position'
          AND LOWER(call_put) = ?
          AND contract_date LIKE '%F%'
        ORDER BY contract_date
        """,
        (d_str, cp),
    )

    if df is None or df.empty:
        return None

    candidates = []

    for contract_date in df["contract_date"].tolist():
        expiry_date = _parse_weekly_friday_option_contract_date(contract_date)

        if expiry_date is None:
            continue

        if expiry_date > d:
            candidates.append(
                (
                    expiry_date,
                    str(contract_date),
                )
            )

    if not candidates:
        return None

    candidates = sorted(candidates, key=lambda x: (x[0], x[1]))
    return candidates[0][1]


def _calc_weekly_friday_option_total_for_day(
    trading_date,
    call_put: str,
    option_id="TXO",
):
    """
    計算單日：
    - call_週五
    - put_週五

    使用：
    - 加權收盤價
    - 大台結算價
    - fm_taiwan_option_daily 的週五選 F 合約

    每筆 value：
    - 不乘 open_interest
    - value < 0 則改算 0
    """

    d = pd.Timestamp(trading_date).normalize()
    d_str = _dstr(d)

    cp = str(call_put).strip().lower()
    if cp not in ("call", "put"):
        raise ValueError("call_put 只能是 call 或 put")

    contract_date = _get_weekly_friday_option_contract_date(
        trading_date=d,
        call_put=cp,
    )

    if contract_date is None:
        print(f"[weekly_friday_option][skip] {d_str} cp={cp} 找不到週五選 contract_date")
        return None

    # =====================================================
    # 1. 讀取加權收盤價 / 大台結算價
    # =====================================================
    price_df = db.query_to_df(
        """
        SELECT
            加權收盤價,
            大台結算價
        FROM futures_report_daily_raw
        WHERE futures_id = 'TX'
          AND 日期 = ?
        LIMIT 1
        """,
        (d_str,),
    )

    if price_df is None or price_df.empty:
        print(f"[weekly_friday_option][skip] {d_str} futures_report_daily_raw 無價格資料")
        return None

    weighted_close = price_df.loc[0, "加權收盤價"]
    tx_settlement = price_df.loc[0, "大台結算價"]

    if pd.isna(weighted_close) or pd.isna(tx_settlement):
        print(f"[weekly_friday_option][skip] {d_str} 加權收盤價 / 大台結算價 為 NULL")
        return None

    # =====================================================
    # 2. 讀取週五選資料
    # =====================================================
    opt_df = db.query_to_df(
        """
        SELECT
            option_id,
            contract_date,
            strike_price,
            call_put,
            settlement_price,
            open_interest
        FROM fm_taiwan_option_daily
        WHERE date = ?
          AND trading_session = 'position'
          AND LOWER(call_put) = ?
          AND contract_date = ?
        ORDER BY contract_date, CAST(strike_price AS REAL)
        """,
        (d_str, cp, contract_date),
    )

    if opt_df is None or opt_df.empty:
        print(
            f"[weekly_friday_option][skip] {d_str} "
            f"cp={cp}, contract_date={contract_date}, 查無資料"
        )
        return None

    # =====================================================
    # 3. 逐筆計算，排除負值後加總
    # =====================================================
    total = 0.0
    has_value = False

    for _, row in opt_df.iterrows():
        row_value = _calc_option_row_value(
            call_put=row["call_put"],
            strike_price=row["strike_price"],
            settlement_price=row["settlement_price"],
            weighted_close=weighted_close,
            tx_base_price=tx_settlement,
        )

        if row_value is None or pd.isna(row_value):
            continue

        total += float(row_value)
        has_value = True

    if not has_value:
        return None

    return total


def _recalc_futures_report_daily_raw_weekly_friday_option(
    futures_id="TX",
    option_id="TXO",
    target_dates=None,
):
    """
    回填 futures_report_daily_raw：
    - call_週五
    - put_週五
    """

    fid = str(futures_id)

    if target_dates is None:
        df_dates = db.query_to_df(
            """
            SELECT 日期
            FROM futures_report_daily_raw
            WHERE futures_id = ?
            ORDER BY 日期
            """,
            (fid,),
        )

        if df_dates is None or df_dates.empty:
            return

        dates = pd.to_datetime(df_dates["日期"]).dt.normalize().tolist()

    else:
        dates = [
            pd.Timestamp(x).normalize()
            for x in target_dates
            if x is not None
        ]

    if not dates:
        return

    update_rows = []

    for d in dates:
        d_str = _dstr(d)

        call_total = _calc_weekly_friday_option_total_for_day(
            trading_date=d,
            call_put="call",
            option_id=option_id,
        )

        put_total = _calc_weekly_friday_option_total_for_day(
            trading_date=d,
            call_put="put",
            option_id=option_id,
        )

        update_rows.append(
            (
                call_total,
                put_total,
                fid,
                d_str,
            )
        )

    if not update_rows:
        return

    db.execute_sql(
        """
        UPDATE futures_report_daily_raw
        SET
            call_週五 = ?,
            put_週五 = ?
        WHERE futures_id = ?
          AND 日期 = ?
        """,
        update_rows,
    )

    print(f"[weekly_friday_option] updated={len(update_rows)}")
