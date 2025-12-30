from typing import Union, Iterable
from datetime import datetime
import pandas as pd
import numpy as np

from common import db
from module.finMind import get_tw_stock_daily_price
from module.twse import (
    get_twse_exchangeReport_fmtqik,
    get_twse_indicesReport_mi_5mins_hist,
)


def _exp_decay_weights(n: int, half_life: int = 20) -> np.ndarray:
    """
    回傳長度 n 的權重（oldest -> newest），newest 權重 = 1
    half_life: 影響力減半所需的交易日數（越小衰減越快）
    """
    if n <= 0:
        return np.array([], dtype=float)

    half_life = max(1, int(half_life))
    lam = np.log(2.0) / half_life

    # oldest delta 最大, newest delta = 0
    deltas = np.arange(n - 1, -1, -1, dtype=float)
    w = np.exp(-lam * deltas)
    w /= w.max()  # 最新 = 1
    return w
def stock_price_involve_days(
    stock_ids: Union[str, Iterable[str]],
    period_days: int,
    last_dt: datetime | None = None,
    half_life: int = 20,  # 時間衰減半衰期（交易日）
) -> pd.DataFrame:
    """
    報告快取：
      - report = {stock_id}-{period_days}-{resolved_dt(yyyymmdd)}
      - 若該 report 已存在，直接 load；不存在才重新算並寫入
    """

    # ---- normalize ----
    if last_dt is None:
        last_dt = datetime.now()

    if isinstance(stock_ids, str):
        stock_ids = [stock_ids]
    else:
        stock_ids = list(stock_ids)

    if period_days <= 0:
        raise ValueError("period_days 必須 > 0")

    req_e = pd.Timestamp(last_dt).normalize()

    ma_list = [5, 10, 20, 60, 120]
    max_ma = max(ma_list)

    # ---- helper: load / save report ----
    def load_report(report_key: str) -> pd.DataFrame:
        return db.query_to_df(
            """
            SELECT
              report,
              stock_id,
              price,
              lastDt_close_distance,
              "lcd%",
              involve_days,
              involve_date,
              date,
              date_distance,
              price_type,
              volume,
              vol_weight,
              vol_wei_pr,
              vol_adj,
              vol_wei_adj,
              vol_wei_pr_adj,
              remark,
              created_at
            FROM stock_price_involve_report
            WHERE report = ?
            ORDER BY stock_id ASC, price DESC
            """,
            (report_key,),
        )

    def save_report(df_out: pd.DataFrame) -> None:
        if df_out is None or df_out.empty:
            return

        insert_sql = """
        INSERT OR IGNORE INTO stock_price_involve_report
        (
          report,
          stock_id,
          price,
          lastDt_close_distance,
          "lcd%",
          involve_days,
          involve_date,
          date,
          date_distance,
          price_type,
          volume,
          vol_weight,
          vol_wei_pr,
          vol_adj,
          vol_wei_adj,
          vol_wei_pr_adj,
          remark
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = list(
            df_out[
                [
                    "report",
                    "stock_id",
                    "price",
                    "lastDt_close_distance",
                    "lcd%",
                    "involve_days",
                    "involve_date",
                    "date",
                    "date_distance",
                    "price_type",
                    "volume",
                    "vol_weight",
                    "vol_wei_pr",
                    "vol_adj",
                    "vol_wei_adj",
                    "vol_wei_pr_adj",
                    "remark",
                ]
            ].itertuples(index=False, name=None)
        )

        db.execute_sql(insert_sql, params)

    # ---- 1) 先用 API 把價量資料補到 req_e ----
    need_trading_days = max(period_days, max_ma)
    fetch_start = req_e - pd.Timedelta(days=need_trading_days * 3)

    df_list: list[pd.DataFrame] = []

    # 1-1) 一般股票：FinMind
    normal_ids = [sid for sid in stock_ids if sid != "TAIEX"]
    if normal_ids:
        df_fin = get_tw_stock_daily_price(
            stock_id=normal_ids,
            start_date=fetch_start.to_pydatetime(),
            end_date=req_e.to_pydatetime(),
        )
        if df_fin is not None and not df_fin.empty:
            df_list.append(df_fin)

    # 1-2) TAIEX：TWSE 兩張表
    if "TAIEX" in stock_ids:
        df_idx = get_twse_indicesReport_mi_5mins_hist(
            fetch_start.to_pydatetime(),
            req_e.to_pydatetime(),
        )
        df_mkt = get_twse_exchangeReport_fmtqik(
            fetch_start.to_pydatetime(),
            req_e.to_pydatetime(),
        )

        if (
            df_idx is not None
            and not df_idx.empty
            and df_mkt is not None
            and not df_mkt.empty
        ):
            df_taiex = pd.merge(
                df_idx,
                df_mkt,
                on=["date_ad", "date_ts"],
                how="inner",
                suffixes=("_idx", "_mkt"),
            )

            if not df_taiex.empty:
                df_taiex = df_taiex.copy()
                df_taiex["date"] = df_taiex["date_ad"]
                df_taiex["stock_id"] = "TAIEX"

                df_taiex.rename(
                    columns={
                        "market_volume": "Trading_Volume",
                        "market_money": "Trading_money",
                        "open_index": "open",
                        "high_index": "max",
                        "low_index": "min",
                        "close_index": "close",
                        "taiex_spread": "spread",
                        "trade_count": "Trading_turnover",
                    },
                    inplace=True,
                )

                df_taiex = df_taiex[
                    [
                        "date",
                        "stock_id",
                        "Trading_Volume",
                        "Trading_money",
                        "open",
                        "max",
                        "min",
                        "close",
                        "spread",
                        "Trading_turnover",
                    ]
                ]

                df_list.append(df_taiex)

    if not df_list:
        # 完全沒有資料
        return pd.DataFrame()

    df_all = pd.concat(df_list, ignore_index=True)
    df_all["date"] = pd.to_datetime(df_all["date"])
    df_all = (
        df_all.sort_values(["stock_id", "date"])
        .drop_duplicates(subset=["stock_id", "date"], keep="last")
        .reset_index(drop=True)
    )

    results: list[pd.DataFrame] = []

    # ---- remark 格式 helper ----
    def _fmt_int(x):
        try:
            return str(int(round(x)))
        except (TypeError, ValueError):
            return ""

    def _fmt_pct(x):
        try:
            return f"{x:.2%}"
        except (TypeError, ValueError):
            return ""

    def _build_remark(row: pd.Series) -> str:
        # 例：12/11 max [28568] (dis：196 , 0.69%) (vol_wei_pr ：6.94%)
        date_str = pd.to_datetime(row["date"]).strftime("%m/%d")
        pt = row["price_type"]
        price_str = _fmt_int(row["price"])
        dist_str = _fmt_int(row["lastDt_close_distance"])
        lcd_pct_str = _fmt_pct(row["lcd%"])
        vol_adj_pct_str = _fmt_pct(row["vol_wei_pr_adj"])
        return (
            f"{date_str} {pt} [{price_str}] "
            f"(dis： {dist_str} , {lcd_pct_str}) "
            f"(vol_wei_pr ：{vol_adj_pct_str})"
        )

    # ---- 2) 每檔股票：用「API 回來的最後一天」決定 report key ----
    for sid, g_all in df_all.groupby("stock_id"):
        # 只看 <= req_e 的資料
        g_all = (
            g_all[g_all["date"] <= req_e]
            .sort_values("date")
            .reset_index(drop=True)
        )
        if g_all.empty:
            continue

        # ★ 有效最後日期（實際有資料的最後一個交易日）
        resolved_e_sid = g_all["date"].max().normalize()
        ymd_sid = resolved_e_sid.strftime("%Y%m%d")
        report_key = f"{sid}-{period_days}-{ymd_sid}"

        # 先看報告在不在快取，如果在就直接用
        exists = db.query_single_value(
            "SELECT 1 FROM stock_price_involve_report WHERE report = ? LIMIT 1",
            (report_key,),
        )
        if exists:
            results.append(load_report(report_key))
            continue

        # ---- 下面開始計算新報告 ----
        g_ma = g_all.tail(need_trading_days).reset_index(drop=True)
        g_win = g_all.tail(period_days).reset_index(drop=True)

        if g_win.empty:
            continue

        # 最後交易日（當作 last_dt 對應的交易日）
        last_row = g_win.loc[g_win["date"].idxmax()]
        last_close = float(last_row["close"])
        if last_close == 0:
            last_close = np.nan

        last_date_ts = pd.to_datetime(last_row["date"]).normalize()
        last_date = last_date_ts.strftime("%Y-%m-%d")
        last_vol = int(last_row["Trading_Volume"])

        # 交易日序列，用來算 date_distance（用 index 差）
        trade_dates = g_win["date"].dt.normalize().unique()
        date_to_idx = {d: i for i, d in enumerate(trade_dates)}
        base_idx = date_to_idx[last_date_ts]  # last_dt 當天 index

        dates = g_win["date"].dt.strftime("%Y%m%d").to_numpy()
        min_arr = g_win["min"].to_numpy(dtype=float)
        max_arr = g_win["max"].to_numpy(dtype=float)
        vol_arr = g_win["Trading_Volume"].to_numpy(dtype=float)

        rng = max_arr - min_arr
        rng_nonzero = rng != 0

        # 時間衰減權重
        decay_w = _exp_decay_weights(len(g_win), half_life=half_life)
        vol_adj_arr = vol_arr * decay_w

        row_date_str_arr = g_win["date"].dt.strftime("%Y-%m-%d").to_numpy()
        vol_adj_by_date = {
            d: float(v) for d, v in zip(row_date_str_arr, vol_adj_arr)
        }

        def calc_involve_fields(price_val: float):
            """
            回傳：
              - involve_days: 有落在 [min, max] 之日數
              - mask       : bool array (同 dates 長度)
              - vol_weight : sum(volume / range)
              - vol_wei_adj: sum(adj_volume / range)
            """
            mask = (price_val >= min_arr) & (price_val <= max_arr)
            involve_days = int(mask.sum())

            mask_w = mask & rng_nonzero

            vol_weight = (
                float((vol_arr[mask_w] / rng[mask_w]).sum())
                if mask_w.any()
                else 0.0
            )
            vol_wei_adj = (
                float((vol_adj_arr[mask_w] / rng[mask_w]).sum())
                if mask_w.any()
                else 0.0
            )

            return involve_days, mask, vol_weight, vol_wei_adj

        def calc_dist(price_val: float):
            if not np.isnan(last_close):
                d = price_val - last_close
                return d, d / last_close
            return np.nan, np.nan

        records = []

        # A) close/open/min/max
        for _, row in g_win.iterrows():
            row_date_ts = pd.to_datetime(row["date"]).normalize()
            row_date = row_date_ts.strftime("%Y-%m-%d")
            row_vol = int(row["Trading_Volume"])
            row_vol_adj = float(vol_adj_by_date.get(row_date, row_vol))

            # 交易日距離：index(row_date) - index(last_date) → 往前為負數
            dd = date_to_idx[row_date_ts] - base_idx  # 0, -1, -2, ...

            for price_type in ["close", "open", "min", "max"]:
                price = float(row[price_type])

                (
                    involve_days,
                    mask,
                    vol_weight,
                    vol_wei_adj,
                ) = calc_involve_fields(price)
                (
                    lastDt_close_distance,
                    lcd_pct,
                ) = calc_dist(price)

                if involve_days > 0:
                    involve_date = "|".join(
                        f"{d}_{price_type}" for d in dates[mask].tolist()
                    )
                else:
                    involve_date = ""

                records.append(
                    {
                        "report": report_key,
                        "stock_id": sid,
                        "price": price,
                        "lastDt_close_distance": lastDt_close_distance,
                        "lcd%": lcd_pct,
                        "involve_days": involve_days,
                        "involve_date": involve_date,
                        "date": row_date,
                        "date_distance": int(dd),
                        "price_type": price_type,
                        "volume": row_vol,
                        "vol_weight": vol_weight,
                        "vol_adj": row_vol_adj,
                        "vol_wei_adj": vol_wei_adj,
                    }
                )

        # B) lastDt NMA（每條均線一筆，date_distance 一律 0）
        close_series = g_ma["close"].astype(float)
        for n in ma_list:
            ma_val = (
                float(close_series.tail(n).mean())
                if len(close_series) >= n
                else np.nan
            )

            if np.isnan(ma_val):
                (
                    involve_days,
                    mask,
                    vol_weight,
                    vol_wei_adj,
                ) = (0, np.zeros_like(dates, dtype=bool), 0.0, 0.0)
                involve_date = ""
                lastDt_close_distance, lcd_pct = np.nan, np.nan
            else:
                (
                    involve_days,
                    mask,
                    vol_weight,
                    vol_wei_adj,
                ) = calc_involve_fields(ma_val)
                if involve_days > 0:
                    involve_date = "|".join(
                        f"{d}_{n}MA" for d in dates[mask].tolist()
                    )
                else:
                    involve_date = ""

                lastDt_close_distance, lcd_pct = calc_dist(ma_val)

            price_type = f"{n}MA"

            records.append(
                {
                    "report": report_key,
                    "stock_id": sid,
                    "price": ma_val,
                    "lastDt_close_distance": lastDt_close_distance,
                    "lcd%": lcd_pct,
                    "involve_days": involve_days,
                    "involve_date": involve_date,
                    "date": last_date,
                    "date_distance": 0,  # last_dt 當天
                    "price_type": price_type,
                    "volume": last_vol,
                    "vol_weight": vol_weight,
                    "vol_adj": float(
                        vol_adj_by_date.get(last_date, last_vol)
                    ),
                    "vol_wei_adj": vol_wei_adj,
                }
            )

        out = pd.DataFrame(records)
        out["vol_wei_pr"] = out["vol_weight"].rank(pct=True)
        out["vol_wei_pr_adj"] = out["vol_wei_adj"].rank(pct=True)

        # remark（照你 Excel 公式）
        out["remark"] = out.apply(_build_remark, axis=1)

        out = (
            out[
                [
                    "report",
                    "stock_id",
                    "price",
                    "lastDt_close_distance",
                    "lcd%",
                    "involve_days",
                    "involve_date",
                    "date",
                    "date_distance",
                    "price_type",
                    "volume",
                    "vol_weight",
                    "vol_wei_pr",
                    "vol_adj",
                    "vol_wei_adj",
                    "vol_wei_pr_adj",
                    "remark",
                ]
            ]
            .sort_values(["stock_id", "price"], ascending=[True, False])
            .reset_index(drop=True)
        )

        save_report(out)
        results.append(out)

    # ---- 3) 合併回傳 ----
    if not results:
        return pd.DataFrame()

    final_df = pd.concat(results, ignore_index=True)
    final_df = (
        final_df.sort_values(["stock_id", "price"], ascending=[True, False])
        .reset_index(drop=True)
    )
    final_df = final_df.drop(columns=["created_at"], errors="ignore")
    return final_df


# python -m main.stock_ana
if __name__ == "__main__":
    # stock_id = ["TAIEX", "0050", "2330", "006201"]
    stock_id = ["TAIEX"]
    df = stock_price_involve_days(stock_id, 60, datetime.now(), half_life=20)
    print(df.head(), df.tail())
    df.to_csv("stock_ana.csv", index=False, encoding="utf-8-sig")
