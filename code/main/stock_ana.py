from typing import Union, Iterable
from datetime import datetime
import pandas as pd
import numpy as np

from common import db
from module.finMind import get_tw_stock_daily_price


def stock_price_involve_days(
    stock_ids: Union[str, Iterable[str]],
    period_days: int,
    last_dt: datetime = datetime.now(),
) -> pd.DataFrame:
    """
    具備報告快取（DB 記憶）：
    1) 以 report = {stock_id}-{period_days}-{last_dt(yyyymmdd)} 判斷報告是否存在
       - 存在：直接從 stock_price_involve_report 撈回傳
       - 不存在：計算後先入庫，再回傳
    """

    # ---- normalize ----
    if isinstance(stock_ids, str):
        stock_ids = [stock_ids]
    else:
        stock_ids = list(stock_ids)

    if period_days <= 0:
        raise ValueError("period_days 必須 > 0")

    req_e = pd.Timestamp(last_dt).normalize()
    ymd = req_e.strftime("%Y%m%d")

    # ---- helper: load report from DB ----
    def load_report(report_key: str) -> pd.DataFrame:
        return db.query_to_df(
            """
            SELECT
              report, stock_id, price, lastDt_close_distance, "lcd%",
              involve_days, involve_date, date, price_type,
              volume, vol_weight, vol_wei_pr, created_at
            FROM stock_price_involve_report
            WHERE report = ?
            ORDER BY stock_id ASC, price DESC
            """,
            (report_key,),
        )

    # ---- helper: insert report to DB (ignore duplicates) ----
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
          price_type,
          volume,
          vol_weight,
          vol_wei_pr
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = list(df_out[[
            "report", "stock_id", "price", "lastDt_close_distance", "lcd%",
            "involve_days", "involve_date",
            "date", "price_type", "volume", "vol_weight", "vol_wei_pr"
        ]].itertuples(index=False, name=None))

        db.execute_sql(insert_sql, params)

    results: list[pd.DataFrame] = []
    need_calc: list[str] = []

    # ---- 1) 先判斷哪些 stock_id 已有 report ----
    for sid in stock_ids:
        report_key = f"{sid}-{period_days}-{ymd}"

        exists = db.query_single_value(
            "SELECT 1 FROM stock_price_involve_report WHERE report = ? LIMIT 1",
            (report_key,),
        )
        if exists:
            results.append(load_report(report_key))
        else:
            need_calc.append(sid)

    # ---- 2) 需要計算的股票：一次抓資料 -> 分檔計算 -> 入庫 ----
    if need_calc:
        fetch_start = req_e - pd.Timedelta(days=period_days * 3)

        df = get_tw_stock_daily_price(
            stock_id=need_calc,
            start_date=fetch_start.to_pydatetime(),
            end_date=req_e.to_pydatetime(),
        )

        if df is not None and not df.empty:
            df["date"] = pd.to_datetime(df["date"])

            # 防重複交易日
            df = df.sort_values(["stock_id", "date"]).drop_duplicates(subset=["stock_id", "date"], keep="last")

            # 每檔只取最後 period_days 個交易日（不使用 apply，避免 warning）
            df = (
                df.sort_values(["stock_id", "date"])
                  .groupby("stock_id", as_index=False)
                  .tail(period_days)
                  .reset_index(drop=True)
            )

            for sid, g in df.groupby("stock_id"):
                g = g.sort_values("date").reset_index(drop=True)

                report_key = f"{sid}-{period_days}-{ymd}"

                last_close = float(g.loc[g["date"].idxmax(), "close"])
                if last_close == 0:
                    last_close = np.nan

                dates = g["date"].dt.strftime("%Y%m%d").to_numpy()
                min_arr = g["min"].to_numpy(dtype=float)
                max_arr = g["max"].to_numpy(dtype=float)
                vol_arr = g["Trading_Volume"].to_numpy(dtype=float)

                rng = max_arr - min_arr
                rng_nonzero = rng != 0

                records = []
                for _, row in g.iterrows():
                    row_date = row["date"]
                    row_vol = int(row["Trading_Volume"])

                    for price_type in ["close", "open", "min", "max"]:
                        price = float(row[price_type])

                        mask = (price >= min_arr) & (price <= max_arr)
                        involve_days = int(mask.sum())
                        involve_date = "|".join(dates[mask].tolist())

                        mask_w = mask & rng_nonzero
                        vol_weight = float((vol_arr[mask_w] / rng[mask_w]).sum()) if mask_w.any() else 0.0

                        if not np.isnan(last_close):
                            lastDt_close_distance = price - last_close      # 點位
                            lcd_pct = (price - last_close) / last_close     # %
                        else:
                            lastDt_close_distance = np.nan
                            lcd_pct = np.nan

                        records.append({
                            "report": report_key,
                            "stock_id": sid,
                            "price": price,
                            "lastDt_close_distance": lastDt_close_distance,
                            "lcd%": lcd_pct,
                            "involve_days": involve_days,
                            "involve_date": involve_date,
                            "date": row_date.strftime("%Y-%m-%d"),
                            "price_type": price_type,
                            "volume": row_vol,
                            "vol_weight": vol_weight,
                        })

                out = pd.DataFrame(records)
                out["vol_wei_pr"] = out["vol_weight"].rank(pct=True)

                # 欄位順序（你要的 + report）
                out = out[[
                    "report",
                    "stock_id",
                    "price",
                    "lastDt_close_distance",
                    "lcd%",
                    "involve_days",
                    "involve_date",
                    "date",
                    "price_type",
                    "volume",
                    "vol_weight",
                    "vol_wei_pr",
                ]].sort_values(["stock_id", "price"], ascending=[True, False]).reset_index(drop=True)

                # 入庫 + 回傳
                save_report(out)
                results.append(out)

    # ---- 3) 合併回傳 ----
    if not results:
        return pd.DataFrame()

    final_df = pd.concat(results, ignore_index=True)

    # 統一排序
    final_df = final_df.sort_values(["stock_id", "price"], ascending=[True, False]).reset_index(drop=True)

    # created_at 只會在「從 DB 撈」的資料出現，安全移除
    final_df = final_df.drop(columns=["created_at"], errors="ignore")

    return final_df

# python -m main.stock_ana
if __name__ == "__main__":
    stock_id = ['TAIEX', '0050', '2330', '006201']
    df = stock_price_involve_days(stock_id, 60)

    print(df.head(), df.tail())
    df.to_csv('stock_ana.csv', index=False, encoding="utf-8-sig")
