from __future__ import annotations

from datetime import datetime

import pandas as pd


SERIES = (
    ("margin_maintenance", "大盤融資維持率", "%", "line", "#3b82f6"),
    ("margin_balance", "融資餘額", "張", "line", "#ef4444"),
    ("short_balance", "融券餘額", "張", "line", "#22c55e"),
    ("margin_balance_change", "融資餘額增減", "張", "bar", "#ef4444"),
    ("short_balance_change", "融券餘額增減", "張", "bar", "#22c55e"),
    ("margin_amount", "融資金額", "億元", "line", "#f59e0b"),
    ("margin_amount_change", "融資金額增減", "億元", "bar", "#f59e0b"),
)


def _parse_period(start_date: str, end_date: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    if pd.isna(start) or pd.isna(end) or start.tzinfo is not None or end.tzinfo is not None:
        raise ValueError("請指定有效的本地日期")
    start, end = start.normalize(), end.normalize()
    if start > end:
        raise ValueError("開始日期不可晚於結束日期")
    return start, end


def _numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _load_frames(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from common import db

    params = (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    prices = db.query_to_df(
        """
        SELECT date, open, max AS high, min AS low, close
        FROM raw_finmind.fm_taiwan_stock_daily
        WHERE stock_id = 'TAIEX' AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        params,
    )
    maintenance = db.query_to_df(
        """
        SELECT date, total_exchange_margin_maintenance AS margin_maintenance
        FROM raw_finmind.fm_taiwan_total_exchange_margin_maintenance
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """,
        params,
    )
    totals = db.query_to_df(
        """
        SELECT date, name, "TodayBalance" AS today_balance, "YesBalance" AS yes_balance
        FROM raw_finmind.fm_taiwan_stock_margin_total
        WHERE date BETWEEN ? AND ?
          AND name IN ('MarginPurchase', 'MarginPurchaseMoney', 'ShortSale')
        ORDER BY date, name
        """,
        params,
    )
    return prices, maintenance, totals


def _prepare_data(prices: pd.DataFrame, maintenance: pd.DataFrame, totals: pd.DataFrame) -> pd.DataFrame:
    prepared: list[pd.DataFrame] = []
    for frame in (prices, maintenance, totals):
        work = frame.copy()
        if "date" in work.columns:
            work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
            work = work.dropna(subset=["date"])
        prepared.append(work)
    prices, maintenance, totals = prepared

    prices = _numeric(prices, ["open", "high", "low", "close"])
    maintenance = _numeric(maintenance, ["margin_maintenance"])
    totals = _numeric(totals, ["today_balance", "yes_balance"])

    if totals.empty:
        pivot = pd.DataFrame(columns=["date"])
    else:
        today = totals.pivot_table(index="date", columns="name", values="today_balance", aggfunc="last")
        yesterday = totals.pivot_table(index="date", columns="name", values="yes_balance", aggfunc="last")
        pivot = pd.DataFrame(index=today.index.union(yesterday.index))
        empty = pd.Series(index=pivot.index, dtype=float)
        today_margin = today.get("MarginPurchase", empty).reindex(pivot.index)
        yesterday_margin = yesterday.get("MarginPurchase", empty).reindex(pivot.index)
        today_short = today.get("ShortSale", empty).reindex(pivot.index)
        yesterday_short = yesterday.get("ShortSale", empty).reindex(pivot.index)
        today_amount = today.get("MarginPurchaseMoney", empty).reindex(pivot.index)
        yesterday_amount = yesterday.get("MarginPurchaseMoney", empty).reindex(pivot.index)
        pivot["margin_balance"] = today_margin
        pivot["short_balance"] = today_short
        # FinMind 的 MarginPurchaseMoney 單位為千元，頁面換算成億元。
        pivot["margin_amount"] = today_amount / 100000
        pivot["margin_balance_change"] = today_margin - yesterday_margin
        pivot["short_balance_change"] = today_short - yesterday_short
        pivot["margin_amount_change"] = (today_amount - yesterday_amount) / 100000
        pivot = pivot.reset_index(names="date")

    result = pd.DataFrame(columns=["date"])
    for frame in (prices, maintenance, pivot):
        if frame.empty:
            continue
        result = frame if result.empty else result.merge(frame, on="date", how="outer")
    if result.empty:
        return result
    return result.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)


def _records(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []
    safe = frame.copy()
    safe["date"] = safe["date"].dt.strftime("%Y-%m-%d")
    return safe.astype(object).where(pd.notna(safe), None).to_dict(orient="records")


def build_margin_maintenance_payload(start_date: str, end_date: str) -> dict:
    start, end = _parse_period(start_date, end_date)
    data = _prepare_data(*_load_frames(start, end))
    available = []
    for key, label, unit, chart_type, color in SERIES:
        if key in data.columns and data[key].notna().any():
            available.append({
                "key": key,
                "label": label,
                "unit": unit,
                "type": chart_type,
                "color": color,
            })
    price_columns = {"open", "high", "low", "close"}
    has_price = price_columns.issubset(data.columns) and data[list(price_columns)].notna().any(axis=None)
    columns = ["date"] + (["open", "high", "low", "close"] if has_price else [])
    columns += [item["key"] for item in available]
    rows = _records(data.reindex(columns=columns)) if not data.empty else []
    maintenance_values = data.get("margin_maintenance", pd.Series(dtype=float)).dropna()

    return {
        "period": {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
        "has_price": bool(has_price),
        "series": available,
        "columns": columns,
        "rows": rows,
        "summary": {
            "row_count": len(rows),
            "latest_date": rows[-1]["date"] if rows else None,
            "latest_maintenance": float(maintenance_values.iloc[-1]) if not maintenance_values.empty else None,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
