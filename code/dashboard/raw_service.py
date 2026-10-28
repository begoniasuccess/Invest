from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from common.report_columns import order_futures_report_columns, order_taiex_report_columns


@dataclass(frozen=True)
class RawReportConfig:
    name: str
    view: str
    date_column: str = "日期"
    where_sql: str = ""
    where_params: tuple = ()


TAIWAN_RAW_REPORTS = {
    # 讀有日期索引的實體快取，避免每次載入／翻頁都重跑多層策略 View。
    "taiex": RawReportConfig("加權指數報表", "taiex_report_dashboard_cache_v2"),
    # 直接讀有索引的實體結果快取，避免每次載入重跑 corrected View 的
    # 全歷史 LAG 視窗與策略函式。快取由期貨報表更新流程同步。
    "futures": RawReportConfig("台指期報表", "v_futures_dashboard_turning"),
}

US_RAW_REPORTS = {
    "us_ixic": RawReportConfig("NASDAQ 原始資料", "fm_us_stock_daily", "date", "stock_id = ?", ("^IXIC",)),
    "us_sox": RawReportConfig("費城半導體原始資料", "fm_us_stock_daily", "date", "stock_id = ?", ("^SOX",)),
    "us_dji": RawReportConfig("道瓊原始資料", "fm_us_stock_daily", "date", "stock_id = ?", ("^DJI",)),
    "us_gspc": RawReportConfig("S&P 500 原始資料", "fm_us_stock_daily", "date", "stock_id = ?", ("^GSPC",)),
    "us_vix": RawReportConfig("VIX 原始資料", "fm_us_stock_daily", "date", "stock_id = ?", ("^VIX",)),
    "us_tsm": RawReportConfig("TSM ADR 原始資料", "fm_us_stock_daily", "date", "stock_id = ?", ("TSM",)),
}

RAW_REPORTS = {**TAIWAN_RAW_REPORTS, **US_RAW_REPORTS}

PAGE_SIZES = (10, 30, 60, 120, 240)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def get_report_config(report: str) -> RawReportConfig:
    try:
        return RAW_REPORTS[report]
    except KeyError as exc:
        raise ValueError("不支援的原始資料報表") from exc


def load_raw_report(
    report: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 30,
    offset: int = 0,
    sort_column: str | None = None,
    sort_direction: str = "desc",
    search_column: str | None = None,
    search_text: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    from common import db

    config = get_report_config(report)
    if report == "taiex":
        from module.taiex_report_cache import taiex_dashboard_cache_available
        if not taiex_dashboard_cache_available():
            raise ValueError("加權報表快取尚未初始化；請先按一次「期貨報表更新」並勾選加權報表。")
    if report == "futures" and db.query_single_value("SELECT to_regclass('report.v_futures_dashboard_turning')") is None:
        from module.futures_turning import ensure_turning_views
        ensure_turning_views()
    if limit not in PAGE_SIZES:
        raise ValueError("不支援的顯示筆數")
    if offset < 0:
        raise ValueError("資料位置不可小於零")

    empty_frame = db.query_to_df(f'SELECT * FROM "{config.view}" LIMIT 0')
    columns = list(empty_frame.columns)
    if report == "taiex":
        columns = order_taiex_report_columns(columns)
    elif report == "futures":
        columns = order_futures_report_columns(columns)
    if not columns:
        raise ValueError("報表目前沒有可用欄位")
    sort_column = sort_column or config.date_column
    sort_direction = sort_direction.lower()
    if sort_column not in columns:
        raise ValueError("找不到指定的排序欄位")
    if sort_direction not in {"asc", "desc"}:
        raise ValueError("排序方向只支援 asc 或 desc")
    search_text = (search_text or "").strip()
    if search_text and search_column not in columns:
        raise ValueError("找不到指定的搜尋欄位")

    source_where = f"WHERE {config.where_sql}" if config.where_sql else ""
    bounds = db.query_to_df(
        f'SELECT MIN("{config.date_column}") AS min_date, MAX("{config.date_column}") AS max_date '
        f'FROM "{config.view}" {source_where}',
        config.where_params,
    )
    min_date = pd.Timestamp(bounds.iloc[0]["min_date"]).strftime("%Y-%m-%d")
    max_date = pd.Timestamp(bounds.iloc[0]["max_date"]).strftime("%Y-%m-%d")
    start_date = start_date or min_date
    end_date = end_date or max_date
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        raise ValueError("開始日期不可晚於結束日期")

    conditions = []
    params: list = []
    if config.where_sql:
        conditions.append(config.where_sql)
        params.extend(config.where_params)
    conditions.append(f'"{config.date_column}" BETWEEN ? AND ?')
    params.extend([start_date, end_date])
    if search_text:
        conditions.append(
            f'CAST({_quote_identifier(search_column)} AS TEXT) ILIKE ? ESCAPE \'\\\''
        )
        params.append(f"%{_escape_like(search_text)}%")
    where = "WHERE " + " AND ".join(conditions)
    total = int(db.query_single_value(f'SELECT COUNT(*) FROM "{config.view}" {where}', params) or 0)
    order_sql = f'{_quote_identifier(sort_column)} {sort_direction.upper()} NULLS LAST'
    if sort_column != config.date_column:
        order_sql += f', "{config.date_column}" DESC'
    select_columns = ", ".join(_quote_identifier(column) for column in columns)
    frame = db.query_to_df(
        f'SELECT {select_columns} FROM "{config.view}" {where} '
        f'ORDER BY {order_sql} LIMIT ? OFFSET ?',
        (*params, limit, offset),
    )
    return frame, {
        "key": report,
        "name": config.name,
        "min_date": min_date,
        "max_date": max_date,
        "start_date": start_date,
        "end_date": end_date,
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort_column": sort_column,
        "sort_direction": sort_direction,
        "search_column": search_column if search_text else "",
        "search_text": search_text,
    }


def export_raw_report(
    report: str,
    start_date: str | None = None,
    end_date: str | None = None,
    columns: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    from common import db

    config = get_report_config(report)
    if report == "taiex":
        from module.taiex_report_cache import taiex_dashboard_cache_available
        if not taiex_dashboard_cache_available():
            raise ValueError("加權報表快取尚未初始化；請先按一次「期貨報表更新」並勾選加權報表。")
    if report == "futures" and db.query_single_value("SELECT to_regclass('report.v_futures_dashboard_turning')") is None:
        from module.futures_turning import ensure_turning_views
        ensure_turning_views()
    source_where = f"WHERE {config.where_sql}" if config.where_sql else ""
    bounds = db.query_to_df(
        f'SELECT MIN("{config.date_column}") AS min_date, MAX("{config.date_column}") AS max_date '
        f'FROM "{config.view}" {source_where}',
        config.where_params,
    )
    min_date = pd.Timestamp(bounds.iloc[0]["min_date"]).strftime("%Y-%m-%d")
    max_date = pd.Timestamp(bounds.iloc[0]["max_date"]).strftime("%Y-%m-%d")
    start_date = start_date or min_date
    end_date = end_date or max_date
    conditions = []
    params: list = []
    if config.where_sql:
        conditions.append(config.where_sql)
        params.extend(config.where_params)
    conditions.append(f'"{config.date_column}" BETWEEN ? AND ?')
    params.extend([start_date, end_date])
    frame = db.query_to_df(
        f'SELECT * FROM "{config.view}" WHERE {" AND ".join(conditions)} '
        f'ORDER BY "{config.date_column}" DESC',
        params,
    )
    if report == "taiex":
        frame = frame.loc[:, order_taiex_report_columns(list(frame.columns))]
    elif report == "futures":
        frame = frame.loc[:, order_futures_report_columns(list(frame.columns))]
    if columns:
        unknown = [column for column in columns if column not in frame.columns]
        if unknown:
            raise ValueError(f"找不到指定欄位：{', '.join(unknown)}")
        frame = frame.loc[:, columns]
    return frame, {
        "key": report,
        "name": config.name,
        "start_date": start_date,
        "end_date": end_date,
    }
