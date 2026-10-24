"""手動強制重算加權指數全歷史成交金額量能欄位。

執行：python -m main.rebuild_taiex_turnover_history

本程式刻意不掛入 Dashboard 日常更新。只有人工執行時才會：
1. 修正 SQL View 內錯用成交股數的量能公式。
2. 用成交金額重算 TAIEX 全歷史衍生欄位並覆寫 core 表。
3. 完整刷新一次 Dashboard 永久快取。
4. 驗證 View 的量能數值已回到「億元」尺度。
"""
from __future__ import annotations

from datetime import datetime

from common import db
from main import stock_report_taiex
from module import stock_report_utils as SRU
from module.taiex_report_cache import CACHE_TABLE, refresh_taiex_dashboard_cache
from module.taiex_turnover_rule import (
    ensure_turnover_view,
    mark_applied,
    migrate_taiex_base_view_to_trading_money,
)


START_DATE = datetime(1999, 1, 1)
VOLUME_FIELDS = [
    "成交金額",
    "5日最大量", "10日最大量", "20日最大量", "60日最大量",
    "5日均量", "10日均量", "20日均量", "60日均量",
    "量增率_pct", "5日量增率_pct", "10日量增率_pct",
    "20日量增率_pct", "60日量增率_pct",
    "當日價量", "5日價量", "10日價量", "20日價量", "60日價量",
]


def _verify() -> None:
    fields = ", ".join(f'"{column}"' for column in VOLUME_FIELDS)
    result = db.query_to_df(
        f"""
        SELECT "日期", "成交股數", {fields}
        FROM {CACHE_TABLE}
        ORDER BY "日期" DESC
        LIMIT 10
        """
    )
    if result is None or result.empty:
        raise RuntimeError("歷史重算完成後找不到 Dashboard 快取資料")

    # 最大量與均量現在皆為億元，正常不應再出現百億股數尺度。
    check_columns = [
        "5日最大量", "10日最大量", "20日最大量", "60日最大量",
        "5日均量", "10日均量", "20日均量", "60日均量",
    ]
    maximum = result[check_columns].apply(lambda s: s.astype(float)).max().max()
    if maximum > 10_000_000:
        raise RuntimeError(
            "驗證失敗：最大量／均量仍呈現成交股數尺度，請勿把本次重算視為成功"
        )

    print("[加權量能] 最近 10 筆驗證結果：")
    print(result.to_string(index=False))


def rebuild() -> None:
    end_date = datetime.now()
    print("[加權量能] 開始獨立全歷史重算；Dashboard 日常更新不會執行本流程。")

    migrate_taiex_base_view_to_trading_money()

    base = SRU._build_base_df(stock_report_taiex.fm, "TAIEX", START_DATE, end_date)
    if base is None or base.empty:
        raise RuntimeError("無法取得 TAIEX 全歷史原始資料，未寫入任何重算結果")

    calculated = SRU._compute_derived(base)
    SRU.upsert(calculated, "TAIEX")
    SRU.recalc_long_ma("TAIEX")
    SRU.update_is_complete()

    ensure_turnover_view()
    refresh_taiex_dashboard_cache(full_rebuild=True)
    _verify()
    mark_applied()
    print("[加權量能] 全歷史成交金額量能欄位重算完成。")


if __name__ == "__main__":
    rebuild()
