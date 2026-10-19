from __future__ import annotations

import calendar
import time
from datetime import datetime

import pandas as pd

from common import db
from module.finMind import (
    build_tw_futures_kbar_1m,
    get_tw_futures_tick,
)


START_YEAR = 2019
END_YEAR = 2020

# 每個月結束後稍微停一下，避免API請求太密集。
SLEEP_SECONDS = 2


def get_month_tick_status(
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """檢查區間內每個 source_date 已有多少筆TX Tick。"""

    return db.query_to_df(
        """
        SELECT
            source_date,
            COUNT(*) AS tick_rows,
            MIN(trade_datetime) AS first_tick_at,
            MAX(trade_datetime) AS last_tick_at,
            COUNT(*) FILTER (
                WHERE trade_datetime::time >= TIME '08:45:00'
                  AND trade_datetime::time < TIME '08:46:00'
            ) AS opening_tick_rows
        FROM raw_finmind.fm_taiwan_futures_tick
        WHERE futures_id = ?
          AND source_date >= ?
          AND source_date <= ?
        GROUP BY source_date
        ORDER BY source_date
        """,
        (
            "TX",
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        ),
    )


def month_already_has_opening_ticks(
    start: datetime,
    end: datetime,
) -> bool:
    """判斷月份是否已存在有效日盤開盤Tick。"""

    status = get_month_tick_status(start, end)

    if status is None or status.empty:
        return False

    opening_days = pd.to_numeric(
        status["opening_tick_rows"],
        errors="coerce",
    ).fillna(0)

    return bool((opening_days > 0).any())


def rebuild_month_kbar(
    start: datetime,
    end: datetime,
) -> None:
    """從現有Tick重建該月一分鐘K。"""

    print(f"[K棒重建] {start:%Y-%m-%d}～{end:%Y-%m-%d}")

    result = build_tw_futures_kbar_1m(
        "TX",
        start,
        end,
    )

    print(result)


def repair_month(
    start: datetime,
    end: datetime,
) -> None:
    """只下載資料庫尚未儲存的日期。"""

    print()
    print("=" * 72)
    print(f"檢查月份：{start:%Y-%m-%d}～{end:%Y-%m-%d}")
    print("=" * 72)

    status = get_month_tick_status(start, end)

    if status is not None and not status.empty:
        stored_days = len(status)
        opening_days = (
            pd.to_numeric(
                status["opening_tick_rows"],
                errors="coerce",
            )
            .fillna(0)
            .gt(0)
            .sum()
        )

        print(
            f"資料庫現有 source_date={stored_days} 天，"
            f"其中有日盤開盤Tick={opening_days} 天"
        )
    else:
        print("本月份目前沒有Tick")

    # force_refresh=False：
    # get_tw_futures_tick會查詢資料庫既有source_date，
    # 已存在的日期回傳cached，只有缺少的日期才呼叫API。
    tick_result = get_tw_futures_tick(
        "TX",
        start,
        end,
        force_refresh=False,
    )

    if tick_result is not None and not tick_result.empty:
        print(
            tick_result.groupby("status", dropna=False)
            .agg(
                days=("date", "size"),
                rows=("rows", "sum"),
            )
            .reset_index()
        )

        stored = tick_result[
            tick_result["status"].eq("stored")
        ]

        if stored.empty:
            print("這個月沒有新增Tick，只重建現有K棒")
        else:
            print(f"本次新增／更新 {len(stored)} 個source_date")

    rebuild_month_kbar(start, end)


def main() -> None:
    failed_months: list[dict] = []

    try:
        for year in range(START_YEAR, END_YEAR + 1):
            for month in range(1, 13):
                last_day = calendar.monthrange(year, month)[1]

                start = datetime(year, month, 1)
                end = datetime(year, month, last_day)

                try:
                    repair_month(start, end)

                except KeyboardInterrupt:
                    print()
                    print("收到Ctrl+C，安全停止。")
                    print("已完成日期仍保留，下次執行會自動跳過。")
                    raise

                except Exception as exc:
                    failed_months.append(
                        {
                            "year": year,
                            "month": month,
                            "error": repr(exc),
                        }
                    )

                    print(
                        f"[失敗] {year}-{month:02d}："
                        f"{exc!r}"
                    )

                time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        pass

    print()
    print("=" * 72)
    print("本次補資料程序結束")

    if failed_months:
        print("本次發生錯誤的月份：")

        for item in failed_months:
            print(
                f"{item['year']}-{item['month']:02d}："
                f"{item['error']}"
            )
    else:
        print("本次沒有記錄到月份錯誤")


if __name__ == "__main__":
    main()