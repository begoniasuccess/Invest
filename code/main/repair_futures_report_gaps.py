from __future__ import annotations

import argparse

from main.futures import repair_futures_report_missing_fields


def main() -> None:
    parser = argparse.ArgumentParser(
        description="找出期貨報表缺漏日期，繞過來源快取重新抓取並回填"
    )
    parser.add_argument("--start", default="2024-01-01", help="開始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="結束日期 YYYY-MM-DD；預設今天")
    parser.add_argument(
        "--no-refresh-cache",
        action="store_true",
        help="修補 raw 後暫不刷新正式期貨報表快取",
    )
    args = parser.parse_args()

    result = repair_futures_report_missing_fields(
        start_date=args.start,
        end_date=args.end,
        refresh_cache=not args.no_refresh_cache,
    )
    print("\n[repair_report_gaps] 修補後資料尾端：")
    print(result.tail(20).to_string(index=False))


if __name__ == "__main__":
    main()
