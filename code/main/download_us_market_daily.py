from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from module.us_market import (  # noqa: E402
    CORE_US_MARKET_SYMBOLS,
    DEFAULT_EXPORT_DIR,
    RECOMMENDED_US_MARKET_SYMBOLS,
    download_us_market_daily,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="下載 FinMind USStockPrice（日資料）並可寫入 PostgreSQL/CSV。"
    )
    parser.add_argument("--start", default="2010-01-01", help="開始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=date.today().isoformat(), help="結束日期 YYYY-MM-DD")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="自訂代碼，例如 --symbols ^IXIC ^SOX ^DJI ^GSPC。省略時使用 preset。",
    )
    parser.add_argument(
        "--preset",
        choices=["core", "recommended"],
        default="core",
        help="core=四大指數；recommended=四大指數+VIX+TSM ADR。",
    )
    parser.add_argument(
        "--csv-dir",
        default=str(DEFAULT_EXPORT_DIR),
        help="CSV 輸出資料夾；預設為專案 data/FinMind/US/StockPrice。",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="只下載 CSV，不寫 PostgreSQL。",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="只寫 PostgreSQL，不輸出 CSV。",
    )
    parser.add_argument(
        "--chunk-years",
        type=int,
        default=5,
        help="每次 FinMind API 請求切分年數，預設 5 年。",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.symbols:
        symbols = args.symbols
    elif args.preset == "recommended":
        symbols = list(CORE_US_MARKET_SYMBOLS) + list(RECOMMENDED_US_MARKET_SYMBOLS)
    else:
        symbols = list(CORE_US_MARKET_SYMBOLS)

    print("FinMind USStockPrice")
    print(f"日期：{args.start} ~ {args.end}")
    print(f"代碼：{', '.join(symbols)}")
    print(f"寫入 DB：{not args.no_db}")
    print(f"輸出 CSV：{not args.no_csv}")

    _, summaries, written = download_us_market_daily(
        symbols,
        args.start,
        args.end,
        save_db=not args.no_db,
        csv_dir=None if args.no_csv else args.csv_dir,
        chunk_years=args.chunk_years,
    )

    print("\n=== 完成摘要 ===")
    for item in summaries:
        print(
            f"{item.stock_id:8s} rows={item.rows:6,d} "
            f"actual={item.first_date} ~ {item.last_date}"
        )

    if written:
        print("\nCSV：")
        for path in written:
            print(f"  {path}")

    print(
        "\n注意：資料表 date 保留美國交易日。之後做台股隔日研究時，"
        "應映射到『下一個台灣交易日』，不要用相同 calendar date 直接 JOIN。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
