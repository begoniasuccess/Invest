from __future__ import annotations

import argparse
from datetime import date

from module.margin_maintenance import update_taiwan_total_margin_maintenance


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="增量下載 FinMind 台灣大盤融資維持率並寫入 PostgreSQL。"
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="下載截止日，格式 YYYY-MM-DD；預設今天。",
    )
    parser.add_argument(
        "--floor-date",
        default="2001-01-05",
        help="首次下載最早日期；預設 FinMind 可用起點 2001-01-05。",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="日常更新回看天數，用於承接延遲發布或修訂；預設 7 天。",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = update_taiwan_total_margin_maintenance(
        args.end_date,
        floor_date=args.floor_date,
        lookback_days=args.lookback_days,
    )
    print(
        "更新完成："
        f"requested={result.requested_start}~{result.requested_end}, "
        f"rows={result.rows}, written={result.written_rows}, "
        f"data={result.first_date}~{result.last_date}"
    )


if __name__ == "__main__":
    main()
