"""從專案 PostgreSQL 報表資料直接統計台指期／加權指數星期效應。

需從 code 專案根目錄執行，沿用 common.db 的 POSTGRES_DSN 連線設定，
並沿用 dashboard.data_service 的市場來源、昨收漲跌及日內振幅算法。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from dashboard.config import MARKETS
from dashboard.cycle_service import build_cycle_statistics
from dashboard.data_service import filter_period, load_market_snapshot

WEEKDAY_ORDER = ["星期一", "星期二", "星期三", "星期四", "星期五"]


def _weekday_amplitude(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["星期"] = work["日期"].dt.weekday.map(
        {0: "星期一", 1: "星期二", 2: "星期三", 3: "星期四", 4: "星期五"}
    )
    rows: list[dict] = []
    for weekday in WEEKDAY_ORDER:
        group = work.loc[work["星期"] == weekday]
        pct = group["range_prev_pct"].dropna()
        points = group["range_points"].dropna()
        rows.append(
            {
                "星期": weekday,
                "樣本數_振幅": int(pct.size),
                "平均日內振幅_點": float(points.mean()) if len(points) else np.nan,
                "平均日內振幅_pct昨收": float(pct.mean()) if len(pct) else np.nan,
                "日內振幅_pct昨收_中位數": float(pct.median()) if len(pct) else np.nan,
                "日內振幅_pct昨收_P90": float(pct.quantile(0.90)) if len(pct) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _weekday_report(frame: pd.DataFrame) -> pd.DataFrame:
    direction, _ = build_cycle_statistics(frame, "weekday")
    direction = direction.rename(columns={"週期": "星期"})
    amplitude = _weekday_amplitude(frame)
    return direction.merge(amplitude, on="星期", how="left")


def _build_outputs(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    overall = _weekday_report(frame)

    annual_parts: list[pd.DataFrame] = []
    for year, year_frame in frame.groupby(frame["日期"].dt.year, sort=True):
        part = _weekday_report(year_frame)
        part.insert(0, "年", int(year))
        annual_parts.append(part)
    annual = pd.concat(annual_parts, ignore_index=True) if annual_parts else pd.DataFrame()

    comparison = overall.loc[overall["星期"].isin(["星期三", "星期五"])].copy()
    comparison.insert(0, "市場觀察", comparison["星期"].map({"星期三": "週三", "星期五": "週五"}))
    return overall, annual, comparison


def main() -> int:
    parser = argparse.ArgumentParser(
        description="直接從專案 PostgreSQL 資料庫統計星期漲跌與日內振幅。"
    )
    parser.add_argument(
        "--market", choices=sorted(MARKETS), default="tx_day",
        help="市場：taiex、tx_day、tx_night、tx_full；預設 tx_day。",
    )
    parser.add_argument("--start", default="2018-01-01", help="統計起日，含當日。")
    parser.add_argument("--end", default=None, help="統計迄日，含當日；省略為資料庫最新日。")
    parser.add_argument(
        "--output-dir", default=None,
        help="輸出資料夾；預設為專案根目錄/output/analysis/futures_weekday_settlement_analysis。",
    )
    args = parser.parse_args()

    snapshot = load_market_snapshot(args.market)
    end_date = args.end or snapshot.source_max_date.strftime("%Y-%m-%d")
    frame = filter_period(snapshot.data, args.start, end_date)
    if frame.empty:
        parser.error(f"{MARKETS[args.market]}在指定日期區間沒有可用資料。")

    overall, annual, comparison = _build_outputs(frame)
    project_root = Path(__file__).resolve().parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else (
        project_root / "output" / "analysis" / "futures_weekday_settlement_analysis"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    overall_path = output_dir / "weekday_overall.csv"
    annual_path = output_dir / "weekday_by_year.csv"
    comparison_path = output_dir / "wednesday_friday_comparison.csv"
    overall.to_csv(overall_path, index=False, encoding="utf-8-sig", float_format="%.6f")
    annual.to_csv(annual_path, index=False, encoding="utf-8-sig", float_format="%.6f")
    comparison.to_csv(comparison_path, index=False, encoding="utf-8-sig", float_format="%.6f")

    print(f"市場：{MARKETS[args.market]}")
    print(f"來源資料日期：{snapshot.source_min_date:%Y-%m-%d}～{snapshot.source_max_date:%Y-%m-%d}")
    print(f"統計期間：{args.start}～{end_date}；有效樣本：{len(frame):,}；排除異常 OHLC：{snapshot.invalid_ohlc_count:,}")
    print("\n星期整體統計（方向口徑與儀表板星期週期統計一致；振幅為最高－最低）：")
    print(overall.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print("\n週三、週五比較：")
    print(comparison.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print(f"\n年別統計：{annual_path}")
    print(f"整體統計：{overall_path}")
    print(f"週三／週五表：{comparison_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
