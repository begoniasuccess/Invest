from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
SIGNAL_COLUMN = "策略_核心_五日價量"
GROUPS = ("多", "空")
METRICS = {
    "close_return_pct": "平均漲跌幅_pct",
    "high_return_pct": "最高點漲跌幅_pct",
    "low_return_pct": "最低點漲跌幅_pct",
}


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    alpha: float = 0.05

    def __post_init__(self) -> None:
        if self.end is not None and self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if not 0 < self.alpha < 1:
            raise ValueError("alpha 必須介於 0 與 1 之間")


def load_source(settings: Settings) -> pd.DataFrame:
    """以 LEAD 對齊下一個實際存在的交易日，再篩訊號日期。"""
    from common import db

    end_filter = "AND signal_date <= ?" if settings.end else ""
    params: tuple[object, ...] = (
        (settings.futures_id, settings.start, settings.end)
        if settings.end
        else (settings.futures_id, settings.start)
    )
    sql = f'''
        WITH ordered AS (
            SELECT
                futures_id,
                "日期" AS signal_date,
                TRIM("{SIGNAL_COLUMN}"::text) AS core_direction,
                "日盤收" AS signal_day_close,
                "夜盤收" AS signal_night_close,
                LEAD("日期") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_date,
                LEAD("日盤高") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_high,
                LEAD("日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_low,
                LEAD("日盤收") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_close
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        )
        SELECT *
        FROM ordered
        WHERE signal_date >= ?
          {end_filter}
          AND core_direction IN ('多', '空')
        ORDER BY signal_date
    '''
    return db.sql_to_df(
        sql, params=params, parse_dates=["signal_date", "target_date"]
    )


def _safe_return(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float).div(denominator.astype(float).replace(0, np.nan)) * 100.0


def prepare_detail(source: pd.DataFrame) -> pd.DataFrame:
    raw = source.copy()
    numeric = [
        "signal_day_close", "signal_night_close", "target_day_high",
        "target_day_low", "target_day_close",
    ]
    for column in numeric:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw["core_direction"] = raw["core_direction"].astype("string").str.strip()
    raw = raw[raw["core_direction"].isin(GROUPS)].copy()

    frames: list[pd.DataFrame] = []
    for base_name, base_label in (
        ("signal_day_close", "前一日日盤收"),
        ("signal_night_close", "前一日夜盤收"),
    ):
        part = raw.dropna(
            subset=["target_date", base_name, "target_day_high", "target_day_low", "target_day_close"]
        ).copy()
        base = part[base_name]
        part["基準"] = base_label
        part["基準價"] = base
        part["close_return_pct"] = _safe_return(part["target_day_close"] - base, base)
        part["high_return_pct"] = _safe_return(part["target_day_high"] - base, base)
        part["low_return_pct"] = _safe_return(part["target_day_low"] - base, base)
        part["up"] = part["target_day_close"] > base
        frames.append(part)

    detail = pd.concat(frames, ignore_index=True)
    labels = {
        "signal_date": "訊號日", "target_date": "隔日日盤日期",
        "core_direction": SIGNAL_COLUMN, "target_day_high": "隔日日盤高",
        "target_day_low": "隔日日盤低", "target_day_close": "隔日日盤收",
        "close_return_pct": "收盤漲跌幅_pct", "high_return_pct": "最高點漲跌幅_pct",
        "low_return_pct": "最低點漲跌幅_pct", "up": "上漲",
    }
    columns = [
        "訊號日", "隔日日盤日期", SIGNAL_COLUMN, "基準", "基準價",
        "隔日日盤高", "隔日日盤低", "隔日日盤收", "收盤漲跌幅_pct",
        "最高點漲跌幅_pct", "最低點漲跌幅_pct", "上漲",
    ]
    return detail.rename(columns=labels)[columns]


def calculate_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for base, base_frame in detail.groupby("基準", sort=False):
        for direction in GROUPS:
            frame = base_frame[base_frame[SIGNAL_COLUMN] == direction]
            rows.append({
                "基準": base,
                SIGNAL_COLUMN: direction,
                "樣本數": len(frame),
                "平均漲跌幅_pct": frame["收盤漲跌幅_pct"].mean(),
                "上漲次數": int(frame["上漲"].sum()),
                "上漲機率_pct": frame["上漲"].mean() * 100.0,
                "最高點漲跌幅_pct": frame["最高點漲跌幅_pct"].mean(),
                "最低點漲跌幅_pct": frame["最低點漲跌幅_pct"].mean(),
            })
    return pd.DataFrame(rows).round(6)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _two_proportion_z_test(up_a: int, n_a: int, up_b: int, n_b: int) -> tuple[float, float]:
    if min(n_a, n_b) == 0:
        return math.nan, math.nan
    pooled = (up_a + up_b) / (n_a + n_b)
    se = math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    if se == 0:
        return 0.0, 1.0
    z = (up_a / n_a - up_b / n_b) / se
    return z, 2 * (1 - _normal_cdf(abs(z)))


def _bh_adjust(p_values: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=p_values.index, dtype=float)
    clean = p_values.dropna().sort_values()
    if clean.empty:
        return result
    adjusted = (clean * len(clean) / np.arange(1, len(clean) + 1))[::-1].cummin()[::-1]
    result.loc[clean.index] = adjusted.clip(upper=1.0)
    return result


def calculate_tests(detail: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    try:
        from scipy.stats import mannwhitneyu, ttest_ind
    except ImportError as exc:
        raise RuntimeError("統計檢定需要 scipy；請先執行 pip install scipy") from exc

    rows: list[dict[str, object]] = []
    for base, frame in detail.groupby("基準", sort=False):
        long_group = frame[frame[SIGNAL_COLUMN] == "多"]
        short_group = frame[frame[SIGNAL_COLUMN] == "空"]
        for column, label in (
            ("收盤漲跌幅_pct", "平均漲跌幅"),
            ("最高點漲跌幅_pct", "最高點漲跌幅"),
            ("最低點漲跌幅_pct", "最低點漲跌幅"),
        ):
            a = pd.to_numeric(long_group[column], errors="coerce").dropna()
            b = pd.to_numeric(short_group[column], errors="coerce").dropna()
            t_stat, t_p = ttest_ind(a, b, equal_var=False, nan_policy="omit")
            u_stat, u_p = mannwhitneyu(a, b, alternative="two-sided")
            rows.append({
                "基準": base, "統計量": label, "檢定": "Welch t（平均數）",
                "多樣本數": len(a), "空樣本數": len(b),
                "多群數值": a.mean(), "空群數值": b.mean(),
                "多減空差異": a.mean() - b.mean(), "檢定統計值": t_stat, "p值": t_p,
            })
            rows.append({
                "基準": base, "統計量": label, "檢定": "Mann-Whitney U（分布）",
                "多樣本數": len(a), "空樣本數": len(b),
                "多群數值": a.median(), "空群數值": b.median(),
                "多減空差異": a.median() - b.median(), "檢定統計值": u_stat, "p值": u_p,
            })

        n_a, n_b = len(long_group), len(short_group)
        up_a, up_b = int(long_group["上漲"].sum()), int(short_group["上漲"].sum())
        z_stat, z_p = _two_proportion_z_test(up_a, n_a, up_b, n_b)
        rate_a = up_a / n_a * 100 if n_a else math.nan
        rate_b = up_b / n_b * 100 if n_b else math.nan
        rows.append({
            "基準": base, "統計量": "上漲機率", "檢定": "兩比例 z（比例）",
            "多樣本數": n_a, "空樣本數": n_b, "多群數值": rate_a,
            "空群數值": rate_b, "多減空差異": rate_a - rate_b,
            "檢定統計值": z_stat, "p值": z_p,
        })

    tests = pd.DataFrame(rows)
    tests["BH校正p值"] = _bh_adjust(tests["p值"])
    tests["原始顯著"] = tests["p值"] < alpha
    tests["校正後顯著"] = tests["BH校正p值"] < alpha
    return tests.round(8)


def export_reports(
    detail: pd.DataFrame, summary: pd.DataFrame, tests: pd.DataFrame,
    settings: Settings, output_dir: Path,
) -> tuple[Path, Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = settings.end or pd.to_datetime(detail["訊號日"]).max().date()
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    detail_path = output_dir / f"core_five_day_next_day_detail_{suffix}.csv"
    summary_path = output_dir / f"core_five_day_next_day_summary_{suffix}.csv"
    tests_path = output_dir / f"core_five_day_next_day_tests_{suffix}.csv"
    manifest_path = output_dir / f"core_five_day_next_day_manifest_{suffix}.json"
    detail.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    tests.to_csv(tests_path, index=False, encoding="utf-8-sig")
    manifest = {
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None},
        "source_view": SOURCE_VIEW,
        "signal_column": SIGNAL_COLUMN,
        "alignment": "訊號日後的下一個實際交易日（SQL LEAD）",
        "return_formula": "(隔日日盤價-基準價)/基準價*100",
        "up_definition": "隔日日盤收 > 基準價；平盤不計上漲",
        "multiple_testing": "所有檢定一起使用 Benjamini-Hochberg 校正",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return detail_path, summary_path, tests_path, manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="五日核心多空與隔日日盤統計及顯著性檢定")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("backtest_output/futures_core_five_day_next_day_statistics"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(args.start, args.end, args.futures_id, args.alpha)
    detail = prepare_detail(load_source(settings))
    if detail.empty:
        raise SystemExit("查無符合條件的有效資料")
    summary = calculate_summary(detail)
    tests = calculate_tests(detail, settings.alpha)
    paths = export_reports(detail, summary, tests, settings, args.output_dir)
    print("\n[摘要]")
    print(summary.to_string(index=False))
    print("\n[顯著性檢定]（多減空；BH 校正後顯著才建議視為較可靠）")
    print(tests.to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
