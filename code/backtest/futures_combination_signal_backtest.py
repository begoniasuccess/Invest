from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from scipy.stats import binomtest, ttest_1samp

from backtest.futures_raw_signal_strength_scan import _rolling_percentile
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
ANALYSIS_NAME = "futures_combination_signal_backtest"
VERSION = "1.1.0"


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    core_column: str = "策略_核心_五日價量"
    history_window: int = 252
    minimum_history: int = 60
    extreme_low_pct: float = 20.0
    extreme_high_pct: float = 80.0
    night_close_low_pct: float = 25.0
    night_close_high_pct: float = 75.0
    round_trip_cost_points: float = 3.0
    point_value_twd: float = 200.0
    minimum_group_size: int = 30

    def __post_init__(self) -> None:
        if self.end and self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if self.core_column not in {"策略_核心_昨價量", "策略_核心_五日價量"}:
            raise ValueError("core_column 只能使用昨價量或五日價量核心")
        if not 10 <= self.minimum_history <= self.history_window:
            raise ValueError("minimum_history 必須介於 10 與 history_window 之間")
        if not 0 <= self.extreme_low_pct < self.extreme_high_pct <= 100:
            raise ValueError("極端百分位設定錯誤")
        if not 0 <= self.night_close_low_pct < self.night_close_high_pct <= 100:
            raise ValueError("夜盤收盤位置門檻錯誤")
        if self.round_trip_cost_points < 0 or self.point_value_twd <= 0:
            raise ValueError("成本不可為負且每點價值必須大於 0")


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").astype(float)


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(pd.NA, index=frame.index, dtype="string")
    return frame[column].astype("string").str.strip()


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.replace(0, np.nan))


def load_source(settings: Settings) -> pd.DataFrame:
    from common import db

    query_start = settings.start - timedelta(days=max(400, settings.history_window * 2))
    sql = f'''
        SELECT
            futures_id, "日期", "{settings.core_column}",
            "夜盤走勢", "夜盤開", "夜盤高", "夜盤低", "夜盤收",
            "日盤開", "日盤高", "日盤低", "日盤收",
            "日盤轉折支撐", "日盤轉折壓力",
            "夜盤轉折支撐", "夜盤轉折壓力",
            "全位置", "全5MA乖離率", "全20均上升幅度_pct",
            "法人總買超_億", "散戶多空比_微台", "散戶多空比_小台",
            "主力新建多單"
        FROM {SOURCE_VIEW}
        WHERE futures_id = ? AND "日期" >= ?
    '''
    params: list[object] = [settings.futures_id, query_start]
    if settings.end:
        sql += ' AND "日期" <= ?'
        params.append(settings.end + timedelta(days=14))
    sql += ' ORDER BY "日期"'
    return db.sql_to_df(sql, params=tuple(params), parse_dates=["日期"])


def prepare_daily(source: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """建立當時可知訊號與下一交易日日盤結果；rolling 百分位不含當日。"""
    daily = source.sort_values("日期").drop_duplicates("日期", keep="last").reset_index(drop=True).copy()
    daily["訊號日"] = pd.to_datetime(daily["日期"])
    daily["核心方向"] = _text(daily, settings.core_column)
    daily["夜盤走勢"] = _text(daily, "夜盤走勢")
    daily["位置"] = _text(daily, "全位置")

    night_open, night_high = _num(daily, "夜盤開"), _num(daily, "夜盤高")
    night_low, night_close = _num(daily, "夜盤低"), _num(daily, "夜盤收")
    day_close = _num(daily, "日盤收")
    daily["夜盤漲跌_pct"] = _safe_divide(night_close - day_close, day_close) * 100.0
    daily["夜盤開收_pct"] = _safe_divide(night_close - night_open, night_open) * 100.0
    daily["夜盤收盤位置_pct"] = _safe_divide(night_close - night_low, night_high - night_low) * 100.0

    retail_micro = _num(daily, "散戶多空比_微台")
    daily["散戶多空比"] = retail_micro.combine_first(_num(daily, "散戶多空比_小台"))
    daily["散戶多空比_微台"] = retail_micro
    daily["法人總買超_億"] = _num(daily, "法人總買超_億")
    daily["主力新建多單"] = _num(daily, "主力新建多單")
    daily["MA5乖離率_pct"] = _num(daily, "全5MA乖離率")
    daily["MA20斜率_pct"] = _num(daily, "全20均上升幅度_pct")

    for column in (
        "夜盤漲跌_pct", "散戶多空比", "散戶多空比_微台",
        "主力新建多單", "MA5乖離率_pct",
    ):
        daily[f"{column}_歷史分位_pct"] = _rolling_percentile(
            daily[column], settings.history_window, settings.minimum_history
        )

    daily["隔日日期"] = daily["訊號日"].shift(-1)
    for source_column, output_column in (
        ("日盤開", "隔日日盤開"), ("日盤高", "隔日日盤高"),
        ("日盤低", "隔日日盤低"), ("日盤收", "隔日日盤收"),
    ):
        daily[output_column] = _num(daily, source_column).shift(-1)
    daily["隔日開收點數"] = daily["隔日日盤收"] - daily["隔日日盤開"]
    daily["隔日開收報酬_pct"] = (
        _safe_divide(daily["隔日開收點數"], daily["隔日日盤開"]) * 100.0
    )
    daily["隔日收盤位置_pct"] = (
        _safe_divide(
            daily["隔日日盤收"] - daily["隔日日盤低"],
            daily["隔日日盤高"] - daily["隔日日盤低"],
        ) * 100.0
    )
    daily["隔日年"] = daily["隔日日期"].dt.year
    return daily.loc[
        daily["隔日日期"].between(pd.Timestamp(settings.start), pd.Timestamp(settings.end or date.today()))
    ].reset_index(drop=True)


def _is_low_position(series: pd.Series) -> pd.Series:
    return series.fillna("").str.contains("低", regex=False)


def _is_high_position(series: pd.Series) -> pd.Series:
    return series.fillna("").str.contains("高", regex=False)


def _add_signals(rows: list[dict], daily: pd.DataFrame, mask: pd.Series, family: str,
                 signal: str, direction: str, rationale: str) -> None:
    selected = daily.loc[mask.fillna(False)].copy()
    if selected.empty:
        return
    selected["組合家族"] = family
    selected["組合訊號"] = signal
    selected["交易方向"] = direction
    selected["組合邏輯"] = rationale
    rows.extend(selected.to_dict("records"))


def build_signals(daily: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """同一天可同時進入不同研究家族；每個家族都是獨立假設，不互相覆蓋。"""
    rows: list[dict] = []
    core_long, core_short = daily["核心方向"].eq("多"), daily["核心方向"].eq("空")
    night_up, night_down = daily["夜盤走勢"].eq("漲勢盤"), daily["夜盤走勢"].eq("跌勢盤")
    night_pct = daily["夜盤漲跌_pct_歷史分位_pct"]
    low, high = settings.extreme_low_pct, settings.extreme_high_pct
    close_loc = daily["夜盤收盤位置_pct"]
    pos_low, pos_high = _is_low_position(daily["位置"]), _is_high_position(daily["位置"])
    retail = daily["散戶多空比_歷史分位_pct"]
    micro = daily["散戶多空比_微台_歷史分位_pct"]
    main_long = daily["主力新建多單_歷史分位_pct"]
    ma5_bias = daily["MA5乖離率_pct"]
    ma5_bias_pct = daily["MA5乖離率_pct_歷史分位_pct"]
    institution_buy = daily["法人總買超_億"] > 0
    institution_sell = daily["法人總買超_億"] < 0
    retail_bullish = daily["散戶多空比"] < 0
    retail_bearish = daily["散戶多空比"] > 0

    # A. 核心方向遇夜盤反向：測回檔後延續核心方向。
    _add_signals(rows, daily, core_long & night_down, "A核心×夜盤反向", "核心多＋夜盤跌", "LONG", "夜盤回檔後順核心做多")
    _add_signals(rows, daily, core_short & night_up, "A核心×夜盤反向", "核心空＋夜盤漲", "SHORT", "夜盤反彈後順核心做空")

    # B. 夜盤已極端反映：反向測試，不與 A 混為同一策略。
    _add_signals(rows, daily, core_long & (night_pct >= high), "B夜盤極端反轉", "核心多＋夜盤極端漲", "SHORT", "測試多方行情已由夜盤提前反映")
    _add_signals(rows, daily, core_short & (night_pct <= low), "B夜盤極端反轉", "核心空＋夜盤極端跌", "LONG", "測試空方行情已由夜盤提前反映")

    # C. 在 A 上要求夜盤末段拉回／回落。
    _add_signals(rows, daily, core_long & night_down & (close_loc >= settings.night_close_low_pct), "C夜盤收盤位置確認", "核心多＋夜跌＋未收最低區", "LONG", "跌勢盤但收盤脫離最低四分位")
    _add_signals(rows, daily, core_short & night_up & (close_loc <= settings.night_close_high_pct), "C夜盤收盤位置確認", "核心空＋夜漲＋未收最高區", "SHORT", "漲勢盤但收盤脫離最高四分位")

    # D. 核心方向與大盤位置。
    _add_signals(rows, daily, core_long & pos_low, "D核心×位置", "核心多＋低檔", "LONG", "低檔多方核心")
    _add_signals(rows, daily, core_short & pos_high, "D核心×位置", "核心空＋高檔", "SHORT", "高檔空方核心")

    # E. 散戶極端反向；合併欄與純微台分開保留。
    for percentile, label in ((retail, "綜合散戶"), (micro, "純微台散戶")):
        _add_signals(rows, daily, core_long & (percentile <= low), f"E核心×{label}極端", f"核心多＋{label}極度偏空", "LONG", "散戶極端反向支持核心多")
        _add_signals(rows, daily, core_short & (percentile >= high), f"E核心×{label}極端", f"核心空＋{label}極度偏多", "SHORT", "散戶極端反向支持核心空")

    # F. 法人方向與散戶反向共振。
    _add_signals(rows, daily, core_long & institution_buy & retail_bullish, "F籌碼共振", "核心多＋法人買＋散戶偏空", "LONG", "法人與散戶反向指標共同支持多方")
    _add_signals(rows, daily, core_short & institution_sell & retail_bearish, "F籌碼共振", "核心空＋法人賣＋散戶偏多", "SHORT", "法人與散戶反向指標共同支持空方")

    # G. 中期趨勢只當環境濾網。
    _add_signals(rows, daily, core_long & (daily["MA20斜率_pct"] > 0) & ~pos_high, "G核心×MA20×位置", "核心多＋MA20上＋非高檔", "LONG", "順中期趨勢且避免高檔追多")
    _add_signals(rows, daily, core_short & (daily["MA20斜率_pct"] < 0) & ~pos_low, "G核心×MA20×位置", "核心空＋MA20下＋非低檔", "SHORT", "順中期趨勢且避免低檔追空")

    # H. 主力新建多單必須搭配位置，避免把高檔追價與低檔布局混在一起。
    _add_signals(rows, daily, core_long & pos_low & (main_long >= high), "H核心×主力×位置", "核心多＋低檔＋主力多單Q5", "LONG", "低檔主力多單增加")
    _add_signals(rows, daily, core_short & pos_high & (main_long <= low), "H核心×主力×位置", "核心空＋高檔＋主力多單Q1", "SHORT", "高檔主力多單偏低")

    # I. 夜盤收盤是否守住／壓回既有轉折點。
    night_close = _num(daily, "夜盤收")
    support = _num(daily, "夜盤轉折支撐").combine_first(_num(daily, "日盤轉折支撐"))
    resistance = _num(daily, "夜盤轉折壓力").combine_first(_num(daily, "日盤轉折壓力"))
    _add_signals(rows, daily, core_long & night_down & support.notna() & (night_close >= support), "I核心×夜盤×轉折", "核心多＋夜跌＋收守支撐", "LONG", "夜盤回檔但收盤仍守轉折支撐")
    _add_signals(rows, daily, core_short & night_up & resistance.notna() & (night_close <= resistance), "I核心×夜盤×轉折", "核心空＋夜漲＋收壓力下", "SHORT", "夜盤反彈但收盤仍在轉折壓力下")

    # J-L. 只替目前值得保留的四組做 MA5 乖離濾網，不全面窮舉所有家族。
    # 「排除追價」具方向性：多單只排除正乖離且位於 Q80 以上；
    # 空單只排除負乖離且位於 Q20 以下。
    long_no_chase = (ma5_bias <= 0) | (ma5_bias_pct < high)
    short_no_chase = (ma5_bias >= 0) | (ma5_bias_pct > low)
    neutral_bias = ma5_bias_pct.between(low, high, inclusive="both")
    negative_extreme = (ma5_bias < 0) & (ma5_bias_pct <= low)
    positive_extreme = (ma5_bias > 0) & (ma5_bias_pct >= high)

    retained_long_masks = (
        ("核心多＋夜盤跌", core_long & night_down),
        ("核心多＋法人買＋散戶偏空", core_long & institution_buy & retail_bullish),
        ("核心多＋綜合散戶極度偏空", core_long & (retail <= low)),
    )
    retained_short_masks = (
        ("核心空＋夜盤漲", core_short & night_up),
    )
    for label, base_mask in retained_long_masks:
        _add_signals(rows, daily, base_mask & long_no_chase, "J保留組合×排除追價", f"{label}＋排除正乖離過大", "LONG", "保留原策略但排除正乖離Q80以上的追多")
        _add_signals(rows, daily, base_mask & negative_extreme, "K保留組合×極端乖離", f"{label}＋負乖離Q20", "LONG", "只保留負乖離且歷史分位Q20以下")
        _add_signals(rows, daily, base_mask & neutral_bias, "L保留組合×非極端乖離", f"{label}＋乖離Q20至Q80", "LONG", "只保留MA5乖離歷史分位Q20至Q80")
    for label, base_mask in retained_short_masks:
        _add_signals(rows, daily, base_mask & short_no_chase, "J保留組合×排除追價", f"{label}＋排除負乖離過大", "SHORT", "保留原策略但排除負乖離Q20以下的追空")
        _add_signals(rows, daily, base_mask & positive_extreme, "K保留組合×極端乖離", f"{label}＋正乖離Q80", "SHORT", "只保留正乖離且歷史分位Q80以上")
        _add_signals(rows, daily, base_mask & neutral_bias, "L保留組合×非極端乖離", f"{label}＋乖離Q20至Q80", "SHORT", "只保留MA5乖離歷史分位Q20至Q80")

    # M. 專門驗證「夜盤已極端反映」是否還需要同方向的極端乖離確認。
    _add_signals(rows, daily, core_long & (night_pct >= high) & positive_extreme, "M夜盤極端×極端乖離", "核心多＋夜盤極端漲＋正乖離Q80", "SHORT", "夜盤漲幅與正乖離同時極端後反向做空")
    _add_signals(rows, daily, core_short & (night_pct <= low) & negative_extreme, "M夜盤極端×極端乖離", "核心空＋夜盤極端跌＋負乖離Q20", "LONG", "夜盤跌幅與負乖離同時極端後反向做多")

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail
    sign = detail["交易方向"].map({"LONG": 1.0, "SHORT": -1.0})
    detail["方向損益點數_未扣成本"] = detail["隔日開收點數"] * sign
    detail["方向損益點數"] = detail["方向損益點數_未扣成本"] - settings.round_trip_cost_points
    detail["方向損益_元"] = detail["方向損益點數"] * settings.point_value_twd
    detail["方向命中"] = detail["方向損益點數_未扣成本"] > 0
    detail["隔日最大有利點數"] = np.where(
        sign > 0,
        detail["隔日日盤高"] - detail["隔日日盤開"],
        detail["隔日日盤開"] - detail["隔日日盤低"],
    )
    detail["隔日最大不利點數"] = np.where(
        sign > 0,
        detail["隔日日盤開"] - detail["隔日日盤低"],
        detail["隔日日盤高"] - detail["隔日日盤開"],
    )
    return detail.sort_values(["組合家族", "組合訊號", "隔日日期"]).reset_index(drop=True)


def _summary_row(frame: pd.DataFrame, keys: dict[str, object], minimum_group_size: int) -> dict:
    pnl = pd.to_numeric(frame["方向損益點數"], errors="coerce").dropna()
    gross = pd.to_numeric(frame["方向損益點數_未扣成本"], errors="coerce").dropna()
    positive = float(pnl[pnl > 0].sum())
    negative = float(-pnl[pnl < 0].sum())
    cumulative = pnl.cumsum()
    drawdown = cumulative - cumulative.cummax()
    wins = int((gross > 0).sum())
    p_hit = binomtest(wins, len(gross), 0.5).pvalue if len(gross) else np.nan
    p_mean = ttest_1samp(pnl, 0, nan_policy="omit").pvalue if len(pnl) > 1 else np.nan
    return {
        **keys,
        "樣本數": len(frame),
        "樣本狀態": "足夠" if len(frame) >= minimum_group_size else "樣本不足",
        "命中率_pct": (gross > 0).mean() * 100 if len(gross) else np.nan,
        "平均淨點數": pnl.mean() if len(pnl) else np.nan,
        "中位淨點數": pnl.median() if len(pnl) else np.nan,
        "總淨點數": pnl.sum() if len(pnl) else np.nan,
        "獲利因子": positive / negative if negative > 0 else np.nan,
        "最大回撤點數": drawdown.min() if len(drawdown) else np.nan,
        "平均最大有利點數": pd.to_numeric(frame["隔日最大有利點數"], errors="coerce").mean(),
        "平均最大不利點數": pd.to_numeric(frame["隔日最大不利點數"], errors="coerce").mean(),
        "命中率原始p值": p_hit,
        "平均損益原始p值": p_mean,
    }


def summarize(detail: pd.DataFrame, settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame]:
    if detail.empty:
        return pd.DataFrame(), pd.DataFrame()
    rows = []
    for keys, frame in detail.groupby(["組合家族", "組合訊號", "交易方向"], sort=True):
        rows.append(_summary_row(frame, dict(zip(["組合家族", "組合訊號", "交易方向"], keys)), settings.minimum_group_size))
    summary = pd.DataFrame(rows)
    summary["命中率FDR_q值"] = _bh_adjust(summary["命中率原始p值"])
    summary["平均損益FDR_q值"] = _bh_adjust(summary["平均損益原始p值"])
    summary["正式通過"] = (
        summary["樣本數"].ge(settings.minimum_group_size)
        & summary["命中率FDR_q值"].lt(0.05)
        & summary["平均損益FDR_q值"].lt(0.05)
        & summary["平均淨點數"].gt(0)
    )

    yearly_rows = []
    for keys, frame in detail.groupby(["組合家族", "組合訊號", "交易方向", "隔日年"], sort=True):
        yearly_rows.append(_summary_row(
            frame,
            dict(zip(["組合家族", "組合訊號", "交易方向", "年度"], keys)),
            settings.minimum_group_size,
        ))
    yearly = pd.DataFrame(yearly_rows)
    stability = yearly.groupby(["組合家族", "組合訊號", "交易方向"], as_index=False).agg(
        有資料年度數=("年度", "nunique"),
        年度獲利數=("總淨點數", lambda s: int((s > 0).sum())),
        年度虧損數=("總淨點數", lambda s: int((s < 0).sum())),
    )
    stability["年度獲利比例_pct"] = stability["年度獲利數"] / stability["有資料年度數"] * 100
    summary = summary.merge(stability, on=["組合家族", "組合訊號", "交易方向"], how="left")
    for frame in (summary, yearly):
        numeric = frame.select_dtypes(include="number").columns
        frame[numeric] = frame[numeric].round(4)
    return summary.sort_values(["正式通過", "平均淨點數", "樣本數"], ascending=[False, False, False]), yearly


def export_outputs(detail: pd.DataFrame, summary: pd.DataFrame, yearly: pd.DataFrame,
                   settings: Settings, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{(settings.end or date.today()):%Y%m%d}"
    paths = {
        "detail": output_dir / f"futures_combination_detail_{suffix}.csv",
        "summary": output_dir / f"futures_combination_summary_{suffix}.csv",
        "yearly": output_dir / f"futures_combination_yearly_{suffix}.csv",
        "manifest": output_dir / f"futures_combination_manifest_{suffix}.json",
    }
    detail.to_csv(paths["detail"], index=False, encoding="utf-8-sig")
    summary.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths["yearly"], index=False, encoding="utf-8-sig")
    manifest = {
        "run_id": str(uuid.uuid4()), "analysis": ANALYSIS_NAME, "version": VERSION,
        "settings": {key: str(value) if isinstance(value, date) else value for key, value in asdict(settings).items()},
        "alignment": "訊號日收盤及其夜盤完成後產生訊號；下一交易日日盤開盤進、收盤出",
        "cost_model": "每筆固定來回成本；未模擬盤中停損停利與滑價時序",
        "anti_lookahead": "夜盤、散戶、主力與MA5乖離的 rolling 百分位僅使用當日前 history_window 筆；隔日行情只作結果",
        "families": sorted(detail["組合家族"].dropna().unique().tolist()) if len(detail) else [],
        "output_rows": {"detail": len(detail), "summary": len(summary), "yearly": len(yearly)},
    }
    paths["manifest"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="核心、夜盤、位置與籌碼組合的隔日日盤回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--core-column", default="策略_核心_五日價量")
    parser.add_argument("--cost-points", type=float, default=3.0)
    parser.add_argument("--output-dir", type=Path, default=Path("output/backtest"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id,
        core_column=args.core_column, round_trip_cost_points=args.cost_points,
    )
    source = load_source(settings)
    daily = prepare_daily(source, settings)
    detail = build_signals(daily, settings)
    summary, yearly = summarize(detail, settings)
    paths = export_outputs(detail, summary, yearly, settings, args.output_dir)
    print(f"[combination_backtest] 完成：逐筆 {len(detail):,}，組合 {len(summary):,}")
    for name, path in paths.items():
        print(f"[combination_backtest] {name}: {path}")


if __name__ == "__main__":
    main()
