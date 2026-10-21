from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import kruskal, mannwhitneyu, spearmanr

from backtest.futures_strategy_next_day_field_scan import (
    _bh_adjust,
    _hedges_g,
)


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
ANALYSIS_NAME = "raw_signal_strength_scan"
METRIC_VERSION = "1.1.0"
BUCKET_LABELS = ("Q1最低20%", "Q2偏低", "Q3中間", "Q4偏高", "Q5最高20%")
BUCKET_IDS = {label: index for index, label in enumerate(BUCKET_LABELS, start=1)}
OUTCOMES = {
    "隔日開收報酬_pct": "方向：正值上漲、負值下跌",
    "隔日上行MFE_pct": "隔日日盤開盤至最高點的最大上行空間",
    "隔日下行MFE_pct": "隔日日盤開盤至最低點的最大下行空間（正數表示幅度）",
    "隔日日內振幅_pct": "隔日日盤高低差／開盤",
    "隔日收盤位置_pct": "收盤位於日內高低區間的位置，0為最低、100為最高",
    "隔日開盤跳空_日收_pct": "隔日開盤相對訊號日日盤收",
    "隔日開盤跳空_夜收_pct": "隔日開盤相對訊號日夜盤收",
}

SOURCE_COLUMNS = (
    "日期", "全日收", "全成交量", "全5均量", "全價漲跌_pct",
    "全量增減_昨日_pct", "全量增減_五日_pct",
    "全5均上升幅度_pct", "全10均上升幅度_pct", "全20均上升幅度_pct",
    "全5MA乖離率", "全布林上軌", "全布林下軌",
    "法人總買超_億", "散戶多空比_微台", "散戶多空比_小台",
    "主力新建多單", "主力新建空單", "未沖銷_全",
    "call_月選", "put_月選", "call_週三", "put_週三", "call_週五", "put_週五",
    "日盤開", "日盤高", "日盤低", "日盤收", "夜盤收",
)


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    split_date: date = date(2023, 1, 1)
    history_window: int = 252
    minimum_history: int = 60
    minimum_group_size: int = 30
    minimum_year_size: int = 20
    alpha: float = 0.05
    winsor_tail: float = 0.005

    def __post_init__(self) -> None:
        if self.end and self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if self.split_date <= self.start:
            raise ValueError("split_date 必須晚於 start")
        if self.history_window < 20:
            raise ValueError("history_window 至少為 20")
        if not 10 <= self.minimum_history <= self.history_window:
            raise ValueError("minimum_history 必須介於 10 與 history_window 之間")
        if self.minimum_group_size < 10 or self.minimum_year_size < 5:
            raise ValueError("樣本門檻過低")
        if not 0 < self.alpha < 1:
            raise ValueError("alpha 必須介於 0 與 1 之間")
        if not 0 <= self.winsor_tail < 0.1:
            raise ValueError("winsor_tail 必須介於 0 與 0.1 之間")


def load_source(settings: Settings) -> pd.DataFrame:
    from common import db

    # 先多取歷史資料供 rolling percentile 使用；end 後多取數日供下一交易日對齊。
    query_start = settings.start - timedelta(days=max(400, settings.history_window * 2))
    columns = ", ".join(f'"{column}"' for column in SOURCE_COLUMNS)
    sql = f'''SELECT futures_id, {columns}
              FROM {SOURCE_VIEW}
              WHERE futures_id = ? AND "日期" >= ?'''
    params: list[object] = [settings.futures_id, query_start]
    if settings.end:
        sql += ' AND "日期" <= ?'
        params.append(settings.end + timedelta(days=14))
    sql += ' ORDER BY "日期"'
    return db.sql_to_df(sql, params=tuple(params), parse_dates=["日期"])


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").astype(float)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.replace(0, np.nan))


def _safe_log_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    ratio = _safe_ratio(numerator.where(numerator > 0), denominator.where(denominator > 0))
    return np.log(ratio.where(ratio > 0))


def _rolling_percentile(series: pd.Series, window: int, minimum_history: int) -> pd.Series:
    """將當日值放進「僅包含先前資料」的歷史分布，回傳 0~100 百分位。"""
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    result = np.full(len(values), np.nan, dtype=float)
    for index, value in enumerate(values):
        if not np.isfinite(value):
            continue
        history = values[max(0, index - window):index]
        history = history[np.isfinite(history)]
        if len(history) < minimum_history:
            continue
        less = np.count_nonzero(history < value)
        equal = np.count_nonzero(history == value)
        result[index] = (less + 0.5 * equal) / len(history) * 100.0
    return pd.Series(result, index=series.index, dtype=float)


def _strength_bucket(percentile: pd.Series) -> pd.Series:
    return pd.cut(
        percentile,
        bins=[-np.inf, 20, 40, 60, 80, np.inf],
        labels=BUCKET_LABELS,
        include_lowest=True,
        ordered=True,
    )


def build_signal_features(source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    """建立彼此獨立的原始／衍生連續訊號，不使用既有策略多空結果。"""
    frame = source.sort_values("日期").reset_index(drop=True).copy()
    weekday = pd.to_datetime(frame["日期"]).dt.weekday

    full_close = _numeric(frame, "全日收")
    volume = _numeric(frame, "全成交量")
    volume_5 = _numeric(frame, "全5均量")
    retail_micro = _numeric(frame, "散戶多空比_微台")
    retail_small = _numeric(frame, "散戶多空比_小台")
    main_long = _numeric(frame, "主力新建多單")
    main_short = _numeric(frame, "主力新建空單")
    oi = _numeric(frame, "未沖銷_全")

    monthly_option = _safe_log_ratio(_numeric(frame, "call_月選"), _numeric(frame, "put_月選"))
    wed_option = _safe_log_ratio(_numeric(frame, "call_週三"), _numeric(frame, "put_週三"))
    fri_option = _safe_log_ratio(_numeric(frame, "call_週五"), _numeric(frame, "put_週五"))
    # 沿用正式 View 的短週選擇：一二看週三、三四看週五、五看下個週三。
    short_option = pd.Series(
        np.select(
            [weekday.isin([0, 1, 4]), weekday.isin([2, 3])],
            [wed_option, fri_option],
            default=np.nan,
        ),
        index=frame.index,
        dtype=float,
    )

    bollinger_position = _safe_ratio(
        full_close - _numeric(frame, "全布林下軌"),
        _numeric(frame, "全布林上軌") - _numeric(frame, "全布林下軌"),
    ) * 100.0
    main_net = main_long - main_short
    main_activity = main_long.abs() + main_short.abs()

    features: dict[str, pd.Series] = {
        "全價漲跌": _numeric(frame, "全價漲跌_pct"),
        "量能相對昨日": _numeric(frame, "全量增減_昨日_pct"),
        "量能相對五日": _numeric(frame, "全量增減_五日_pct"),
        "MA5斜率": _numeric(frame, "全5均上升幅度_pct"),
        "MA10斜率": _numeric(frame, "全10均上升幅度_pct"),
        "MA20斜率": _numeric(frame, "全20均上升幅度_pct"),
        "MA5乖離": _numeric(frame, "全5MA乖離率"),
        "布林通道位置": bollinger_position,
        "法人總買超": _numeric(frame, "法人總買超_億"),
        "散戶多空比": retail_micro.combine_first(retail_small),
        "主力新建多單": main_long,
        "主力新建空單": main_short,
        "主力淨多空": main_net,
        "主力活動量": main_activity,
        "主力淨多空_OI比": _safe_ratio(main_net, oi) * 100.0,
        "月選CallPut對數比": monthly_option,
        "週三CallPut對數比": wed_option,
        "週五CallPut對數比": fri_option,
        "主要短週CallPut對數比": short_option,
    }
    metadata: dict[str, dict[str, object]] = {
        "全價漲跌": {"類別": "價量", "公式": "全價漲跌_pct", "天然方向": True},
        "量能相對昨日": {"類別": "價量", "公式": "全量增減_昨日_pct", "天然方向": False},
        "量能相對五日": {"類別": "價量", "公式": "全量增減_五日_pct", "天然方向": False},
        "MA5斜率": {"類別": "線型", "公式": "全5均上升幅度_pct", "天然方向": True},
        "MA10斜率": {"類別": "線型", "公式": "全10均上升幅度_pct", "天然方向": True},
        "MA20斜率": {"類別": "線型", "公式": "全20均上升幅度_pct", "天然方向": True},
        "MA5乖離": {"類別": "位置", "公式": "全5MA乖離率", "天然方向": True},
        "布林通道位置": {"類別": "位置", "公式": "(全日收-下軌)/(上軌-下軌)*100", "天然方向": True},
        "法人總買超": {"類別": "法人散戶", "公式": "法人總買超_億", "天然方向": True},
        "散戶多空比": {"類別": "法人散戶", "公式": "微台優先，缺值用小台", "天然方向": False},
        "主力新建多單": {"類別": "期貨主力", "公式": "主力新建多單", "天然方向": False},
        "主力新建空單": {"類別": "期貨主力", "公式": "主力新建空單", "天然方向": False},
        "主力淨多空": {"類別": "期貨主力", "公式": "主力新建多單-主力新建空單", "天然方向": True},
        "主力活動量": {"類別": "期貨主力", "公式": "abs(多單)+abs(空單)", "天然方向": False},
        "主力淨多空_OI比": {"類別": "期貨主力", "公式": "主力淨多空/未沖銷_全*100", "天然方向": True},
        "月選CallPut對數比": {"類別": "選擇權", "公式": "ln(call_月選/put_月選)", "天然方向": True},
        "週三CallPut對數比": {"類別": "選擇權", "公式": "ln(call_週三/put_週三)", "天然方向": True},
        "週五CallPut對數比": {"類別": "選擇權", "公式": "ln(call_週五/put_週五)", "天然方向": True},
        "主要短週CallPut對數比": {"類別": "選擇權", "公式": "一二五用週三；三四用週五", "天然方向": True},
    }
    output = frame[["日期"]].copy()
    for name, values in features.items():
        output[name] = values
    return output, metadata


def _pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _safe_ratio(numerator, denominator) * 100.0


def prepare_detail(source: pd.DataFrame, settings: Settings) -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    signals, metadata = build_signal_features(ordered)
    day_open = _numeric(ordered, "日盤開").shift(-1)
    day_high = _numeric(ordered, "日盤高").shift(-1)
    day_low = _numeric(ordered, "日盤低").shift(-1)
    day_close = _numeric(ordered, "日盤收").shift(-1)
    signal_day_close = _numeric(ordered, "日盤收")
    signal_night_close = _numeric(ordered, "夜盤收")
    target_date = pd.to_datetime(ordered["日期"]).shift(-1)

    outcomes = pd.DataFrame({
        "隔日日期": target_date,
        "隔日開收報酬_pct": _pct(day_close - day_open, day_open),
        "隔日上行MFE_pct": _pct(day_high - day_open, day_open),
        "隔日下行MFE_pct": _pct(day_open - day_low, day_open),
        "隔日日內振幅_pct": _pct(day_high - day_low, day_open),
        "隔日收盤位置_pct": _safe_ratio(day_close - day_low, day_high - day_low) * 100.0,
        "隔日開盤跳空_日收_pct": _pct(day_open - signal_day_close, signal_day_close),
        "隔日開盤跳空_夜收_pct": _pct(day_open - signal_night_close, signal_night_close),
    })

    parts: list[pd.DataFrame] = []
    for name, meta in metadata.items():
        raw = pd.to_numeric(signals[name], errors="coerce")
        percentile = _rolling_percentile(raw, settings.history_window, settings.minimum_history)
        part = pd.concat([
            pd.DataFrame({
                "訊號日": pd.to_datetime(ordered["日期"]),
                "指標類別": meta["類別"],
                "指標代碼": name,
                "公式": meta["公式"],
                "天然方向": meta["天然方向"],
                "原始值": raw,
                "歷史分位_pct": percentile,
                "強度層": _strength_bucket(percentile),
                "原始正負": np.select([raw > 0, raw < 0], ["正", "負"], default="零"),
            }),
            outcomes,
        ], axis=1)
        parts.append(part)
    detail = pd.concat(parts, ignore_index=True)
    dates = pd.to_datetime(detail["訊號日"]).dt.date
    detail = detail[(dates >= settings.start) & detail["隔日日期"].notna()].copy()
    if settings.end:
        detail = detail[dates <= settings.end].copy()
    return detail.reset_index(drop=True), metadata


def build_bucket_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (category, signal, bucket), group in detail.dropna(subset=["強度層"]).groupby(
        ["指標類別", "指標代碼", "強度層"], observed=True, sort=False
    ):
        close = pd.to_numeric(group["隔日開收報酬_pct"], errors="coerce")
        up = pd.to_numeric(group["隔日上行MFE_pct"], errors="coerce")
        down = pd.to_numeric(group["隔日下行MFE_pct"], errors="coerce")
        row: dict[str, object] = {
            "指標類別": category, "指標代碼": signal, "強度層": str(bucket),
            "樣本數": int(close.notna().sum()), "原始值平均": group["原始值"].mean(),
            "原始值中位數": group["原始值"].median(),
            "歷史分位平均_pct": group["歷史分位_pct"].mean(),
            "隔日開收平均_pct": close.mean(), "隔日開收中位數_pct": close.median(),
            "隔日上漲機率_pct": (close > 0).mean() * 100.0,
            "隔日上行MFE平均_pct": up.mean(), "隔日下行MFE平均_pct": down.mean(),
            "上行MFE大於下行MFE_pct": (up > down).mean() * 100.0,
            "隔日日內振幅平均_pct": group["隔日日內振幅_pct"].mean(),
            "隔日收盤位置平均_pct": group["隔日收盤位置_pct"].mean(),
        }
        for threshold in (0.25, 0.50, 0.75, 1.00):
            label = f"{threshold:.2f}pct"
            row[f"上行至少{label}_機率"] = (up >= threshold).mean() * 100.0
            row[f"下行至少{label}_機率"] = (down >= threshold).mean() * 100.0
        rows.append(row)
    return pd.DataFrame(rows)


def _clip(values: pd.Series, tail: float) -> pd.Series:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty or tail <= 0:
        return clean
    low, high = clean.quantile([tail, 1 - tail])
    return clean.clip(low, high)


def scan_period(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for signal, signal_data in detail.groupby("指標代碼", sort=False):
        category = signal_data["指標類別"].iloc[0]
        for outcome in OUTCOMES:
            data = signal_data[["歷史分位_pct", "強度層", outcome]].dropna()
            if len(data) < settings.minimum_group_size * 2:
                continue
            outcome_values = _clip(data[outcome], settings.winsor_tail)
            ranked = data.loc[outcome_values.index, "歷史分位_pct"]
            if ranked.nunique() >= 2 and outcome_values.nunique() >= 2:
                rho, p_value = spearmanr(ranked, outcome_values)
            else:
                rho, p_value = math.nan, math.nan
            if np.isfinite(rho) and np.isfinite(p_value):
                rows.append({
                    "指標類別": category, "指標代碼": signal, "結果指標": outcome,
                    "檢定類型": "Spearman分位趨勢", "樣本數": len(outcome_values),
                    "最低層樣本數": math.nan, "最高層樣本數": math.nan,
                    "統計值": rho, "效果值": rho, "效果方向": "正向" if rho > 0 else "負向",
                    "效果量": abs(rho), "p值": p_value,
                })

            low = _clip(data.loc[data["強度層"] == BUCKET_LABELS[0], outcome], settings.winsor_tail)
            high = _clip(data.loc[data["強度層"] == BUCKET_LABELS[-1], outcome], settings.winsor_tail)
            if min(len(low), len(high)) >= settings.minimum_group_size:
                u_stat, p_value = mannwhitneyu(high, low, alternative="two-sided")
                difference = high.mean() - low.mean()
                rows.append({
                    "指標類別": category, "指標代碼": signal, "結果指標": outcome,
                    "檢定類型": "最高20%對最低20%", "樣本數": len(low) + len(high),
                    "最低層樣本數": len(low), "最高層樣本數": len(high),
                    "最低層平均": low.mean(), "最高層平均": high.mean(),
                    "統計值": u_stat, "效果值": difference,
                    "效果方向": "最高層較高" if difference > 0 else "最低層較高",
                    "效果量": abs(_hedges_g(high, low)), "p值": p_value,
                })

            groups = [
                _clip(data.loc[data["強度層"] == bucket, outcome], settings.winsor_tail)
                for bucket in BUCKET_LABELS
            ]
            groups = [group for group in groups if len(group) >= settings.minimum_group_size]
            combined_unique = pd.concat(groups).nunique() if groups else 0
            if len(groups) >= 3 and combined_unique >= 2:
                statistic, p_value = kruskal(*groups)
                count = sum(len(group) for group in groups)
                epsilon = max(0.0, (statistic - len(groups) + 1) / (count - len(groups)))
                rows.append({
                    "指標類別": category, "指標代碼": signal, "結果指標": outcome,
                    "檢定類型": "Kruskal五層差異", "樣本數": count,
                    "最低層樣本數": math.nan, "最高層樣本數": math.nan,
                    "統計值": statistic, "效果值": math.nan, "效果方向": "非方向性",
                    "效果量": epsilon, "p值": p_value,
                })
    result = pd.DataFrame(rows)
    if not result.empty:
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR顯著"] = result["FDR_q值"] < settings.alpha
    return result


def _merge_periods(full: pd.DataFrame, detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    if full.empty:
        return full
    signal_dates = pd.to_datetime(detail["訊號日"]).dt.date
    discovery = scan_period(detail[signal_dates < settings.split_date], settings)
    validation = scan_period(detail[signal_dates >= settings.split_date], settings)
    keys = ["指標代碼", "結果指標", "檢定類型"]
    period_columns = keys + ["效果值", "p值", "FDR_q值"]
    discovery = (
        discovery[period_columns] if not discovery.empty
        else pd.DataFrame(columns=period_columns)
    ).rename(columns={"效果值": "探索期_效果值", "p值": "探索期_p值", "FDR_q值": "探索期_q值"})
    validation = (
        validation[period_columns] if not validation.empty
        else pd.DataFrame(columns=period_columns)
    ).rename(columns={"效果值": "驗證期_效果值", "p值": "驗證期_p值", "FDR_q值": "驗證期_q值"})
    result = full.merge(discovery, on=keys, how="left").merge(validation, on=keys, how="left")
    directional = result["檢定類型"] != "Kruskal五層差異"
    same_sign = np.sign(result["探索期_效果值"]) == np.sign(result["驗證期_效果值"])
    result["前後期同方向"] = directional & same_sign

    year_info: list[tuple[int, int, float]] = []
    for row in result.itertuples(index=False):
        if row.檢定類型 == "Kruskal五層差異":
            year_info.append((0, 0, math.nan))
            continue
        signs: list[int] = []
        expected = int(np.sign(row.效果值))
        signal_history = detail[detail["指標代碼"] == row.指標代碼].copy()
        signal_history["_year"] = pd.to_datetime(signal_history["隔日日期"]).dt.year
        for _, year_group in signal_history.groupby("_year"):
            values = year_group[["歷史分位_pct", "強度層", row.結果指標]].dropna()
            if row.檢定類型 == "Spearman分位趨勢" and len(values) >= settings.minimum_year_size:
                if values["歷史分位_pct"].nunique() < 2 or values[row.結果指標].nunique() < 2:
                    continue
                effect = spearmanr(values["歷史分位_pct"], values[row.結果指標]).statistic
            else:
                low = values.loc[values["強度層"] == BUCKET_LABELS[0], row.結果指標]
                high = values.loc[values["強度層"] == BUCKET_LABELS[-1], row.結果指標]
                if min(len(low), len(high)) < max(5, settings.minimum_year_size // 5):
                    continue
                effect = high.mean() - low.mean()
            if np.isfinite(effect) and effect != 0:
                signs.append(int(np.sign(effect)))
        matches = sum(sign == expected for sign in signs)
        year_info.append((len(signs), matches, matches / len(signs) if signs else math.nan))
    result[["可比較年度數", "同方向年度數", "年度方向一致率"]] = pd.DataFrame(
        year_info, index=result.index
    )
    result["重複驗證通過"] = (
        directional & result["FDR顯著"] & result["前後期同方向"]
        & (result["探索期_q值"] < 0.10) & (result["驗證期_p值"] < settings.alpha)
        & (result["年度方向一致率"] >= 0.60)
    )
    return result.sort_values(
        ["重複驗證通過", "FDR顯著", "FDR_q值", "效果量"],
        ascending=[False, False, True, False], na_position="last",
    ).reset_index(drop=True)


def build_yearly_summary(detail: pd.DataFrame, minimum_size: int) -> pd.DataFrame:
    work = detail.copy()
    work["年"] = pd.to_datetime(work["隔日日期"]).dt.year
    # build_bucket_summary 不保留年，因此逐年重建後串接。
    parts = []
    for year, group in work.groupby("年"):
        part = build_bucket_summary(group)
        if not part.empty:
            part.insert(0, "年", int(year))
            parts.append(part[part["樣本數"] >= minimum_size])
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


DAILY_DB_COLUMNS = [
    "run_id", "signal_date", "result_date", "signal_category", "signal_code",
    "formula", "natural_direction", "raw_value", "history_percentile_pct",
    "bucket_id", "bucket_label", "raw_sign", "next_open_close_return_pct",
    "next_up_mfe_pct", "next_down_mfe_pct", "next_intraday_range_pct",
    "next_close_location_pct", "next_open_gap_day_close_pct",
    "next_open_gap_night_close_pct",
]
BUCKET_DB_COLUMNS = [
    "run_id", "signal_category", "signal_code", "bucket_id", "bucket_label",
    "sample_count", "raw_mean", "raw_median", "history_percentile_mean_pct",
    "next_open_close_mean_pct", "next_open_close_median_pct",
    "next_up_probability_pct", "next_up_mfe_mean_pct", "next_down_mfe_mean_pct",
    "up_mfe_greater_probability_pct", "next_intraday_range_mean_pct",
    "next_close_location_mean_pct", "up_ge_025_probability_pct",
    "down_ge_025_probability_pct", "up_ge_050_probability_pct",
    "down_ge_050_probability_pct", "up_ge_075_probability_pct",
    "down_ge_075_probability_pct", "up_ge_100_probability_pct",
    "down_ge_100_probability_pct",
]
TEST_DB_COLUMNS = [
    "run_id", "signal_category", "signal_code", "outcome_name", "test_type",
    "sample_count", "low_bucket_count", "high_bucket_count", "statistic",
    "effect_value", "effect_direction", "effect_size", "p_value",
    "low_bucket_mean", "high_bucket_mean", "fdr_q_value", "fdr_significant",
    "discovery_effect_value", "discovery_p_value", "discovery_q_value",
    "validation_effect_value", "validation_p_value", "validation_q_value",
    "same_direction", "comparable_year_count", "same_direction_year_count",
    "yearly_direction_consistency", "repeated_validation_pass",
]
YEARLY_DB_COLUMNS = ["run_id", "result_year", *BUCKET_DB_COLUMNS[1:]]

SUMMARY_DB_RENAME = {
    "指標類別": "signal_category", "指標代碼": "signal_code", "強度層": "bucket_label",
    "樣本數": "sample_count", "原始值平均": "raw_mean", "原始值中位數": "raw_median",
    "歷史分位平均_pct": "history_percentile_mean_pct",
    "隔日開收平均_pct": "next_open_close_mean_pct",
    "隔日開收中位數_pct": "next_open_close_median_pct",
    "隔日上漲機率_pct": "next_up_probability_pct",
    "隔日上行MFE平均_pct": "next_up_mfe_mean_pct",
    "隔日下行MFE平均_pct": "next_down_mfe_mean_pct",
    "上行MFE大於下行MFE_pct": "up_mfe_greater_probability_pct",
    "隔日日內振幅平均_pct": "next_intraday_range_mean_pct",
    "隔日收盤位置平均_pct": "next_close_location_mean_pct",
    "上行至少0.25pct_機率": "up_ge_025_probability_pct",
    "下行至少0.25pct_機率": "down_ge_025_probability_pct",
    "上行至少0.50pct_機率": "up_ge_050_probability_pct",
    "下行至少0.50pct_機率": "down_ge_050_probability_pct",
    "上行至少0.75pct_機率": "up_ge_075_probability_pct",
    "下行至少0.75pct_機率": "down_ge_075_probability_pct",
    "上行至少1.00pct_機率": "up_ge_100_probability_pct",
    "下行至少1.00pct_機率": "down_ge_100_probability_pct",
}


def build_database_frames(
    run_id: uuid.UUID,
    detail: pd.DataFrame,
    summary: pd.DataFrame,
    tests: pd.DataFrame,
    yearly: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """將中文分析輸出轉成穩定的資料庫欄位；不連線，方便單元測試。"""
    daily_db = detail.rename(columns={
        "訊號日": "signal_date", "隔日日期": "result_date",
        "指標類別": "signal_category", "指標代碼": "signal_code",
        "公式": "formula", "天然方向": "natural_direction", "原始值": "raw_value",
        "歷史分位_pct": "history_percentile_pct", "強度層": "bucket_label",
        "原始正負": "raw_sign", "隔日開收報酬_pct": "next_open_close_return_pct",
        "隔日上行MFE_pct": "next_up_mfe_pct", "隔日下行MFE_pct": "next_down_mfe_pct",
        "隔日日內振幅_pct": "next_intraday_range_pct",
        "隔日收盤位置_pct": "next_close_location_pct",
        "隔日開盤跳空_日收_pct": "next_open_gap_day_close_pct",
        "隔日開盤跳空_夜收_pct": "next_open_gap_night_close_pct",
    }).copy()
    daily_db.insert(0, "run_id", run_id)
    daily_db["bucket_id"] = daily_db["bucket_label"].map(BUCKET_IDS)
    daily_db["bucket_label"] = daily_db["bucket_label"].astype(object)

    bucket_db = summary.rename(columns=SUMMARY_DB_RENAME).copy()
    bucket_db.insert(0, "run_id", run_id)
    bucket_db["bucket_id"] = bucket_db["bucket_label"].map(BUCKET_IDS)

    test_db = tests.rename(columns={
        "指標類別": "signal_category", "指標代碼": "signal_code",
        "結果指標": "outcome_name", "檢定類型": "test_type", "樣本數": "sample_count",
        "最低層樣本數": "low_bucket_count", "最高層樣本數": "high_bucket_count",
        "統計值": "statistic", "效果值": "effect_value", "效果方向": "effect_direction",
        "效果量": "effect_size", "p值": "p_value", "最低層平均": "low_bucket_mean",
        "最高層平均": "high_bucket_mean", "FDR_q值": "fdr_q_value",
        "FDR顯著": "fdr_significant", "探索期_效果值": "discovery_effect_value",
        "探索期_p值": "discovery_p_value", "探索期_q值": "discovery_q_value",
        "驗證期_效果值": "validation_effect_value", "驗證期_p值": "validation_p_value",
        "驗證期_q值": "validation_q_value", "前後期同方向": "same_direction",
        "可比較年度數": "comparable_year_count", "同方向年度數": "same_direction_year_count",
        "年度方向一致率": "yearly_direction_consistency",
        "重複驗證通過": "repeated_validation_pass",
    }).copy()
    test_db.insert(0, "run_id", run_id)
    for column in ("fdr_significant", "same_direction", "repeated_validation_pass"):
        test_db[column] = test_db[column].fillna(False).astype(bool)
    for column in ("comparable_year_count", "same_direction_year_count"):
        test_db[column] = pd.to_numeric(test_db[column], errors="coerce").fillna(0).astype(int)

    yearly_db = yearly.rename(columns={"年": "result_year", **SUMMARY_DB_RENAME}).copy()
    yearly_db.insert(0, "run_id", run_id)
    yearly_db["bucket_id"] = yearly_db["bucket_label"].map(BUCKET_IDS)
    return (
        daily_db[DAILY_DB_COLUMNS], bucket_db[BUCKET_DB_COLUMNS],
        test_db[TEST_DB_COLUMNS], yearly_db[YEARLY_DB_COLUMNS],
    )


def _db_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.date()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    try:
        return None if bool(pd.isna(value)) else value
    except (TypeError, ValueError):
        return value


def _records(frame: pd.DataFrame, columns: list[str]) -> list[tuple[Any, ...]]:
    return [tuple(_db_value(value) for value in row) for row in frame[columns].itertuples(index=False, name=None)]


def _insert_rows(cursor: Any, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    rows = _records(frame, columns)
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    cursor.executemany(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        rows,
    )


def save_to_database(
    run_id: uuid.UUID,
    detail: pd.DataFrame,
    summary: pd.DataFrame,
    tests: pd.DataFrame,
    yearly: pd.DataFrame,
    settings: Settings,
    source_rows: int,
) -> None:
    """以單一交易保存分析版本及四種輸出；任何失敗都不留下半套資料。"""
    from common.db import get_connection

    daily_db, bucket_db, test_db, yearly_db = build_database_frames(
        run_id, detail, summary, tests, yearly
    )
    required_tables = (
        "research.raw_signal_strength_daily",
        "research.raw_signal_strength_bucket",
        "research.raw_signal_strength_test",
        "research.raw_signal_strength_yearly",
    )
    run_parameters = {
        "percentile_edges": [0, 20, 40, 60, 80, 100],
        "split_date": settings.split_date.isoformat(),
        "history_window": settings.history_window,
        "minimum_history": settings.minimum_history,
        "minimum_group_size": settings.minimum_group_size,
        "minimum_year_size": settings.minimum_year_size,
        "alpha": settings.alpha,
        "winsor_tail": settings.winsor_tail,
    }
    diagnostics = {
        "signal_count": int(detail["指標代碼"].nunique()),
        "detail_rows": len(detail),
        "test_rows": len(tests),
        "candidate_rows": int(tests.get("重複驗證通過", pd.Series(dtype=bool)).fillna(False).sum()),
    }
    valid_signal_dates = int(pd.to_datetime(detail["訊號日"]).nunique())
    excluded_source_rows = max(int(source_rows) - valid_signal_dates, 0)
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for table in required_tables:
                cursor.execute("SELECT to_regclass(%s)", (table,))
                if cursor.fetchone()[0] is None:
                    raise RuntimeError(
                        "尚未建立原始訊號強度研究表；請先執行 "
                        "db_migrate/20261020_add_raw_signal_strength_research.sql"
                    )
            cursor.execute(
                """
                INSERT INTO research.analysis_run (
                    run_id, analysis_name, futures_id, start_date, end_date,
                    bucket_edges, metric_version, source_relation, status,
                    source_rows, valid_rows, excluded_rows, exclusion_summary
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, 'running', %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id, ANALYSIS_NAME, settings.futures_id, settings.start,
                    settings.end or pd.to_datetime(detail["訊號日"]).max().date(),
                    json.dumps(run_parameters, ensure_ascii=False), METRIC_VERSION, SOURCE_VIEW,
                    source_rows, valid_signal_dates, excluded_source_rows,
                    json.dumps(diagnostics, ensure_ascii=False),
                ),
            )
            _insert_rows(cursor, "research.raw_signal_strength_daily", DAILY_DB_COLUMNS, daily_db)
            _insert_rows(cursor, "research.raw_signal_strength_bucket", BUCKET_DB_COLUMNS, bucket_db)
            _insert_rows(cursor, "research.raw_signal_strength_test", TEST_DB_COLUMNS, test_db)
            _insert_rows(cursor, "research.raw_signal_strength_yearly", YEARLY_DB_COLUMNS, yearly_db)
            cursor.execute(
                """
                UPDATE research.analysis_run
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE run_id = %s
                """,
                (run_id,),
            )
        conn.commit()


def export_results(
    detail: pd.DataFrame,
    summary: pd.DataFrame,
    tests: pd.DataFrame,
    yearly: pd.DataFrame,
    settings: Settings,
    metadata: dict[str, dict[str, object]],
    output_dir: Path,
    run_id: uuid.UUID | None = None,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_label = settings.end.strftime("%Y%m%d") if settings.end else "latest"
    suffix = f"{settings.start:%Y%m%d}_{end_label}"
    paths = {
        "detail": output_dir / f"raw_signal_strength_detail_{suffix}.csv",
        "summary": output_dir / f"raw_signal_strength_by_bucket_{suffix}.csv",
        "tests": output_dir / f"raw_signal_strength_tests_{suffix}.csv",
        "candidates": output_dir / f"raw_signal_strength_candidates_{suffix}.csv",
        "yearly": output_dir / f"raw_signal_strength_yearly_{suffix}.csv",
        "manifest": output_dir / f"raw_signal_strength_manifest_{suffix}.json",
    }
    detail.to_csv(paths["detail"], index=False, encoding="utf-8-sig")
    summary.to_csv(paths["summary"], index=False, encoding="utf-8-sig")
    tests.to_csv(paths["tests"], index=False, encoding="utf-8-sig")
    candidates = (
        tests[tests["重複驗證通過"].fillna(False)]
        if "重複驗證通過" in tests else tests.iloc[0:0]
    )
    candidates.to_csv(paths["candidates"], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths["yearly"], index=False, encoding="utf-8-sig")
    manifest = {
        "run_id": str(run_id) if run_id else None,
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "settings": {
            **asdict(settings),
            "start": settings.start.isoformat(),
            "end": settings.end.isoformat() if settings.end else None,
            "split_date": settings.split_date.isoformat(),
        },
        "source_view": SOURCE_VIEW,
        "objective": "逐一檢驗原始連續訊號強度與隔日台指期日盤當沖結果的關係",
        "signals": metadata,
        "outcomes": OUTCOMES,
        "percentile_rule": "當日值只和此前 history_window 筆資料比較；不包含當日與未來資料",
        "buckets": list(BUCKET_LABELS),
        "tests": ["Spearman分位趨勢", "最高20%對最低20% Mann-Whitney U", "Kruskal五層差異"],
        "multiple_testing": "每個期間的所有指標×結果×檢定一起做 Benjamini-Hochberg FDR",
        "candidate_rule": "全期q<alpha、探索期q<0.10、驗證期p<alpha、前後期同方向且年度方向一致率>=60%",
        "warnings": [
            "本程式不使用既有策略多空，也不建立綜合分數",
            "MFE/MAE為日K可達空間，無法判斷盤中先後順序",
            "顯著關係仍需加入交易成本、分鐘K與真正樣本外回測",
        ],
    }
    paths["manifest"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return tuple(paths.values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="原始訊號強度與隔日台指期日盤結果掃描")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--minimum-group-size", type=int, default=30)
    parser.add_argument("--minimum-year-size", type=int, default=20)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--winsor-tail", type=float, default=0.005)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("backtest_output/futures_raw_signal_strength_scan"),
    )
    parser.add_argument(
        "--save-db", action="store_true",
        help="同時將本次明細與統計結果寫入 research schema（需先執行 migration）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id,
        split_date=args.split_date, history_window=args.history_window,
        minimum_history=args.minimum_history, minimum_group_size=args.minimum_group_size,
        minimum_year_size=args.minimum_year_size, alpha=args.alpha,
        winsor_tail=args.winsor_tail,
    )
    source = load_source(settings)
    detail, metadata = prepare_detail(source, settings)
    summary = build_bucket_summary(detail)
    full_tests = scan_period(detail, settings)
    tests = _merge_periods(full_tests, detail, settings)
    yearly = build_yearly_summary(detail, settings.minimum_year_size)
    run_id = uuid.uuid4()
    paths = export_results(
        detail, summary, tests, yearly, settings, metadata, args.output_dir, run_id
    )
    if args.save_db:
        save_to_database(
            run_id, detail, summary, tests, yearly, settings, source_rows=len(source)
        )
    print(f"原始強度指標：{len(metadata)} 個")
    print(f"強度明細：{len(detail):,} 筆")
    print(f"有效檢定：{len(tests):,} 組")
    print(f"全期FDR顯著：{int(tests['FDR顯著'].sum()) if len(tests) else 0} 組")
    print(f"前後期與年度穩定候選：{int(tests['重複驗證通過'].sum()) if len(tests) else 0} 組")
    print(f"研究 run_id：{run_id}")
    print(f"資料庫保存：{'完成' if args.save_db else '未啟用（加 --save-db 可保存）'}")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
