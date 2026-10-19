from __future__ import annotations

import numpy as np
import pandas as pd


CYCLE_TYPES = {
    "weekday": "星期統計",
    "month": "月份統計",
    "week": "週次統計",
}


def _cycle_definition(frame: pd.DataFrame, cycle_type: str) -> tuple[pd.Series, list[int], dict[int, str]]:
    if cycle_type == "weekday":
        keys = frame["日期"].dt.weekday
        order = list(range(5))
        labels = {0: "星期一", 1: "星期二", 2: "星期三", 3: "星期四", 4: "星期五"}
    elif cycle_type == "month":
        keys = frame["日期"].dt.month
        order = list(range(1, 13))
        labels = {value: f"{value}月" for value in order}
    elif cycle_type == "week":
        keys = frame["日期"].dt.isocalendar().week.astype(int)
        order = list(range(1, 54))
        labels = {value: f"{value}W" for value in order}
    else:
        raise ValueError("不支援的週期類型")
    return keys, order, labels


def build_cycle_statistics(frame: pd.DataFrame, cycle_type: str) -> tuple[pd.DataFrame, dict[str, float | int]]:
    working = frame[["日期", "close_prev_points", "close_prev_pct"]].copy()
    working["close_prev_points"] = pd.to_numeric(working["close_prev_points"], errors="coerce")
    working["close_prev_pct"] = pd.to_numeric(working["close_prev_pct"], errors="coerce")
    working = working.dropna(subset=["日期", "close_prev_points", "close_prev_pct"])
    if working.empty:
        raise ValueError("所選期間沒有可統計的有效資料")

    working["週期鍵"], order, labels = _cycle_definition(working, cycle_type)
    working = working.loc[working["週期鍵"].isin(order)].copy()
    working["方向"] = np.select(
        [working["close_prev_points"] > 0, working["close_prev_points"] < 0],
        ["up", "down"],
        default="flat",
    )

    rows: list[dict[str, float | int | str]] = []
    for key in order:
        group = working.loc[working["週期鍵"] == key]
        total = len(group)
        up = int((group["方向"] == "up").sum())
        down = int((group["方向"] == "down").sum())
        flat = int((group["方向"] == "flat").sum())
        rows.append(
            {
                "週期": labels[key],
                "上漲次數": up,
                "下跌次數": down,
                "平盤": flat,
                "上漲機率": up / total * 100 if total else 0.0,
                "下跌機率": down / total * 100 if total else 0.0,
                "平均上漲點": float(group.loc[group["方向"] == "up", "close_prev_points"].mean()) if up else 0.0,
                "平均下跌點": float(group.loc[group["方向"] == "down", "close_prev_points"].mean()) if down else 0.0,
                "平均漲跌點數": float(group["close_prev_points"].mean()) if total else 0.0,
                "平均漲跌幅": float(group["close_prev_pct"].mean()) if total else 0.0,
            }
        )

    table = pd.DataFrame(rows)
    total = len(working)
    total_up = int((working["方向"] == "up").sum())
    total_down = int((working["方向"] == "down").sum())
    summary: dict[str, float | int] = {
        "count": total,
        "up_count": total_up,
        "down_count": total_down,
        "flat_count": int((working["方向"] == "flat").sum()),
        "up_rate": total_up / total * 100,
        "down_rate": total_down / total * 100,
        "mean_points": float(working["close_prev_points"].mean()),
        "mean_pct": float(working["close_prev_pct"].mean()),
    }
    return table, summary
