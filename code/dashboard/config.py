from __future__ import annotations

from dataclasses import dataclass
from math import inf


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    title: str
    unit: str
    bins: tuple[float, ...]
    labels: tuple[str, ...]

    @property
    def default_bin_width(self) -> float:
        return 0.5 if self.unit == "%" else 50.0


def _range_labels(start: float, stop: float, step: float, *, percent: bool = False) -> tuple[str, ...]:
    labels: list[str] = []
    value = start
    decimals = 1 if percent else 0
    suffix = "%" if percent else ""
    while value < stop:
        upper = value + step
        labels.append(f"{value:.{decimals}f}{suffix}~{upper:.{decimals}f}{suffix}")
        value = upper
    return tuple(labels)


POINT_POSITIVE_BINS = tuple(float(x) for x in range(0, 501, 25)) + (inf,)
POINT_POSITIVE_LABELS = _range_labels(0, 500, 25) + (">=500",)

PCT_POSITIVE_BINS = tuple(x / 2 for x in range(0, 19)) + (inf,)
PCT_POSITIVE_LABELS = _range_labels(0, 9, 0.5, percent=True) + (">=9.0%",)

POINT_SIGNED_BINS = (-inf,) + tuple(float(x) for x in range(-500, 501, 25)) + (inf,)
POINT_SIGNED_LABELS = ("<-500",) + _range_labels(-500, 500, 25) + (">=500",)

PCT_BODY_BINS = (-inf,) + tuple(x / 2 for x in range(-18, 19)) + (inf,)
PCT_BODY_LABELS = ("<-9.0%",) + _range_labels(-9, 9, 0.5, percent=True) + (">=9.0%",)

PCT_CHANGE_BINS = (-inf,) + tuple(x / 2 for x in range(-19, 20)) + (inf,)
PCT_CHANGE_LABELS = ("<-9.5%",) + _range_labels(-9.5, 9.5, 0.5, percent=True) + (">=9.5%",)


INDICATORS: dict[str, IndicatorSpec] = {
    "range_points": IndicatorSpec(
        "range_points", "1. 最高－最低（點）", "點", POINT_POSITIVE_BINS, POINT_POSITIVE_LABELS
    ),
    "range_prev_pct": IndicatorSpec(
        "range_prev_pct", "2-1.（最高－最低）／昨收（%）", "%", PCT_POSITIVE_BINS, PCT_POSITIVE_LABELS
    ),
    "range_open_pct": IndicatorSpec(
        "range_open_pct", "2-2.（最高－最低）／開盤（%）", "%", PCT_POSITIVE_BINS, PCT_POSITIVE_LABELS
    ),
    "close_open_points": IndicatorSpec(
        "close_open_points", "3. 收盤－開盤（點）", "點", POINT_SIGNED_BINS, POINT_SIGNED_LABELS
    ),
    "close_open_prev_pct": IndicatorSpec(
        "close_open_prev_pct", "4.（收盤－開盤）／昨收（%）", "%", PCT_BODY_BINS, PCT_BODY_LABELS
    ),
    "close_prev_points": IndicatorSpec(
        "close_prev_points", "5. 今收－昨收（點）", "點", POINT_SIGNED_BINS, POINT_SIGNED_LABELS
    ),
    "close_prev_pct": IndicatorSpec(
        "close_prev_pct", "6.（今收－昨收）／昨收（%）", "%", PCT_CHANGE_BINS, PCT_CHANGE_LABELS
    ),
}

MARKETS = {
    "taiex": "加權指數",
    "tx_day": "台指期日盤",
    "tx_night": "台指期夜盤",
    "tx_full": "台指期全日",
}

DEFAULT_START_DATE = "2000-01-01"
