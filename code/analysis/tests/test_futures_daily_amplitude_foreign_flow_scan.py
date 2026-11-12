from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from analysis.futures_daily_amplitude_foreign_flow_scan import (
    FOREIGN_COLUMN,
    NIGHT_RANGE_FEATURE,
    RECENT_RANGE_FEATURE,
    TURNOVER_COLUMN,
    Settings,
    _build_source_query,
    build_foreign_feature,
    build_rules,
    build_summary,
    build_yearly,
    prepare_model_frame,
)


class TestFuturesDailyAmplitudeForeignFlowScan(unittest.TestCase):
    @staticmethod
    def _settings() -> Settings:
        return Settings(
            start=date(2018, 1, 1),
            split_date=date(2021, 1, 1),
            history_window=80,
            minimum_history=20,
            minimum_rule_sample=10,
            minimum_year_rule_sample=3,
        )

    @staticmethod
    def _source(count: int = 1600) -> pd.DataFrame:
        dates = pd.bdate_range("2017-01-02", periods=count)
        index = np.arange(count)
        foreign = np.sin(index / 9.0) * 80 + np.where(index % 37 < 5, -220, 0)
        return pd.DataFrame({
            "日期": dates,
            FOREIGN_COLUMN: foreign,
            TURNOVER_COLUMN: 2500 + (index % 200),
        })

    @staticmethod
    def _feature_detail(source: pd.DataFrame, foreign: pd.DataFrame) -> pd.DataFrame:
        ratio_map = foreign.set_index("訊號日")["外資歷史分位_pct"]
        rows = []
        dates = pd.to_datetime(source["日期"])
        for i in range(len(dates) - 1):
            signal_date = dates.iloc[i]
            result_date = dates.iloc[i + 1]
            foreign_pct = ratio_map.get(signal_date, np.nan)
            recent_pct = 90.0 if i % 4 != 0 else 50.0
            night_pct = 90.0 if i % 5 != 0 else 50.0
            event = int(
                (recent_pct >= 80 and night_pct >= 80 and foreign_pct <= 20)
                or i % 29 == 0
            )
            for feature, percentile in (
                (RECENT_RANGE_FEATURE, recent_pct),
                (NIGHT_RANGE_FEATURE, night_pct),
            ):
                rows.append({
                    "訊號日": signal_date,
                    "結果日": result_date,
                    "range_ratio": 1.8 if event else 0.9,
                    "劇烈以上": event,
                    "指標代碼": feature,
                    "原始值": percentile / 100,
                    "歷史分位_pct": percentile,
                    "強度層": "Q5最高20%" if percentile >= 80 else "Q3中間",
                })
        return pd.DataFrame(rows)

    def test_foreign_percentile_does_not_use_current_value(self) -> None:
        settings = self._settings()
        source = self._source(120)
        feature = build_foreign_feature(source, settings)
        changed = source.copy()
        changed.loc[100, FOREIGN_COLUMN] = 999999
        changed_feature = build_foreign_feature(changed, settings)
        self.assertEqual(
            feature.loc[99, "外資歷史分位_pct"],
            changed_feature.loc[99, "外資歷史分位_pct"],
        )

    def test_builds_four_foreign_rules(self) -> None:
        rules = build_rules(self._settings())
        self.assertEqual([rule.rule_id for rule in rules], ["F01", "F02", "F03", "F04"])
        self.assertEqual(sum(rule.require_base_rule for rule in rules), 2)

    def test_loads_foreign_data_from_taiex_stock_table(self) -> None:
        sql, params = _build_source_query(self._settings())
        self.assertIn("LEFT JOIN core.stock_report_daily AS stock", sql)
        self.assertIn('stock."股票代號" = \'TAIEX\'', sql)
        self.assertIn('stock."買超_外資_億"', sql)
        self.assertIn('stock."總成交金額_億"', sql)
        self.assertNotIn('f."買超_外資_億"', sql)
        self.assertEqual(params[0], "TX")

    def test_summary_measures_increment_over_base_rule(self) -> None:
        settings = self._settings()
        source = self._source()
        foreign = build_foreign_feature(source, settings)
        detail = self._feature_detail(source, foreign)
        frame = prepare_model_frame(detail, foreign, settings)
        rules = build_rules(settings)
        yearly = build_yearly(frame, rules, settings)
        summary = build_summary(frame, rules, yearly, settings)
        row = summary[summary["規則代碼"].eq("F03")].iloc[0]
        self.assertEqual(row["驗證期_參考組"], "近期振幅＋夜盤振幅皆為Q5")
        self.assertGreater(row["驗證期_相對參考組Lift"], 1.0)
        self.assertGreater(row["驗證期_事件率_pct"], row["驗證期_參考組事件率_pct"])


if __name__ == "__main__":
    unittest.main()
