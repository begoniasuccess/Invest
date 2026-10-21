from __future__ import annotations

import unittest
import uuid
from datetime import date

import pandas as pd

from backtest.futures_raw_signal_strength_scan import Settings
from backtest.futures_volatility_strength_backtest import (
    SCORE_RULES,
    build_daily_score,
    build_database_frames,
    build_summary,
    build_tests,
    build_yearly_summary,
)


class TestFuturesVolatilityStrengthBacktest(unittest.TestCase):
    @staticmethod
    def _detail(size: int = 300) -> pd.DataFrame:
        rows = []
        dates = pd.bdate_range("2021-01-04", periods=size)
        signals = list(SCORE_RULES)
        for index, signal_date in enumerate(dates):
            score = index % 5
            conditions = {
                "布林通道位置": 10.0 if score >= 1 else 50.0,
                "散戶多空比": 90.0 if score >= 2 else 50.0,
                "MA10斜率": 10.0 if score >= 3 else 50.0,
                "主力淨多空_OI比": 10.0 if score >= 4 else 50.0,
            }
            intraday_range = 0.8 + score * 0.3
            for signal in signals:
                rows.append({
                    "訊號日": signal_date,
                    "隔日日期": signal_date + pd.offsets.BDay(1),
                    "指標代碼": signal,
                    "原始值": conditions[signal],
                    "歷史分位_pct": conditions[signal],
                    "隔日開收報酬_pct": 0.05 if index % 2 else -0.05,
                    "隔日上行MFE_pct": intraday_range * 0.55,
                    "隔日下行MFE_pct": intraday_range * 0.45,
                    "隔日日內振幅_pct": intraday_range,
                    "隔日收盤位置_pct": 50.0,
                    "隔日開盤跳空_日收_pct": 0.0,
                    "隔日開盤跳空_夜收_pct": 0.0,
                })
        return pd.DataFrame(rows)

    def test_score_and_level_rules(self) -> None:
        daily = build_daily_score(self._detail(10))
        self.assertEqual(daily["波動分數"].tolist(), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4])
        self.assertEqual(
            daily["波動等級"].tolist()[:5],
            ["低波動", "低波動", "中波動", "高波動", "高波動"],
        )

    def test_missing_signal_makes_score_unavailable(self) -> None:
        detail = self._detail(10)
        mask = (detail["訊號日"] == detail["訊號日"].min()) & (detail["指標代碼"] == "散戶多空比")
        detail.loc[mask, "歷史分位_pct"] = pd.NA
        daily = build_daily_score(detail)
        self.assertTrue(pd.isna(daily.iloc[0]["波動分數"]))
        self.assertEqual(daily.iloc[0]["有效條件數"], 3)

    def test_summary_and_tests_show_higher_range(self) -> None:
        daily = build_daily_score(self._detail())
        summary = build_summary(daily, date(2022, 1, 1))
        full_levels = summary[(summary["期間"] == "全期") & (summary["分組方式"] == "等級")]
        values = full_levels.set_index("分組")["隔日日內振幅平均_pct"]
        self.assertGreater(values["高波動"], values["低波動"])
        settings = Settings(
            start=date(2021, 1, 1), split_date=date(2022, 1, 1),
            minimum_group_size=10, minimum_year_size=5,
        )
        tests = build_tests(daily, settings)
        full_trend = tests[(tests["期間"] == "全期") & (tests["檢定類型"] == "Spearman分數趨勢")].iloc[0]
        self.assertGreater(full_trend["效果值"], 0.9)
        self.assertTrue(full_trend["FDR顯著"])

    def test_database_frames_have_unique_keys(self) -> None:
        daily = build_daily_score(self._detail())
        summary = build_summary(daily, date(2022, 1, 1))
        yearly = build_yearly_summary(daily, minimum_size=5)
        settings = Settings(
            start=date(2021, 1, 1), split_date=date(2022, 1, 1),
            minimum_group_size=10, minimum_year_size=5,
        )
        tests = build_tests(daily, settings)
        run_id = uuid.uuid4()
        daily_db, summary_db, yearly_db, test_db = build_database_frames(
            run_id, daily, summary, yearly, tests
        )
        self.assertFalse(daily_db.duplicated(["run_id", "signal_date"]).any())
        self.assertFalse(summary_db.duplicated(["run_id", "period_name", "group_type", "group_label"]).any())
        self.assertFalse(yearly_db.duplicated(["run_id", "result_year", "volatility_level"]).any())
        self.assertFalse(test_db.duplicated(["run_id", "period_name", "test_type"]).any())


if __name__ == "__main__":
    unittest.main()
