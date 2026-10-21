from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from backtest.futures_volatility_model_compare import (
    FEATURES,
    build_metrics,
    prepare_model_data,
    walk_forward_predict,
)


class TestFuturesVolatilityModelCompare(unittest.TestCase):
    @staticmethod
    def _daily() -> pd.DataFrame:
        rng = np.random.default_rng(20261020)
        dates = pd.bdate_range("2018-01-02", "2026-10-16")
        bollinger = rng.uniform(0, 100, len(dates))
        retail = rng.uniform(0, 100, len(dates))
        ma10 = rng.uniform(0, 100, len(dates))
        main_oi = rng.uniform(0, 100, len(dates))
        strengths = np.column_stack((100-bollinger, retail, 100-ma10, 100-main_oi))
        score = (strengths >= 80).sum(axis=1)
        intraday_range = 0.55 + strengths @ np.array([0.004, 0.003, 0.002, 0.005])
        intraday_range += rng.normal(0, 0.12, len(dates))
        return pd.DataFrame({
            "訊號日": dates,
            "隔日日期": dates + pd.offsets.BDay(1),
            "波動分數": score,
            "布林通道位置_歷史分位_pct": bollinger,
            "散戶多空比_歷史分位_pct": retail,
            "MA10斜率_歷史分位_pct": ma10,
            "主力淨多空_OI比_歷史分位_pct": main_oi,
            "隔日日內振幅_pct": intraday_range,
        })

    def test_prepare_model_data_builds_continuous_strengths(self) -> None:
        daily = self._daily().head(3)
        data = prepare_model_data(daily)
        self.assertAlmostEqual(data.iloc[0]["布林低位強度"], 100-daily.iloc[0]["布林通道位置_歷史分位_pct"])
        self.assertAlmostEqual(data.iloc[0]["散戶比偏高強度"], daily.iloc[0]["散戶多空比_歷史分位_pct"])
        self.assertEqual(set(FEATURES), set(data.columns) & set(FEATURES))

    def test_walk_forward_predictions_only_start_after_training_window(self) -> None:
        data = prepare_model_data(self._daily())
        predictions, coefficients = walk_forward_predict(data, first_test_year=2021, minimum_train_size=500)
        self.assertEqual(predictions["樣本外年度"].min(), 2021)
        self.assertEqual(predictions["樣本外年度"].max(), 2026)
        self.assertFalse(predictions.isna().any().any())
        self.assertEqual(set(coefficients["模型"]), {"Ridge", "Logistic"})

    def test_future_changes_do_not_affect_earlier_predictions(self) -> None:
        data = prepare_model_data(self._daily())
        baseline, _ = walk_forward_predict(data, first_test_year=2021, minimum_train_size=500)
        changed = data.copy()
        changed.loc[changed["訊號日"].dt.year >= 2023, "隔日日內振幅_pct"] += 20
        changed["振幅至少1.0pct"] = (changed["隔日日內振幅_pct"] >= 1.0).astype(int)
        changed["振幅至少1.5pct"] = (changed["隔日日內振幅_pct"] >= 1.5).astype(int)
        modified, _ = walk_forward_predict(changed, first_test_year=2021, minimum_train_size=500)
        columns = [column for column in baseline.columns if "預測" in column or "機率" in column]
        pd.testing.assert_frame_equal(
            baseline.loc[baseline["樣本外年度"] <= 2023, columns].reset_index(drop=True),
            modified.loc[modified["樣本外年度"] <= 2023, columns].reset_index(drop=True),
        )

    def test_metrics_compare_baseline_and_regularized_models(self) -> None:
        data = prepare_model_data(self._daily())
        predictions, _ = walk_forward_predict(data, first_test_year=2021, minimum_train_size=500)
        metrics = build_metrics(predictions)
        overall = metrics[metrics["評估期間"] == "全部樣本外"]
        self.assertEqual(len(overall), 6)
        self.assertEqual(set(overall["模型"]), {"簡單分數", "Ridge", "Logistic"})
        ridge = overall[(overall["目標"] == "隔日日內振幅_pct") & (overall["模型"] == "Ridge")].iloc[0]
        baseline = overall[(overall["目標"] == "隔日日內振幅_pct") & (overall["模型"] == "簡單分數")].iloc[0]
        self.assertLess(ridge["RMSE"], baseline["RMSE"])


if __name__ == "__main__":
    unittest.main()
