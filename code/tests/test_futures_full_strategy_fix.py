from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "futures_report_full_strategy_fix.sql").read_text(encoding="utf-8")


def test_corrected_view_uses_full_day_inputs():
    for field in (
        '"全日收"',
        '"全成交量"',
        '"全5均量"',
        '"全5MA"',
        '"全10MA"',
        '"全20MA"',
        '"全布林上軌"',
        '"全布林下軌"',
    ):
        assert field in SQL


def test_all_corrected_outputs_are_overridden():
    for field in (
        "全位置",
        "策略_全價量昨日",
        "策略_全價量五日",
        "全MA方向_5",
        "全MA方向_10",
        "全MA方向_20",
        "策略_線型",
        "策略_價量昨日_線型",
        "策略_價量五日_線型",
        "策略_價量昨日_籌碼",
        "策略_價量五日_籌碼",
        "策略_核心",
    ):
        assert f"'{field}'" in SQL


def test_dashboard_and_csv_use_corrected_view():
    raw_service = (ROOT / "dashboard" / "raw_service.py").read_text(encoding="utf-8")
    futures = (ROOT / "main" / "futures.py").read_text(encoding="utf-8")
    assert '"v_futures_report_daily_corrected"' in raw_service
    assert "FROM report.v_futures_report_daily_corrected" in futures
