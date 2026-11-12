from datetime import date
from types import SimpleNamespace

import pandas as pd

from dashboard import report_updater
from module import margin_maintenance as subject


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "status": 200,
            "data": [
                {"date": "2026-11-06", "TotalExchangeMarginMaintenance": 167.095},
                {"date": "2026-11-05", "TotalExchangeMarginMaintenance": 166.123},
            ],
        }


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse()


def test_fetch_normalizes_finmind_payload(monkeypatch):
    monkeypatch.setattr(subject, "_finmind_token", lambda: "test-token")
    session = FakeSession()

    frame = subject.fetch_taiwan_total_margin_maintenance(
        "2026-11-01", "2026-11-09", session=session
    )

    assert list(frame.columns) == ["date", "total_exchange_margin_maintenance"]
    assert frame["date"].tolist() == [date(2026, 11, 5), date(2026, 11, 6)]
    assert frame["total_exchange_margin_maintenance"].tolist() == [166.123, 167.095]
    _, request = session.calls[0]
    assert request["params"]["dataset"] == "TaiwanTotalExchangeMarginMaintenance"
    assert request["params"]["start_date"] == "2026-11-01"
    assert request["params"]["end_date"] == "2026-11-09"


def test_incremental_update_uses_cached_end_with_lookback(monkeypatch):
    monkeypatch.setattr(subject, "_cached_span", lambda: (date(2001, 1, 5), date(2026, 11, 6)))
    captured = {}

    def fake_fetch(start, end):
        captured["range"] = (start, end)
        return pd.DataFrame({
            "date": [date(2026, 11, 7)],
            "total_exchange_margin_maintenance": [165.5],
        })

    monkeypatch.setattr(subject, "fetch_taiwan_total_margin_maintenance", fake_fetch)
    monkeypatch.setattr(subject, "upsert_taiwan_total_margin_maintenance", lambda frame: len(frame))
    monkeypatch.setattr(subject, "_sync_span_from_table", lambda: None)

    result = subject.update_taiwan_total_margin_maintenance(
        date(2026, 11, 9), lookback_days=7
    )

    assert captured["range"] == (date(2026, 10, 30), date(2026, 11, 9))
    assert result.written_rows == 1
    assert result.last_date == date(2026, 11, 7)


def test_dashboard_catalog_contains_other_margin_section(monkeypatch):
    monkeypatch.setattr(report_updater, "_get_cache_rows", lambda: {})
    catalog = report_updater.get_update_catalog()
    other = next(section for section in catalog["sections"] if section["id"] == "other")
    assert [item["id"] for item in other["items"]] == ["tw_margin_maintenance"]


def test_dashboard_runner_returns_margin_details(monkeypatch):
    fake = SimpleNamespace(
        requested_start=date(2026, 11, 1),
        requested_end=date(2026, 11, 9),
        written_rows=5,
        first_date=date(2026, 11, 3),
        last_date=date(2026, 11, 7),
    )
    monkeypatch.setattr(subject, "update_taiwan_total_margin_maintenance", lambda value: fake)
    rows, detail = report_updater._run_module(
        "tw_margin_maintenance", selected_modules={"tw_margin_maintenance"}
    )
    assert rows == 5
    assert detail["last_date"] == "2026-11-07"
