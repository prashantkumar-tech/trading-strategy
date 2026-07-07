"""Tests for the daily incremental price-refresh wrapper + universe freshness."""

from data.update_prices import refresh_universe, universe_as_of


def test_refresh_universe_runs_incremental_and_returns_summary():
    calls = {}

    def fake_runner(tickers, refresh_mode="full"):
        calls["tickers"] = tickers
        calls["refresh_mode"] = refresh_mode
        return {"ok": tickers, "failed": []}

    summary = refresh_universe(["AAPL", "MSFT"], runner=fake_runner)

    assert calls["refresh_mode"] == "incremental"   # never a full re-download
    assert calls["tickers"] == ["AAPL", "MSFT"]
    assert summary == {"ok": ["AAPL", "MSFT"], "failed": []}


def test_universe_as_of_returns_most_common_max_date():
    details = {
        "A": {"max_date": "2026-07-06"},
        "B": {"max_date": "2026-07-06"},
        "C": {"max_date": "2026-07-06"},
        "D": {"max_date": "2026-07-02"},   # one laggard shouldn't drag the whole set
    }

    def details_fn(symbol, bar_size, source):
        return details.get(symbol)

    assert universe_as_of(["A", "B", "C", "D"], details_fn=details_fn) == "2026-07-06"


def test_universe_as_of_none_when_no_data():
    assert universe_as_of([], details_fn=lambda *a, **k: None) is None
