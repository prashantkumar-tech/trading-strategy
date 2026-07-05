from data.fetch_universe import fetch_universe


def test_fetch_universe_iterates_and_isolates_failures():
    calls = []

    def fake_fetch(symbol, **kwargs):
        calls.append(symbol)
        if symbol == "BAD":
            raise RuntimeError("no data")
        return None

    result = fetch_universe(["AAPL", "BAD", "MSFT"], fetch_fn=fake_fetch)

    assert calls == ["AAPL", "BAD", "MSFT"]
    assert result["ok"] == ["AAPL", "MSFT"]
    assert result["failed"] == [("BAD", "no data")]
