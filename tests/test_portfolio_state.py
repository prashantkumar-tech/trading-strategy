"""Round-trip and mutation tests for the live portfolio state model + JSON store."""

import pandas as pd

from portfolio.state import Portfolio, Position, Transaction, new_portfolio
from portfolio import store


def test_new_portfolio_starts_flat_with_cash():
    p = new_portfolio("live", config={"top_n": 10}, initial_capital=10_000.0,
                       inception_date="2026-07-07")
    assert p.name == "live"
    assert p.cash == 10_000.0
    assert p.positions == {}
    assert p.transactions == []
    assert p.inception_date == "2026-07-07"
    assert p.last_rebalance_date is None
    assert p.config["top_n"] == 10


def test_portfolio_round_trips_through_dict():
    p = new_portfolio("live", config={"top_n": 3}, initial_capital=5_000.0,
                      inception_date="2026-01-02")
    p.cash = 1_234.56
    p.positions["MU"] = Position(symbol="MU", shares=2.5, cost_basis=1600.0,
                                 entry_price=640.0, entry_date="2026-01-02")
    p.transactions.append(Transaction(date="2026-01-02", symbol="MU", action="BUY",
                                      shares=2.5, price=640.0, reason="new top-10"))
    p.last_rebalance_date = "2026-01-02"

    restored = Portfolio.from_dict(p.to_dict())
    assert restored == p
    assert restored.positions["MU"].shares == 2.5
    assert restored.transactions[0].action == "BUY"


def test_store_saves_and_loads_by_name(tmp_path):
    p = new_portfolio("mine", config={"top_n": 10}, initial_capital=10_000.0,
                      inception_date="2026-07-07")
    p.positions["NVDA"] = Position(symbol="NVDA", shares=1.0, cost_basis=170.0,
                                   entry_price=170.0, entry_date="2026-07-07")

    store.save(p, base_dir=tmp_path)
    loaded = store.load("mine", base_dir=tmp_path)

    assert loaded == p
    assert store.exists("mine", base_dir=tmp_path)
    assert not store.exists("other", base_dir=tmp_path)
    assert "mine" in store.list_portfolios(base_dir=tmp_path)


def test_store_load_missing_returns_none(tmp_path):
    assert store.load("nope", base_dir=tmp_path) is None
