"""Tests for the live rebalance engine: propose_actions + apply_rebalance."""

import pandas as pd
import pytest

from portfolio.state import new_portfolio, Position
from portfolio.engine import Ranks, propose_actions, apply_rebalance


def _ranks(momentum: dict, prices: dict, above: dict = None, as_of="2026-07-31"):
    """Build a Ranks from plain dicts; default every name above its 200-MA."""
    if above is None:
        above = {s: True for s in momentum}
    return Ranks(momentum=pd.Series(momentum, dtype=float), price=dict(prices),
                 above_200ma=dict(above), as_of=as_of)


def _fresh(top_n=3, buffer_rank=6, cash=3_000.0):
    return new_portfolio("t", config={"top_n": top_n, "buffer_rank": buffer_rank},
                         initial_capital=cash, inception_date="2026-07-01")


# ── Ranks derived fields ────────────────────────────────────────────────────

def test_ranks_computes_rank_and_eligible():
    r = _ranks({"A": 0.5, "B": 0.3, "C": 0.1}, {"A": 10, "B": 10, "C": 10},
               above={"A": True, "B": False, "C": True})
    assert r.rank == {"A": 1, "B": 2, "C": 3}
    assert r.eligible == {"A", "C"}       # B fails the 200-MA filter


# ── propose_actions ─────────────────────────────────────────────────────────

def test_propose_actions_buys_top_n_from_flat_portfolio():
    p = _fresh(top_n=3, cash=3_000.0)
    r = _ranks({"A": 0.5, "B": 0.4, "C": 0.3, "D": 0.2},
               {"A": 100, "B": 100, "C": 100, "D": 100})
    actions = propose_actions(p, r)
    buys = {a["symbol"]: a for a in actions if a["action"] == "BUY"}
    assert set(buys) == {"A", "B", "C"}          # top 3, D left out
    # equal weight: $3000 / 3 = $1000 per name -> 10 shares each
    assert buys["A"]["target_shares"] == pytest.approx(10.0)
    assert buys["A"]["dollar_amount"] == pytest.approx(1000.0)


def test_propose_actions_sells_holding_past_buffer_and_below_ma():
    p = _fresh(top_n=3, buffer_rank=4, cash=0.0)
    p.positions["OLD"] = Position("OLD", shares=5, cost_basis=500, entry_price=100,
                                  entry_date="2026-06-01")
    p.positions["DIP"] = Position("DIP", shares=5, cost_basis=500, entry_price=100,
                                  entry_date="2026-06-01")
    # OLD ranks 6 (> buffer 4) -> sell; DIP is rank 2 but below its 200-MA -> sell
    r = _ranks({"A": 9, "B": 8, "DIP": 7, "C": 6, "D": 5, "OLD": 4},
               {s: 100 for s in ["A", "B", "DIP", "C", "D", "OLD"]},
               above={"A": True, "B": True, "DIP": False, "C": True, "D": True, "OLD": True})
    actions = propose_actions(p, r)
    sells = {a["symbol"]: a for a in actions if a["action"] == "SELL"}
    assert "OLD" in sells and "fell out" in sells["OLD"]["reason"].lower()
    assert "DIP" in sells and "200" in sells["DIP"]["reason"]


def test_propose_actions_holds_name_still_in_basket():
    p = _fresh(top_n=3, buffer_rank=6, cash=0.0)
    p.positions["A"] = Position("A", shares=10, cost_basis=1000, entry_price=100,
                                entry_date="2026-06-01")
    r = _ranks({"A": 9, "B": 8, "C": 7}, {"A": 100, "B": 100, "C": 100})
    actions = propose_actions(p, r)
    a_action = next(a for a in actions if a["symbol"] == "A")
    assert a_action["action"] in {"HOLD", "TRIM", "BUY"}
    assert a_action["action"] != "SELL"   # A still ranks top-3, never a full exit


# ── apply_rebalance ─────────────────────────────────────────────────────────

def portfolio_value(p, prices):
    return p.cash + sum(pos.shares * prices[s] for s, pos in p.positions.items())


def test_apply_rebalance_conserves_value_and_hits_equal_weight():
    p = _fresh(top_n=3, cash=3_000.0)
    prices = {"A": 100, "B": 100, "C": 100, "D": 100}
    r = _ranks({"A": 0.5, "B": 0.4, "C": 0.3, "D": 0.2}, prices)

    before = portfolio_value(p, prices)
    apply_rebalance(p, ["A", "B", "C"], r, as_of_date="2026-07-31")
    after = portfolio_value(p, prices)

    assert before == pytest.approx(after)           # buy/sell at price is value-neutral
    assert set(p.positions) == {"A", "B", "C"}
    for s in ["A", "B", "C"]:
        assert p.positions[s].shares * prices[s] == pytest.approx(1000.0)  # equal weight
    assert p.last_rebalance_date == "2026-07-31"
    assert len([t for t in p.transactions if t.action == "BUY"]) == 3


def test_apply_rebalance_rotates_out_and_logs_reason():
    p = _fresh(top_n=2, buffer_rank=3, cash=0.0)
    p.positions["OLD"] = Position("OLD", shares=10, cost_basis=1000, entry_price=100,
                                  entry_date="2026-06-01")
    prices = {"A": 100, "B": 100, "OLD": 100}
    r = _ranks({"A": 9, "B": 8, "OLD": 4}, prices)  # OLD rank 3 == buffer? make it exit

    apply_rebalance(p, ["A", "B"], r, as_of_date="2026-07-31")

    assert "OLD" not in p.positions
    sell = next(t for t in p.transactions if t.symbol == "OLD" and t.action == "SELL")
    assert sell.shares == pytest.approx(10.0)
    assert set(p.positions) == {"A", "B"}


def test_apply_rebalance_trims_overweight_holding():
    p = _fresh(top_n=2, cash=0.0)
    # A is grossly overweight; rebalance should trim it toward equal weight
    p.positions["A"] = Position("A", shares=30, cost_basis=3000, entry_price=100,
                                entry_date="2026-06-01")
    prices = {"A": 100, "B": 100}
    r = _ranks({"A": 0.5, "B": 0.4}, prices)

    apply_rebalance(p, ["A", "B"], r, as_of_date="2026-07-31")

    # total value $3000 -> $1500 each
    assert p.positions["A"].shares * prices["A"] == pytest.approx(1500.0)
    assert p.positions["B"].shares * prices["B"] == pytest.approx(1500.0)
    assert any(t.symbol == "A" and t.action == "SELL" for t in p.transactions)  # a trim
