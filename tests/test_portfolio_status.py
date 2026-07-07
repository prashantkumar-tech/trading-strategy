"""Tests for check_status, latest_ranks, value_portfolio, and freshness helpers."""

import pandas as pd
import pytest

from portfolio.state import new_portfolio, Position, Transaction
from portfolio.engine import (
    Ranks, check_status, latest_ranks, value_portfolio,
    previous_trading_day, is_stale,
)


def _ranks(momentum, prices, above=None, as_of="2026-07-31"):
    if above is None:
        above = {s: True for s in momentum}
    return Ranks(momentum=pd.Series(momentum, dtype=float), price=dict(prices),
                 above_200ma=dict(above), as_of=as_of)


# ── freshness helpers ───────────────────────────────────────────────────────

def test_previous_trading_day_skips_weekend():
    # 2026-07-06 is a Monday -> previous trading day is Friday 2026-07-03
    assert previous_trading_day("2026-07-06") == "2026-07-03"
    # 2026-07-07 Tuesday -> Monday 2026-07-06
    assert previous_trading_day("2026-07-07") == "2026-07-06"


def test_is_stale_flags_data_more_than_a_day_behind():
    assert not is_stale("2026-07-06", today="2026-07-07")   # yesterday's close: fresh
    assert not is_stale("2026-07-07", today="2026-07-07")   # today's close: fresh
    assert is_stale("2026-07-02", today="2026-07-07")       # several days behind: stale


# ── check_status ────────────────────────────────────────────────────────────

def test_check_status_rebalance_due_when_never_rebalanced():
    p = new_portfolio("t", {"top_n": 3, "buffer_rank": 6}, 3_000.0, "2026-07-01")
    r = _ranks({"A": 3, "B": 2, "C": 1}, {"A": 100, "B": 100, "C": 100})
    status = check_status(p, r, today="2026-07-15")
    assert status["rebalance_due"] is True


def test_check_status_due_next_month_not_same_month():
    p = new_portfolio("t", {"top_n": 3, "buffer_rank": 6}, 3_000.0, "2026-06-01")
    r = _ranks({"A": 3, "B": 2, "C": 1}, {"A": 100, "B": 100, "C": 100})
    p.last_rebalance_date = "2026-07-01"
    assert check_status(p, r, today="2026-07-20")["rebalance_due"] is False
    assert check_status(p, r, today="2026-08-03")["rebalance_due"] is True


def test_check_status_alerts_on_breaches():
    p = new_portfolio("t", {"top_n": 3, "buffer_rank": 3}, 0.0, "2026-06-01")
    p.positions["DIP"] = Position("DIP", 5, 500, 100, "2026-06-01")   # below MA
    p.positions["FADE"] = Position("FADE", 5, 500, 100, "2026-06-01")  # past buffer
    p.positions["OK"] = Position("OK", 5, 500, 100, "2026-06-01")     # healthy
    r = _ranks(
        {"OK": 9, "A": 8, "B": 7, "FADE": 6, "DIP": 5},
        {s: 100 for s in ["OK", "A", "B", "FADE", "DIP"]},
        above={"OK": True, "A": True, "B": True, "FADE": True, "DIP": False},
    )
    alerts = {a["symbol"]: a for a in check_status(p, r, today="2026-07-15")["alerts"]}
    assert "DIP" in alerts and "200" in alerts["DIP"]["message"]
    assert "FADE" in alerts        # eligible rank 4 > buffer 3
    assert "OK" not in alerts


def test_check_status_blocks_rebalance_on_stale_prices():
    p = new_portfolio("t", {"top_n": 3, "buffer_rank": 6}, 3_000.0, "2026-07-01")
    r = _ranks({"A": 3, "B": 2, "C": 1}, {"A": 100, "B": 100, "C": 100},
               as_of="2026-07-02")
    status = check_status(p, r, today="2026-07-07", prices_as_of="2026-07-02")
    assert status["stale"] is True
    assert status["can_rebalance"] is False


# ── latest_ranks ────────────────────────────────────────────────────────────

def test_latest_ranks_builds_snapshot_from_loader():
    dates = pd.date_range("2020-01-01", periods=6, freq="D")

    def loader(symbol, start=None, end=None):
        series = {
            "A": [100, 110, 121, 133, 146, 161],
            "B": [100, 103, 106, 109, 112, 115],
            "C": [100, 101, 102, 103, 104, 105],
        }[symbol]
        ma = [s * 0.5 for s in series]
        if symbol == "C":
            ma = [s * 2 for s in series]        # C below its 200-MA
        return pd.DataFrame({"date": dates, "close": series, "ma200": ma})

    r = latest_ranks(["A", "B", "C"], lookback_days=2, skip_days=1, loader=loader)
    assert r.rank["A"] == 1 and r.rank["C"] == 3
    assert r.price["A"] == 161
    assert r.above_200ma["A"] is True and r.above_200ma["C"] is False
    assert "C" not in r.eligible
    assert r.as_of == "2020-01-06"


# ── value_portfolio ─────────────────────────────────────────────────────────

def test_value_portfolio_reconstructs_curve_from_transactions():
    p = new_portfolio("t", {"top_n": 1}, 1_000.0, "2020-01-01")
    p.cash = 0.0
    p.positions["A"] = Position("A", 10, 1000, 100, "2020-01-01")
    p.transactions = [Transaction("2020-01-01", "A", "BUY", 10, 100, "buy")]

    dates = pd.date_range("2020-01-01", periods=3, freq="D")

    def loader(symbol, start=None, end=None):
        return pd.DataFrame({"date": dates, "close": [100, 110, 120],
                             "ma200": [50, 50, 50]})

    res = value_portfolio(p, loader=loader)
    curve = res["equity_curve"]
    assert curve.iloc[0] == pytest.approx(1000.0)   # 10 sh * $100
    assert curve.iloc[-1] == pytest.approx(1200.0)  # 10 sh * $120
    assert res["current_value"] == pytest.approx(1200.0)
    assert res["total_return_pct"] == pytest.approx(20.0)


def test_value_portfolio_flat_portfolio_is_all_cash():
    p = new_portfolio("t", {"top_n": 1}, 1_000.0, "2020-01-01")

    def loader(symbol, start=None, end=None):
        return pd.DataFrame(columns=["date", "close", "ma200"])

    res = value_portfolio(p, loader=loader)
    assert res["current_value"] == pytest.approx(1000.0)
    assert res["cash"] == pytest.approx(1000.0)
