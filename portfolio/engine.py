"""Live rotation/rebalance logic for a paper portfolio.

Built directly on the existing momentum engine — ``select_basket`` /
``compute_momentum`` / ``build_price_panel`` from ``backtest.momentum_rotation``
— so the live strategy and the backtest stay in lock-step. This module is
data-layer-agnostic: the ranking helpers accept an injectable ``loader`` and the
core (``propose_actions`` / ``apply_rebalance``) operates on a plain ``Ranks``
value object, so everything is unit-testable without a database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable

import pandas as pd

from portfolio.state import Portfolio, Position, Transaction
from backtest.momentum_rotation import (
    select_basket, compute_momentum, build_price_panel,
)
from backtest.metrics import compute_metrics
from data.database import load_prices


@dataclass
class Ranks:
    """A snapshot of the universe's momentum ranking on one date.

    ``momentum`` is a symbol->momentum Series; ``price`` and ``above_200ma`` are
    per-symbol dicts. Derived on construction: ``rank`` (1-based over every named
    symbol, matching the leaderboard) and ``eligible`` / ``eligible_rank``
    (restricted to names that pass the 200-MA filter and have a price — the set
    the basket is actually drawn from, matching the backtest).
    """
    momentum: pd.Series
    price: Dict[str, float]
    above_200ma: Dict[str, bool]
    as_of: str
    rank: Dict[str, int] = field(init=False)
    eligible: set = field(init=False)
    eligible_rank: Dict[str, int] = field(init=False)

    def __post_init__(self):
        ranked = self.momentum.dropna().sort_values(ascending=False)
        self.rank = {s: i + 1 for i, s in enumerate(ranked.index)}
        self.eligible = {
            s for s in ranked.index
            if self.above_200ma.get(s)
            and s in self.price and pd.notna(self.price[s])
        }
        elig_sorted = [s for s in ranked.index if s in self.eligible]
        self.eligible_rank = {s: i + 1 for i, s in enumerate(elig_sorted)}


def _exit_reason(symbol: str, ranks: Ranks, buffer_rank: int) -> str:
    """Why a currently-held name is leaving the basket."""
    if symbol not in ranks.eligible:
        return "below 200MA"
    er = ranks.eligible_rank.get(symbol)
    if er is None or er > buffer_rank:
        return f"fell out of top {buffer_rank}"
    return "rotated out"


def _target_per_slot(portfolio: Portfolio, ranks: Ranks, top_n: int) -> float:
    held = sum(pos.shares * ranks.price[s]
               for s, pos in portfolio.positions.items()
               if s in ranks.price and pd.notna(ranks.price[s]))
    return (portfolio.cash + held) / top_n


def propose_actions(portfolio: Portfolio, ranks: Ranks,
                    sectors: Optional[dict] = None) -> List[dict]:
    """Diff the target basket against current holdings → a list of proposed actions.

    Each action is a dict {action, symbol, current_shares, target_shares, price,
    dollar_amount, reason, rank}. ``action`` is SELL (full exit), BUY (new name or
    equal-weight top-up), TRIM (equal-weight reduce), or HOLD (already on target).
    This is what the approve-flow UI renders and lets the user veto/promote.
    """
    cfg = portfolio.config
    top_n = int(cfg["top_n"])
    buffer_rank = int(cfg.get("buffer_rank", 2 * top_n))
    max_per_sector = cfg.get("max_per_sector") or None
    holdings = portfolio.holdings()

    basket, _ = select_basket(
        ranks.momentum, ranks.eligible, holdings,
        top_n=top_n, buffer_rank=buffer_rank,
        sectors=sectors, max_per_sector=max_per_sector,
    )
    target = _target_per_slot(portfolio, ranks, top_n)
    actions: List[dict] = []

    # ── Exits: current holdings no longer in the basket ──────────────────
    for s in holdings:
        if s in basket:
            continue
        price = ranks.price.get(s)
        cur = portfolio.positions[s].shares
        actions.append({
            "action": "SELL", "symbol": s, "current_shares": round(cur, 6),
            "target_shares": 0.0, "price": price,
            "dollar_amount": round(cur * price, 2) if price is not None else None,
            "reason": _exit_reason(s, ranks, buffer_rank), "rank": ranks.rank.get(s),
        })

    # ── Basket: buys, top-ups, trims, holds ──────────────────────────────
    for s in basket:
        price = ranks.price.get(s)
        cur = portfolio.positions[s].shares if s in portfolio.positions else 0.0
        target_shares = (target / price) if price else 0.0
        delta = target_shares - cur
        held = s in portfolio.positions
        if not held:
            action, reason = "BUY", f"entered basket (rank {ranks.rank.get(s)})"
        elif price is None or abs(delta * price) < 1.0:
            action, reason = "HOLD", "in basket"
        elif delta > 0:
            action, reason = "BUY", "rebalance top-up"
        else:
            action, reason = "TRIM", "rebalance trim"
        actions.append({
            "action": action, "symbol": s, "current_shares": round(cur, 6),
            "target_shares": round(target_shares, 6), "price": price,
            "dollar_amount": round(abs(delta) * price, 2) if price else None,
            "reason": reason, "rank": ranks.rank.get(s),
        })
    return actions


def apply_rebalance(portfolio: Portfolio, approved_basket: List[str],
                    ranks: Ranks, as_of_date: str) -> List[Transaction]:
    """Execute the approved basket against the paper portfolio, mutating it.

    Sells every holding not in ``approved_basket``, then rebalances the basket to
    equal weight (portfolio_value / top_n per name) — trims first to free cash,
    then buys. Appends one ``Transaction`` per fill, updates cash, and stamps
    ``last_rebalance_date``. Returns the executed transactions.
    """
    cfg = portfolio.config
    top_n = int(cfg["top_n"])
    buffer_rank = int(cfg.get("buffer_rank", 2 * top_n))
    price = ranks.price
    approved = set(approved_basket)

    def priced(s) -> bool:
        return s in price and pd.notna(price[s])

    target = _target_per_slot(portfolio, ranks, top_n)
    executed: List[Transaction] = []

    # ── Exits ────────────────────────────────────────────────────────────
    for s in list(portfolio.positions.keys()):
        if s in approved or not priced(s):
            continue
        pos = portfolio.positions[s]
        proceeds = pos.shares * price[s]
        portfolio.cash += proceeds
        executed.append(Transaction(as_of_date, s, "SELL", round(pos.shares, 6),
                                    float(price[s]), _exit_reason(s, ranks, buffer_rank)))
        del portfolio.positions[s]

    # ── Rebalance to equal weight: trims first (free cash), then buys ─────
    deltas = {s: (target / price[s]) - (portfolio.positions[s].shares
                                        if s in portfolio.positions else 0.0)
              for s in approved_basket if priced(s)}

    for s, delta in deltas.items():
        if delta >= 0:
            continue
        pos = portfolio.positions[s]
        sell_shares = -delta
        pos.cost_basis *= (pos.shares - sell_shares) / pos.shares if pos.shares else 0.0
        pos.shares -= sell_shares
        portfolio.cash += sell_shares * price[s]
        executed.append(Transaction(as_of_date, s, "SELL", round(sell_shares, 6),
                                    float(price[s]), "rebalance trim"))

    for s, delta in deltas.items():
        if delta <= 0:
            continue
        p = price[s]
        cost = min(delta * p, portfolio.cash)
        if cost <= 0:
            continue
        buy_shares = cost / p
        if s in portfolio.positions:
            pos = portfolio.positions[s]
            pos.shares += buy_shares
            pos.cost_basis += cost
            reason = "rebalance top-up"
        else:
            portfolio.positions[s] = Position(s, buy_shares, cost, float(p), as_of_date)
            reason = f"entered basket (rank {ranks.rank.get(s)})"
        portfolio.cash -= cost
        executed.append(Transaction(as_of_date, s, "BUY", round(buy_shares, 6),
                                    float(p), reason))

    portfolio.last_rebalance_date = as_of_date
    portfolio.transactions.extend(executed)
    return executed


# ── Freshness helpers ────────────────────────────────────────────────────────

def previous_trading_day(date) -> str:
    """The most recent weekday strictly before ``date`` (holidays not modelled)."""
    prev = pd.Timestamp(date) - pd.tseries.offsets.BDay(1)
    return prev.strftime("%Y-%m-%d")


def is_stale(prices_as_of, today) -> bool:
    """True if the latest price bar is more than one trading day behind ``today``.

    Lenient by design: having yesterday's (or today's) close counts as fresh, so a
    tool run in the evening or next morning is never falsely flagged.
    """
    if prices_as_of is None:
        return True
    return pd.Timestamp(prices_as_of) < pd.Timestamp(previous_trading_day(today))


# ── Status: rebalance-due + interim alerts + freshness guard ──────────────────

def check_status(portfolio: Portfolio, ranks: Ranks, today: str,
                 prices_as_of: Optional[str] = None) -> dict:
    """Whether a rebalance is due, any interim breach alerts, and data freshness.

    ``rebalance_due`` is calendar-driven (a new month since the last rebalance, or
    never rebalanced). ``alerts`` flags any holding that broke its 200-MA or fell
    past the rank buffer. ``can_rebalance`` is False when prices are stale — the
    guard against acting on an out-of-date signal.
    """
    cfg = portfolio.config
    buffer_rank = int(cfg.get("buffer_rank", 2 * int(cfg["top_n"])))

    if portfolio.last_rebalance_date is None:
        due = True
    else:
        last = pd.Timestamp(portfolio.last_rebalance_date)
        now = pd.Timestamp(today)
        due = (now.year, now.month) > (last.year, last.month)

    alerts: List[dict] = []
    for s in portfolio.holdings():
        if s not in ranks.eligible:
            alerts.append({"symbol": s, "kind": "below_ma",
                           "message": f"{s} is below its 200-day MA"})
            continue
        er = ranks.eligible_rank.get(s)
        if er is None or er > buffer_rank:
            alerts.append({"symbol": s, "kind": "buffer",
                           "message": f"{s} fell to rank {er} (buffer {buffer_rank})"})

    as_of = prices_as_of or ranks.as_of
    stale = is_stale(as_of, today)
    return {
        "rebalance_due": due,
        "alerts": alerts,
        "stale": stale,
        "prices_as_of": as_of,
        "can_rebalance": not stale,
    }


# ── Ranking snapshot from stored prices ───────────────────────────────────────

def latest_ranks(symbols: List[str], lookback_days: int = 252, skip_days: int = 21,
                 loader: Callable = load_prices) -> Ranks:
    """Build a ``Ranks`` snapshot as of the latest available date for ``symbols``.

    Mirrors ``momentum_leaderboard``'s ranking/eligibility, packaged for the live
    engine. ``loader`` is injectable so this is testable without a database.
    """
    close, ma200 = build_price_panel(symbols, loader=loader)
    if close.empty:
        raise ValueError("No price data available for the selected symbols.")
    momentum = compute_momentum(close, lookback_days, skip_days)
    latest_close = close.iloc[-1]
    latest_ma = ma200.iloc[-1]
    as_of = close.index[-1].strftime("%Y-%m-%d")

    price = {s: (float(latest_close[s]) if pd.notna(latest_close[s]) else None)
             for s in close.columns}
    above = {s: bool(pd.notna(latest_ma.get(s)) and pd.notna(latest_close[s])
                     and latest_close[s] > latest_ma[s])
             for s in close.columns}
    return Ranks(momentum=momentum.iloc[-1].dropna(), price=price,
                 above_200ma=above, as_of=as_of)


# ── Forward performance tracking ──────────────────────────────────────────────

def value_portfolio(portfolio: Portfolio, loader: Callable = load_prices,
                    spy_loader: Optional[Callable] = None,
                    today: Optional[str] = None) -> dict:
    """Reconstruct the equity curve from inception by replaying the transactions.

    Starting from ``config['initial_capital']`` in cash, replay every transaction
    in date order and value the resulting holdings against stored daily closes.
    Returns the equity curve, current snapshot, total return, optional SPY curve,
    and metrics (reusing ``compute_metrics``).
    """
    initial = float(portfolio.config.get("initial_capital", portfolio.cash))
    symbols = sorted(set(list(portfolio.positions.keys())
                         + [t.symbol for t in portfolio.transactions]))

    # Current snapshot always values holdings at their latest available close
    # (independent of whether a curve can be built) so the header is accurate on
    # day zero — inception day, before a bar dated >= inception exists.
    holdings_value = 0.0
    for s, pos in portfolio.positions.items():
        lc = _latest_close(loader, s)
        holdings_value += pos.shares * (lc if lc is not None else pos.entry_price)
    current_value = portfolio.cash + holdings_value
    snapshot = {
        "equity_curve": pd.Series(dtype=float),
        "current_value": round(current_value, 2), "cash": round(portfolio.cash, 2),
        "holdings_value": round(holdings_value, 2),
        "total_return_pct": round((current_value / initial - 1) * 100, 2) if initial else 0.0,
        "spy_curve": None, "metrics": {},
    }
    if not symbols:
        return snapshot

    close, _ = build_price_panel(symbols, start=portfolio.inception_date,
                                 end=today, loader=loader)
    if close.empty:
        return snapshot

    tx_by_date: Dict[str, list] = {}
    for t in portfolio.transactions:
        tx_by_date.setdefault(t.date, []).append(t)

    cash = initial
    shares: Dict[str, float] = {s: 0.0 for s in symbols}
    values = {}
    for d in close.index:
        for t in tx_by_date.get(d.strftime("%Y-%m-%d"), []):
            if t.action == "BUY":
                cash -= t.shares * t.price
                shares[t.symbol] = shares.get(t.symbol, 0.0) + t.shares
            else:
                cash += t.shares * t.price
                shares[t.symbol] = shares.get(t.symbol, 0.0) - t.shares
        row = close.loc[d]
        values[d] = cash + sum(shares[s] * row[s]
                               for s in symbols if pd.notna(row[s]))

    curve = pd.Series(values).sort_index()

    spy_curve = None
    if spy_loader is not None:
        spy = spy_loader("SPY", start=portfolio.inception_date, end=today)
        if spy is not None and not spy.empty:
            spy_close = spy.set_index("date")["close"]
            spy_curve = spy_close / spy_close.iloc[0] * initial

    # Curve + metrics come from the reconstruction; the current snapshot (value,
    # cash, holdings, return) stays authoritative so the header is right even when
    # the latest bar predates a same-day rebalance.
    return {
        **snapshot,
        "equity_curve": curve,
        "spy_curve": spy_curve,
        "metrics": compute_metrics(curve, []),
    }


def _latest_close(loader: Callable, symbol: str):
    """Most recent non-NaN close for ``symbol`` from the loader, or None."""
    df = loader(symbol)
    if df is None or getattr(df, "empty", True):
        return None
    s = df.set_index("date")["close"].dropna()
    return float(s.iloc[-1]) if len(s) else None
