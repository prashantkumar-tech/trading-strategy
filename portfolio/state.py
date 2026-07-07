"""Persistent state model for a live paper portfolio.

Plain dataclasses with explicit ``to_dict`` / ``from_dict`` so the whole
portfolio serializes to human-inspectable JSON (see ``portfolio.store``). Kept
free of any data-layer or Streamlit imports so it stays trivially testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


@dataclass
class Position:
    """A currently-held lot, aggregated per symbol."""
    symbol: str
    shares: float
    cost_basis: float          # total $ invested still tied up in this lot
    entry_price: float         # price of the first buy (for return-since-entry)
    entry_date: str            # YYYY-MM-DD

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        return cls(**d)


@dataclass
class Transaction:
    """A single executed fill in the paper portfolio."""
    date: str                  # YYYY-MM-DD
    symbol: str
    action: str                # "BUY" | "SELL"
    shares: float
    price: float
    reason: str

    @property
    def amount(self) -> float:
        return round(self.shares * self.price, 2)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Transaction":
        return cls(**d)


@dataclass
class Portfolio:
    """The full persistent state of one paper portfolio."""
    name: str
    config: dict
    cash: float
    inception_date: str
    positions: Dict[str, Position] = field(default_factory=dict)
    transactions: List[Transaction] = field(default_factory=list)
    last_rebalance_date: Optional[str] = None

    # ── serialization ────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "config": self.config,
            "cash": self.cash,
            "inception_date": self.inception_date,
            "positions": {s: p.to_dict() for s, p in self.positions.items()},
            "transactions": [t.to_dict() for t in self.transactions],
            "last_rebalance_date": self.last_rebalance_date,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Portfolio":
        return cls(
            name=d["name"],
            config=d.get("config", {}),
            cash=d["cash"],
            inception_date=d["inception_date"],
            positions={s: Position.from_dict(p) for s, p in d.get("positions", {}).items()},
            transactions=[Transaction.from_dict(t) for t in d.get("transactions", [])],
            last_rebalance_date=d.get("last_rebalance_date"),
        )

    # ── convenience ──────────────────────────────────────────────────────
    def holdings(self) -> List[str]:
        return list(self.positions.keys())


def new_portfolio(name: str, config: dict, initial_capital: float,
                  inception_date: str) -> Portfolio:
    """Create a fresh, flat portfolio: all cash, no positions, no history.

    ``initial_capital`` is also recorded in ``config`` so the equity curve can be
    reconstructed from inception (see ``engine.value_portfolio``).
    """
    cfg = dict(config)
    cfg.setdefault("initial_capital", float(initial_capital))
    return Portfolio(
        name=name,
        config=cfg,
        cash=float(initial_capital),
        inception_date=inception_date,
    )
