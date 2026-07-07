"""JSON-backed persistence for paper portfolios.

Each portfolio is one file at ``db/portfolio/<name>.json`` — small and
human-inspectable. The store is keyed by portfolio name so multiple portfolios
(and, later, multiple users) drop in without a schema change.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from portfolio.state import Portfolio

_DEFAULT_DIR = Path(__file__).resolve().parent.parent / "db" / "portfolio"


def _dir(base_dir=None) -> Path:
    return Path(base_dir) if base_dir is not None else _DEFAULT_DIR


def _path(name: str, base_dir=None) -> Path:
    return _dir(base_dir) / f"{name}.json"


def save(portfolio: Portfolio, base_dir=None) -> None:
    """Write the portfolio to ``<base_dir>/<name>.json`` (pretty-printed)."""
    p = _path(portfolio.name, base_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(portfolio.to_dict(), f, indent=2)


def load(name: str, base_dir=None) -> Optional[Portfolio]:
    """Return the portfolio saved under ``name``, or None if missing/unreadable."""
    p = _path(name, base_dir)
    if not p.exists():
        return None
    try:
        with open(p) as f:
            return Portfolio.from_dict(json.load(f))
    except Exception:
        return None


def exists(name: str, base_dir=None) -> bool:
    return _path(name, base_dir).exists()


def list_portfolios(base_dir=None) -> List[str]:
    """Names of all saved portfolios (sorted)."""
    d = _dir(base_dir)
    if not d.exists():
        return []
    return sorted(f.stem for f in d.glob("*.json"))
