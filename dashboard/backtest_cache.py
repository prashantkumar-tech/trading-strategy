"""Persist the dashboard's last backtest / leaderboard across app restarts.

Streamlit's st.session_state only lives for the running server process, so a
restart drops the last result. These helpers pickle a named object to a
gitignored cache dir under db/ and load it back on demand.
"""

import pickle
from pathlib import Path

_DEFAULT_DIR = Path(__file__).resolve().parent.parent / "db" / "dashboard_cache"


def _path(name: str, cache_dir=None) -> Path:
    base = Path(cache_dir) if cache_dir is not None else _DEFAULT_DIR
    return base / f"{name}.pkl"


def save(name: str, obj, cache_dir=None) -> None:
    """Pickle ``obj`` under ``name``. Failures are swallowed (cache is best-effort)."""
    try:
        p = _path(name, cache_dir)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "wb") as f:
            pickle.dump(obj, f)
    except Exception:
        pass


def load(name: str, cache_dir=None):
    """Return the object saved under ``name``, or None if missing/unreadable."""
    p = _path(name, cache_dir)
    if not p.exists():
        return None
    try:
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None
