"""Daily incremental price refresh for a portfolio's universe.

Thin orchestration over the existing incremental fetch path — `fetch_universe`
already loads context history, pulls only bars since each symbol's last stored
date, recomputes the moving averages, and upserts (see `data/fetcher.py`). This
module just points it at a universe and adds a freshness read for the UI, plus a
CLI entry point the scheduled `launchd` job runs each evening.

Run manually:  python3 -m data.update_prices           # S&P 500 universe
"""

from collections import Counter
from typing import Callable, List, Optional

from data.fetch_universe import fetch_universe
from data.database import get_dataset_details


def refresh_universe(tickers: List[str], runner: Callable = None) -> dict:
    """Incrementally refresh daily bars for every ticker.

    Returns the fetch summary {"ok": [...], "failed": [(sym, err), ...]}. Always
    uses the incremental refresh mode — never a full re-download.
    """
    runner = runner or fetch_universe
    return runner(tickers, refresh_mode="incremental")


def universe_as_of(symbols: List[str], details_fn: Callable = get_dataset_details,
                   source: str = "yfinance") -> Optional[str]:
    """The date the bulk of the universe is current to (the most common max_date).

    Uses the mode rather than the min so a single lagging/delisted symbol doesn't
    make the whole universe look stale. Returns None when nothing has data.
    """
    dates = []
    for s in symbols:
        d = details_fn(s, "1d", source)
        if d and d.get("max_date"):
            dates.append(d["max_date"])
    if not dates:
        return None
    counts = Counter(dates)
    top = max(counts.values())
    return max(d for d, c in counts.items() if c == top)


if __name__ == "__main__":
    from data.sp500_universe import get_sp500_tickers

    tickers = get_sp500_tickers()
    print(f"Incrementally refreshing {len(tickers)} S&P 500 symbols (daily)...")
    summary = refresh_universe(tickers)
    print(f"Done. ok={len(summary['ok'])} failed={len(summary['failed'])}")
    if summary["failed"]:
        print("Failed:", summary["failed"][:20])
