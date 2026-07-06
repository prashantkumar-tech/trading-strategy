"""Bulk-fetch daily bars for the broad-market universe (NYSE+NASDAQ, >= $1B).

Skips symbols already present in the DB (e.g. the S&P 500 names fetched
earlier), so re-running is cheap.
"""

from data.broad_universe import get_broad_universe, build_market_cap_cache, MARKETCAP_CACHE
from data.database import list_symbols
from data.fetch_universe import fetch_universe


def main(min_market_cap: float = 1e9) -> dict:
    if not MARKETCAP_CACHE.exists():
        print("Building market-cap cache (one bulk NASDAQ screener call)...")
        build_market_cap_cache()
    universe = get_broad_universe(min_market_cap=min_market_cap)
    have = set(list_symbols(bar_size="1d", source="yfinance"))
    todo = [s for s in universe if s not in have]
    print(f"Broad universe (>=${min_market_cap/1e9:.0f}B): {len(universe)} names; "
          f"{len(have)} already in DB; fetching {len(todo)} new...")
    summary = fetch_universe(todo)
    print(f"Done. ok={len(summary['ok'])} failed={len(summary['failed'])}")
    if summary["failed"]:
        print("Failed sample:", summary["failed"][:20])
    return summary


if __name__ == "__main__":
    main()
