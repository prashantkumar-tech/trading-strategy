"""Bulk-download daily bars for a list of tickers into the prices table.

Each symbol is fetched independently so one bad ticker never aborts the run.
"""

from typing import Callable, List
from data.fetcher import fetch_and_store


def fetch_universe(
    tickers: List[str],
    fetch_fn: Callable = fetch_and_store,
    start: str = None,
    end: str = None,
    refresh_mode: str = "incremental",
) -> dict:
    """Fetch daily bars for every ticker. Returns {"ok": [...], "failed": [(sym, err), ...]}."""
    ok, failed = [], []
    for i, symbol in enumerate(tickers, 1):
        try:
            fetch_fn(
                symbol,
                bar_size="1d",
                source="yfinance",
                start=start,
                end=end,
                refresh_mode=refresh_mode,
            )
            ok.append(symbol)
        except Exception as exc:  # isolate per-symbol failures
            failed.append((symbol, str(exc)))
        print(f"[{i}/{len(tickers)}] {symbol}: {'ok' if symbol in ok else 'FAILED'}")
    return {"ok": ok, "failed": failed}


if __name__ == "__main__":
    from data.sp500_universe import get_sp500_tickers

    tickers = get_sp500_tickers()
    print(f"Fetching {len(tickers)} S&P 500 symbols (daily)...")
    summary = fetch_universe(tickers)
    print(f"Done. ok={len(summary['ok'])} failed={len(summary['failed'])}")
    if summary["failed"]:
        print("Failed:", summary["failed"])
