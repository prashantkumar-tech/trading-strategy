"""
setup_data.py — Bootstrap all market data on a fresh machine.

Fetches every symbol / bar-size combination used by this project.
Requires a .env file (or environment variables) with:
    POLYGON_API_KEY=<your key>

Usage
-----
    python3 setup_data.py                  # fetch everything
    python3 setup_data.py --dry-run        # show what would be fetched
    python3 setup_data.py --symbol TQQQ    # fetch one symbol only
"""

import argparse
import sys
import time
from datetime import date

# ── Dataset manifest ─────────────────────────────────────────────────────────
# Each entry: (symbol, bar_size, source, start_date)
# start_date = None means use the source's default (maximum history).

DATASETS = [
    # ── Daily bars (yfinance, full history) ──────────────────────────────────
    ("SPY",   "1d", "yfinance", None),
    ("QQQ",   "1d", "yfinance", None),
    ("TQQQ",  "1d", "yfinance", None),
    ("SPXL",  "1d", "yfinance", None),
    ("SQQQ",  "1d", "yfinance", None),
    ("SPXU",  "1d", "yfinance", None),
    ("SSO",   "1d", "yfinance", None),
    ("^VIX",  "1d", "yfinance", None),

    # ── 5-minute bars (Polygon, ~5 years) ────────────────────────────────────
    ("TQQQ",  "5m", "polygon",  "2021-04-01"),
    ("SPXL",  "5m", "polygon",  "2021-04-01"),
    ("SPY",   "5m", "polygon",  "2021-04-01"),
    ("QQQ",   "5m", "polygon",  "2021-04-01"),
    ("SQQQ",  "5m", "polygon",  "2021-04-01"),
    ("SPXU",  "5m", "polygon",  "2021-04-01"),
    ("SSO",   "5m", "polygon",  "2021-04-01"),

    # ── 5-minute bars (Twelve Data) ───────────────────────────────────────────
    ("TQQQ",  "5m", "twelve_data", "2021-04-01"),
    ("SPXL",  "5m", "twelve_data", "2021-04-01"),
    ("QQQ",   "5m", "twelve_data", "2021-04-01"),
    ("SPY",   "5m", "twelve_data", "2021-04-01"),
]


def check_env() -> bool:
    """Verify required API keys are present."""
    import os
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv optional; keys may already be in environment

    missing = []
    if not os.environ.get("POLYGON_API_KEY"):
        missing.append("POLYGON_API_KEY")

    if missing:
        print("ERROR: Missing environment variables:")
        for k in missing:
            print(f"  {k}")
        print("\nCreate a .env file in the project root:")
        print("  POLYGON_API_KEY=your_key_here")
        return False
    return True


def run(dry_run: bool = False, only_symbol: str = None) -> None:
    today = date.today().isoformat()

    datasets = DATASETS
    if only_symbol:
        datasets = [d for d in DATASETS if d[0].upper() == only_symbol.upper()]
        if not datasets:
            print(f"Symbol '{only_symbol}' not in manifest. Available:")
            syms = sorted({d[0] for d in DATASETS})
            for s in syms:
                print(f"  {s}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Data Setup  —  {len(datasets)} dataset(s)  —  today: {today}")
    print(f"{'='*60}\n")

    if dry_run:
        print("DRY RUN — nothing will be fetched.\n")
        print(f"  {'Symbol':<8}  {'Bar':<5}  {'Source':<12}  {'Start'}")
        print(f"  {'-'*8}  {'-'*5}  {'-'*12}  {'-'*10}")
        for sym, bar, src, start in datasets:
            print(f"  {sym:<8}  {bar:<5}  {src:<12}  {start or 'max history'}")
        return

    if not check_env():
        sys.exit(1)

    from data.fetcher import fetch_and_store
    from data.database import init_db
    init_db()

    success, failed = [], []

    for i, (sym, bar, src, start) in enumerate(datasets, 1):
        label = f"[{i}/{len(datasets)}] {sym} {bar} via {src}"
        print(f"{label} ...", flush=True)
        t0 = time.time()
        try:
            df = fetch_and_store(sym, bar_size=bar, source=src, start=start, end=today)
            elapsed = time.time() - t0
            print(f"  ✓  {len(df):>7,} bars  ({elapsed:.1f}s)\n")
            success.append((sym, bar, src))
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  ✗  FAILED ({elapsed:.1f}s): {e}\n")
            failed.append((sym, bar, src, str(e)))

        # Brief pause between Polygon requests to stay within rate limits
        if src == "polygon" and i < len(datasets):
            time.sleep(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Done.  {len(success)} succeeded  /  {len(failed)} failed")
    if failed:
        print("\n  Failed datasets:")
        for sym, bar, src, err in failed:
            print(f"    {sym} {bar} {src}: {err}")
    print(f"{'='*60}\n")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrap market data on a fresh machine")
    parser.add_argument("--dry-run",  action="store_true", help="Show what would be fetched")
    parser.add_argument("--symbol",   default=None,        help="Fetch one symbol only")
    args = parser.parse_args()

    run(dry_run=args.dry_run, only_symbol=args.symbol)
