"""
Compare TQQQ 5-minute bars: IBKR vs Polygon (Oct 2 – Dec 19, 2025).

Fetches historical bars from IB Gateway in monthly chunks to respect
IBKR pacing rules, then merges with Polygon data on bar timestamp and
reports differences in open, high, low, close.

Requires IB Gateway running on localhost (paper port 4002 or live port 4001).

Usage:
    python examples/compare_ibkr_vs_polygon_5m.py [--live]
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date

import pandas as pd
from ib_insync import IB, Stock, util

sys.path.insert(0, str(Path(__file__).parent.parent))
from data.sources import polygon as poly_src

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL         = "TQQQ"
START          = date(2025, 10, 2)
END            = date(2025, 12, 19)
PAPER_PORT     = 4002
LIVE_PORT      = 4001
CLIENT_ID      = 11        # use different clientId from live quotes (10)
PAUSE_BETWEEN  = 12        # seconds between IBKR chunk requests (pacing safety)

parser = argparse.ArgumentParser()
parser.add_argument("--live", action="store_true", help="Use live port 4001 instead of paper 4002")
args = parser.parse_args()
PORT = LIVE_PORT if args.live else PAPER_PORT


# ── IBKR historical fetch (chunked by month) ──────────────────────────────────
def fetch_ibkr_5m(symbol: str, start: date, end: date, port: int) -> pd.DataFrame:
    """
    Fetch 5-minute historical bars from IB Gateway for the given date range.
    Chunks by month to stay within IBKR pacing limits.
    Returns DataFrame with columns: date (ET timestamp), open, high, low, close, volume.
    """
    ib = IB()
    util.logToConsole(False)   # suppress ib_insync internal logs

    print(f"  Connecting to IB Gateway at 127.0.0.1:{port} (clientId={CLIENT_ID}) ...")
    ib.connect("127.0.0.1", port, clientId=CLIENT_ID, timeout=15, readonly=True)
    print("  Connected.\n")

    contract = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(contract)

    all_bars = []

    # Build monthly chunks: [start, end_of_oct], [start_of_nov, end_of_nov], ...
    chunks = _build_chunks(start, end)
    print(f"  Fetching {len(chunks)} chunk(s) ...")

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        n_days = (chunk_end - chunk_start).days + 1
        end_dt_str = chunk_end.strftime("%Y%m%d 23:59:59")
        duration   = f"{n_days} D"

        print(f"  Chunk {i+1}/{len(chunks)}: {chunk_start} → {chunk_end}  "
              f"(endDateTime={end_dt_str}, duration={duration})")

        bars = ib.reqHistoricalData(
            contract,
            endDateTime   = end_dt_str,
            durationStr   = duration,
            barSizeSetting= "5 mins",
            whatToShow    = "TRADES",
            useRTH        = False,      # all hours (match Polygon)
            formatDate    = 1,          # return datetime objects
            keepUpToDate  = False,
        )

        if not bars:
            print(f"    WARNING: No bars returned for this chunk.")
        else:
            chunk_df = util.df(bars)[["date", "open", "high", "low", "close", "volume"]]
            # Filter strictly to requested window (IBKR sometimes returns extra bars)
            chunk_df["date"] = pd.to_datetime(chunk_df["date"])
            chunk_df = chunk_df[
                (chunk_df["date"].dt.date >= chunk_start) &
                (chunk_df["date"].dt.date <= chunk_end)
            ]
            all_bars.append(chunk_df)
            print(f"    {len(chunk_df):,} bars")

        if i < len(chunks) - 1:
            print(f"  Pausing {PAUSE_BETWEEN}s for IBKR pacing ...")
            time.sleep(PAUSE_BETWEEN)

    ib.disconnect()
    print("  Disconnected from IB Gateway.\n")

    if not all_bars:
        return pd.DataFrame()

    df = pd.concat(all_bars, ignore_index=True)
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    return df


def _build_chunks(start: date, end: date):
    """Split a date range into calendar-month chunks."""
    chunks = []
    cursor = start
    while cursor <= end:
        # End of cursor's month
        if cursor.month == 12:
            month_end = date(cursor.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(cursor.year, cursor.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


# ── Polygon fetch ─────────────────────────────────────────────────────────────
def fetch_polygon_5m(symbol: str, start: date, end: date) -> pd.DataFrame:
    df = poly_src.fetch(symbol, bar_size="5m",
                        start=str(start), end=str(end))
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"TQQQ 5-min comparison: IBKR vs Polygon  ({START} → {END})")
print(f"{'='*70}\n")

print("── IBKR ─────────────────────────────────────────────────────────────")
ibkr_df = fetch_ibkr_5m(SYMBOL, START, END, PORT)

if ibkr_df.empty:
    print("ERROR: No data returned from IBKR. Is IB Gateway running?")
    sys.exit(1)

print(f"IBKR  : {len(ibkr_df):,} bars  "
      f"({ibkr_df['date'].min()} → {ibkr_df['date'].max()})")

print("\n── Polygon ──────────────────────────────────────────────────────────")
poly_df = fetch_polygon_5m(SYMBOL, START, END)
print(f"Polygon: {len(poly_df):,} bars  "
      f"({poly_df['date'].min()} → {poly_df['date'].max()})")

# ── Merge on timestamp ────────────────────────────────────────────────────────
ibkr_df  = ibkr_df.rename(columns={
    "open":   "ibkr_open",  "high":  "ibkr_high",
    "low":    "ibkr_low",   "close": "ibkr_close",
    "volume": "ibkr_volume",
})
poly_df = poly_df.rename(columns={
    "open":   "poly_open",  "high":  "poly_high",
    "low":    "poly_low",   "close": "poly_close",
    "volume": "poly_volume",
})

# Normalize timezones: IBKR returns tz-aware datetimes; Polygon returns tz-naive ET
ibkr_df["date"] = ibkr_df["date"].dt.tz_localize(None)

df = ibkr_df.merge(poly_df, on="date", how="outer").sort_values("date").reset_index(drop=True)

# ── Diff columns ──────────────────────────────────────────────────────────────
df["d_open"]   = (df["ibkr_open"]  - df["poly_open"]).round(4)
df["d_high"]   = (df["ibkr_high"]  - df["poly_high"]).round(4)
df["d_low"]    = (df["ibkr_low"]   - df["poly_low"]).round(4)
df["d_close"]  = (df["ibkr_close"] - df["poly_close"]).round(4)

# ── Bars only in one source ───────────────────────────────────────────────────
only_ibkr  = df[df["poly_open"].isna()]
only_poly  = df[df["ibkr_open"].isna()]
matched    = df[df["poly_open"].notna() & df["ibkr_open"].notna()]

# ── Display ───────────────────────────────────────────────────────────────────
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)
pd.set_option("display.float_format", "{:.4f}".format)

print(f"\n── Bar count summary ────────────────────────────────────────────────")
print(f"  Matched (both sources) : {len(matched):,}")
print(f"  Only in IBKR           : {len(only_ibkr):,}")
print(f"  Only in Polygon        : {len(only_poly):,}")

print(f"\n── Diff stats on matched bars ───────────────────────────────────────")
print(matched[["d_open","d_high","d_low","d_close"]].describe().round(4))

print(f"\n── Bars with |close diff| > $0.01 ───────────────────────────────────")
outliers = matched[matched["d_close"].abs() > 0.01][
    ["date","ibkr_open","poly_open","d_open",
           "ibkr_close","poly_close","d_close"]
].head(40)
if outliers.empty:
    print("  None — close prices match within $0.01 on all bars.")
else:
    print(f"  {len(matched[matched['d_close'].abs() > 0.01]):,} bars with divergence > $0.01")
    print(outliers.to_string(index=False))

print(f"\n── Sample: last 10 matched bars ─────────────────────────────────────")
cols = ["date","ibkr_open","poly_open","d_open","ibkr_close","poly_close","d_close"]
print(matched[cols].tail(10).to_string(index=False))

if not only_ibkr.empty:
    print(f"\n── Bars only in IBKR (sample) ───────────────────────────────────────")
    print(only_ibkr[["date","ibkr_open","ibkr_close"]].head(10).to_string(index=False))

if not only_poly.empty:
    print(f"\n── Bars only in Polygon (sample) ────────────────────────────────────")
    print(only_poly[["date","poly_open","poly_close"]].head(10).to_string(index=False))

# ── Save ──────────────────────────────────────────────────────────────────────
out_cols = ["date",
            "ibkr_open","ibkr_high","ibkr_low","ibkr_close","ibkr_volume",
            "poly_open","poly_high","poly_low","poly_close","poly_volume",
            "d_open","d_high","d_low","d_close"]
out_path = Path(__file__).parent / "compare_ibkr_vs_polygon_TQQQ_5m.csv"
df[out_cols].to_csv(out_path, index=False)
print(f"\nFull comparison saved to: {out_path}")
