"""
Compare TQQQ 5-minute bars: Alpaca vs Polygon (Oct 2 – Dec 19, 2025).

Add your Alpaca keys to .env:
    ALPACA_API_KEY=<your key>
    ALPACA_SECRET_KEY=<your secret>

Alpaca free (paper) accounts include historical market data access.
Get keys at: https://app.alpaca.markets → API Keys

Usage:
    python examples/compare_alpaca_vs_polygon_5m.py
"""

import os
import sys
from pathlib import Path
from datetime import date

import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from data.sources import polygon as poly_src

SYMBOL = "TQQQ"
START  = "2025-10-02"
END    = "2025-12-19"
ALPACA_END = "2025-12-20"  # Alpaca end is exclusive, so fetch through Dec 20 to include Dec 19


# ── Alpaca fetch ──────────────────────────────────────────────────────────────
def fetch_alpaca_5m(symbol: str, start: str, end: str, alpaca_end: str) -> pd.DataFrame:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    api_key    = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        raise EnvironmentError(
            "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in .env"
        )

    client = StockHistoricalDataClient(api_key, secret_key)

    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),
        start=start,
        end=alpaca_end,
        adjustment="raw",   # unadjusted — same as Polygon default
    )

    print(f"  Requesting {symbol} 5m bars from Alpaca ({start} → {alpaca_end}) ...")
    bars = client.get_stock_bars(request)
    df = bars.df

    if df.empty:
        raise ValueError(f"Alpaca returned no data for {symbol}")

    # bars.df has a MultiIndex (symbol, timestamp) — drop symbol level
    df = df.reset_index()
    df = df.rename(columns={"timestamp": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.tz_convert("America/New_York").dt.tz_localize(None)
    df = df[df["date"].dt.date <= pd.Timestamp(end).date()]
    return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


# ── Polygon fetch ─────────────────────────────────────────────────────────────
def fetch_polygon_5m(symbol: str, start: str, end: str) -> pd.DataFrame:
    df = poly_src.fetch(symbol, bar_size="5m", start=start, end=end)
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"TQQQ 5-min: Alpaca vs Polygon  ({START} → {END})")
print(f"{'='*70}\n")

print("── Alpaca ───────────────────────────────────────────────────────────")
alp = fetch_alpaca_5m(SYMBOL, START, END, ALPACA_END)
print(f"  {len(alp):,} bars  ({alp['date'].min()} → {alp['date'].max()})")

print("\n── Polygon ──────────────────────────────────────────────────────────")
poly = fetch_polygon_5m(SYMBOL, START, END)
print(f"  {len(poly):,} bars  ({poly['date'].min()} → {poly['date'].max()})")

# ── Merge ─────────────────────────────────────────────────────────────────────
alp  = alp.rename(columns={c: f"alp_{c}"  for c in ["open","high","low","close","volume"]})
poly = poly.rename(columns={c: f"poly_{c}" for c in ["open","high","low","close","volume"]})

df = alp.merge(poly, on="date", how="outer").sort_values("date").reset_index(drop=True)

# ── Diffs ─────────────────────────────────────────────────────────────────────
df["d_open"]  = (df["alp_open"]  - df["poly_open"]).round(4)
df["d_high"]  = (df["alp_high"]  - df["poly_high"]).round(4)
df["d_low"]   = (df["alp_low"]   - df["poly_low"]).round(4)
df["d_close"] = (df["alp_close"] - df["poly_close"]).round(4)

matched   = df[df["alp_open"].notna() & df["poly_open"].notna()].copy()
only_alp  = df[df["poly_open"].isna()]
only_poly = df[df["alp_open"].isna()]

# ── Report ────────────────────────────────────────────────────────────────────
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)
pd.set_option("display.float_format", "{:.4f}".format)

print(f"\n── Bar count ────────────────────────────────────────────────────────")
print(f"  Matched (both)   : {len(matched):,}")
print(f"  Only in Alpaca   : {len(only_alp):,}")
print(f"  Only in Polygon  : {len(only_poly):,}")

print(f"\n── Diff stats on matched bars ───────────────────────────────────────")
print(matched[["d_open","d_high","d_low","d_close"]].describe().round(4))

# Percentage of bars with close diff > $0.01
n_diverge = (matched["d_close"].abs() > 0.01).sum()
pct = n_diverge / len(matched) * 100
print(f"\n  Bars with |close diff| > $0.01 : {n_diverge:,}  ({pct:.1f}%)")

print(f"\n── Bars with |close diff| > $0.01 (first 30) ───────────────────────")
outliers = matched[matched["d_close"].abs() > 0.01][
    ["date","alp_open","poly_open","d_open","alp_close","poly_close","d_close"]
].head(30)
if outliers.empty:
    print("  None — prices match within $0.01 on all bars.")
else:
    print(outliers.to_string(index=False))

print(f"\n── Last 10 matched bars ─────────────────────────────────────────────")
cols = ["date","alp_open","poly_open","d_open","alp_close","poly_close","d_close"]
print(matched[cols].tail(10).to_string(index=False))

if not only_alp.empty:
    print(f"\n── Bars only in Alpaca (first 10) ───────────────────────────────────")
    print(only_alp[["date","alp_open","alp_close"]].head(10).to_string(index=False))

if not only_poly.empty:
    print(f"\n── Bars only in Polygon (first 10) ──────────────────────────────────")
    print(only_poly[["date","poly_open","poly_close"]].head(10).to_string(index=False))

# ── Save ──────────────────────────────────────────────────────────────────────
out_cols = ["date",
            "alp_open","alp_high","alp_low","alp_close","alp_volume",
            "poly_open","poly_high","poly_low","poly_close","poly_volume",
            "d_open","d_high","d_low","d_close"]
out_path = Path(__file__).parent / "compare_alpaca_vs_polygon_TQQQ_5m.csv"
df[out_cols].to_csv(out_path, index=False)
print(f"\nSaved to: {out_path}")
