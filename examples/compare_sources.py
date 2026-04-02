"""
Compare daily open/close prices across data sources:
  - Yahoo Finance
  - Polygon.io
  - TradingView (tvdatafeed)

Usage:
    python examples/compare_sources.py [SYMBOL] [DAYS]

    SYMBOL  defaults to SPY
    DAYS    defaults to 365

Install tvdatafeed (required for TradingView section):
    pip install git+https://github.com/StreamAlpha/tvdatafeed.git
"""

import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Config ──────────────────────────────────────────────────────────────────
SYMBOL = sys.argv[1] if len(sys.argv) > 1 else "SPY"
DAYS   = int(sys.argv[2]) if len(sys.argv) > 2 else 365
START  = str(date.today() - timedelta(days=DAYS))
END    = str(date.today())


def fetch_yfinance(symbol, start, end) -> pd.DataFrame:
    from data.sources import yfinance as yf_src
    df = yf_src.fetch(symbol, bar_size="1d", start=start, end=end)
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "open", "close"]].rename(
        columns={"open": "yf_open", "close": "yf_close"}
    )


def fetch_polygon(symbol, start, end) -> pd.DataFrame:
    from data.sources import polygon as poly_src
    df = poly_src.fetch(symbol, bar_size="1d", start=start, end=end)
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "open", "close"]].rename(
        columns={"open": "poly_open", "close": "poly_close"}
    )


def fetch_tradingview(symbol, start, end) -> pd.DataFrame:
    try:
        from tvdatafeed import TvDatafeed, Interval
    except ImportError:
        print(
            "\n[TradingView] tvdatafeed not installed.\n"
            "  Install with:\n"
            "    pip install git+https://github.com/StreamAlpha/tvdatafeed.git\n"
        )
        return pd.DataFrame()

    tv = TvDatafeed()
    n_bars = (pd.Timestamp(end) - pd.Timestamp(start)).days + 10
    raw = tv.get_hist(symbol=symbol, exchange="AMEX", interval=Interval.in_daily, n_bars=n_bars)
    if raw is None or raw.empty:
        print(f"[TradingView] No data returned for {symbol}")
        return pd.DataFrame()

    raw = raw.reset_index()
    raw.columns = [c.lower() for c in raw.columns]
    raw["date"] = pd.to_datetime(raw["datetime"]).dt.normalize()
    raw = raw[(raw["date"] >= pd.Timestamp(start)) & (raw["date"] <= pd.Timestamp(end))]
    return raw[["date", "open", "close"]].rename(
        columns={"open": "tv_open", "close": "tv_close"}
    )


# ── Fetch ────────────────────────────────────────────────────────────────────
print(f"\nFetching {SYMBOL} daily bars: {START} → {END}\n")

print("Fetching Yahoo Finance ...")
df_yf = fetch_yfinance(SYMBOL, START, END)
print(f"  {len(df_yf)} bars\n")

print("Fetching Polygon ...")
df_poly = fetch_polygon(SYMBOL, START, END)
print(f"  {len(df_poly)} bars\n")

print("Fetching TradingView ...")
df_tv = fetch_tradingview(SYMBOL, START, END)
if not df_tv.empty:
    print(f"  {len(df_tv)} bars\n")

# ── Merge ────────────────────────────────────────────────────────────────────
df = df_yf.merge(df_poly, on="date", how="outer")
if not df_tv.empty:
    df = df.merge(df_tv, on="date", how="outer")
df = df.sort_values("date").reset_index(drop=True)

# ── Open price differences ────────────────────────────────────────────────────
df["open_yf_vs_poly"] = (df["yf_open"] - df["poly_open"]).round(4)
if not df_tv.empty:
    df["open_yf_vs_tv"]   = (df["yf_open"] - df["tv_open"]).round(4)
    df["open_poly_vs_tv"] = (df["poly_open"] - df["tv_open"]).round(4)

# ── Close price differences ───────────────────────────────────────────────────
df["close_yf_vs_poly"] = (df["yf_close"] - df["poly_close"]).round(4)
if not df_tv.empty:
    df["close_yf_vs_tv"]   = (df["yf_close"] - df["tv_close"]).round(4)
    df["close_poly_vs_tv"] = (df["poly_close"] - df["tv_close"]).round(4)

# ── Summary stats ─────────────────────────────────────────────────────────────
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)
pd.set_option("display.float_format", "{:.4f}".format)

print("=" * 80)
print(f"OPEN PRICE COMPARISON — {SYMBOL}  ({START} to {END})")
print("=" * 80)

open_cols = [c for c in ["yf_open", "poly_open", "tv_open"] if c in df.columns]
diff_open = [c for c in df.columns if c.startswith("open_")]
print(df[["date"] + open_cols + diff_open].tail(20).to_string(index=False))

print()
print("Diff summary (open):")
print(df[diff_open].describe().round(4))

print()
print("=" * 80)
print(f"CLOSE PRICE COMPARISON — {SYMBOL}  ({START} to {END})")
print("=" * 80)

close_cols = [c for c in ["yf_close", "poly_close", "tv_close"] if c in df.columns]
diff_close = [c for c in df.columns if c.startswith("close_")]
print(df[["date"] + close_cols + diff_close].tail(20).to_string(index=False))

print()
print("Diff summary (close):")
print(df[diff_close].describe().round(4))

# ── Days with non-trivial divergence ─────────────────────────────────────────
print()
print("=" * 80)
print("DAYS WHERE |open_yf_vs_poly| > $0.05  (adjusted vs unadjusted divergence)")
print("=" * 80)
outliers = df[df["open_yf_vs_poly"].abs() > 0.05][
    ["date", "yf_open", "poly_open", "open_yf_vs_poly",
               "yf_close", "poly_close", "close_yf_vs_poly"]
]
if outliers.empty:
    print("  None — prices are tightly aligned.")
else:
    print(outliers.to_string(index=False))

# ── Save CSV ──────────────────────────────────────────────────────────────────
out_path = Path(__file__).parent / f"compare_{SYMBOL}_{START}_{END}.csv"
df.to_csv(out_path, index=False)
print(f"\nFull comparison saved to: {out_path}")
