"""
Compare 5-minute bars: Twelve Data vs Polygon for the last month.

Add your Twelve Data key to .env:
    TWELVE_DATA_API_KEY=<your key>

Usage:
    python examples/compare_twelve_data_vs_polygon_5m.py [SYMBOL]
"""

import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv()

from data.sources import polygon as poly_src
from data.sources import twelve_data as td_src

SYMBOL = sys.argv[1] if len(sys.argv) > 1 else "TQQQ"
START = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today() - timedelta(days=31)
END = date.fromisoformat(sys.argv[3]) if len(sys.argv) > 3 else date.today()
RTH_START = "09:30:00"
RTH_END = "15:55:00"


def fetch_twelve_data_5m(symbol: str, start: str, end: str) -> pd.DataFrame:
    df = td_src.fetch(symbol, bar_size="5m", start=start, end=end)
    df["date"] = pd.to_datetime(df["date"])
    df = _filter_rth(df)
    return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


def fetch_polygon_5m(symbol: str, start: str, end: str) -> pd.DataFrame:
    df = poly_src.fetch(symbol, bar_size="5m", start=start, end=end)
    df["date"] = pd.to_datetime(df["date"])
    df = _filter_rth(df)
    return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)


def _filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["date"].dt.strftime("%H:%M:%S").between(RTH_START, RTH_END)].copy()


print(f"\n{'=' * 78}")
print(f"{SYMBOL} 5-minute comparison: Twelve Data vs Polygon RTH only ({START} -> {END})")
print(f"{'=' * 78}\n")

print("── Twelve Data ─────────────────────────────────────────────────────")
td = fetch_twelve_data_5m(SYMBOL, str(START), str(END))
print(f"  {len(td):,} bars  ({td['date'].min()} -> {td['date'].max()})")

print("\n── Polygon ─────────────────────────────────────────────────────────")
poly = fetch_polygon_5m(SYMBOL, str(START), str(END))
print(f"  {len(poly):,} bars  ({poly['date'].min()} -> {poly['date'].max()})")

td = td.rename(columns={c: f"td_{c}" for c in ["open", "high", "low", "close", "volume"]})
poly = poly.rename(columns={c: f"poly_{c}" for c in ["open", "high", "low", "close", "volume"]})

df = td.merge(poly, on="date", how="outer").sort_values("date").reset_index(drop=True)

df["d_open"] = (df["td_open"] - df["poly_open"]).round(4)
df["d_high"] = (df["td_high"] - df["poly_high"]).round(4)
df["d_low"] = (df["td_low"] - df["poly_low"]).round(4)
df["d_close"] = (df["td_close"] - df["poly_close"]).round(4)

matched = df[df["td_open"].notna() & df["poly_open"].notna()].copy()
only_td = df[df["poly_open"].isna()]
only_poly = df[df["td_open"].isna()]

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)
pd.set_option("display.float_format", "{:.4f}".format)

print("\n── Bar count ───────────────────────────────────────────────────────")
print(f"  Matched (both)      : {len(matched):,}")
print(f"  Only in Twelve Data : {len(only_td):,}")
print(f"  Only in Polygon     : {len(only_poly):,}")

if not matched.empty:
    print("\n── Diff stats on matched bars ──────────────────────────────────────")
    print(matched[["d_open", "d_high", "d_low", "d_close"]].describe().round(4))

    n_diverge = (matched["d_close"].abs() > 0.01).sum()
    pct = n_diverge / len(matched) * 100
    print(f"\n  Bars with |close diff| > $0.01 : {n_diverge:,} ({pct:.1f}%)")

    outliers = matched[matched["d_close"].abs() > 0.01][
        ["date", "td_open", "poly_open", "d_open", "td_close", "poly_close", "d_close"]
    ].head(30)
    print("\n── Bars with |close diff| > $0.01 (first 30) ──────────────────────")
    if outliers.empty:
        print("  None")
    else:
        print(outliers.to_string(index=False))

if not only_td.empty:
    print("\n── Bars only in Twelve Data (first 10) ─────────────────────────────")
    print(only_td[["date", "td_open", "td_close"]].head(10).to_string(index=False))

if not only_poly.empty:
    print("\n── Bars only in Polygon (first 10) ─────────────────────────────────")
    print(only_poly[["date", "poly_open", "poly_close"]].head(10).to_string(index=False))

out_cols = [
    "date",
    "td_open", "td_high", "td_low", "td_close", "td_volume",
    "poly_open", "poly_high", "poly_low", "poly_close", "poly_volume",
    "d_open", "d_high", "d_low", "d_close",
]
out_path = Path(__file__).parent / f"compare_twelve_data_vs_polygon_{SYMBOL}_5m_{START}_{END}.csv"
df[out_cols].to_csv(out_path, index=False)
print(f"\nSaved to: {out_path}")
