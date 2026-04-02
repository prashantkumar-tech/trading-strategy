"""
TQQQ open/close comparison (Yahoo Finance vs Polygon) with QQQ MA50.

Fetches:
  - TQQQ daily open/close from Yahoo Finance (adjusted) and Polygon (unadjusted)
  - QQQ daily close from Yahoo Finance to compute 50-day MA
    (with extra lookback so MA50 is fully warmed up on the first date)

Output: CSV at examples/compare_TQQQ_<start>_<end>.csv
"""

import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.sources import yfinance as yf_src
from data.sources import polygon as poly_src

# ── Date range ───────────────────────────────────────────────────────────────
DAYS  = 365
END   = date.today()
START = END - timedelta(days=DAYS)

# MA50 needs 50 trading days of prior data; ~70 calendar days is a safe buffer
MA50_LOOKBACK_DAYS = 80
QQQ_START = START - timedelta(days=MA50_LOOKBACK_DAYS)

START_STR     = str(START)
END_STR       = str(END)
QQQ_START_STR = str(QQQ_START)

print(f"\nWindow : {START_STR} → {END_STR}")
print(f"QQQ MA50 lookback starts: {QQQ_START_STR}\n")

# ── Fetch TQQQ ────────────────────────────────────────────────────────────────
print("Fetching TQQQ from Yahoo Finance ...")
tqqq_yf = yf_src.fetch("TQQQ", bar_size="1d", start=START_STR, end=END_STR)
tqqq_yf["date"] = pd.to_datetime(tqqq_yf["date"])
tqqq_yf = tqqq_yf[["date", "open", "close"]].rename(
    columns={"open": "tqqq_yf_open", "close": "tqqq_yf_close"}
)
print(f"  {len(tqqq_yf)} bars")

print("Fetching TQQQ from Polygon ...")
tqqq_poly = poly_src.fetch("TQQQ", bar_size="1d", start=START_STR, end=END_STR)
tqqq_poly["date"] = pd.to_datetime(tqqq_poly["date"])
tqqq_poly = tqqq_poly[["date", "open", "close"]].rename(
    columns={"open": "tqqq_poly_open", "close": "tqqq_poly_close"}
)
print(f"  {len(tqqq_poly)} bars")

# ── Fetch QQQ for MA50 ────────────────────────────────────────────────────────
print("Fetching QQQ from Yahoo Finance (with MA50 lookback) ...")
qqq_raw = yf_src.fetch("QQQ", bar_size="1d", start=QQQ_START_STR, end=END_STR)
qqq_raw["date"] = pd.to_datetime(qqq_raw["date"])
qqq_raw = qqq_raw.sort_values("date").reset_index(drop=True)
qqq_raw["qqq_ma50"] = qqq_raw["close"].rolling(window=50, min_periods=50).mean()
print(f"  {len(qqq_raw)} bars fetched (including lookback)")

# Trim to the study window
qqq = qqq_raw[qqq_raw["date"] >= pd.Timestamp(START_STR)][
    ["date", "close", "qqq_ma50"]
].rename(columns={"close": "qqq_close"}).reset_index(drop=True)
print(f"  {len(qqq)} bars in study window (MA50 warm-up rows: {qqq['qqq_ma50'].isna().sum()})")

# ── Merge all on date ─────────────────────────────────────────────────────────
df = tqqq_yf.merge(tqqq_poly, on="date", how="outer")
df = df.merge(qqq, on="date", how="left")
df = df.sort_values("date").reset_index(drop=True)

# ── Diff columns ──────────────────────────────────────────────────────────────
df["open_yf_vs_poly"]  = (df["tqqq_yf_open"]  - df["tqqq_poly_open"]).round(4)
df["close_yf_vs_poly"] = (df["tqqq_yf_close"] - df["tqqq_poly_close"]).round(4)

# ── Round prices for readability ──────────────────────────────────────────────
for col in ["tqqq_yf_open", "tqqq_yf_close", "tqqq_poly_open", "tqqq_poly_close",
            "qqq_close", "qqq_ma50"]:
    df[col] = df[col].round(4)

# ── Display ───────────────────────────────────────────────────────────────────
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)
pd.set_option("display.float_format", "{:.4f}".format)

print()
print("=" * 120)
print(f"TQQQ Open/Close — Yahoo Finance vs Polygon  |  QQQ MA50   ({START_STR} → {END_STR})")
print("=" * 120)

cols = ["date",
        "tqqq_yf_open", "tqqq_poly_open", "open_yf_vs_poly",
        "tqqq_yf_close", "tqqq_poly_close", "close_yf_vs_poly",
        "qqq_close", "qqq_ma50"]

print(df[cols].tail(30).to_string(index=False))

print()
print("── Diff summary ──────────────────────────────────────────────────────────")
print(df[["open_yf_vs_poly", "close_yf_vs_poly"]].describe().round(4))

print()
print("── QQQ MA50 summary ──────────────────────────────────────────────────────")
print(df[["qqq_close", "qqq_ma50"]].describe().round(4))

print()
above = (df["qqq_close"] > df["qqq_ma50"]).sum()
below = (df["qqq_close"] <= df["qqq_ma50"]).sum()
print(f"QQQ above MA50: {above} days   |   below/at MA50: {below} days")

# ── Save CSV ──────────────────────────────────────────────────────────────────
out_path = Path(__file__).parent / f"compare_TQQQ_{START_STR}_{END_STR}.csv"
df[cols].to_csv(out_path, index=False)
print(f"\nSaved to: {out_path}")
