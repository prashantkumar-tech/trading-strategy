"""
Build a 5-minute intraday sheet for TQQQ (Oct 2 – Dec 19 2025) with QQQ MA50.

Columns:
  date        — bar timestamp (ET, YYYY-MM-DD HH:MM:SS)
  tqqq_open, tqqq_high, tqqq_low, tqqq_close, tqqq_volume  — from Polygon (unadjusted)
  qqq_close   — QQQ daily close for that trading day (from Yahoo Finance)
  qqq_ma50    — QQQ 50-day MA as of that trading day's close

QQQ MA50 is computed on daily bars; a 50-day lookback requires data from roughly
early July 2025, so QQQ is fetched from 2025-06-01 to ensure full warm-up.

Output: examples/tqqq_intraday_5m_oct_dec_2025.csv
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.sources import polygon as poly_src
from data.sources import yfinance as yf_src

INTRADAY_START = "2025-10-02"
INTRADAY_END   = "2025-12-19"
QQQ_START      = "2025-06-01"   # ~88 calendar days before Oct 2 → enough for 50-bar warmup
QQQ_END        = "2025-12-20"  # yfinance end is exclusive, so fetch through Dec 20 to include Dec 19

# ── Fetch TQQQ 5-minute bars ──────────────────────────────────────────────────
print(f"Fetching TQQQ 5m bars from Polygon ({INTRADAY_START} → {INTRADAY_END}) ...")
tqqq = poly_src.fetch("TQQQ", bar_size="5m", start=INTRADAY_START, end=INTRADAY_END)
tqqq["date"] = pd.to_datetime(tqqq["date"])
tqqq = tqqq.sort_values("date").reset_index(drop=True)
tqqq = tqqq.rename(columns={
    "open":   "tqqq_open",
    "high":   "tqqq_high",
    "low":    "tqqq_low",
    "close":  "tqqq_close",
    "volume": "tqqq_volume",
})
print(f"  {len(tqqq):,} bars\n")

# ── Fetch QQQ daily + compute MA50 ───────────────────────────────────────────
print(f"Fetching QQQ daily from Yahoo Finance ({QQQ_START} → {QQQ_END}) ...")
qqq = yf_src.fetch("QQQ", bar_size="1d", start=QQQ_START, end=QQQ_END)
qqq["date"] = pd.to_datetime(qqq["date"])
qqq = qqq.sort_values("date").reset_index(drop=True)
qqq["qqq_ma50"] = qqq["close"].rolling(window=50, min_periods=50).mean()

# Keep only the study window
qqq = qqq[qqq["date"] >= pd.Timestamp(INTRADAY_START)][
    ["date", "close", "qqq_ma50"]
].rename(columns={"close": "qqq_close"}).reset_index(drop=True)
qqq["date_only"] = qqq["date"].dt.date
print(f"  {len(qqq)} trading days in window (NaN MA50 rows: {qqq['qqq_ma50'].isna().sum()})\n")

# ── Join QQQ MA50 onto each 5-min bar by trading date ────────────────────────
tqqq["date_only"] = tqqq["date"].dt.date
df = tqqq.merge(
    qqq[["date_only", "qqq_close", "qqq_ma50"]],
    on="date_only",
    how="left",
).drop(columns="date_only")

# ── Column order ──────────────────────────────────────────────────────────────
df = df[["date", "tqqq_open", "tqqq_high", "tqqq_low", "tqqq_close", "tqqq_volume",
         "qqq_close", "qqq_ma50"]]

# Round for readability
for col in ["tqqq_open", "tqqq_high", "tqqq_low", "tqqq_close",
            "qqq_close", "qqq_ma50"]:
    df[col] = df[col].round(4)

# ── Preview ───────────────────────────────────────────────────────────────────
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)
pd.set_option("display.float_format", "{:.4f}".format)

print("First 5 bars:")
print(df.head().to_string(index=False))
print("\nLast 5 bars:")
print(df.tail().to_string(index=False))

print(f"\nTotal rows : {len(df):,}")
print(f"Date range : {df['date'].min()}  →  {df['date'].max()}")
print(f"Trading days covered: {df['date'].dt.date.nunique()}")

print("\nQQQ MA50 on first/last trading day:")
first_day = df[df["date"].dt.date == df["date"].dt.date.min()][["qqq_close","qqq_ma50"]].iloc[0]
last_day  = df[df["date"].dt.date == df["date"].dt.date.max()][["qqq_close","qqq_ma50"]].iloc[0]
print(f"  {INTRADAY_START}: QQQ close={first_day.qqq_close:.2f}  MA50={first_day.qqq_ma50:.2f}")
print(f"  {INTRADAY_END}:   QQQ close={last_day.qqq_close:.2f}  MA50={last_day.qqq_ma50:.2f}")

# ── Save ──────────────────────────────────────────────────────────────────────
out_path = Path(__file__).parent / "tqqq_intraday_5m_oct_dec_2025.csv"
df.to_csv(out_path, index=False)
print(f"\nSaved to: {out_path}")
