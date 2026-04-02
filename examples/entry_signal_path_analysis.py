"""
Post-entry path analysis for the Dip Sniper daily signal.

For every day the entry signal fires (5% dip from 5-day high, SPY > MA200),
track what happens to price over the next 20 trading days:
  - Max gain / max loss achieved in each forward window
  - Hit-rate: % of events that reach +5/+10/+15/+20% BEFORE hitting -12%
  - Median time to reach each target
  - Intraday entry-day behaviour (open→close on signal day)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from data.database import load_prices


# ── Parameters matching best daily config ────────────────────────────────────
DIP_ENTRY   = 0.05   # 5% from 5-day high
HIGH_LB     = 5      # 5-day rolling high
STOP_LOSS   = 0.12   # hard stop reference
TARGETS     = [0.05, 0.08, 0.10, 0.12, 0.15, 0.20]
WINDOWS     = [1, 2, 3, 5, 10, 20]   # trading days forward
TICKERS     = ["TQQQ", "SPXL"]


def load_aligned(ticker: str) -> pd.DataFrame:
    t   = load_prices(ticker, bar_size="1d", source="yfinance")[
              ["date", "open", "close"]].copy()
    spy = load_prices("SPY",  bar_size="1d", source="yfinance")[
              ["date", "close", "ma200"]].rename(
              columns={"close": "spy_close", "ma200": "spy_ma200"})
    for df in (t, spy):
        df["date"] = pd.to_datetime(df["date"])
    df = t.merge(spy, on="date", how="inner").sort_values("date").reset_index(drop=True)
    df["rolling_high"] = df["close"].rolling(HIGH_LB, min_periods=1).max().shift(1)
    df["dip_pct"]      = (df["rolling_high"] - df["close"]) / df["rolling_high"]
    df["spy_bull"]     = df["spy_close"] > df["spy_ma200"]
    return df


def find_signals(df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        df["dip_pct"] >= DIP_ENTRY) & (
        df["spy_bull"]) & (
        df["rolling_high"].notna()
    )
    return df[mask].copy()


def forward_path(df: pd.DataFrame, entry_idx: int, max_days: int = 20) -> pd.Series:
    """Return a Series of close prices from entry_idx+1 to entry_idx+max_days (inclusive)."""
    end = min(entry_idx + max_days + 1, len(df))
    future = df.iloc[entry_idx + 1 : end]["close"].values
    return future


def analyse_ticker(ticker: str) -> None:
    print(f"\n{'═'*65}")
    print(f"  PATH ANALYSIS — {ticker}")
    print(f"{'═'*65}")

    df      = load_aligned(ticker)
    signals = find_signals(df)

    print(f"  Data range   : {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"  Total signals: {len(signals)}")

    entry_prices = []
    max_gains    = {w: [] for w in WINDOWS}
    max_losses   = {w: [] for w in WINDOWS}
    hit_before   = {t: {"hit": 0, "stopped": 0, "neither": 0, "days": []} for t in TARGETS}
    intraday_ret = []   # open-to-close on entry day

    for _, row in signals.iterrows():
        idx = df.index.get_loc(row.name)
        entry_price = row["close"]

        # Intraday return on signal day (open → close)
        intraday_ret.append((row["close"] - row["open"]) / row["open"])

        # Forward path
        future = forward_path(df, idx, max_days=max(WINDOWS))

        if len(future) == 0:
            continue

        returns = (future - entry_price) / entry_price

        # Max gain / loss in each window
        for w in WINDOWS:
            slice_ = returns[:w]
            if len(slice_) > 0:
                max_gains[w].append(slice_.max())
                max_losses[w].append(slice_.min())

        # Hit-rate: does price reach target before stop?
        for tgt in TARGETS:
            hit_day  = None
            stop_day = None
            for d, r in enumerate(returns, 1):
                if r >= tgt and hit_day is None:
                    hit_day = d
                if r <= -STOP_LOSS and stop_day is None:
                    stop_day = d
            if hit_day is not None and (stop_day is None or hit_day <= stop_day):
                hit_before[tgt]["hit"] += 1
                hit_before[tgt]["days"].append(hit_day)
            elif stop_day is not None:
                hit_before[tgt]["stopped"] += 1
            else:
                hit_before[tgt]["neither"] += 1

    n = len(signals)

    # ── Forward window stats ─────────────────────────────────────────────────
    print(f"\n  ── Max Gain / Max Drawdown in forward N days (median across {n} signals) ──")
    print(f"  {'Days':>5}  {'Median MaxGain':>15}  {'Median MaxLoss':>15}  "
          f"{'90th MaxGain':>13}  {'10th MaxLoss':>13}")
    for w in WINDOWS:
        mg = np.array(max_gains[w])
        ml = np.array(max_losses[w])
        print(f"  {w:>5}  {np.median(mg)*100:>14.1f}%  {np.median(ml)*100:>14.1f}%  "
              f"{np.percentile(mg, 90)*100:>12.1f}%  {np.percentile(ml, 10)*100:>12.1f}%")

    # ── Hit-rate table ────────────────────────────────────────────────────────
    print(f"\n  ── Reach target BEFORE -{STOP_LOSS*100:.0f}% stop ──")
    print(f"  {'Target':>8}  {'Hit%':>7}  {'Stopped%':>10}  {'Neither%':>10}  "
          f"{'Median days to hit':>20}")
    for tgt in TARGETS:
        d   = hit_before[tgt]
        tot = d["hit"] + d["stopped"] + d["neither"]
        hit_pct  = d["hit"]     / tot * 100 if tot else 0
        stop_pct = d["stopped"] / tot * 100 if tot else 0
        nei_pct  = d["neither"] / tot * 100 if tot else 0
        med_days = np.median(d["days"]) if d["days"] else float("nan")
        print(f"  {tgt*100:>7.0f}%  {hit_pct:>7.1f}%  {stop_pct:>10.1f}%  "
              f"{nei_pct:>10.1f}%  {med_days:>20.1f}")

    # ── Intraday entry-day stats ──────────────────────────────────────────────
    ir = np.array(intraday_ret)
    print(f"\n  ── Entry-day intraday return (open → close) ──")
    print(f"  Mean   : {ir.mean()*100:+.2f}%")
    print(f"  Median : {np.median(ir)*100:+.2f}%")
    print(f"  Positive days : {(ir > 0).sum()} / {len(ir)} ({(ir>0).mean()*100:.1f}%)")
    print(f"  > +2% same day: {(ir > 0.02).sum()} ({(ir>0.02).mean()*100:.1f}%)")
    print(f"  < -2% same day: {(ir < -0.02).sum()} ({(ir<-0.02).mean()*100:.1f}%)")

    # ── Next-day open stats ───────────────────────────────────────────────────
    next_day_open_rets = []
    for _, row in signals.iterrows():
        idx = df.index.get_loc(row.name)
        if idx + 1 < len(df):
            nd = df.iloc[idx + 1]
            # Gap from prev close to next open
            next_day_open_rets.append((nd["open"] - row["close"]) / row["close"])

    ndo = np.array(next_day_open_rets)
    print(f"\n  ── Next-day gap (signal close → next open) ──")
    print(f"  Mean   : {ndo.mean()*100:+.2f}%")
    print(f"  Median : {np.median(ndo)*100:+.2f}%")
    print(f"  Gap up (>0)    : {(ndo > 0).sum()} / {len(ndo)} ({(ndo>0).mean()*100:.1f}%)")
    print(f"  Gap > +1%      : {(ndo > 0.01).sum()} ({(ndo>0.01).mean()*100:.1f}%)")
    print(f"  Gap < -1%      : {(ndo < -0.01).sum()} ({(ndo<-0.01).mean()*100:.1f}%)")

    # ── Distribution: where does price go first? ─────────────────────────────
    print(f"\n  ── Within 5 days: where does it go first after entry? ──")
    first_move = []
    for _, row in signals.iterrows():
        idx = df.index.get_loc(row.name)
        future = forward_path(df, idx, max_days=5)
        if len(future) == 0:
            continue
        returns = (future - row["close"]) / row["close"]
        first_move.append(returns[-1])   # close of day 5

    fm = np.array(first_move)
    buckets = [(-1, -0.10), (-0.10, -0.05), (-0.05, 0), (0, 0.05), (0.05, 0.10), (0.10, 1)]
    print(f"  {'Range':>20}  {'Count':>6}  {'%':>7}")
    for lo, hi in buckets:
        cnt = ((fm >= lo) & (fm < hi)).sum()
        print(f"  {lo*100:>+6.0f}% to {hi*100:>+6.0f}%  {cnt:>6}  {cnt/len(fm)*100:>6.1f}%")


if __name__ == "__main__":
    for ticker in TICKERS:
        analyse_ticker(ticker)
    print()
