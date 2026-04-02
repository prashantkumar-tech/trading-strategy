"""
5-minute Dip Sniper backtest for TQQQ / SPXL.

Architecture
------------
  Daily layer  : same 5%-dip / SPY>MA200 signal — fires at close of day D
  Intraday layer : on day D+1 (and up to max_active_sessions), look for a
                   5-min entry trigger (VWAP reclaim, RSI cross, or both)
                   then manage partial exits at target1 / target2.

Position sizing (50% max)
-------------------------
  Tranche 1  : 25% of capital on first intraday trigger
  Tranche 2  : 25% of capital if price drops tranche2_drop% below T1 entry
  Exit 1     : sell 50% of open position when +target1_pct from avg cost
  Exit 2     : sell remaining when +target2_pct from avg cost
  Hard stop  : exit everything when −stop_pct from avg cost
  Time stop  : exit everything at close of session max_active_sessions after signal

Usage
-----
    python3 -m backtest.dip_sniper_5min               # both tickers, full grid
    python3 -m backtest.dip_sniper_5min --ticker TQQQ  --detail
"""

import argparse
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.database import load_prices
from backtest.metrics import compute_metrics


# ── Constants ────────────────────────────────────────────────────────────────

MARKET_OPEN_H, MARKET_OPEN_M   = 9,  30
MARKET_CLOSE_H, MARKET_CLOSE_M = 16,  0


# ── Indicator helpers ─────────────────────────────────────────────────────────

def compute_vwap(df: pd.DataFrame) -> pd.Series:
    """Intraday VWAP, reset each calendar day. Expects df sorted by date."""
    typical  = (df["high"] + df["low"] + df["close"]) / 3
    tp_vol   = typical * df["volume"]
    day_key  = pd.to_datetime(df["date"]).dt.date

    vwap = pd.Series(np.nan, index=df.index)
    for _, grp in df.groupby(day_key, sort=False):
        idx = grp.index
        vwap[idx] = tp_vol[idx].cumsum() / df["volume"][idx].cumsum()
    return vwap


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ── Data loading ──────────────────────────────────────────────────────────────

def load_5min(ticker: str) -> pd.DataFrame:
    """Load 5-min bars, add VWAP + RSI, mark regular-hours bars."""
    df = load_prices(ticker, bar_size="5m", source="polygon").copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    t = df["date"].dt.time
    import datetime
    reg_open  = datetime.time(MARKET_OPEN_H,  MARKET_OPEN_M)
    reg_close = datetime.time(MARKET_CLOSE_H, MARKET_CLOSE_M)
    df["regular"] = (t >= reg_open) & (t <= reg_close)
    df["day"]     = df["date"].dt.date

    df["vwap"] = compute_vwap(df)
    df["rsi"]  = compute_rsi(df["close"], period=14)
    return df


def load_daily_signals(ticker: str,
                       dip_entry: float = 0.05,
                       high_lb: int = 5) -> set:
    """
    Return the set of dates (datetime.date) on which the daily dip signal fires:
      close < rolling_high * (1 - dip_entry)  AND  SPY close > SPY MA200
    Signal date = day D; we look for intraday entries on D+1 onwards.
    """
    t   = load_prices(ticker, bar_size="1d", source="yfinance")[["date", "close"]].copy()
    spy = load_prices("SPY",  bar_size="1d", source="yfinance")[
              ["date", "close", "ma200"]].rename(
              columns={"close": "spy_close", "ma200": "spy_ma200"})

    t["date"]   = pd.to_datetime(t["date"])
    spy["date"] = pd.to_datetime(spy["date"])

    df = t.merge(spy, on="date", how="inner").sort_values("date").reset_index(drop=True)
    df["rolling_high"] = df["close"].rolling(high_lb, min_periods=1).max().shift(1)
    df["dip_pct"]      = (df["rolling_high"] - df["close"]) / df["rolling_high"]
    df["spy_bull"]     = df["spy_close"] > df["spy_ma200"]

    mask = df["dip_pct"] >= dip_entry
    mask &= df["spy_bull"]
    mask &= df["rolling_high"].notna()

    return set(pd.to_datetime(df.loc[mask, "date"]).dt.date)


# ── Episode builder ───────────────────────────────────────────────────────────

def build_episodes(df5: pd.DataFrame,
                   signal_dates: set,
                   max_sessions: int = 3) -> list:
    """
    For every signal date D, collect up to max_sessions of 5-min regular-hours
    bars starting from D+1. Returns a list of DataFrames (one per signal).
    """
    all_days   = sorted(df5["day"].unique())
    day_index  = {d: i for i, d in enumerate(all_days)}

    episodes = []
    for sig_date in sorted(signal_dates):
        if sig_date not in day_index:
            # find next available trading day
            later = [d for d in all_days if d > sig_date]
            if not later:
                continue
            start_day = later[0]
        else:
            idx = day_index[sig_date]
            if idx + 1 >= len(all_days):
                continue
            start_day = all_days[idx + 1]

        start_idx = day_index.get(start_day)
        if start_idx is None:
            continue
        end_idx   = min(start_idx + max_sessions, len(all_days))
        ep_days   = set(all_days[start_idx:end_idx])

        ep = df5[df5["day"].isin(ep_days) & df5["regular"]].copy()
        if ep.empty:
            continue

        ep["session_num"] = ep["day"].map(
            {d: i for i, d in enumerate(sorted(ep_days))}
        )
        ep = ep.reset_index(drop=True)
        episodes.append((sig_date, ep))

    return episodes


# ── Episode simulator ─────────────────────────────────────────────────────────

def simulate_episode(sig_date, ep: pd.DataFrame,
                     params: dict, capital: float):
    """
    Simulate one signal event on its 5-min episode bars.
    Returns a trade dict or None if no entry was taken.

    State is tracked as (open_shares, avg_cost_per_share, realized_pnl)
    so partial exits are exact.
    """
    entry_type     = params["entry_type"]
    rsi_thresh     = params["rsi_thresh"]
    tranche2_drop  = params["tranche2_drop"]
    target1        = params["target1"]
    target2        = params["target2"]
    stop_pct       = params["stop_pct"]
    entry_hour_cut = params["entry_hour_cut"]
    max_sessions   = params["max_sessions"]

    tranche_usd = capital * 0.25   # each tranche = 25% of capital

    # Clean position state
    open_shares      = 0.0
    avg_cost         = 0.0    # weighted average cost per share
    total_invested   = 0.0    # total cash deployed (for return_pct denom)
    realized_pnl     = 0.0
    t1_exit_done     = False
    tranche2_added   = False
    entry_price1     = None
    entry_price2     = None
    entry_bar        = None
    exit_bar         = None
    exit_reason      = None

    prev_below_vwap  = False
    prev_rsi         = np.nan

    for row in ep.itertuples(index=False):
        price    = row.close
        vwap     = row.vwap
        rsi      = row.rsi
        sess     = row.session_num
        bar_hour = row.date.hour

        if sess >= max_sessions:
            break

        # ── Manage open position ──────────────────────────────────────────
        if open_shares > 0:
            pnl_pct = (price - avg_cost) / avg_cost

            # Hard stop — exit everything
            if pnl_pct <= -stop_pct:
                realized_pnl += (price - avg_cost) * open_shares
                open_shares   = 0.0
                exit_bar      = row.date
                exit_reason   = "stop_loss"
                break

            # Partial exit at target1 — sell half
            if not t1_exit_done and pnl_pct >= target1:
                sell_shares   = open_shares * 0.5
                realized_pnl += (price - avg_cost) * sell_shares
                open_shares  -= sell_shares
                # avg_cost unchanged — remaining shares keep their cost basis
                t1_exit_done  = True

            # Full exit at target2 (after partial exit taken)
            if t1_exit_done and open_shares > 0:
                if (price - avg_cost) / avg_cost >= target2:
                    realized_pnl += (price - avg_cost) * open_shares
                    open_shares   = 0.0
                    exit_bar      = row.date
                    exit_reason   = "target2"
                    break

            # Add tranche 2 if price drops enough from first entry
            if not tranche2_added and entry_price1 is not None:
                drop_from_t1 = (entry_price1 - price) / entry_price1
                if drop_from_t1 >= tranche2_drop:
                    new_shares    = tranche_usd / price
                    # Update weighted average cost
                    total_cost    = avg_cost * open_shares + tranche_usd
                    open_shares  += new_shares
                    avg_cost      = total_cost / open_shares
                    total_invested += tranche_usd
                    tranche2_added = True
                    entry_price2   = price

        # ── Look for entry (no position open yet) ────────────────────────
        if open_shares == 0 and entry_bar is None and bar_hour < entry_hour_cut:
            vwap_signal = False
            rsi_signal  = False

            if not np.isnan(vwap):
                vwap_signal = (price > vwap) and prev_below_vwap

            if not np.isnan(rsi) and not np.isnan(prev_rsi):
                rsi_signal = (rsi >= rsi_thresh) and (prev_rsi < rsi_thresh)

            trigger = False
            if entry_type == "vwap_reclaim":
                trigger = vwap_signal
            elif entry_type == "rsi_cross":
                trigger = rsi_signal
            elif entry_type == "either":
                trigger = vwap_signal or rsi_signal
            elif entry_type == "both":
                trigger = vwap_signal and rsi_signal

            if trigger:
                open_shares     = tranche_usd / price
                avg_cost        = price
                total_invested  = tranche_usd
                entry_price1    = price
                entry_bar       = row.date

        # Track prev-bar state
        if not np.isnan(vwap):
            prev_below_vwap = price < vwap
        if not np.isnan(rsi):
            prev_rsi = rsi

    # ── Time-stop: close remaining open position at last bar ─────────────
    if open_shares > 0 and exit_reason is None:
        last_price    = ep["close"].iloc[-1]
        realized_pnl += (last_price - avg_cost) * open_shares
        open_shares   = 0.0
        exit_bar      = ep["date"].iloc[-1]
        exit_reason   = "time_stop"

    if entry_bar is None:
        return None

    # Final P&L and metrics
    pnl        = realized_pnl
    return_pct = pnl / total_invested * 100 if total_invested > 0 else 0.0

    exit_price_val = ep.loc[ep["date"] == exit_bar, "close"]
    exit_price_val = exit_price_val.iloc[0] if not exit_price_val.empty else ep["close"].iloc[-1]

    # Hold duration in 5-min bars
    entry_idx = ep.index[ep["date"] == entry_bar]
    exit_idx  = ep.index[ep["date"] == exit_bar]
    hold_bars = int(exit_idx[0] - entry_idx[0]) if len(entry_idx) and len(exit_idx) else 0

    return {
        "signal_date"  : sig_date,
        "entry_date"   : entry_bar,
        "exit_date"    : exit_bar,
        "entry_price"  : round(entry_price1, 4),
        "exit_price"   : round(float(exit_price_val), 4),
        "n_tranches"   : 2 if tranche2_added else 1,
        "invested_usd" : round(total_invested, 2),
        "pnl"          : round(pnl, 4),
        "return_pct"   : round(return_pct, 3),
        "exit_reason"  : exit_reason,
        "hold_bars"    : hold_bars,
    }


# ── Grid search ───────────────────────────────────────────────────────────────

PARAM_GRID = {
    "entry_type"     : ["vwap_reclaim", "rsi_cross", "either"],
    "rsi_thresh"     : [30, 35, 40],
    "tranche2_drop"  : [0.02, 0.04, 0.06],
    "target1"        : [0.03, 0.05, 0.08],
    "target2"        : [0.08, 0.12, 0.15],
    "stop_pct"       : [0.10, 0.12, 0.15],
    "entry_hour_cut" : [12, 14, 16],
    "max_sessions"   : [1, 2, 3],
}


def grid_combinations(grid: dict):
    keys   = list(grid.keys())
    values = list(grid.values())
    for combo in itertools.product(*values):
        yield dict(zip(keys, combo))


def run_grid(ticker: str, capital: float = 10_000.0, top_n: int = 20) -> pd.DataFrame:
    import warnings
    warnings.filterwarnings("ignore")

    print(f"\n{'='*65}")
    print(f"  Dip Sniper 5-min Grid  |  {ticker}")
    print(f"{'='*65}")

    df5    = load_5min(ticker)
    sigs   = load_daily_signals(ticker)

    print(f"  5-min bars : {df5['date'].min().date()} → {df5['date'].max().date()}  "
          f"({len(df5):,} bars)")
    print(f"  Daily signals in range: "
          f"{sum(1 for s in sigs if s >= df5['date'].min().date())} signals")

    # Only use signals within the 5-min data window
    data_start = df5["date"].min().date()
    sigs_in_range = {s for s in sigs if s >= data_start}
    print(f"  Signals in 5-min range: {len(sigs_in_range)}")

    # Build episodes once per max_sessions value (avoid per-combo slicing)
    episodes_by_ms = {
        ms: build_episodes(df5, sigs_in_range, max_sessions=ms)
        for ms in PARAM_GRID["max_sessions"]
    }
    n_ep = len(episodes_by_ms[max(PARAM_GRID["max_sessions"])])
    print(f"  Episodes built : {n_ep} (max sessions = {max(PARAM_GRID['max_sessions'])})")

    combos = list(grid_combinations(PARAM_GRID))
    print(f"  Grid combos  : {len(combos):,}\n")

    results = []
    for idx, params in enumerate(combos, 1):
        if idx % 100 == 0:
            print(f"  ... {idx}/{len(combos)}", flush=True)

        ms     = params["max_sessions"]
        trades = []
        equity = capital
        equity_curve = {}

        for sig_date, ep in episodes_by_ms[ms]:
            t = simulate_episode(sig_date, ep, params, capital)
            if t is None:
                continue

            equity += t["pnl"]
            equity_curve[t["exit_date"]] = equity
            trades.append(t)

        if not equity_curve:
            continue

        # Build a proper business-day equity curve so n_years is correct.
        # equity_curve has intraday exit timestamps → normalize to date,
        # build a sparse series, reindex to all business days, ffill.
        eq_sparse_dict = {pd.Timestamp(data_start): capital}
        for ts, val in equity_curve.items():
            eq_sparse_dict[pd.Timestamp(ts).normalize()] = val
        eq_sparse = pd.Series(eq_sparse_dict, dtype=float).sort_index()
        bday_range = pd.bdate_range(
            start=pd.Timestamp(data_start),
            end=pd.Timestamp(df5["date"].max().date()),
        )
        eq_full = eq_sparse.reindex(bday_range.union(eq_sparse.index)).ffill()
        eq_full = eq_full.reindex(bday_range).ffill()

        try:
            m = compute_metrics(eq_full, trades)
        except Exception:
            continue

        results.append({
            **{f"p_{k}": v for k, v in params.items()},
            **m,
            "n_entries"  : sum(1 for t in trades if t["pnl"] > -9999),
            "pct_entered": round(sum(1 for t in trades) / max(n_ep, 1) * 100, 1),
        })

    res_df = pd.DataFrame(results)

    filtered = res_df[
        (res_df["max_drawdown_pct"] >= -20.0) &   # loosened; DD budget discussed separately
        (res_df["num_trades"]       >=  20)   &
        (res_df["win_rate_pct"]     >=  55.0) &
        (res_df["annualized_return_pct"] > 2.0)
    ]

    print(f"\n  Total combos : {len(res_df):,}")
    print(f"  Pass filters : {len(filtered):,}")

    top = (
        filtered
        .sort_values(["sharpe_ratio", "annualized_return_pct"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    out_path = Path(__file__).parent.parent / "examples" / f"dip_sniper_5min_{ticker}.csv"
    res_df.to_csv(out_path, index=False)
    print(f"  Full results → {out_path}")
    return top


def print_top(top: pd.DataFrame, ticker: str) -> None:
    print(f"\n{'='*65}")
    print(f"  TOP RESULTS — {ticker}  (5-min)")
    print(f"{'='*65}")
    cols = [
        "p_entry_type", "p_rsi_thresh", "p_tranche2_drop",
        "p_target1", "p_target2", "p_stop_pct",
        "p_entry_hour_cut", "p_max_sessions",
        "annualized_return_pct", "sharpe_ratio", "max_drawdown_pct",
        "num_trades", "win_rate_pct", "pct_entered",
        "avg_win_usd", "avg_loss_usd",
    ]
    avail = [c for c in cols if c in top.columns]
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 220)
    pd.set_option("display.float_format", "{:.3f}".format)
    print(top[avail].to_string(index=True))


def print_trade_log(result_trades: list, n: int = 15) -> None:
    if not result_trades:
        print("  No trades.")
        return
    df = pd.DataFrame(result_trades)
    print(f"\n  Trades: {len(df)}  |  Win rate: {(df['pnl']>0).mean()*100:.1f}%")
    print(f"  Exit reasons: {df['exit_reason'].value_counts().to_dict()}")
    print(f"  Avg hold (bars): {df['hold_bars'].mean():.0f}  "
          f"(≈{df['hold_bars'].mean()/78:.1f} sessions)")
    print(f"\n  First {n} trades:")
    print(df.head(n).to_string(index=False))


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker",  default=None)
    parser.add_argument("--capital", type=float, default=10_000.0)
    parser.add_argument("--top",     type=int,   default=20)
    parser.add_argument("--detail",  action="store_true")
    args = parser.parse_args()

    tickers = [args.ticker] if args.ticker else ["TQQQ", "SPXL"]

    for ticker in tickers:
        top = run_grid(ticker, capital=args.capital, top_n=args.top)
        print_top(top, ticker)

        if args.detail and not top.empty:
            best = {}
            for k, v in top.iloc[0].items():
                if not k.startswith("p_"):
                    continue
                key = k[2:]
                if isinstance(v, float) and v != v:
                    v = None
                best[key] = v
            best["max_sessions"] = int(best.get("max_sessions", 3))
            best["rsi_thresh"]   = int(best.get("rsi_thresh", 35))
            best["entry_hour_cut"] = int(best.get("entry_hour_cut", 16))

            df5  = load_5min(ticker)
            sigs = load_daily_signals(ticker)
            sigs = {s for s in sigs if s >= df5["date"].min().date()}
            eps  = build_episodes(df5, sigs, max_sessions=best["max_sessions"])
            trades = []
            for sig_date, ep in eps:
                t = simulate_episode(sig_date, ep, best, args.capital)
                if t:
                    trades.append(t)
            print_trade_log(trades)
