"""
Daily-bar Dip Sniper backtest for TQQQ / SPXL.

Strategy
--------
  Regime gate : SPY close > SPY MA200  AND  VIX < vix_thresh
  Entry       : ticker drops >= dip_entry % from its rolling N-day high (close basis)
  Tranches    : add up to n_tranches positions, each spaced tranche_gap % further down
  Exit        : profit target, hard stop, or regime breach — whichever comes first

Position sizing
---------------
  Each tranche = (max_pos_pct / n_tranches) × initial_capital  (fixed dollar amounts)
  Max simultaneous exposure = max_pos_pct × initial_capital  (default 50%)

Usage
-----
    python3 -m backtest.dip_sniper_daily               # full grid, both tickers
    python3 -m backtest.dip_sniper_daily --ticker TQQQ # single ticker
"""

import argparse
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Allow running as a module from the project root
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.database import load_prices
from backtest.metrics import compute_metrics


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_aligned(ticker: str) -> pd.DataFrame:
    """Return a daily DataFrame with ticker OHLC + SPY regime cols + VIX."""
    t = load_prices(ticker, bar_size="1d", source="yfinance")[
        ["date", "open", "close", "ma50", "ma200"]
    ]
    spy = load_prices("SPY", bar_size="1d", source="yfinance")[
        ["date", "close", "ma200"]
    ].rename(columns={"close": "spy_close", "ma200": "spy_ma200"})
    vix = load_prices("^VIX", bar_size="1d", source="yfinance")[
        ["date", "close"]
    ].rename(columns={"close": "vix"})

    for df in (t, spy, vix):
        df["date"] = pd.to_datetime(df["date"])

    merged = (
        t.merge(spy, on="date", how="inner")
         .merge(vix, on="date", how="inner")
         .sort_values("date")
         .reset_index(drop=True)
    )
    return merged


# ---------------------------------------------------------------------------
# Core backtest engine
# ---------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame, params: dict, capital: float = 10_000.0) -> dict:
    """
    Simulate one parameter combination on pre-loaded DataFrame.

    Returns a dict with:
        metrics   – output of compute_metrics()
        trades    – list of trade dicts
        equity    – pd.Series (date-indexed)
        params    – echo of input params
    """
    max_pos_pct  = params["max_pos_pct"]    # 0.50
    n_tranches   = params["n_tranches"]     # 1, 2, or 3
    dip_entry    = params["dip_entry"]      # first entry threshold, e.g. 0.08
    tranche_gap  = params["tranche_gap"]    # spacing between tranches, e.g. 0.05
    high_lb      = params["high_lb"]        # rolling-high lookback in days
    profit_tgt   = params["profit_tgt"]     # e.g. 0.12 → exit when +12%
    stop_loss    = params["stop_loss"]      # e.g. 0.15 → exit when −15%
    vix_thresh   = params.get("vix_thresh") # None means no VIX filter
    spy_bull     = params.get("spy_bull", True)

    tranche_usd  = (max_pos_pct * capital) / n_tranches   # fixed $ per tranche

    # Pre-compute lagged rolling high to avoid lookahead bias
    df = df.copy()
    df["rolling_high"] = df["close"].rolling(window=high_lb, min_periods=1).max().shift(1)

    cash         = capital
    tranches     = []   # list of {"price": float, "shares": float}
    trade_entry  = None # date of first tranche
    trade_cost   = 0.0  # total $ invested in open trade

    equity_dates  = []
    equity_values = []
    trades        = []

    for row in df.itertuples(index=False):
        price       = row.close
        date        = row.date
        rh          = row.rolling_high

        spy_ok = (not spy_bull) or (
            not np.isnan(row.spy_ma200) and row.spy_close > row.spy_ma200
        )
        vix_ok = (vix_thresh is None) or (
            not np.isnan(row.vix) and row.vix < vix_thresh
        )
        regime_ok = spy_ok and vix_ok

        # ---- Manage open position ----------------------------------------
        if tranches:
            open_value   = sum(t["shares"] * price for t in tranches)
            total_pnl_pct = (open_value - trade_cost) / trade_cost

            exit_now    = False
            exit_reason = ""

            if total_pnl_pct >= profit_tgt:
                exit_now    = True
                exit_reason = "profit_target"
            elif total_pnl_pct <= -stop_loss:
                exit_now    = True
                exit_reason = "stop_loss"
            elif not regime_ok:
                exit_now    = True
                exit_reason = "regime_exit"

            if exit_now:
                proceeds  = open_value
                pnl       = proceeds - trade_cost
                cash     += proceeds
                trades.append({
                    "entry_date"  : trade_entry,
                    "exit_date"   : date,
                    "n_tranches"  : len(tranches),
                    "cost_usd"    : round(trade_cost, 2),
                    "pnl"         : round(pnl, 4),
                    "return_pct"  : round(total_pnl_pct * 100, 3),
                    "exit_reason" : exit_reason,
                    "hold_days"   : (date - trade_entry).days,
                })
                tranches    = []
                trade_entry = None
                trade_cost  = 0.0

        # ---- Entry: open first tranche -----------------------------------
        if not tranches and regime_ok and not np.isnan(rh) and rh > 0:
            dip_pct = (rh - price) / rh
            if dip_pct >= dip_entry:
                shares       = tranche_usd / price
                cash        -= tranche_usd
                trade_cost  += tranche_usd
                tranches.append({"price": price, "shares": shares})
                trade_entry  = date

        # ---- Entry: add subsequent tranches ------------------------------
        elif tranches and len(tranches) < n_tranches and regime_ok and not np.isnan(rh) and rh > 0:
            next_idx      = len(tranches)
            required_dip  = dip_entry + tranche_gap * next_idx
            dip_pct       = (rh - price) / rh

            # Don't double-enter at nearly the same price
            last_price    = tranches[-1]["price"]
            price_moved   = (last_price - price) / last_price

            if dip_pct >= required_dip and price_moved >= 0.005:
                shares       = tranche_usd / price
                cash        -= tranche_usd
                trade_cost  += tranche_usd
                tranches.append({"price": price, "shares": shares})

        # ---- Mark-to-market equity ---------------------------------------
        open_value         = sum(t["shares"] * price for t in tranches)
        equity_dates.append(date)
        equity_values.append(cash + open_value)

    # Close any open position at end of data at last price
    if tranches:
        price     = df["close"].iloc[-1]
        proceeds  = sum(t["shares"] * price for t in tranches)
        pnl       = proceeds - trade_cost
        trades.append({
            "entry_date"  : trade_entry,
            "exit_date"   : df["date"].iloc[-1],
            "n_tranches"  : len(tranches),
            "cost_usd"    : round(trade_cost, 2),
            "pnl"         : round(pnl, 4),
            "return_pct"  : round(pnl / trade_cost * 100, 3),
            "exit_reason" : "end_of_data",
            "hold_days"   : (df["date"].iloc[-1] - trade_entry).days,
        })

    equity_curve = pd.Series(equity_values, index=pd.to_datetime(equity_dates))
    metrics      = compute_metrics(equity_curve, trades)

    return {
        "params"  : params,
        "metrics" : metrics,
        "trades"  : trades,
        "equity"  : equity_curve,
    }


# ---------------------------------------------------------------------------
# Grid search
# ---------------------------------------------------------------------------

PARAM_GRID = {
    "max_pos_pct" : [0.50],
    "n_tranches"  : [1, 2, 3],
    "dip_entry"   : [0.05, 0.08, 0.10, 0.12, 0.15, 0.20],
    "tranche_gap" : [0.03, 0.05, 0.08],
    "high_lb"     : [5, 10, 20],
    "profit_tgt"  : [0.08, 0.12, 0.15, 0.20],
    "stop_loss"   : [0.12, 0.15, 0.20],
    "vix_thresh"  : [25, 28, 32, None],
    "spy_bull"    : [True, False],
}


def grid_combinations(grid: dict):
    keys   = list(grid.keys())
    values = list(grid.values())
    for combo in itertools.product(*values):
        yield dict(zip(keys, combo))


def run_grid(ticker: str, capital: float = 10_000.0, top_n: int = 20) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print(f"  Dip Sniper — Daily Grid Search  |  {ticker}")
    print(f"{'='*60}")

    df = load_aligned(ticker)
    print(f"  Data: {df['date'].min().date()} → {df['date'].max().date()}  "
          f"({len(df):,} trading days)")

    combos  = list(grid_combinations(PARAM_GRID))
    total   = len(combos)
    print(f"  Combinations: {total:,}\n")

    results = []
    for idx, params in enumerate(combos, 1):
        if idx % 500 == 0:
            print(f"  ... {idx}/{total}", flush=True)
        try:
            r = run_backtest(df, params, capital=capital)
            m = r["metrics"]
            results.append({
                **{f"p_{k}": v for k, v in params.items()},
                **m,
            })
        except Exception:
            pass

    res_df = pd.DataFrame(results)

    # Filter: must beat buy-and-hold basics and respect drawdown constraint
    filtered = res_df[
        (res_df["max_drawdown_pct"] >= -10.0) &   # within 10% drawdown limit
        (res_df["num_trades"]       >=  10)   &   # enough trades to be meaningful
        (res_df["win_rate_pct"]     >=  50.0) &   # majority winners
        (res_df["annualized_return_pct"] > 0)
    ]

    print(f"\n  Total combos run : {len(res_df):,}")
    print(f"  Pass filters     : {len(filtered):,}")

    # Sort by Sharpe, then annualized return
    top = (
        filtered
        .sort_values(["sharpe_ratio", "annualized_return_pct"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    out_path = Path(__file__).parent.parent / "examples" / f"dip_sniper_daily_{ticker}.csv"
    res_df.to_csv(out_path, index=False)
    print(f"  Full results saved → {out_path}")

    return top


def print_top(top: pd.DataFrame, ticker: str) -> None:
    print(f"\n{'='*60}")
    print(f"  TOP RESULTS — {ticker}")
    print(f"{'='*60}")

    display_cols = [
        "p_n_tranches", "p_dip_entry", "p_tranche_gap", "p_high_lb",
        "p_profit_tgt", "p_stop_loss", "p_vix_thresh", "p_spy_bull",
        "annualized_return_pct", "sharpe_ratio", "max_drawdown_pct",
        "num_trades", "win_rate_pct", "avg_win_usd", "avg_loss_usd",
    ]
    available = [c for c in display_cols if c in top.columns]
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", "{:.3f}".format)
    print(top[available].to_string(index=True))


def print_trade_log(result: dict) -> None:
    trades_df = pd.DataFrame(result["trades"])
    if trades_df.empty:
        print("  No trades.")
        return
    print(f"\n  Total trades : {len(trades_df)}")
    print(f"  Win rate     : {(trades_df['pnl'] > 0).mean()*100:.1f}%")
    print(f"  Avg hold     : {trades_df['hold_days'].mean():.1f} days")
    print(f"  Exit reasons : {trades_df['exit_reason'].value_counts().to_dict()}")
    print("\n  Sample trades (first 10):")
    print(trades_df.head(10).to_string(index=False))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dip Sniper daily grid search")
    parser.add_argument("--ticker", default=None, help="TQQQ or SPXL (default: both)")
    parser.add_argument("--capital", type=float, default=10_000.0)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--detail", action="store_true",
                        help="Print trade log for best combo")
    args = parser.parse_args()

    tickers = [args.ticker] if args.ticker else ["TQQQ", "SPXL"]

    for ticker in tickers:
        top = run_grid(ticker, capital=args.capital, top_n=args.top)
        print_top(top, ticker)

        if args.detail and not top.empty:
            # Re-run best combo and print trade log
            best_params = {}
            for k, v in top.iloc[0].items():
                if not k.startswith("p_"):
                    continue
                key = k[2:]  # strip "p_" prefix
                # pandas stores None as NaN; convert back for vix_thresh
                if key == "vix_thresh" and (v != v):  # NaN check
                    v = None
                elif key == "spy_bull":
                    v = bool(v)
                best_params[key] = v
            df = load_aligned(ticker)
            result = run_backtest(df, best_params, capital=args.capital)
            print_trade_log(result)
