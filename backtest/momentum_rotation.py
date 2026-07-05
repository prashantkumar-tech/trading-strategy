"""S&P 500 cross-sectional momentum rotation engine.

Each month: rank the universe by 12-1 momentum, hold an equal-weighted
top-10 basket of names above their own 200-day MA, and rotate a holding out
only once it exits the top 20 (hold buffer). Reuses stored ma200 and the
shared metrics module.
"""

from typing import Callable, List
import pandas as pd
from data.database import load_prices
from backtest.metrics import compute_metrics


def compute_momentum(close: pd.DataFrame, lookback_days: int = 252,
                     skip_days: int = 21) -> pd.DataFrame:
    """12-1 style momentum: return from lookback_days ago to skip_days ago.

    momentum[t] = close[t - skip_days] / close[t - lookback_days] - 1
    """
    return close.shift(skip_days) / close.shift(lookback_days) - 1.0


def month_end_dates(dates) -> List[pd.Timestamp]:
    """Return the last available trading day of each calendar month."""
    idx = pd.DatetimeIndex(sorted(pd.to_datetime(list(dates))))
    ends = []
    for i, d in enumerate(idx):
        is_last = (
            i == len(idx) - 1
            or idx[i + 1].month != d.month
            or idx[i + 1].year != d.year
        )
        if is_last:
            ends.append(d)
    return ends


def select_basket(momentum: pd.Series, eligible: set, current_holdings: list,
                  top_n: int = 10, buffer_rank: int = 20):
    """Pick the target basket applying the top-N hold with a top-buffer_rank sell buffer.

    Returns (basket, rank_map). Holdings still ranked within buffer_rank are kept;
    open slots are refilled from the highest-ranked eligible names not already held.
    """
    ranked = sorted(
        (s for s in eligible if s in momentum.index and pd.notna(momentum[s])),
        key=lambda s: momentum[s],
        reverse=True,
    )
    rank = {s: i + 1 for i, s in enumerate(ranked)}

    kept = [s for s in current_holdings if rank.get(s, 10 ** 9) <= buffer_rank]
    basket = list(kept)
    for s in ranked:
        if len(basket) >= top_n:
            break
        if s not in basket:
            basket.append(s)
    return basket[:top_n], rank


def build_price_panel(symbols: List[str], start=None, end=None,
                      loader: Callable = load_prices):
    """Load each symbol and pivot into aligned wide close/ma200 panels."""
    closes, ma200s = {}, {}
    for sym in symbols:
        df = loader(sym, start=start, end=end)
        if df is None or df.empty:
            continue
        df = df.set_index("date")
        closes[sym] = df["close"]
        ma200s[sym] = df["ma200"]

    close = pd.DataFrame(closes).sort_index()
    ma200 = pd.DataFrame(ma200s).reindex(close.index)
    return close, ma200


def run_momentum_rotation(symbols, start=None, end=None, top_n=10, buffer_rank=20,
                          lookback_days=252, skip_days=21, initial_capital=10_000.0,
                          loader: Callable = load_prices) -> dict:
    """Simulate the monthly equal-weight momentum rotation. See module docstring."""
    close, ma200 = build_price_panel(symbols, start, end, loader)
    if close.empty:
        raise ValueError("No price data available for the selected symbols and date range.")
    momentum = compute_momentum(close, lookback_days, skip_days)
    rebalance_days = set(month_end_dates(close.index))

    cash = float(initial_capital)
    lots = {}          # symbol -> {shares, total_cost, total_proceeds, entry_price, entry_date}
    trades = []
    equity_curve = []
    last_price = {}    # symbol -> most recent non-NaN close, for valuation only

    def holdings_value(prices_row):
        return sum(l["shares"] * last_price[s]
                   for s, l in lots.items() if s in last_price)

    for d in close.index:
        prices_row = close.loc[d]
        for s in prices_row.index:
            p = prices_row[s]
            if pd.notna(p):
                last_price[s] = p

        if d in rebalance_days:
            mom_row = momentum.loc[d]
            ma_row = ma200.loc[d]
            eligible = {
                s for s in close.columns
                if pd.notna(mom_row.get(s)) and pd.notna(prices_row.get(s))
                and pd.notna(ma_row.get(s)) and prices_row[s] > ma_row[s]
            }
            basket, rank = select_basket(mom_row, eligible, list(lots.keys()),
                                         top_n, buffer_rank)
            portfolio_value = cash + holdings_value(prices_row)
            target = portfolio_value / top_n

            # ── Exits: fully close any holding no longer in the basket ──────
            for s in list(lots.keys()):
                if s in basket:
                    continue
                p = prices_row.get(s)
                if pd.isna(p):
                    continue  # can't price this bar; defer
                lot = lots[s]
                proceeds = lot["shares"] * p
                cash += proceeds
                lot["total_proceeds"] += proceeds
                pnl = lot["total_proceeds"] - lot["total_cost"]
                reason = "below 200MA" if s not in eligible else "fell out of top 20"
                trades.append({
                    "symbol": s,
                    "entry_date": lot["entry_date"],
                    "exit_date": str(d.date()),
                    "entry_price": round(lot["entry_price"], 4),
                    "exit_price": round(p, 4),
                    "shares": round(lot["shares"], 6),
                    "pnl": round(pnl, 2),
                    "return_pct": round(pnl / lot["total_cost"] * 100, 2) if lot["total_cost"] else 0.0,
                    "exit_reason": reason,
                    "rank_at_exit": rank.get(s),
                })
                del lots[s]

            # ── Rebalance basket to equal weight (target per name) ──────────
            for s in basket:
                p = prices_row.get(s)
                if pd.isna(p):
                    continue
                cur_shares = lots[s]["shares"] if s in lots else 0.0
                delta = (target / p) - cur_shares
                if delta > 0:                      # buy / top up
                    cost = min(delta * p, cash)
                    if cost <= 0:
                        continue
                    buy_shares = cost / p
                    if s in lots:
                        lots[s]["shares"] += buy_shares
                        lots[s]["total_cost"] += cost
                    else:
                        lots[s] = {"shares": buy_shares, "total_cost": cost,
                                   "total_proceeds": 0.0, "entry_price": p,
                                   "entry_date": str(d.date())}
                    cash -= cost
                elif delta < 0:                    # trim
                    sell_shares = -delta
                    proceeds = sell_shares * p
                    lots[s]["shares"] -= sell_shares
                    lots[s]["total_proceeds"] += proceeds
                    cash += proceeds

        equity_curve.append({"date": d, "portfolio_value": cash + holdings_value(prices_row)})

    equity_series = pd.DataFrame(equity_curve).set_index("date")["portfolio_value"]
    metrics = compute_metrics(equity_series, trades)
    return {
        "equity_curve": equity_series,
        "trades": trades,
        "metrics": metrics,
        "final_value": round(float(equity_series.iloc[-1]), 2) if not equity_series.empty else initial_capital,
        "holdings": list(lots.keys()),
    }
