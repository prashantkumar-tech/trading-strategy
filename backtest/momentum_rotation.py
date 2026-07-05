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
