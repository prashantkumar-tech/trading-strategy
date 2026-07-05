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
