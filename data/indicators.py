"""Technical indicator calculations applied to a price DataFrame."""

import datetime
import numpy as np
import pandas as pd

# Regular market hours in ET
_MARKET_OPEN  = datetime.time(9, 30)
_MARKET_CLOSE = datetime.time(16, 0)

# For intraday bars, MA windows are expressed in regular-hours bars only.
# 5-min bars: 78 bars/day (9:30–16:00 ET) → MA50 = 50 × 78 = 3,900 bars
INTRADAY_MA_WINDOWS = {
    "1m":  {"ma50": 50 * 390, "ma200": 200 * 390},  # 390 bars/day
    "5m":  {"ma50": 50 * 78,  "ma200": 200 * 78},   # 78 bars/day
    "15m": {"ma50": 50 * 26,  "ma200": 200 * 26},   # 26 bars/day
    "30m": {"ma50": 50 * 13,  "ma200": 200 * 13},   # 13 bars/day
    "1h":  {"ma50": 50 * 7,   "ma200": 200 * 7},    # ~7 bars/day
}

DAILY_WINDOWS = {"ma50": 50, "ma200": 200}


def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ma50 and ma200 columns. Bar-size aware:
      - daily ("1d"): standard 50/200-day MAs on adjusted closes
      - intraday: MA computed on regular-hours bars only (9:30–16:00 ET),
                  then forward-filled to premarket / after-hours bars.
                  This ensures the window truly covers 50/200 trading days
                  regardless of how many extended-hours bars are in the data.
    """
    df = df.copy()
    bar_size = df["bar_size"].iloc[0] if "bar_size" in df.columns else "1d"

    if bar_size == "1d":
        windows = DAILY_WINDOWS
        df["ma50"]  = df["close"].rolling(window=windows["ma50"],  min_periods=windows["ma50"]).mean()
        df["ma200"] = df["close"].rolling(window=windows["ma200"], min_periods=windows["ma200"]).mean()
    else:
        windows = INTRADAY_MA_WINDOWS.get(bar_size, DAILY_WINDOWS)

        # Identify regular-hours bars (9:30 AM – 4:00 PM ET)
        bar_time = pd.to_datetime(df["date"]).dt.time
        reg_mask = (bar_time >= _MARKET_OPEN) & (bar_time <= _MARKET_CLOSE)

        # Compute MA on regular-hours bars only
        reg_close = df.loc[reg_mask, "close"]
        ma50_reg  = reg_close.rolling(window=windows["ma50"],  min_periods=windows["ma50"]).mean()
        ma200_reg = reg_close.rolling(window=windows["ma200"], min_periods=windows["ma200"]).mean()

        # Write back to full df, then forward-fill to extended-hours bars
        df["ma50"]  = np.nan
        df["ma200"] = np.nan
        df.loc[reg_mask, "ma50"]  = ma50_reg.values
        df.loc[reg_mask, "ma200"] = ma200_reg.values
        df["ma50"]  = df["ma50"].ffill()
        df["ma200"] = df["ma200"].ffill()

    return df
