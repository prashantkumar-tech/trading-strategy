"""Technical indicator calculations applied to a price DataFrame."""

import pandas as pd

# For intraday bars, MA windows are expressed in bars not days.
# 5-min bars: 78 bars/day → MA50 ≈ 50 days × 78 = 3,900 bars
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
      - daily ("1d"): standard 50/200-day MAs
      - intraday: equivalent bar counts for the same calendar-day span
    """
    df = df.copy()
    bar_size = df["bar_size"].iloc[0] if "bar_size" in df.columns else "1d"
    windows = INTRADAY_MA_WINDOWS.get(bar_size, DAILY_WINDOWS)

    df["ma50"]  = df["close"].rolling(window=windows["ma50"],  min_periods=windows["ma50"]).mean()
    df["ma200"] = df["close"].rolling(window=windows["ma200"], min_periods=windows["ma200"]).mean()
    return df
