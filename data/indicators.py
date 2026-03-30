"""Technical indicator calculations applied to a price DataFrame."""

import pandas as pd


def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Add ma50 and ma200 columns. Requires a 'close' column sorted by date."""
    df = df.copy()
    df["ma50"] = df["close"].rolling(window=50, min_periods=50).mean()
    df["ma200"] = df["close"].rolling(window=200, min_periods=200).mean()
    return df
