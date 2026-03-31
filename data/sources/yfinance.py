"""Yahoo Finance data source — daily bars only."""

import yfinance as yf
import pandas as pd

SUPPORTED_BAR_SIZES = ["1d"]


def fetch(symbol: str, bar_size: str = "1d", start: str = None, end: str = None) -> pd.DataFrame:
    """
    Fetch daily OHLCV from Yahoo Finance.
    Returns a normalised DataFrame with columns:
      symbol, date, bar_size, open, high, low, close, volume
    """
    if bar_size not in SUPPORTED_BAR_SIZES:
        raise ValueError(f"yfinance only supports daily bars ('1d'). Got: {bar_size}")

    ticker = yf.Ticker(symbol)
    kwargs = {"interval": "1d", "auto_adjust": True}
    if start and end:
        kwargs["start"] = start
        kwargs["end"] = end
    else:
        kwargs["period"] = "20y"

    raw = ticker.history(**kwargs)
    if raw.empty:
        raise ValueError(f"yfinance returned no data for {symbol}")

    df = raw.reset_index()
    df.columns = [c.lower() for c in df.columns]
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["symbol"] = symbol
    df["bar_size"] = bar_size
    return df
