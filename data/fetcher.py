"""Fetch historical OHLCV data from Yahoo Finance and store in SQLite."""

import yfinance as yf
import pandas as pd
from data.database import init_db, upsert_prices
from data.indicators import add_moving_averages


def fetch_and_store(symbol: str, period: str = "20y") -> pd.DataFrame:
    """
    Download daily OHLCV for `symbol`, compute MAs, persist to DB.
    Returns the cleaned DataFrame.
    """
    print(f"Fetching {symbol} ({period})...")
    ticker = yf.Ticker(symbol)
    raw = ticker.history(period=period, interval="1d", auto_adjust=True)

    if raw.empty:
        raise ValueError(f"No data returned for {symbol}")

    df = raw.reset_index()
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"date": "date"})

    # Keep only needed columns
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["symbol"] = symbol

    df = add_moving_averages(df)

    init_db()
    upsert_prices(df, symbol)
    print(f"Stored {len(df)} rows for {symbol}.")
    return df


if __name__ == "__main__":
    fetch_and_store("SPY")
