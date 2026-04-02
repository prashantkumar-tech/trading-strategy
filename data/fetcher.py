"""
Data fetcher — orchestrates source selection, indicator calculation, and storage.

Usage:
    fetch_and_store("SPY")                          # daily via Yahoo Finance
    fetch_and_store("SPY", bar_size="5m",
                   source="polygon",
                   start="2020-01-01")              # 5-min via Polygon
"""

import pandas as pd
from data.database import init_db, upsert_prices
from data.indicators import add_moving_averages

# Available sources
from data.sources import yfinance as _yf_source
from data.sources import polygon  as _poly_source
from data.sources import twelve_data as _twelve_source

SOURCES = {
    "yfinance": _yf_source,
    "polygon":  _poly_source,
    "twelve_data": _twelve_source,
}

# Default source per bar size
DEFAULT_SOURCE = {
    "1d":  "yfinance",
    "5m":  "polygon",
    "15m": "polygon",
    "1m":  "polygon",
    "30m": "polygon",
    "1h":  "polygon",
}


def fetch_and_store(
    symbol: str,
    bar_size: str = "1d",
    source: str = None,
    start: str = None,
    end: str = None,
) -> pd.DataFrame:
    """
    Download bars, compute indicators, and persist to SQLite.

    Parameters
    ----------
    symbol   : ticker, e.g. "SPY"
    bar_size : "1d" | "5m" | "15m" | "1m" | "30m" | "1h"
    source   : "yfinance" | "polygon" | "twelve_data" (auto-selected if None)
    start    : "YYYY-MM-DD"  (optional, source default if None)
    end      : "YYYY-MM-DD"  (optional, today if None)
    """
    if source is None:
        source = DEFAULT_SOURCE.get(bar_size, "polygon")

    if source not in SOURCES:
        raise ValueError(f"Unknown source '{source}'. Choose from: {list(SOURCES)}")

    print(f"Fetching {symbol} ({bar_size}) via {source}...")
    df = SOURCES[source].fetch(symbol, bar_size=bar_size, start=start, end=end)

    df = add_moving_averages(df)

    init_db()
    upsert_prices(df, symbol, bar_size=bar_size, source=source)
    print(f"Stored {len(df):,} {bar_size} bars for {symbol}.")
    return df
