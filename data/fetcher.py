"""
Data fetcher — orchestrates source selection, indicator calculation, and storage.

Usage:
    fetch_and_store("SPY")                          # daily via Yahoo Finance
    fetch_and_store("SPY", bar_size="5m",
                   source="polygon",
                   start="2020-01-01")              # 5-min via Polygon
"""

from datetime import timedelta
import pandas as pd
from data.database import init_db, load_prices, get_dataset_details, upsert_prices
from data.indicators import add_moving_averages

# Available sources
from data.sources import yfinance as _yf_source
from data.sources import polygon  as _poly_source
from data.sources import twelve_data as _twelve_source
from data.sources import databento as _db_source

SOURCES = {
    "yfinance": _yf_source,
    "polygon":  _poly_source,
    "twelve_data": _twelve_source,
    "databento": _db_source,
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

INDICATOR_CONTEXT_DAYS = {
    "1d": 260,
    "1m": 400,
    "5m": 400,
    "15m": 400,
    "30m": 400,
    "1h": 400,
}


def _context_start(max_date: str, bar_size: str) -> str:
    dt = pd.Timestamp(max_date)
    overlap_days = INDICATOR_CONTEXT_DAYS.get(bar_size, 400)
    return (dt - timedelta(days=overlap_days)).strftime("%Y-%m-%d")


def _normalize_date_strings(df: pd.DataFrame, bar_size: str) -> pd.DataFrame:
    df = df.copy()
    if bar_size == "1d":
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    else:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def _normalize_date_dtype(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_and_store(
    symbol: str,
    bar_size: str = "1d",
    source: str = None,
    start: str = None,
    end: str = None,
    refresh_mode: str = "incremental",
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

    init_db()
    dataset = get_dataset_details(symbol, bar_size=bar_size, source=source)

    if refresh_mode not in {"incremental", "full"}:
        raise ValueError("refresh_mode must be 'incremental' or 'full'")

    use_incremental = refresh_mode == "incremental" and dataset is not None

    print(f"Fetching {symbol} ({bar_size}) via {source}...")

    if not use_incremental:
        df = SOURCES[source].fetch(symbol, bar_size=bar_size, start=start, end=end)
        df = add_moving_averages(df)
        df = _normalize_date_strings(df, bar_size)
        upsert_prices(df, symbol, bar_size=bar_size, source=source)
        print(f"Stored {len(df):,} {bar_size} bars for {symbol}.")
        return df

    max_date = dataset["max_date"]
    fetch_start = max_date
    context_start = _context_start(max_date, bar_size)

    history_df = load_prices(
        symbol,
        start=context_start,
        end=max_date,
        bar_size=bar_size,
        source=source,
    )
    history_df = history_df[["date", "open", "high", "low", "close", "volume", "symbol", "bar_size"]].copy()

    new_df = SOURCES[source].fetch(symbol, bar_size=bar_size, start=fetch_start, end=end)

    combined = pd.concat(
        [_normalize_date_dtype(history_df), _normalize_date_dtype(new_df)],
        ignore_index=True,
    )
    combined = combined.drop_duplicates(subset="date", keep="last").sort_values("date").reset_index(drop=True)
    combined = add_moving_averages(combined)
    combined = _normalize_date_strings(combined, bar_size)

    replace_from = combined["date"].iloc[0][:10]
    upsert_prices(
        combined,
        symbol,
        bar_size=bar_size,
        source=source,
        replace_from=replace_from,
    )
    print(
        f"Stored {len(combined):,} recalculated bars for {symbol}; "
        f"incremental refresh from {max_date} through {end or 'today'}."
    )
    return combined
