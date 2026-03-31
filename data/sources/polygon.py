"""Polygon.io (Massive) data source — daily and intraday bars."""

import os
import time
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from typing import Optional

ET = ZoneInfo("America/New_York")
import pandas as pd
from polygon import RESTClient
from dotenv import load_dotenv

load_dotenv()

# Map our internal bar_size labels to Polygon multiplier + timespan
BAR_SIZE_MAP = {
    "1m":  (1,  "minute"),
    "5m":  (5,  "minute"),
    "15m": (15, "minute"),
    "30m": (30, "minute"),
    "1h":  (1,  "hour"),
    "1d":  (1,  "day"),
}

SUPPORTED_BAR_SIZES = list(BAR_SIZE_MAP.keys())
MAX_RESULTS_PER_PAGE = 50_000


def _get_client() -> RESTClient:
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "POLYGON_API_KEY not set. Add it to your .env file."
        )
    return RESTClient(api_key)


def fetch(
    symbol: str,
    bar_size: str = "5m",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV bars from Polygon.io.
    Returns a normalised DataFrame with columns:
      symbol, date, bar_size, open, high, low, close, volume

    start / end: "YYYY-MM-DD" strings. Defaults to 5 years back → today.
    """
    if bar_size not in BAR_SIZE_MAP:
        raise ValueError(f"Unsupported bar_size '{bar_size}'. Choose from: {SUPPORTED_BAR_SIZES}")

    multiplier, timespan = BAR_SIZE_MAP[bar_size]

    if not start:
        start = str(date.today().replace(year=date.today().year - 5))
    if not end:
        end = str(date.today())

    client = _get_client()
    print(f"  Fetching {symbol} {bar_size} bars from Polygon ({start} → {end})...")

    rows = []
    # Polygon paginates automatically via the iterator
    for agg in client.list_aggs(
        ticker=symbol,
        multiplier=multiplier,
        timespan=timespan,
        from_=start,
        to=end,
        adjusted=True,
        sort="asc",
        limit=MAX_RESULTS_PER_PAGE,
    ):
        rows.append({
            "date":   _ts_to_str(agg.timestamp, bar_size),
            "open":   agg.open,
            "high":   agg.high,
            "low":    agg.low,
            "close":  agg.close,
            "volume": int(agg.volume) if agg.volume else 0,
        })

    if not rows:
        raise ValueError(f"Polygon returned no data for {symbol} {bar_size} ({start} → {end})")

    df = pd.DataFrame(rows)
    df["symbol"]   = symbol
    df["bar_size"] = bar_size
    print(f"  Retrieved {len(df):,} bars for {symbol} ({bar_size})")
    return df


def _ts_to_str(timestamp_ms: int, bar_size: str) -> str:
    """Convert Polygon millisecond timestamp to Eastern Time string."""
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    dt_et  = dt_utc.astimezone(ET)
    if bar_size == "1d":
        return dt_et.strftime("%Y-%m-%d")
    return dt_et.strftime("%Y-%m-%d %H:%M:%S")
