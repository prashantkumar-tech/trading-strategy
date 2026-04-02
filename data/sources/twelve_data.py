"""Twelve Data source for daily and intraday OHLCV bars."""

import json
import os
import time
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.twelvedata.com/time_series"

BAR_SIZE_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "1d": "1day",
}

SUPPORTED_BAR_SIZES = list(BAR_SIZE_MAP.keys())


def fetch(
    symbol: str,
    bar_size: str = "5m",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV bars from Twelve Data.
    Returns a normalised DataFrame with columns:
      symbol, date, bar_size, open, high, low, close, volume
    """
    if bar_size not in BAR_SIZE_MAP:
        raise ValueError(f"Unsupported bar_size '{bar_size}'. Choose from: {SUPPORTED_BAR_SIZES}")

    api_key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TWELVEDATA_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "TWELVE_DATA_API_KEY not set. Add it to your .env file."
        )

    if not start:
        start = str(date.today().replace(year=date.today().year - 1))
    if not end:
        end = str(date.today())

    print(f"  Fetching {symbol} {bar_size} bars from Twelve Data ({start} -> {end})...")

    rows = []
    chunks = _build_chunks(start, end, bar_size)
    for chunk_start, chunk_end in chunks:
        rows.extend(_fetch_chunk(symbol, bar_size, chunk_start, chunk_end, api_key))

    if not rows:
        raise ValueError(f"Twelve Data returned no data for {symbol} {bar_size} ({start} -> {end})")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    df["symbol"] = symbol
    df["bar_size"] = bar_size
    print(f"  Retrieved {len(df):,} bars for {symbol} ({bar_size})")
    return df


def _fetch_chunk(symbol: str, bar_size: str, start: str, end: str, api_key: str) -> list[dict]:
    params = {
        "symbol": symbol,
        "interval": BAR_SIZE_MAP[bar_size],
        "start_date": _format_start(start),
        "end_date": _format_end(end, bar_size),
        "order": "ASC",
        "timezone": "America/New_York",
        "format": "JSON",
        "outputsize": 5000,
        "apikey": api_key,
    }
    try_prepost = bar_size != "1d"
    payload = _request_payload(params, try_prepost=try_prepost)

    values = payload.get("values", [])
    rows = []
    for bar in values:
        rows.append({
            "date": _normalise_date(bar["datetime"], bar_size),
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"]),
            "volume": int(float(bar.get("volume", 0) or 0)),
        })
    return rows


def _request_payload(params: dict, try_prepost: bool) -> dict:
    request_params = dict(params)
    if try_prepost:
        request_params["prepost"] = "true"

    for attempt in range(3):
        url = f"{BASE_URL}?{urlencode(request_params)}"
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))

        if payload.get("status") != "error":
            return payload

        message = payload.get("message", "unknown error")
        if "Pre-market and post-market data are available on the Pro plan" in message and try_prepost:
            print("  Twelve Data plan does not include pre/post-market. Falling back to regular hours.")
            request_params.pop("prepost", None)
            try_prepost = False
            continue

        if "run out of API credits for the current minute" in message and attempt < 2:
            print("  Twelve Data minute credit limit reached. Waiting 65s before retrying...")
            time.sleep(65)
            continue

        raise ValueError(f"Twelve Data error: {message}")

    raise ValueError("Twelve Data error: request failed after retries")


def _build_chunks(start: str, end: str, bar_size: str) -> list[tuple[str, str]]:
    if bar_size == "1d":
        return [(start, end)]

    start_dt = datetime.strptime(start[:10], "%Y-%m-%d").date()
    end_dt = datetime.strptime(end[:10], "%Y-%m-%d").date()
    chunks = []
    cursor = start_dt
    while cursor <= end_dt:
        if cursor.month == 12:
            month_end = date(cursor.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(cursor.year, cursor.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, end_dt)
        chunks.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _format_start(value: str) -> str:
    return f"{value} 00:00:00" if len(value) == 10 else value


def _format_end(value: str, bar_size: str) -> str:
    if len(value) != 10:
        return value
    if bar_size == "1d":
        return value
    dt = datetime.strptime(value, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _normalise_date(value: str, bar_size: str) -> str:
    if bar_size == "1d":
        return value
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")
