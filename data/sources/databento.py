"""Databento historical futures source for CME Globex products."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

import databento as db
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ET = ZoneInfo("America/New_York")

SUPPORTED_BAR_SIZES = ["1m", "5m", "15m", "30m", "1h"]
RESAMPLE_RULES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
}
DEFAULT_DATASET = "GLBX.MDP3"


def _get_client() -> db.Historical:
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise EnvironmentError("DATABENTO_API_KEY not set. Add it to your .env file.")
    return db.Historical(api_key)


def _resolve_symbol(symbol: str) -> Tuple[str, str]:
    symbol = symbol.strip().upper()
    if "." in symbol:
        if ".V." in symbol:
            return symbol, "continuous"
        if symbol.endswith(".FUT"):
            return symbol, "parent"
        return symbol, "raw_symbol"
    return f"{symbol}.v.0", "continuous"


def _format_range(start: Optional[str], end: Optional[str]) -> Tuple[str, str]:
    if not start:
        start = str(date.today().replace(year=date.today().year - 2))
    if not end:
        end = str(date.today())

    start_ts = pd.Timestamp(start)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize(ET)
    else:
        start_ts = start_ts.tz_convert(ET)

    end_ts = pd.Timestamp(end)
    if end_ts.tzinfo is None:
        if len(str(end)) == 10:
            end_ts = end_ts.tz_localize(ET) + timedelta(days=1)
        else:
            end_ts = end_ts.tz_localize(ET)
    else:
        end_ts = end_ts.tz_convert(ET)

    return start_ts.tz_convert("UTC").isoformat(), end_ts.tz_convert("UTC").isoformat()


def _to_ohlcv_frame(store, output_symbol: str) -> pd.DataFrame:
    df = store.to_df().reset_index()
    if df.empty:
        return df

    ts = pd.to_datetime(df["ts_event"], utc=True).dt.tz_convert(ET)
    out = pd.DataFrame({
        "date": ts,
        "open": df["open"].astype(float),
        "high": df["high"].astype(float),
        "low": df["low"].astype(float),
        "close": df["close"].astype(float),
        "volume": df["volume"].astype(int),
    })
    out["symbol"] = output_symbol
    return out


def _resample_intraday(df: pd.DataFrame, bar_size: str) -> pd.DataFrame:
    if bar_size == "1m":
        return df

    resampled = (
        df.set_index("date")
        .resample(RESAMPLE_RULES[bar_size], label="left", closed="left")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    resampled["symbol"] = df["symbol"].iloc[0]
    return resampled


def fetch(
    symbol: str,
    bar_size: str = "5m",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch historical CME Globex futures bars from Databento.

    For root symbols like MES, uses continuous front-month symbology (e.g. MES.v.0).
    Always pulls ohlcv-1m and resamples client-side for custom intraday intervals.
    """
    if bar_size not in SUPPORTED_BAR_SIZES:
        raise ValueError(f"Unsupported bar_size '{bar_size}'. Choose from: {SUPPORTED_BAR_SIZES}")

    db_symbol, stype_in = _resolve_symbol(symbol)
    start_utc, end_utc = _format_range(start, end)

    client = _get_client()
    print(f"  Fetching {symbol} {bar_size} bars from Databento ({db_symbol}, {start_utc} -> {end_utc})...")

    store = client.timeseries.get_range(
        dataset=DEFAULT_DATASET,
        symbols=db_symbol,
        stype_in=stype_in,
        schema="ohlcv-1m",
        start=start_utc,
        end=end_utc,
    )

    df = _to_ohlcv_frame(store, symbol.strip().upper())
    if df.empty:
        raise ValueError(f"Databento returned no data for {symbol} ({start} -> {end})")

    df = _resample_intraday(df, bar_size)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["bar_size"] = bar_size
    print(f"  Retrieved {len(df):,} bars for {symbol} ({bar_size})")
    return df[["date", "open", "high", "low", "close", "volume", "symbol", "bar_size"]]
