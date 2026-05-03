"""Databento OPRA option-chain data source."""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Iterable, Optional

import databento as db
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATASET = "OPRA.PILLAR"
SOURCE = "databento"
SUPPORTED_BAR_SIZES = {
    "1d": "ohlcv-1d",
    "1h": "ohlcv-1h",
    "1m": "ohlcv-1m",
}


def get_client() -> db.Historical:
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise EnvironmentError("DATABENTO_API_KEY not set. Add it to your .env file.")
    return db.Historical(api_key)


def option_parent_symbol(underlying: str) -> str:
    return f"{underlying.strip().upper()}.OPT"


def fetch_definitions(
    underlying: str = "SPY",
    start: str = "2020-01-01",
    end: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Fetch point-in-time OPRA definitions for all options under an underlying."""
    end = end or date.today().isoformat()
    underlying = underlying.strip().upper()
    client = get_client()

    store = client.timeseries.get_range(
        dataset=DATASET,
        symbols=option_parent_symbol(underlying),
        stype_in="parent",
        schema="definition",
        start=start,
        end=end,
        limit=limit,
    )
    raw = store.to_df().reset_index()
    if raw.empty:
        return pd.DataFrame()

    raw = raw[raw["underlying"].astype(str).str.upper() == underlying].copy()
    raw = raw[raw["instrument_class"].isin(["C", "P"])].copy()
    if raw.empty:
        return pd.DataFrame()

    raw["definition_date"] = pd.to_datetime(raw["ts_event"], utc=True).dt.date.astype(str)
    raw["expiration"] = pd.to_datetime(raw["expiration"], utc=True).dt.date.astype(str)
    raw["strike"] = raw["strike_price"].astype(float)
    raw["option_type"] = raw["instrument_class"].astype(str)

    grouped = (
        raw.sort_values("ts_event")
        .groupby("raw_symbol", as_index=False)
        .agg({
            "underlying": "last",
            "instrument_id": "last",
            "option_type": "last",
            "strike": "last",
            "expiration": "last",
            "definition_date": ["min", "max"],
            "contract_multiplier": "last",
        })
    )
    grouped.columns = [
        "raw_symbol",
        "underlying",
        "instrument_id",
        "option_type",
        "strike",
        "expiration",
        "first_seen",
        "last_seen",
        "contract_multiplier",
    ]
    return grouped.sort_values(["expiration", "strike", "option_type", "raw_symbol"]).reset_index(drop=True)


def fetch_definition_snapshots(
    underlying: str = "SPY",
    start: str = "2020-01-01",
    end: Optional[str] = None,
    frequency: str = "monthly",
) -> pd.DataFrame:
    """Fetch lower-cost point-in-time definition snapshots across a date range."""
    if frequency != "monthly":
        return fetch_definitions(underlying=underlying, start=start, end=end)

    end = end or date.today().isoformat()
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    if end_ts <= start_ts:
        raise ValueError("end must be after start")

    snapshot_dates = [start_ts]
    for ts in pd.date_range(start_ts, end_ts, freq="MS"):
        if start_ts < ts < end_ts:
            snapshot_dates.append(ts)
    snapshot_dates = sorted(set(snapshot_dates))

    frames = []
    for snapshot in snapshot_dates:
        snapshot_end = min(snapshot + timedelta(days=1), end_ts)
        if snapshot_end <= snapshot:
            continue
        frame = fetch_definitions(
            underlying=underlying,
            start=snapshot.date().isoformat(),
            end=snapshot_end.date().isoformat(),
        )
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)
    grouped = (
        raw.sort_values(["raw_symbol", "first_seen"])
        .groupby("raw_symbol", as_index=False)
        .agg({
            "underlying": "last",
            "instrument_id": "last",
            "option_type": "last",
            "strike": "last",
            "expiration": "last",
            "first_seen": "min",
            "last_seen": "max",
            "contract_multiplier": "last",
        })
    )
    return grouped.sort_values(["expiration", "strike", "option_type", "raw_symbol"]).reset_index(drop=True)


def filter_leaps(
    contracts: pd.DataFrame,
    backtest_start: str,
    min_dte: int = 365,
    option_type: Optional[str] = None,
) -> pd.DataFrame:
    """Select contracts that have at least min_dte from the backtest start/listing date."""
    if contracts.empty:
        return contracts

    out = contracts.copy()
    if option_type:
        out = out[out["option_type"] == option_type.upper()].copy()

    start_ts = pd.Timestamp(backtest_start).normalize()
    first_seen = pd.to_datetime(out["first_seen"], errors="coerce")
    active_from = first_seen.where(first_seen > start_ts, start_ts)
    expiration = pd.to_datetime(out["expiration"], errors="coerce")
    out["dte_at_start"] = (expiration - active_from).dt.days
    out = out[out["dte_at_start"] >= min_dte].copy()
    return out.sort_values(["expiration", "strike", "option_type", "raw_symbol"]).reset_index(drop=True)


def estimate_cost(
    symbols: Iterable[str] | str,
    schema: str,
    start: str,
    end: Optional[str] = None,
    stype_in: str = "raw_symbol",
) -> float:
    client = get_client()
    return float(
        client.metadata.get_cost(
            dataset=DATASET,
            symbols=symbols,
            stype_in=stype_in,
            schema=schema,
            start=start,
            end=end,
        )
    )


def fetch_ohlcv(
    raw_symbols: list[str],
    bar_size: str = "1d",
    start: str = "2020-01-01",
    end: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch OPRA OHLCV bars and consolidate venue-publisher rows per symbol/date."""
    if bar_size not in SUPPORTED_BAR_SIZES:
        raise ValueError(f"Unsupported bar_size '{bar_size}'. Choose from: {sorted(SUPPORTED_BAR_SIZES)}")
    if not raw_symbols:
        return pd.DataFrame()

    end = end or date.today().isoformat()
    client = get_client()
    store = client.timeseries.get_range(
        dataset=DATASET,
        symbols=raw_symbols,
        stype_in="raw_symbol",
        schema=SUPPORTED_BAR_SIZES[bar_size],
        start=start,
        end=end,
    )
    raw = store.to_df().reset_index()
    if raw.empty:
        return pd.DataFrame(columns=["raw_symbol", "date", "open", "high", "low", "close", "volume"])

    raw = raw.rename(columns={"symbol": "raw_symbol"}).copy()
    if bar_size == "1d":
        raw["date"] = pd.to_datetime(raw["ts_event"], utc=True).dt.date.astype(str)
    else:
        raw["date"] = pd.to_datetime(raw["ts_event"], utc=True).dt.tz_convert("America/New_York")
        raw["date"] = raw["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    raw = raw.sort_values(["raw_symbol", "ts_event", "publisher_id"]).reset_index(drop=True)
    consolidated = (
        raw.groupby(["raw_symbol", "date"], as_index=False)
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
    )
    consolidated["volume"] = consolidated["volume"].astype(int)
    return consolidated.sort_values(["date", "raw_symbol"]).reset_index(drop=True)


def fetch_eod_midpoint_marks(
    raw_symbols: list[str],
    start: str,
    end: Optional[str] = None,
    window_start: str = "15:55:00",
    window_end: str = "16:15:00",
) -> pd.DataFrame:
    """Fetch cbbo-1m quotes and keep the last valid quote in the EOD window."""
    if not raw_symbols:
        return pd.DataFrame()

    end = end or date.today().isoformat()
    client = get_client()
    store = client.timeseries.get_range(
        dataset=DATASET,
        symbols=raw_symbols,
        stype_in="raw_symbol",
        schema="cbbo-1m",
        start=start,
        end=end,
    )
    raw = store.to_df().reset_index()
    if raw.empty:
        return pd.DataFrame(columns=[
            "raw_symbol", "date", "mark_time", "bid", "ask", "mid",
            "spread", "spread_pct", "bid_size", "ask_size",
        ])

    raw = raw.rename(columns={"symbol": "raw_symbol"}).copy()
    ts_col = "ts_event" if raw["ts_event"].notna().any() else "ts_recv"
    raw["mark_ts"] = pd.to_datetime(raw[ts_col], utc=True).dt.tz_convert("America/New_York")
    raw["date"] = raw["mark_ts"].dt.date.astype(str)
    raw["time"] = raw["mark_ts"].dt.strftime("%H:%M:%S")
    raw["bid"] = raw["bid_px_00"].astype(float)
    raw["ask"] = raw["ask_px_00"].astype(float)
    raw = raw[
        (raw["time"] >= window_start) &
        (raw["time"] <= window_end) &
        (raw["bid"] > 0) &
        (raw["ask"] > raw["bid"])
    ].copy()
    if raw.empty:
        return pd.DataFrame(columns=[
            "raw_symbol", "date", "mark_time", "bid", "ask", "mid",
            "spread", "spread_pct", "bid_size", "ask_size",
        ])

    raw = raw.sort_values(["raw_symbol", "date", "mark_ts"]).drop_duplicates(
        subset=["raw_symbol", "date"],
        keep="last",
    )
    raw["mid"] = (raw["bid"] + raw["ask"]) / 2.0
    raw["spread"] = raw["ask"] - raw["bid"]
    raw["spread_pct"] = raw["spread"] / raw["mid"]
    raw["mark_time"] = raw["mark_ts"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
    raw["bid_size"] = raw["bid_sz_00"].astype(int)
    raw["ask_size"] = raw["ask_sz_00"].astype(int)
    return raw[[
        "raw_symbol",
        "date",
        "mark_time",
        "bid",
        "ask",
        "mid",
        "spread",
        "spread_pct",
        "bid_size",
        "ask_size",
    ]].sort_values(["date", "raw_symbol"]).reset_index(drop=True)
