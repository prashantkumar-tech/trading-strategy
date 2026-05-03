"""Fetch a daily SPY 1-year LEAPS target-contract series from Databento OPRA.

Default rule:
  - call option
  - annual January expiration closest to 365 DTE
  - strike closest to SPY close on a $5 grid

The script stores:
  - selected contracts in option_contracts
  - downloaded OHLCV rows in option_prices
  - daily strategy mapping in option_daily_targets
"""

from __future__ import annotations

import argparse
import calendar
import sys
import time
from datetime import date, timedelta
from typing import Iterable

import pandas as pd
import yfinance as yf

from data.options_database import (
    init_options_db,
    replace_option_daily_targets,
    replace_option_prices,
    upsert_option_contracts,
)
from data.sources import databento_options as dbo


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(values), size):
        yield values[idx:idx + size]


def _third_friday(year: int, month: int) -> pd.Timestamp:
    cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
    fridays = [
        day for day in cal.itermonthdates(year, month)
        if day.month == month and day.weekday() == calendar.FRIDAY
    ]
    return pd.Timestamp(fridays[2])


def _standard_monthly_expirations(start: str, end: str, min_dte: int, max_dte: int) -> list[pd.Timestamp]:
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    month_start = (start_ts + timedelta(days=min_dte)).replace(day=1)
    month_end = (end_ts + timedelta(days=max_dte)).replace(day=1)

    expirations = []
    for month in pd.date_range(month_start, month_end, freq="MS"):
        expirations.append(_third_friday(month.year, month.month))
    return expirations


def _annual_january_expirations(start: str, end: str, min_dte: int, max_dte: int) -> list[pd.Timestamp]:
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    first_year = (start_ts + timedelta(days=min_dte)).year
    last_year = (end_ts + timedelta(days=max_dte)).year
    return [_third_friday(year, 1) for year in range(first_year, last_year + 1)]


def _occ_raw_symbol(underlying: str, expiration: pd.Timestamp, option_type: str, strike: float) -> str:
    root = f"{underlying.upper():<6}"[:6]
    yymmdd = expiration.strftime("%y%m%d")
    strike_code = int(round(strike * 1000))
    return f"{root}{yymmdd}{option_type.upper()}{strike_code:08d}"


def _round_strike(price: float, increment: float) -> float:
    return round(round(price / increment) * increment, 2)


def _load_underlying_prices(underlying: str, start: str, end: str) -> pd.DataFrame:
    ticker = yf.Ticker(underlying)
    raw = ticker.history(start=start, end=end, interval="1d", auto_adjust=False)
    if raw.empty:
        raise ValueError(f"yfinance returned no unadjusted {underlying} prices for {start} -> {end}")

    df = raw.reset_index()
    df.columns = [str(col).lower() for col in df.columns]
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df.sort_values("date").reset_index(drop=True)


def build_daily_targets(
    underlying_prices: pd.DataFrame,
    underlying: str,
    option_type: str,
    target_dte: int,
    min_dte: int,
    max_dte: int,
    strike_moneyness: float,
    strike_increment: float,
    expiration_cycle: str,
    fixed_expiration: str | None = None,
) -> pd.DataFrame:
    start = pd.to_datetime(underlying_prices["date"].min()).date().isoformat()
    end = pd.to_datetime(underlying_prices["date"].max()).date().isoformat()
    if fixed_expiration:
        expirations = [pd.Timestamp(fixed_expiration).normalize()]
    elif expiration_cycle == "annual_january":
        expirations = _annual_january_expirations(start, end, min_dte=min_dte, max_dte=max_dte)
    else:
        expirations = _standard_monthly_expirations(start, end, min_dte=min_dte, max_dte=max_dte)

    rows = []
    for row in underlying_prices.itertuples(index=False):
        trade_date = pd.Timestamp(row.date).normalize()
        valid_expirations = [
            exp for exp in expirations
            if min_dte <= (exp - trade_date).days <= max_dte
        ]
        if not valid_expirations:
            continue

        expiration = min(valid_expirations, key=lambda exp: abs((exp - trade_date).days - target_dte))
        strike = _round_strike(float(row.close) * strike_moneyness, strike_increment)
        raw_symbol = _occ_raw_symbol(underlying, expiration, option_type, strike)
        dte = int((expiration - trade_date).days)
        rows.append({
            "date": trade_date.date().isoformat(),
            "underlying": underlying,
            "raw_symbol": raw_symbol,
            "option_type": option_type,
            "strike": strike,
            "expiration": expiration.date().isoformat(),
            "dte": dte,
            "underlying_close": float(row.close),
            "target_moneyness": float(strike_moneyness),
        })

    return pd.DataFrame(rows)


def build_contracts_from_targets(targets: pd.DataFrame) -> pd.DataFrame:
    contracts = (
        targets.groupby("raw_symbol", as_index=False)
        .agg({
            "underlying": "last",
            "option_type": "last",
            "strike": "last",
            "expiration": "last",
            "date": ["min", "max"],
        })
    )
    contracts.columns = [
        "raw_symbol",
        "underlying",
        "option_type",
        "strike",
        "expiration",
        "first_seen",
        "last_seen",
    ]
    contracts["instrument_id"] = pd.NA
    contracts["contract_multiplier"] = 100.0
    return contracts[[
        "raw_symbol",
        "underlying",
        "instrument_id",
        "option_type",
        "strike",
        "expiration",
        "first_seen",
        "last_seen",
        "contract_multiplier",
    ]]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch daily ATM 1-year SPY LEAPS call OHLCV from Databento OPRA."
    )
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument(
        "--end",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Exclusive end date for source requests",
    )
    parser.add_argument("--option-type", default="C", choices=["C", "P"])
    parser.add_argument("--target-dte", type=int, default=365)
    parser.add_argument("--min-dte", type=int, default=180)
    parser.add_argument("--max-dte", type=int, default=730)
    parser.add_argument("--strike-moneyness", type=float, default=1.0)
    parser.add_argument("--strike-increment", type=float, default=5.0)
    parser.add_argument(
        "--fixed-expiration",
        help="Use one fixed option expiration, e.g. 2028-01-21",
    )
    parser.add_argument(
        "--expiration-cycle",
        default="annual_january",
        choices=["annual_january", "standard_monthly"],
    )
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--limit-contracts", type=int, help="Limit unique contracts for test downloads")
    parser.add_argument("--skip-cost", action="store_true")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    underlying = args.underlying.upper()
    expiry_label = args.fixed_expiration or args.expiration_cycle
    strategy_id = (
        f"{underlying}_{args.option_type}_"
        f"{args.target_dte}dte_{args.strike_moneyness:g}mny_{expiry_label}"
    )
    strategy_id = strategy_id.replace("-", "")

    if args.chunk_size < 1 or args.chunk_size > 2000:
        print("--chunk-size must be between 1 and 2000")
        return 2
    if args.min_dte > args.target_dte or args.max_dte < args.target_dte:
        print("--target-dte must be between --min-dte and --max-dte")
        return 2

    init_options_db()

    print(f"Loading {underlying} daily prices ({args.start} -> {args.end})...")
    underlying_prices = _load_underlying_prices(underlying, args.start, args.end)
    targets = build_daily_targets(
        underlying_prices=underlying_prices,
        underlying=underlying,
        option_type=args.option_type,
        target_dte=args.target_dte,
        min_dte=args.min_dte,
        max_dte=args.max_dte,
        strike_moneyness=args.strike_moneyness,
        strike_increment=args.strike_increment,
        expiration_cycle=args.expiration_cycle,
        fixed_expiration=args.fixed_expiration,
    )
    if targets.empty:
        print("No daily target contracts could be built.")
        return 1

    contracts = build_contracts_from_targets(targets)
    if args.limit_contracts:
        limited_symbols = contracts["raw_symbol"].head(args.limit_contracts).tolist()
        targets = targets[targets["raw_symbol"].isin(limited_symbols)].copy()
        contracts = contracts[contracts["raw_symbol"].isin(limited_symbols)].copy()

    upsert_option_contracts(contracts, source=dbo.SOURCE)
    replace_option_daily_targets(targets, strategy_id=strategy_id, source=dbo.SOURCE)

    print(
        f"Built {len(targets):,} daily target row(s) using "
        f"{len(contracts):,} unique contract(s)."
    )
    print(
        f"Expiration range: {targets['expiration'].min()} -> {targets['expiration'].max()}; "
        f"strike range: {targets['strike'].min():.0f} -> {targets['strike'].max():.0f}."
    )
    print(f"Strategy id: {strategy_id}")

    symbols = contracts["raw_symbol"].tolist()
    if not args.skip_cost:
        print("Estimating Databento cost for ohlcv-1d bars...")
        total_cost = 0.0
        for chunk in _chunks(symbols, args.chunk_size):
            total_cost += dbo.estimate_cost(
                symbols=chunk,
                schema="ohlcv-1d",
                start=args.start,
                end=args.end,
            )
        print(f"Estimated OHLCV download cost: ${total_cost:.4f}")

    if not args.execute:
        print("Dry run only. Re-run with --execute to download and store bars.")
        return 0

    total_rows = 0
    for idx, chunk in enumerate(_chunks(symbols, args.chunk_size), 1):
        print(f"Downloading chunk {idx} ({len(chunk):,} contract(s))...")
        bars = dbo.fetch_ohlcv(
            raw_symbols=chunk,
            bar_size="1d",
            start=args.start,
            end=args.end,
        )
        replace_option_prices(
            bars,
            raw_symbols=chunk,
            underlying=underlying,
            bar_size="1d",
            source=dbo.SOURCE,
        )
        total_rows += len(bars)
        print(f"  Stored {len(bars):,} consolidated bar(s).")
        time.sleep(0.25)

    print(f"Done. Stored {total_rows:,} daily option bars for {strategy_id}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
