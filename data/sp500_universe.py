"""Fetch and normalize the current S&P 500 constituent list.

Source: Wikipedia 'List of S&P 500 companies'. Uses today's membership
(survivorship caveat accepted for v1). Cached to a local text file so the
network fetch happens at most once unless refreshed.
"""

from pathlib import Path
from typing import List
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
CACHE_PATH = Path(__file__).parent / "sp500_tickers.txt"


def normalize_ticker(ticker: str) -> str:
    """Uppercase, strip, and convert Wikipedia dots to yfinance dashes (BRK.B -> BRK-B)."""
    return ticker.strip().upper().replace(".", "-")


def parse_sp500_table(df: pd.DataFrame) -> List[str]:
    """Extract normalized tickers from a Wikipedia constituents DataFrame."""
    return [normalize_ticker(str(s)) for s in df["Symbol"].tolist()]


def get_sp500_tickers(use_cache: bool = True) -> List[str]:
    """Return the S&P 500 tickers, reading a local cache when present."""
    if use_cache and CACHE_PATH.exists():
        return [line.strip() for line in CACHE_PATH.read_text().splitlines() if line.strip()]

    tables = pd.read_html(WIKI_URL)
    tickers = parse_sp500_table(tables[0])
    CACHE_PATH.write_text("\n".join(tickers) + "\n")
    return tickers
