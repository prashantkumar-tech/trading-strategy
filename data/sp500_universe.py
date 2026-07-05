"""Fetch and normalize the current S&P 500 constituent list.

Source: Wikipedia 'List of S&P 500 companies'. Uses today's membership
(survivorship caveat accepted for v1). Cached to a local text file so the
network fetch happens at most once unless refreshed.
"""

import io
from pathlib import Path
from typing import Dict, List
from urllib.request import Request, urlopen
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
CACHE_PATH = Path(__file__).parent / "sp500_tickers.txt"
NAMES_CACHE_PATH = Path(__file__).parent / "sp500_names.tsv"

# Wikipedia 403s the default urllib user-agent, so identify as a browser.
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def normalize_ticker(ticker: str) -> str:
    """Uppercase, strip, and convert Wikipedia dots to yfinance dashes (BRK.B -> BRK-B)."""
    return ticker.strip().upper().replace(".", "-")


def parse_sp500_table(df: pd.DataFrame) -> List[str]:
    """Extract normalized tickers from a Wikipedia constituents DataFrame."""
    return [normalize_ticker(str(s)) for s in df["Symbol"].tolist()]


def parse_sp500_names(df: pd.DataFrame) -> Dict[str, str]:
    """Map normalized ticker -> company name from a Wikipedia constituents DataFrame."""
    return {normalize_ticker(str(s)): str(n) for s, n in zip(df["Symbol"], df["Security"])}


def _fetch_wiki_tables() -> List[pd.DataFrame]:
    """Download the Wikipedia constituents page with a browser UA and parse its tables."""
    req = Request(WIKI_URL, headers={"User-Agent": _USER_AGENT})
    with urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8")
    return pd.read_html(io.StringIO(html))


def get_sp500_tickers(use_cache: bool = True) -> List[str]:
    """Return the S&P 500 tickers, reading a local cache when present.

    Raises RuntimeError with a clear message if the Wikipedia list cannot be
    fetched or parsed (e.g. network error, blocked request, page layout change).
    """
    if use_cache and CACHE_PATH.exists():
        return [normalize_ticker(line) for line in CACHE_PATH.read_text().splitlines() if line.strip()]

    try:
        tables = _fetch_wiki_tables()
        tickers = parse_sp500_table(tables[0])
    except Exception as exc:
        raise RuntimeError(
            f"Could not fetch the S&P 500 constituent list from Wikipedia ({exc})."
        ) from exc

    CACHE_PATH.write_text("\n".join(tickers) + "\n")
    return tickers


def get_sp500_names(use_cache: bool = True) -> Dict[str, str]:
    """Return a {ticker: company name} mapping from Wikipedia, cached to a TSV file.

    Raises RuntimeError with a clear message if the list cannot be fetched or parsed.
    """
    if use_cache and NAMES_CACHE_PATH.exists():
        names = {}
        for line in NAMES_CACHE_PATH.read_text().splitlines():
            if "\t" in line:
                sym, name = line.split("\t", 1)
                names[normalize_ticker(sym)] = name
        return names

    try:
        tables = _fetch_wiki_tables()
        names = parse_sp500_names(tables[0])
    except Exception as exc:
        raise RuntimeError(
            f"Could not fetch the S&P 500 company names from Wikipedia ({exc})."
        ) from exc

    NAMES_CACHE_PATH.write_text("\n".join(f"{s}\t{n}" for s, n in names.items()) + "\n")
    return names
