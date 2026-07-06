"""Broad US-listed common-stock universe.

Source: the official NASDAQ Trader symbol directories (NASDAQ + NYSE/other),
which are free and reliable. We filter to operating-company common stock
(dropping ETFs, test issues, preferred/warrants/units), then optionally to a
market-cap floor fetched from yfinance. This yields a broad universe that
includes the entire S&P 500 plus mid/smaller names.
"""

import json
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.request import Request, urlopen

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
# Bulk market-cap source: one call returns market cap for every listed stock.
SCREENER_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&offset=0"
TICKERS_CACHE = Path(__file__).parent / "broad_tickers.tsv"       # symbol \t name
MARKETCAP_CACHE = Path(__file__).parent / "broad_marketcap.tsv"   # symbol \t market_cap

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Characters that mark non-common issues (preferred, warrants, rights, units, when-issued).
_SKIP_CHARS = set("$^ =")


def normalize_symbol(symbol: str):
    """Uppercase/strip a directory symbol for yfinance, or None to drop it.

    Dots become dashes (BRK.B -> BRK-B); symbols carrying preferred/warrant/unit
    markers are dropped.
    """
    s = symbol.strip().upper()
    if not s or any(c in _SKIP_CHARS for c in s):
        return None
    return s.replace(".", "-")


def parse_symbol_directory(text: str, symbol_col: str) -> List[Tuple[str, str]]:
    """Parse a NASDAQ Trader directory file into [(symbol, security_name)].

    Keeps only common stock: ETF != 'Y', Test Issue != 'Y', and 'Common Stock'
    in the security name. ``symbol_col`` is 'Symbol' (nasdaqlisted) or
    'ACT Symbol' (otherlisted).
    """
    lines = text.splitlines()
    if not lines:
        return []
    header = lines[0].split("|")
    idx = {name: i for i, name in enumerate(header)}
    if symbol_col not in idx or "Security Name" not in idx:
        return []

    out = []
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) < len(header):
            continue
        if parts[idx.get("ETF", -1)] == "Y":
            continue
        if parts[idx.get("Test Issue", -1)] == "Y":
            continue
        name = parts[idx["Security Name"]]
        if "Common Stock" not in name:
            continue
        sym = normalize_symbol(parts[idx[symbol_col]])
        if sym:
            out.append((sym, name))
    return out


def _fetch(url: str) -> str:
    req = Request(url, headers={"User-Agent": _UA})
    with urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def get_broad_tickers(use_cache: bool = True) -> Dict[str, str]:
    """Return {ticker: security_name} for all US-listed common stock.

    Cached to a TSV. Raises RuntimeError if the directories can't be fetched.
    """
    if use_cache and TICKERS_CACHE.exists():
        out = {}
        for line in TICKERS_CACHE.read_text().splitlines():
            if "\t" in line:
                sym, name = line.split("\t", 1)
                out[sym] = name
        return out

    try:
        nasdaq = parse_symbol_directory(_fetch(NASDAQ_URL), "Symbol")
        other = parse_symbol_directory(_fetch(OTHER_URL), "ACT Symbol")
    except Exception as exc:
        raise RuntimeError(f"Could not fetch NASDAQ/NYSE symbol directories ({exc}).") from exc

    merged = {}
    for sym, name in nasdaq + other:
        merged.setdefault(sym, name)
    TICKERS_CACHE.write_text("\n".join(f"{s}\t{n}" for s, n in sorted(merged.items())) + "\n")
    return merged


def parse_screener_market_caps(payload: dict) -> Dict[str, float]:
    """Extract {normalized_symbol: market_cap} from a NASDAQ screener JSON payload."""
    rows = (payload.get("data") or {}).get("table", {}).get("rows") \
        or (payload.get("data") or {}).get("rows") or []
    caps = {}
    for r in rows:
        sym = normalize_symbol(str(r.get("symbol", "")))
        raw = str(r.get("marketCap", "")).replace(",", "").replace("$", "").strip()
        if not sym or not raw:
            continue
        try:
            mc = float(raw)
        except ValueError:
            continue
        if mc > 0:
            caps[sym] = mc
    return caps


def fetch_market_caps() -> Dict[str, float]:
    """Fetch market cap for every listed stock in one bulk NASDAQ screener call."""
    req = Request(SCREENER_URL, headers={"User-Agent": _UA, "Accept": "application/json"})
    with urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    return parse_screener_market_caps(payload)


def _read_market_cap_cache() -> Dict[str, float]:
    caps = {}
    if MARKETCAP_CACHE.exists():
        for line in MARKETCAP_CACHE.read_text().splitlines():
            if "\t" in line:
                sym, mc = line.split("\t", 1)
                try:
                    caps[sym] = float(mc)
                except ValueError:
                    pass
    return caps


def build_market_cap_cache() -> Dict[str, float]:
    """Fetch and cache market caps for all listed stocks (one bulk call).

    Run explicitly — the dashboard only ever reads the cache, never triggers this.
    """
    caps = fetch_market_caps()
    MARKETCAP_CACHE.write_text("\n".join(f"{s}\t{c}" for s, c in sorted(caps.items())) + "\n")
    return caps


def get_broad_universe(min_market_cap: float = 1e9) -> List[str]:
    """Return sorted common-stock tickers whose cached market cap >= ``min_market_cap``.

    Cache-only: returns [] if the market-cap cache hasn't been built yet
    (run build_market_cap_cache / `python3 -m data.fetch_broad_universe`).
    """
    caps = _read_market_cap_cache()
    commons = set(get_broad_tickers().keys())
    return sorted(s for s in commons if caps.get(s, 0.0) >= min_market_cap)


def get_broad_meta(use_cache: bool = True) -> Dict[str, dict]:
    """Return {ticker: {"name": .., "sector": ""}} for the broad universe.

    Sector is unavailable from the exchange directories, so it is left blank;
    the leaderboard shows names and simply omits the sector column.
    """
    return {sym: {"name": name, "sector": ""}
            for sym, name in get_broad_tickers(use_cache=use_cache).items()}
