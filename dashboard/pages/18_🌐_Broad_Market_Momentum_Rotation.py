"""Broad Market Momentum Rotation — Streamlit page.

Universe: all NYSE + NASDAQ common stocks with current market cap >= $1B
(includes the entire S&P 500 plus a much wider mid/smaller-cap field).
Benchmark is SPY so results line up against the S&P 500 page.
"""

from data.broad_universe import get_broad_universe, get_broad_meta
from dashboard.momentum_page import render_momentum_page

render_momentum_page(
    page_title="Broad Market Momentum Rotation",
    page_icon="🌐",
    universe_label="broad-market (≥$1B)",
    get_tickers=get_broad_universe,
    get_meta=get_broad_meta,
    cache_prefix="broad",
    fetch_hint="python3 -m data.fetch_broad_universe",
)
