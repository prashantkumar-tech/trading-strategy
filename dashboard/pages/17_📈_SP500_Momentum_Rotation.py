"""S&P 500 Momentum Rotation — Streamlit page."""

from data.sp500_universe import get_sp500_tickers, get_sp500_meta
from dashboard.momentum_page import render_momentum_page

render_momentum_page(
    page_title="S&P 500 Momentum Rotation",
    page_icon="📈",
    universe_label="S&P 500",
    get_tickers=get_sp500_tickers,
    get_meta=get_sp500_meta,
    cache_prefix="sp500",
    fetch_hint="python3 -m data.fetch_universe",
)
