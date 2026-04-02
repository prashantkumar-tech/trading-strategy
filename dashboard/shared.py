"""Shared sidebar and session-state helpers used by every page."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
from data.database import list_symbols, list_bar_sizes, init_db
from data.fetcher import fetch_and_store


def init_state():
    defaults = {
        "rules_by_symbol": {},
        "symbol":          "SPY",
        "bar_size":        "1d",
        "source":          "yfinance",
        "initial_capital": 10_000,
        "default_pos_pct": 10,
        "start_date":      pd.Timestamp("2010-01-01").date(),
        "end_date":        pd.Timestamp.today().date(),
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def render_sidebar():
    init_state()
    init_db()

    with st.sidebar:
        st.header("Symbol")
        available = list_symbols()

        symbol = st.selectbox(
            "Active symbol",
            options=available if available else ["SPY"],
            index=available.index(st.session_state.symbol)
                  if st.session_state.symbol in available else 0,
            key="symbol",
        )

        bar_size_options = ["1d", "5m", "15m", "1h"]
        bar_size = st.selectbox(
            "Bar size",
            options=bar_size_options,
            index=bar_size_options.index(st.session_state.bar_size)
                  if st.session_state.bar_size in bar_size_options else 0,
            key="bar_size",
            help="Choose the active timeframe for reads and fetches.",
        )

        source_options = ["yfinance", "polygon", "twelve_data"] if bar_size == "1d" else ["polygon", "twelve_data"]
        default_source = st.session_state.source if st.session_state.source in source_options else source_options[0]
        source = st.selectbox(
            "Active source",
            options=source_options,
            index=source_options.index(default_source),
            key="source",
        )

        stored_bar_sizes = list_bar_sizes(symbol, source=source) if available else ["1d"]
        if bar_size not in stored_bar_sizes:
            st.caption(f"No {bar_size} data for {symbol} from {source} yet — fetch below.")

        new_sym = st.text_input("Add / refresh symbol", placeholder="e.g. TQQQ, QQQ")
        fetch_source = st.selectbox(
            "Source",
            [
                "yfinance",
                "polygon",
                "twelve_data",
            ],
        )
        allowed_bar_sizes = ["1d"] if fetch_source == "yfinance" else ["1d", "5m", "15m", "1h"]
        if bar_size not in allowed_bar_sizes:
            st.caption(f"`{fetch_source}` does not support the current `{bar_size}` selection.")

        if st.button("Fetch / Refresh", use_container_width=True):
            target = new_sym.upper().strip() if new_sym.strip() else symbol
            if bar_size not in allowed_bar_sizes:
                st.error(f"{fetch_source} does not support {bar_size}.")
            else:
                with st.spinner(f"Downloading {target} ({bar_size})..."):
                    try:
                        fetch_and_store(target, bar_size=bar_size, source=fetch_source)
                        st.success(f"{target} {bar_size} updated from {fetch_source}.")
                    except Exception as e:
                        st.error(str(e))
            st.rerun()

        if available:
            st.caption(f"Stored: {', '.join(available)}")

        st.divider()
        st.header("Portfolio")
        initial_capital = st.number_input(
            "Starting Capital ($)", value=st.session_state.initial_capital, step=500,
            key="initial_capital",
        )
        default_pos_pct = st.slider(
            "Default position size (%)", 1, 50,
            value=st.session_state.default_pos_pct,
            key="default_pos_pct",
            help="% of portfolio deployed per entry signal",
        )
        st.caption(
            f"At **${initial_capital:,.0f}** → **${initial_capital * default_pos_pct / 100:,.0f}** per trade\n\n"
            f"At **$15,000** → **${15_000 * default_pos_pct / 100:,.0f}** per trade"
        )

        st.divider()
        st.header("Date Range")
        start_date = st.date_input(
            "From", value=st.session_state.start_date, key="start_date",
        )
        end_date = st.date_input(
            "To", value=st.session_state.end_date, key="end_date",
        )

    return dict(
        symbol=symbol,
        bar_size=bar_size,
        source=source,
        initial_capital=initial_capital,
        default_pos_pct=default_pos_pct,
        start_date=start_date,
        end_date=end_date,
    )


def get_rules(sym: str) -> list:
    init_state()
    return st.session_state.rules_by_symbol.get(sym, [])


def set_rules(sym: str, rules: list):
    init_state()
    st.session_state.rules_by_symbol[sym] = rules
