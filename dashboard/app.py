"""Home page — overview and quick stats."""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.shared import render_sidebar
from data.database import load_dataset_inventory

st.set_page_config(page_title="Trading Strategy", layout="wide", page_icon="📈")
st.title("📈 Trading Strategy Backtester")
st.caption("Rule-based strategy builder, backtester, and optimizer for leveraged ETFs.")

render_sidebar()

st.divider()

# ── Data inventory ────────────────────────────────────────────────────────────
st.subheader("Data Inventory")

df_inv = load_dataset_inventory()

if df_inv.empty:
    st.info("No data yet. Go to **Market Data** and fetch a symbol.")
else:
    df_inv["bars"] = df_inv["bars"].map(lambda x: f"{x:,}")
    st.dataframe(df_inv, use_container_width=True, hide_index=True)

st.divider()

# ── Navigation guide ──────────────────────────────────────────────────────────
st.subheader("Pages")

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.info("**📊 Market Data**\nPrice charts, MA overlays, and raw bar browser for any symbol and bar size.")
c2.info("**📋 Strategy Builder**\nDefine entry and exit rules with a UI. Rules are saved per symbol.")
c3.info("**🔬 Backtest**\nRun your strategy against historical data. See equity curve, signals, and trade history.")
c4.info("**🎯 Optimizer**\nSweep parameter combinations across multiple symbols to find the best settings.")
c5.info("**📈 TQQQ Intraday Pyramid**\nRun the dedicated 5-minute breakout pyramid study with tier hit rates, exits, and campaign details.")
c6.info("**📈 MES Breakout**\nRun the dedicated MES prior-day-high breakout study on Databento historical futures data.")
c7.info("**📈 MES Intraday Pyramid**\nRun the MES same-day add-on contract study and adjust year, triggers, stops, and contract tiers.")
