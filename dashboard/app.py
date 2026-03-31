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

c1, c2, c3, c4 = st.columns(4)
c1.info("**📊 Market Data**\nPrice charts, MA overlays, and raw bar browser for any symbol and bar size.")
c2.info("**📋 Strategy Builder**\nDefine entry and exit rules with a UI. Rules are saved per symbol.")
c3.info("**🔬 Backtest**\nRun your strategy against historical data. See equity curve, signals, and trade history.")
c4.info("**🎯 Optimizer**\nSweep parameter combinations across multiple symbols to find the best settings.")
