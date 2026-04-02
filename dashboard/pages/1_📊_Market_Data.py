"""Market Data — index performance charts and raw bar explorer."""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar
from data.database import load_prices, list_symbols, list_bar_sizes

st.set_page_config(page_title="Market Data", layout="wide", page_icon="📊")
st.title("📊 Market Data")

cfg = render_sidebar()
symbol        = cfg["symbol"]
bar_size      = cfg["bar_size"]
source        = cfg["source"]
start_date    = cfg["start_date"]
end_date      = cfg["end_date"]

# ── Index Performance ─────────────────────────────────────────────────────────
st.header(f"{symbol} — {bar_size} Performance ({source})")

df = load_prices(symbol, start=str(start_date), end=str(end_date), bar_size=bar_size, source=source)

if df.empty:
    st.info(f"No {bar_size} data for {symbol} from {source}. Use the sidebar to fetch it.")
    st.stop()

first, last = df["close"].iloc[0], df["close"].iloc[-1]
n_years     = len(df) / (252 if bar_size == "1d" else 252 * 78)
total_ret   = (last / first - 1) * 100
ann_ret     = ((1 + total_ret / 100) ** (1 / n_years) - 1) * 100 if n_years > 0 else 0
daily_ret   = df["close"].pct_change().dropna()
sharpe      = (daily_ret.mean() / daily_ret.std()) * np.sqrt(252) if daily_ret.std() > 0 else 0
roll_max    = df["close"].cummax()
max_dd      = ((df["close"] - roll_max) / roll_max).min() * 100

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Return",  f"{total_ret:.1f}%")
c2.metric("Ann. Return",   f"{ann_ret:.1f}%")
c3.metric("Sharpe Ratio",  f"{sharpe:.2f}")
c4.metric("Max Drawdown",  f"{max_dd:.1f}%")

fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25],
                    subplot_titles=(f"{symbol} Price", "Volume"))
fig.add_trace(go.Scatter(
    x=df["date"], y=df["close"],
    line=dict(color="#90caf9", width=1.5), name="Close",
    fill="tozeroy", fillcolor="rgba(144,202,249,0.08)",
), row=1, col=1)
if df["ma50"].notna().any():
    fig.add_trace(go.Scatter(x=df["date"], y=df["ma50"],
                             line=dict(color="#f39c12", width=1.5), name="MA50"), row=1, col=1)
if df["ma200"].notna().any():
    fig.add_trace(go.Scatter(x=df["date"], y=df["ma200"],
                             line=dict(color="#3498db", width=1.5), name="MA200"), row=1, col=1)
fig.add_trace(go.Bar(x=df["date"], y=df["volume"],
                     marker_color="#546e7a", showlegend=False), row=2, col=1)
fig.update_layout(height=500, template="plotly_dark",
                  xaxis_rangeslider_visible=False, margin=dict(t=40, b=20))
st.plotly_chart(fig, use_container_width=True)

# ── Data Explorer ─────────────────────────────────────────────────────────────
st.divider()
st.header("Data Explorer")

with st.expander("Browse raw bars", expanded=False):
    de_col1, de_col2, de_col3 = st.columns(3)
    with de_col1:
        de_source = st.selectbox("Source", ["yfinance", "polygon", "twelve_data"], key="de_source")
    with de_col2:
        de_sym = st.selectbox("Symbol", list_symbols(source=de_source), key="de_sym")
    with de_col3:
        de_bar = st.selectbox("Bar size", list_bar_sizes(de_sym, source=de_source) if de_sym else ["1d"], key="de_bar")

    de_col4, = st.columns(1)
    with de_col4:
        de_rows = st.selectbox("Rows to show", [100, 500, 1000, 5000, "All"], key="de_rows")

    de_start = st.date_input("From", value=pd.Timestamp("2024-01-01"), key="de_start")
    de_end   = st.date_input("To",   value=pd.Timestamp.today(),       key="de_end")

    if st.button("Load Data", key="de_load"):
        de_df = load_prices(de_sym, start=str(de_start), end=str(de_end), bar_size=de_bar, source=de_source)
        if de_df.empty:
            st.warning(f"No {de_bar} data for {de_sym} from {de_source} in that range.")
        else:
            st.caption(f"{len(de_df):,} bars — {str(de_df['date'].iloc[0])[:16]} → {str(de_df['date'].iloc[-1])[:16]}")

            de_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25],
                                    subplot_titles=(f"{de_sym} ({de_bar})", "Volume"))
            de_fig.add_trace(go.Candlestick(
                x=de_df["date"], open=de_df["open"], high=de_df["high"],
                low=de_df["low"], close=de_df["close"], name="Price",
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
            ), row=1, col=1)
            if de_df["ma50"].notna().any():
                de_fig.add_trace(go.Scatter(x=de_df["date"], y=de_df["ma50"],
                                             line=dict(color="#f39c12", width=1), name="MA50"), row=1, col=1)
            if de_df["ma200"].notna().any():
                de_fig.add_trace(go.Scatter(x=de_df["date"], y=de_df["ma200"],
                                             line=dict(color="#3498db", width=1), name="MA200"), row=1, col=1)
            de_fig.add_trace(go.Bar(x=de_df["date"], y=de_df["volume"],
                                     marker_color="#546e7a", showlegend=False), row=2, col=1)
            de_fig.update_layout(height=500, template="plotly_dark",
                                  xaxis_rangeslider_visible=False, margin=dict(t=30, b=20))
            st.plotly_chart(de_fig, use_container_width=True)

            show_n = len(de_df) if de_rows == "All" else int(de_rows)
            disp = de_df.head(show_n).copy()
            disp["date"]   = disp["date"].astype(str).str[:19]
            disp["volume"] = disp["volume"].map(lambda x: f"{int(x):,}")
            for c in ["open", "high", "low", "close"]:
                disp[c] = disp[c].map(lambda x: f"{x:.2f}")
            st.dataframe(disp[["date","open","high","low","close","volume","ma50","ma200"]],
                         use_container_width=True, hide_index=True)
