"""Optimizer — parameter sweep for the MA50 Momentum strategy."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar
from data.database import load_prices, list_symbols, get_date_range
from backtest.optimizer import run_optimization

st.set_page_config(page_title="Optimizer", layout="wide", page_icon="🎯")
st.title("🎯 Optimizer")

cfg = render_sidebar()
initial_capital = cfg["initial_capital"]

bar_size = st.selectbox(
    "Bar size for optimization",
    options=["5m", "15m", "1h", "1d"],
    index=0,
    help="5m uses Polygon intraday data (~5 years). 1d uses Yahoo Finance daily data (~20 years).",
    key="opt_bar_size",
)

st.caption("Sweeps all combinations of position size, profit target, and time stop. "
           "Optionally add intraday gap-up and premarket rules (requires 5m data).")

# ── Parameter controls ────────────────────────────────────────────────────────
st.subheader("Parameters")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**Position size above MA50 (%)**")
    pos_above_min = st.number_input("Min", 5,  30,  8,  step=1, key="pa_min")
    pos_above_max = st.number_input("Max", 5,  50, 20,  step=1, key="pa_max")
    pos_above_step = st.number_input("Step", 1, 10,  2,  step=1, key="pa_step")

with col2:
    st.markdown("**Profit target (%)**")
    pt_min  = st.number_input("Min",  0.5, 10.0, 1.5, step=0.5, key="pt_min")
    pt_max  = st.number_input("Max",  0.5, 20.0, 4.0, step=0.5, key="pt_max")
    pt_step = st.number_input("Step", 0.5,  5.0, 0.5, step=0.5, key="pt_step")

with col3:
    st.markdown("**Time stop (days)**")
    ts_min  = st.number_input("Min",  1, 20, 2, step=1, key="ts_min")
    ts_max  = st.number_input("Max",  1, 60, 6, step=1, key="ts_max")
    ts_step = st.number_input("Step", 1, 10, 1, step=1, key="ts_step")

# ── Intraday rules (5m data only) ────────────────────────────────────────────
st.subheader("Intraday Entry Rules")
st.caption("These rules require 5-minute bar data with corrected Eastern Time timestamps. "
           "Re-fetch symbols after updating to the latest version if timestamps look off.")
id_col1, id_col2 = st.columns(2)

with id_col1:
    gap_up_rule = st.checkbox(
        "Gap-up rule: if positive gap vs prev day close, buy at 9:45 AM",
        value=False, key="gap_up_rule",
    )
    gap_up_pct = st.slider("Gap-up position size (%)", 1, 30, 10, key="gap_up_pct",
                            disabled=not gap_up_rule)

with id_col2:
    premarket_rule = st.checkbox(
        "Premarket rule: if price < prev day close during premarket, buy",
        value=False, key="premarket_rule",
    )
    premarket_pct = st.slider("Premarket position size (%)", 1, 30, 5, key="premarket_pct",
                               disabled=not premarket_rule)

# ── Symbol multiselect ────────────────────────────────────────────────────────
available = list_symbols()
selected_symbols = st.multiselect(
    "Symbols to optimize",
    options=available if available else ["SPY"],
    default=available[:1] if available else ["SPY"],
)

# ── Date range: auto-detected from DB for each symbol ────────────────────────
import numpy as np

if selected_symbols:
    starts, ends = [], []
    for sym in selected_symbols:
        s, e = get_date_range(sym, bar_size)
        if s:
            starts.append(s)
            ends.append(e)
    opt_start = min(starts) if starts else None
    opt_end   = max(ends)   if ends   else None
    if opt_start:
        st.info(f"Using data range **{opt_start}** → **{opt_end}** ({bar_size}) from the database.")
    else:
        st.warning(f"No {bar_size} data found for selected symbols. Fetch data first.")
        opt_start = opt_end = None
else:
    opt_start = opt_end = None

# Estimate combo count
pos_vals = list(np.arange(pos_above_min, pos_above_max + pos_above_step, pos_above_step) / 100)
pt_vals  = list(np.arange(pt_min, pt_max + pt_step, pt_step))
ts_vals  = list(range(int(ts_min), int(ts_max) + 1, int(ts_step)))
n_combos = len(pos_vals) * len(pt_vals) * len(ts_vals)
st.caption(f"**{n_combos} combinations × {len(selected_symbols)} symbol(s) = {n_combos * len(selected_symbols)} backtests**")

if st.button("▶ Run Optimization", type="primary"):
    if not selected_symbols:
        st.error("Select at least one symbol.")
    elif not opt_start:
        st.error(f"No {bar_size} data in DB. Fetch symbols first.")
    else:
        all_results = {}
        progress    = st.progress(0)
        total       = len(selected_symbols)

        for idx, sym in enumerate(selected_symbols):
            df = load_prices(sym, start=opt_start, end=opt_end, bar_size=bar_size)
            if df.empty:
                st.warning(f"No {bar_size} data for {sym} — skipping.")
                continue
            with st.spinner(f"Optimizing {sym} ({n_combos} combos)…"):
                res = run_optimization(
                    df, pos_vals, pt_vals, ts_vals, initial_capital,
                    gap_up_rule=gap_up_rule,
                    premarket_rule=premarket_rule,
                    gap_up_pct=gap_up_pct / 100,
                    premarket_pct=premarket_pct / 100,
                )
            all_results[sym] = res
            progress.progress((idx + 1) / total)

        progress.empty()
        st.session_state["opt_results"] = all_results
        st.success("Optimization complete.")

# ── Results ───────────────────────────────────────────────────────────────────
opt_results = st.session_state.get("opt_results")
if not opt_results:
    st.info("Press **▶ Run Optimization** to see results.")
    st.stop()

syms = list(opt_results.keys())
if len(syms) == 1:
    sym = syms[0]
    _df = opt_results[sym]

    tab_r, tab_s = st.tabs(["Results Table", "Scatter Plots"])

    with tab_r:
        st.subheader(f"Top Results — {sym}")
        st.dataframe(_df, use_container_width=True)

    with tab_s:
        sc1, sc2 = st.columns(2)
        with sc1:
            fig = px.scatter(_df, x="profit_target_%", y="sharpe",
                             color="above_MA50_%", size="num_trades",
                             title="Sharpe vs Profit Target",
                             template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        with sc2:
            fig2 = px.scatter(_df, x="time_stop_days", y="total_return_%",
                              color="above_MA50_%", size="num_trades",
                              title="Total Return vs Time Stop",
                              template="plotly_dark")
            st.plotly_chart(fig2, use_container_width=True)

else:
    # Multi-symbol: per-symbol tabs + head-to-head
    sym_tabs = st.tabs(syms + ["Head-to-Head"])

    for i, sym in enumerate(syms):
        with sym_tabs[i]:
            _df = opt_results[sym]
            st.subheader(f"Top Results — {sym}")
            st.dataframe(_df, use_container_width=True)

            sc1, sc2 = st.columns(2)
            with sc1:
                fig = px.scatter(_df, x="profit_target_%", y="sharpe",
                                 color="above_MA50_%", size="num_trades",
                                 title="Sharpe vs Profit Target",
                                 template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)
            with sc2:
                fig2 = px.scatter(_df, x="time_stop_days", y="total_return_%",
                                  color="above_MA50_%", size="num_trades",
                                  title="Total Return vs Time Stop",
                                  template="plotly_dark")
                st.plotly_chart(fig2, use_container_width=True)

    with sym_tabs[-1]:
        st.subheader("Best Result Per Symbol")
        rows = []
        for sym, _df in opt_results.items():
            best = _df.iloc[0].to_dict()
            best["symbol"] = sym
            rows.append(best)
        hth = pd.DataFrame(rows).set_index("symbol")
        st.dataframe(hth, use_container_width=True)

        fig_hth = go.Figure()
        metric_col = st.selectbox("Compare metric", ["sharpe", "total_return_%",
                                                      "ann_return_%", "max_drawdown_%",
                                                      "win_rate_%"], key="hth_metric")
        fig_hth.add_trace(go.Bar(x=list(opt_results.keys()),
                                  y=[opt_results[s].iloc[0][metric_col] for s in opt_results],
                                  marker_color="#90caf9"))
        fig_hth.update_layout(template="plotly_dark", yaxis_title=metric_col,
                               margin=dict(t=20, b=20), height=320)
        st.plotly_chart(fig_hth, use_container_width=True)
