"""Optimizer — parameter sweep for the MA50 Momentum strategy."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar
from data.database import load_prices, list_symbols
from backtest.optimizer import run_optimization

st.set_page_config(page_title="Optimizer", layout="wide", page_icon="🎯")
st.title("🎯 Optimizer")

cfg = render_sidebar()
bar_size        = cfg["bar_size"]
initial_capital = cfg["initial_capital"]
start_date      = cfg["start_date"]
end_date        = cfg["end_date"]

st.caption("Sweeps all combinations of position size, profit target, and time stop. "
           "Only the MA50 Momentum strategy is supported here.")

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

# ── Symbol multiselect ────────────────────────────────────────────────────────
available = list_symbols()
selected_symbols = st.multiselect(
    "Symbols to optimize",
    options=available if available else ["SPY"],
    default=available[:1] if available else ["SPY"],
)

# Estimate combo count
import numpy as np
pos_vals = list(np.arange(pos_above_min, pos_above_max + pos_above_step, pos_above_step) / 100)
pt_vals  = list(np.arange(pt_min, pt_max + pt_step, pt_step))
ts_vals  = list(range(int(ts_min), int(ts_max) + 1, int(ts_step)))
n_combos = len(pos_vals) * len(pt_vals) * len(ts_vals)
st.caption(f"**{n_combos} combinations × {len(selected_symbols)} symbol(s) = {n_combos * len(selected_symbols)} backtests**")

if st.button("▶ Run Optimization", type="primary"):
    if not selected_symbols:
        st.error("Select at least one symbol.")
    else:
        all_results = {}
        progress    = st.progress(0)
        total       = len(selected_symbols)

        for idx, sym in enumerate(selected_symbols):
            df = load_prices(sym, start=str(start_date), end=str(end_date), bar_size=bar_size)
            if df.empty:
                st.warning(f"No {bar_size} data for {sym} — skipping.")
                continue
            with st.spinner(f"Optimizing {sym} ({n_combos} combos)…"):
                res = run_optimization(df, pos_vals, pt_vals, ts_vals, initial_capital)
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
