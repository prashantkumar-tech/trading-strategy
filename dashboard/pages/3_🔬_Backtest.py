"""Backtest — run strategy rules against historical data and inspect results."""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar, get_rules
from data.database import load_prices
from backtest.simulator import run_backtest

st.set_page_config(page_title="Backtest", layout="wide", page_icon="🔬")
st.title("🔬 Backtest")

cfg = render_sidebar()
symbol          = cfg["symbol"]
bar_size        = cfg["bar_size"]
initial_capital = cfg["initial_capital"]
start_date      = cfg["start_date"]
end_date        = cfg["end_date"]

rules = get_rules(symbol)
if not rules:
    st.warning(f"No rules for **{symbol}**. Go to **Strategy Builder** and add some first.")
    st.stop()

df = load_prices(symbol, start=str(start_date), end=str(end_date), bar_size=bar_size)
if df.empty:
    st.info(f"No {bar_size} data for {symbol}. Use the sidebar to fetch it.")
    st.stop()

if st.button("▶ Run Backtest", type="primary", use_container_width=False):
    with st.spinner("Running backtest…"):
        result = run_backtest(df, rules, initial_capital=initial_capital)
    st.session_state[f"bt_result_{symbol}"] = result

result = st.session_state.get(f"bt_result_{symbol}")
if result is None:
    st.info("Press **▶ Run Backtest** to see results.")
    st.stop()

m  = result["metrics"]
ec = result["equity_curve"]

# ── Metrics ───────────────────────────────────────────────────────────────────
st.subheader("Performance Summary")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Return",  f"{m['total_return_pct']:.1f}%")
c2.metric("Ann. Return",   f"{m['annualized_return_pct']:.1f}%")
c3.metric("Sharpe",        f"{m['sharpe_ratio']:.2f}")
c4.metric("Max Drawdown",  f"{m['max_drawdown_pct']:.1f}%")
c5.metric("Win Rate",      f"{m['win_rate_pct']:.1f}%  ({m['num_trades']} trades)")

col_a, col_b, col_c = st.columns(3)
col_a.metric("Final Value",  f"${result['final_value']:,.2f}")
col_b.metric("Avg Win",      f"${m['avg_win_usd']:,.2f}")
col_c.metric("Avg Loss",     f"${m['avg_loss_usd']:,.2f}")

# ── Price chart with signals ──────────────────────────────────────────────────
st.subheader(f"{symbol} — Price & Signals")
sig_df = result["signals_df"]
buys   = sig_df[sig_df["signal"].str.contains("BUY",  na=False)]
sells  = sig_df[sig_df["signal"].str.contains("SELL", na=False)]

price_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25],
                          subplot_titles=(f"{symbol} Price", "Volume"))

price_fig.add_trace(go.Scatter(
    x=df["date"], y=df["close"],
    line=dict(color="#90caf9", width=1.2), name="Close",
), row=1, col=1)

if df["ma50"].notna().any():
    price_fig.add_trace(go.Scatter(x=df["date"], y=df["ma50"],
                                   line=dict(color="#f39c12", width=1), name="MA50"), row=1, col=1)
if df["ma200"].notna().any():
    price_fig.add_trace(go.Scatter(x=df["date"], y=df["ma200"],
                                   line=dict(color="#3498db", width=1), name="MA200"), row=1, col=1)

if not buys.empty:
    buy_prices = df[df["date"].isin(buys["date"])]["close"]
    buy_dates  = df[df["date"].isin(buys["date"])]["date"]
    price_fig.add_trace(go.Scatter(
        x=buy_dates, y=buy_prices, mode="markers",
        marker=dict(symbol="triangle-up", color="#26a69a", size=9),
        name="Buy",
    ), row=1, col=1)

if not sells.empty:
    sell_prices = df[df["date"].isin(sells["date"])]["close"]
    sell_dates  = df[df["date"].isin(sells["date"])]["date"]
    price_fig.add_trace(go.Scatter(
        x=sell_dates, y=sell_prices, mode="markers",
        marker=dict(symbol="triangle-down", color="#ef5350", size=9),
        name="Sell",
    ), row=1, col=1)

price_fig.add_trace(go.Bar(x=df["date"], y=df["volume"],
                           marker_color="#546e7a", showlegend=False), row=2, col=1)
price_fig.update_layout(height=480, template="plotly_dark",
                        xaxis_rangeslider_visible=False, margin=dict(t=40, b=20))
st.plotly_chart(price_fig, use_container_width=True)

# ── Equity curve ──────────────────────────────────────────────────────────────
st.subheader("Equity Curve")
eq_fig = go.Figure()
eq_fig.add_trace(go.Scatter(
    x=ec.index, y=ec.values,
    line=dict(color="#90caf9", width=1.5), name="Portfolio",
    fill="tozeroy", fillcolor="rgba(144,202,249,0.08)",
))
eq_fig.add_hline(y=initial_capital, line_dash="dash", line_color="#546e7a",
                 annotation_text="Starting capital", annotation_position="bottom right")
eq_fig.update_layout(height=320, template="plotly_dark",
                     margin=dict(t=20, b=20),
                     yaxis_title="Portfolio Value ($)")
st.plotly_chart(eq_fig, use_container_width=True)

# ── Trade history ─────────────────────────────────────────────────────────────
st.subheader("Trade History")
trades = result["trades"]
if trades:
    tdf = pd.DataFrame(trades)
    tdf["pnl"]  = tdf["pnl"].map(lambda x: f"${x:+,.2f}")
    tdf["return_pct"] = tdf["return_pct"].map(lambda x: f"{x:+.2f}%")
    tdf["entry_price"] = tdf["entry_price"].map(lambda x: f"${x:,.4f}")
    tdf["exit_price"]  = tdf["exit_price"].map(lambda x: f"${x:,.4f}")
    tdf["shares"]      = tdf["shares"].map(lambda x: f"{x:,.4f}")
    if "unallocated_capital" in tdf.columns:
        tdf["unallocated_capital"] = tdf["unallocated_capital"].map(lambda x: f"${x:,.2f}")
    st.dataframe(tdf, use_container_width=True, hide_index=True)
else:
    st.info("No trades were executed.")
