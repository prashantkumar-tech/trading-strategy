"""MES prior-day-high breakout study page."""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backtest.mes_breakout import DEFAULT_PARAMS, run_backtest
from dashboard.shared import render_sidebar
from data.database import get_date_range, load_prices

st.set_page_config(page_title="MES Breakout", layout="wide", page_icon="📈")
st.title("📈 MES Breakout")
st.caption("Version 1: long only, 5-minute MES Databento data, 3 confirming closes above PDH, VWAP, and today’s open, next-bar-open entry.")

cfg = render_sidebar()
initial_capital = cfg["initial_capital"]

SYMBOL = "MES"
BAR_SIZE = "5m"
SOURCE = "databento"

st.info(f"Fixed settings for this study: **{SYMBOL}**, **{BAR_SIZE}**, **{SOURCE}**, one trade per day.")

data_start_str, data_end_str = get_date_range(SYMBOL, BAR_SIZE, SOURCE)
if not data_start_str or not data_end_str:
    st.warning("No MES 5-minute Databento data available yet. Use the sidebar to fetch MES from Databento first.")
    st.stop()

available_start = pd.Timestamp(data_start_str).date()
available_end = pd.Timestamp(data_end_str).date()

st.subheader("Simulation Range")
default_start = max(pd.Timestamp(available_end) - pd.Timedelta(days=90), pd.Timestamp(available_start))
default_start_date = default_start.date()

PRESET_DAYS = {
    "Quarter": 90,
    "Half Year": 182,
    "Year": 365,
    "Two Years": 730,
    "Custom": None,
}
available_years = list(range(available_end.year, available_start.year - 1, -1))

if "mes_breakout_preset" not in st.session_state:
    st.session_state["mes_breakout_preset"] = "Quarter"
if "mes_breakout_range" not in st.session_state:
    st.session_state["mes_breakout_range"] = (default_start_date, available_end)
if "mes_breakout_year" not in st.session_state:
    st.session_state["mes_breakout_year"] = "None"


def _apply_preset(preset_label: str) -> None:
    st.session_state["mes_breakout_preset"] = preset_label
    days = PRESET_DAYS[preset_label]
    if days is None:
        return
    start = max(pd.Timestamp(available_end) - pd.Timedelta(days=days), pd.Timestamp(available_start))
    st.session_state["mes_breakout_range"] = (start.date(), available_end)


preset_cols = st.columns(len(PRESET_DAYS))
for idx, preset_label in enumerate(PRESET_DAYS):
    with preset_cols[idx]:
        if st.button(
            preset_label,
            key=f"mes_preset_{preset_label}",
            use_container_width=True,
            type="primary" if st.session_state["mes_breakout_preset"] == preset_label else "secondary",
        ):
            _apply_preset(preset_label)

st.markdown("**Specific year**")
year_labels = ["None"] + [str(year) for year in available_years]
year_cols = st.columns(min(len(year_labels), 5))
for idx, year_label in enumerate(year_labels):
    with year_cols[idx % len(year_cols)]:
        if st.button(
            year_label,
            key=f"mes_year_{year_label}",
            use_container_width=True,
            type="primary" if st.session_state["mes_breakout_year"] == year_label else "secondary",
        ):
            st.session_state["mes_breakout_year"] = year_label
            if year_label != "None":
                selected_year = int(year_label)
                start = max(pd.Timestamp(f"{selected_year}-01-01"), pd.Timestamp(available_start))
                end = min(pd.Timestamp(f"{selected_year}-12-31"), pd.Timestamp(available_end))
                st.session_state["mes_breakout_range"] = (start.date(), end.date())
                st.session_state["mes_breakout_preset"] = "Custom"

range_value = st.date_input(
    "Date range",
    value=st.session_state["mes_breakout_range"],
    min_value=available_start,
    max_value=available_end,
    key="mes_breakout_range",
)
if isinstance(range_value, tuple) and len(range_value) == 2:
    start_date, end_date = range_value
else:
    start_date, end_date = default_start_date, available_end

if start_date > end_date:
    st.error("Simulation start date must be on or before the end date.")
    st.stop()

if st.session_state["mes_breakout_preset"] != "Custom":
    preset_days = PRESET_DAYS[st.session_state["mes_breakout_preset"]]
    expected_start = max(pd.Timestamp(available_end) - pd.Timedelta(days=preset_days), pd.Timestamp(available_start)).date()
    if (start_date, end_date) != (expected_start, available_end):
        st.session_state["mes_breakout_preset"] = "Custom"

st.caption(f"Running the simulation from **{start_date}** to **{end_date}**.")

pc1, pc2, pc3, pc4, pc5, pc6 = st.columns(6)
with pc1:
    contracts = st.number_input("Contracts", min_value=1, max_value=20, value=int(DEFAULT_PARAMS["contracts"]), step=1)
with pc2:
    first_entry_time = st.selectbox(
        "First entry after",
        options=["10:00", "10:05", "10:10", "10:15", "10:20", "10:25", "10:30"],
        index=["10:00", "10:05", "10:10", "10:15", "10:20", "10:25", "10:30"].index("10:15"),
    )
with pc3:
    last_entry_time = st.selectbox(
        "Last entry by",
        options=["11:00", "11:05", "11:10", "11:15", "11:20", "11:25", "11:30"],
        index=["11:00", "11:05", "11:10", "11:15", "11:20", "11:25", "11:30"].index("11:15"),
    )
with pc4:
    breakout_buffer_points = st.number_input("Breakout buffer (pts)", min_value=0.0, max_value=10.0, value=float(DEFAULT_PARAMS["breakout_buffer_points"]), step=0.25)
with pc5:
    stop_loss_points = st.number_input("Stop loss (pts)", min_value=1.0, max_value=50.0, value=float(DEFAULT_PARAMS["stop_loss_points"]), step=0.5)
with pc6:
    take_profit_points = st.number_input("Take profit (pts)", min_value=1.0, max_value=100.0, value=float(DEFAULT_PARAMS["take_profit_points"]), step=0.5)


def _time_label_to_minute(label: str) -> int:
    hour, minute = label.split(":")
    return int(hour) * 60 + int(minute)

params = {
    **DEFAULT_PARAMS,
    "contracts": contracts,
    "first_entry_minute": _time_label_to_minute(first_entry_time),
    "last_entry_minute": _time_label_to_minute(last_entry_time),
    "breakout_buffer_points": breakout_buffer_points,
    "stop_loss_points": stop_loss_points,
    "take_profit_points": take_profit_points,
}

df = load_prices(SYMBOL, start=str(start_date), end=str(end_date), bar_size=BAR_SIZE, source=SOURCE)
if df.empty:
    st.warning("No MES Databento data in the selected range.")
    st.stop()

result_key = f"mes_breakout_{start_date}_{end_date}"
if st.button("▶ Run MES Backtest", type="primary"):
    with st.spinner("Running MES breakout backtest…"):
        result = run_backtest(df, params=params, initial_capital=initial_capital)
    st.session_state[result_key] = result

result = st.session_state.get(result_key)
if result is None:
    st.info("Press **▶ Run MES Backtest** to see results.")
    st.stop()

m = result["metrics"]
summary = result["summary"]
price_df = result["price_df"]
signals_df = result["signals_df"]
equity_curve = result["equity_curve"]
trades = result["trades"]

st.subheader("Performance Summary")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Return", f"{m['total_return_pct']:.1f}%")
c2.metric("Ann. Return", f"{m['annualized_return_pct']:.1f}%")
c3.metric("Sharpe", f"{m['sharpe_ratio']:.2f}")
c4.metric("Max Drawdown", f"{m['max_drawdown_pct']:.1f}%")
c5.metric("Win Rate", f"{m['win_rate_pct']:.1f}%  ({m['num_trades']} trades)")

s1, s2, s3, s4 = st.columns(4)
s1.metric("Final Value", f"${result['final_value']:,.2f}")
s2.metric("Setup Days", f"{summary['setup_days']}")
s3.metric("Trade Days", f"{summary['trade_days']}")
s4.metric("Avg Hold (bars)", f"{summary['avg_hold_bars']:.1f}")

st.subheader("MES Price, VWAP, and Events")
price_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], subplot_titles=("Price / VWAP / PDH", "Volume"))
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["close"], line=dict(color="#90caf9", width=1.2), name="Close"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["vwap"], line=dict(color="#f39c12", width=1.0), name="VWAP"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["pdh"], line=dict(color="#ef5350", width=1.0, dash="dot"), name="PDH"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["session_open"], line=dict(color="#d4e157", width=1.0, dash="dash"), name="Open"), row=1, col=1)

if not signals_df.empty:
    buy_df = signals_df[signals_df["signal"] == "BUY"]
    sell_df = signals_df[signals_df["signal"] == "SELL"]
    if not buy_df.empty:
        price_fig.add_trace(go.Scatter(x=buy_df["date"], y=buy_df["price"], mode="markers", marker=dict(symbol="triangle-up", color="#26a69a", size=10), name="Buy", text=buy_df["detail"]), row=1, col=1)
    if not sell_df.empty:
        price_fig.add_trace(go.Scatter(x=sell_df["date"], y=sell_df["price"], mode="markers", marker=dict(symbol="triangle-down", color="#ef5350", size=9), name="Sell", text=sell_df["detail"]), row=1, col=1)

price_fig.add_trace(go.Bar(x=price_df["date"], y=price_df["volume"], marker_color="#546e7a", showlegend=False), row=2, col=1)
price_fig.update_layout(height=560, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(t=40, b=20))
st.plotly_chart(price_fig, use_container_width=True)

st.subheader("Equity Curve")
eq_fig = go.Figure()
eq_fig.add_trace(go.Scatter(x=equity_curve.index, y=equity_curve.values, line=dict(color="#90caf9", width=1.5), fill="tozeroy", fillcolor="rgba(144,202,249,0.08)", name="Portfolio"))
eq_fig.add_hline(y=initial_capital, line_dash="dash", line_color="#546e7a", annotation_text="Starting capital", annotation_position="bottom right")
eq_fig.update_layout(height=320, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="Portfolio Value ($)")
st.plotly_chart(eq_fig, use_container_width=True)

st.subheader("Trade History")
if trades:
    tdf = pd.DataFrame(trades).copy()
    tdf["pnl"] = tdf["pnl"].map(lambda x: f"${x:+,.2f}")
    tdf["return_pct"] = tdf["return_pct"].map(lambda x: f"{x:+.2f}%")
    tdf["entry_price"] = tdf["entry_price"].map(lambda x: f"{x:,.2f}")
    tdf["exit_price"] = tdf["exit_price"].map(lambda x: f"{x:,.2f}")
    st.dataframe(tdf, use_container_width=True, hide_index=True)
else:
    st.info("No trades were executed in the selected range.")

st.subheader("Exit Mix")
if summary["exit_reason_counts"]:
    exit_df = pd.DataFrame([{"Exit": key, "Count": value} for key, value in summary["exit_reason_counts"].items()])
    exit_fig = go.Figure(go.Bar(x=exit_df["Exit"], y=exit_df["Count"], marker_color="#90caf9"))
    exit_fig.update_layout(height=260, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="Trades")
    st.plotly_chart(exit_fig, use_container_width=True)
else:
    st.info("No exits to summarize.")
