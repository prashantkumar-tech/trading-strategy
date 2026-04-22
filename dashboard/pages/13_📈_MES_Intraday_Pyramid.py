"""MES intraday pyramid study page."""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backtest.mes_intraday_pyramid import DEFAULT_PARAMS, run_backtest
from dashboard.shared import render_sidebar
from data.database import get_date_range, load_prices

PAGE_DEFAULTS = {
    **DEFAULT_PARAMS,
    "add2_tighten_delay_bars": DEFAULT_PARAMS.get("add2_tighten_delay_bars", 1),
    "short_regime_filter": DEFAULT_PARAMS.get("short_regime_filter", "always"),
}


def _build_yearly_comparison_table(initial_capital: float, params: dict) -> pd.DataFrame:
    mes_end = pd.Timestamp(data_end_str)
    qqq_df = load_prices("QQQ", start="2020-01-01", end=str(mes_end.date()), bar_size="1d", source="yfinance")
    qqq_df = qqq_df.sort_values("date").reset_index(drop=True)

    rows = []
    for year in range(2020, mes_end.year + 1):
        year_start = pd.Timestamp(f"{year}-01-01")
        year_end = min(pd.Timestamp(f"{year}-12-31"), mes_end)
        if year_end < year_start:
            continue

        mes_df = load_prices("MES", start=str(year_start.date()), end=str(year_end.date()), bar_size="5m", source="databento")
        mes_result = run_backtest(mes_df, params=params, initial_capital=initial_capital)
        mes_metrics = mes_result["metrics"]
        mes_summary = mes_result["summary"]

        qqq_year = qqq_df[(qqq_df["date"] >= year_start) & (qqq_df["date"] <= year_end)].copy()
        if qqq_year.empty:
            qqq_return_pct = None
            qqq_final_value = None
        else:
            qqq_start_close = float(qqq_year["close"].iloc[0])
            qqq_end_close = float(qqq_year["close"].iloc[-1])
            qqq_return_pct = (qqq_end_close / qqq_start_close - 1) * 100
            qqq_final_value = initial_capital * (qqq_end_close / qqq_start_close)

        mes_return_pct = float(mes_metrics["total_return_pct"])
        rows.append({
            "Year": str(year),
            "Period End": str(year_end.date()),
            "MES Final": f"${float(mes_result['final_value']):,.2f}",
            "MES Return": f"{mes_return_pct:+.2f}%",
            "MES Trades": int(mes_metrics["num_trades"]),
            "MES Win Rate": f"{float(mes_metrics['win_rate_pct']):.2f}%",
            "MES Sharpe": f"{float(mes_metrics['sharpe_ratio']):.3f}",
            "MES Max DD": f"{float(mes_metrics['max_drawdown_pct']):.2f}%",
            "Campaign Days": int(mes_summary["campaign_days"]),
            "QQQ Final": f"${float(qqq_final_value):,.2f}" if qqq_final_value is not None else "N/A",
            "QQQ Return": f"{float(qqq_return_pct):+.2f}%" if qqq_return_pct is not None else "N/A",
            "MES - QQQ": f"{mes_return_pct - float(qqq_return_pct):+.2f}%" if qqq_return_pct is not None else "N/A",
        })

    return pd.DataFrame(rows)


st.set_page_config(page_title="MES Intraday Pyramid", layout="wide", page_icon="📈")
st.title("📈 MES Intraday Pyramid")
st.caption("Version 1: MES intraday pyramid with same-day add-on contracts for bullish breakouts.")

cfg = render_sidebar()
initial_capital = cfg["initial_capital"]

SYMBOL = "MES"
BAR_SIZE = "5m"
SOURCE = "databento"

st.info(f"Fixed settings for this study: **{SYMBOL}**, **{BAR_SIZE}**, **{SOURCE}**, long only.")
st.caption("Current tuned long defaults: 1 base contract, then add 2 at +7 points and 1 at +9 points, with an 11-point stop and 1-bar Add 2 tighten delay.")

data_start_str, data_end_str = get_date_range(SYMBOL, BAR_SIZE, SOURCE)
if not data_start_str or not data_end_str:
    st.warning("No MES 5-minute Databento data available yet. Use the sidebar to fetch MES from Databento first.")
    st.stop()

available_start = pd.Timestamp(data_start_str).date()
available_end = pd.Timestamp(data_end_str).date()

st.subheader("Simulation Range")
default_start = max(pd.Timestamp("2020-01-01"), pd.Timestamp(available_start))
default_start_date = default_start.date()

PRESET_DAYS = {
    "Quarter": 90,
    "Half Year": 182,
    "Year": 365,
    "Two Years": 730,
    "Custom": None,
}
available_years = list(range(available_end.year, available_start.year - 1, -1))

if "mes_intraday_pyramid_preset" not in st.session_state:
    st.session_state["mes_intraday_pyramid_preset"] = "Quarter"
if "mes_intraday_pyramid_range" not in st.session_state:
    st.session_state["mes_intraday_pyramid_range"] = (default_start_date, available_end)
if "mes_intraday_pyramid_year" not in st.session_state:
    st.session_state["mes_intraday_pyramid_year"] = "None"


def _apply_preset(preset_label: str) -> None:
    st.session_state["mes_intraday_pyramid_preset"] = preset_label
    days = PRESET_DAYS[preset_label]
    if days is None:
        return
    start = max(pd.Timestamp(available_end) - pd.Timedelta(days=days), pd.Timestamp(available_start))
    st.session_state["mes_intraday_pyramid_range"] = (start.date(), available_end)


def _time_label_to_minute(label: str) -> int:
    hour, minute = label.split(":")
    return int(hour) * 60 + int(minute)


preset_cols = st.columns(len(PRESET_DAYS))
for idx, preset_label in enumerate(PRESET_DAYS):
    with preset_cols[idx]:
        if st.button(
            preset_label,
            key=f"mes_intraday_preset_{preset_label}",
            use_container_width=True,
            type="primary" if st.session_state["mes_intraday_pyramid_preset"] == preset_label else "secondary",
        ):
            _apply_preset(preset_label)

st.markdown("**Specific year**")
year_labels = ["None"] + [str(year) for year in available_years]
year_cols = st.columns(min(len(year_labels), 5))
for idx, year_label in enumerate(year_labels):
    with year_cols[idx % len(year_cols)]:
        if st.button(
            year_label,
            key=f"mes_intraday_year_{year_label}",
            use_container_width=True,
            type="primary" if st.session_state["mes_intraday_pyramid_year"] == year_label else "secondary",
        ):
            st.session_state["mes_intraday_pyramid_year"] = year_label
            if year_label != "None":
                selected_year = int(year_label)
                start = max(pd.Timestamp(f"{selected_year}-01-01"), pd.Timestamp(available_start))
                end = min(pd.Timestamp(f"{selected_year}-12-31"), pd.Timestamp(available_end))
                st.session_state["mes_intraday_pyramid_range"] = (start.date(), end.date())
                st.session_state["mes_intraday_pyramid_preset"] = "Custom"

range_value = st.date_input(
    "Date range",
    value=st.session_state["mes_intraday_pyramid_range"],
    min_value=available_start,
    max_value=available_end,
    key="mes_intraday_pyramid_range",
)
if isinstance(range_value, tuple) and len(range_value) == 2:
    start_date, end_date = range_value
else:
    start_date, end_date = default_start_date, available_end

if start_date > end_date:
    st.error("Simulation start date must be on or before the end date.")
    st.stop()

st.caption(f"Running the simulation from **{start_date}** to **{end_date}**.")

time_options = ["10:00", "10:05", "10:10", "10:15", "10:20", "10:25", "10:30", "11:00", "11:15", "11:30", "12:00", "12:30", "13:00"]

pc1, pc2, pc3, pc4 = st.columns(4)
with pc1:
    base_contracts = st.number_input("Base contracts", min_value=1, max_value=5, value=int(DEFAULT_PARAMS["base_contracts"]), step=1)
with pc2:
    add1_contracts = st.number_input("Add 1 contracts", min_value=0, max_value=5, value=int(PAGE_DEFAULTS["add_contracts"][0]), step=1)
with pc3:
    add2_contracts = st.number_input("Add 2 contracts", min_value=0, max_value=5, value=int(PAGE_DEFAULTS["add_contracts"][1]), step=1)
with pc4:
    breakout_buffer_points = st.number_input("Breakout buffer (pts)", min_value=0.0, max_value=10.0, value=float(PAGE_DEFAULTS["breakout_buffer_points"]), step=0.25)

pc5, pc6, pc7, pc8 = st.columns(4)
with pc5:
    stop_loss_points = st.number_input("Stop loss (pts)", min_value=1.0, max_value=50.0, value=float(PAGE_DEFAULTS["stop_loss_points"]), step=0.5)
with pc6:
    take_profit_points = st.number_input("Take profit (pts)", min_value=1.0, max_value=100.0, value=float(PAGE_DEFAULTS["take_profit_points"]), step=0.5)
with pc7:
    add1_trigger_points = st.number_input("Add 1 trigger (pts)", min_value=0.5, max_value=30.0, value=float(PAGE_DEFAULTS["add_trigger_points"][0]), step=0.5)
with pc8:
    add2_trigger_points = st.number_input("Add 2 trigger (pts)", min_value=1.0, max_value=40.0, value=float(PAGE_DEFAULTS["add_trigger_points"][1]), step=0.5)

pc9, pc10, pc11, pc12 = st.columns(4)
with pc9:
    first_entry_time = st.selectbox("First entry after", options=time_options[:7], index=time_options[:7].index("10:15"))
with pc10:
    last_entry_time = st.selectbox("Last base entry by", options=time_options[7:10], index=time_options[7:10].index("11:15"))
with pc11:
    last_add_time = st.selectbox("Last add by", options=time_options[7:], index=time_options[7:].index("13:00"))
with pc12:
    profit_lock_points_after_add2 = st.number_input(
        "Base lock after Add 2 (pts)",
        min_value=0.0,
        max_value=15.0,
        value=float(PAGE_DEFAULTS["profit_lock_points_after_add2"]),
        step=0.5,
    )

pc13, _pc14, _pc15, _pc16 = st.columns(4)
with pc13:
    add2_tighten_delay_bars = st.number_input(
        "Add 2 tighten delay (bars)",
        min_value=0,
        max_value=10,
        value=int(PAGE_DEFAULTS["add2_tighten_delay_bars"]),
        step=1,
        help="How many 5-minute bars to wait after Add 2 entry before tightening Add 2's stop toward the Add 1 entry area.",
    )

params = {
    **DEFAULT_PARAMS,
    "base_contracts": int(base_contracts),
    "add_contracts": [int(add1_contracts), int(add2_contracts)],
    "add_trigger_points": [float(add1_trigger_points), float(add2_trigger_points)],
    "breakout_buffer_points": float(breakout_buffer_points),
    "stop_loss_points": float(stop_loss_points),
    "take_profit_points": float(take_profit_points),
    "first_entry_minute": _time_label_to_minute(first_entry_time),
    "last_entry_minute": _time_label_to_minute(last_entry_time),
    "last_add_minute": _time_label_to_minute(last_add_time),
    "trade_direction": "long",
    "add2_tighten_delay_bars": int(add2_tighten_delay_bars),
    "profit_lock_points_after_add2": float(profit_lock_points_after_add2),
}

st.subheader("Stop Behavior Guide")
example_base_entry = 5000.0
example_add1_entry = example_base_entry + float(add1_trigger_points)
example_add2_entry = example_base_entry + float(add2_trigger_points)

guide_rows = [
    {
        "Tier": "Base",
        "Entry Example": f"{example_base_entry:,.2f}",
        "Initial Stop": f"{example_base_entry - float(stop_loss_points):,.2f}",
        "After Add 1": f"{example_base_entry:,.2f}",
        "After Add 2": f"{example_base_entry + float(profit_lock_points_after_add2):,.2f}",
        "Meaning": "Starts with full stop, then moves to breakeven after Add 1, then locks profit after Add 2.",
    },
    {
        "Tier": "Add 1",
        "Entry Example": f"{example_add1_entry:,.2f}",
        "Initial Stop": f"{example_add1_entry - float(stop_loss_points):,.2f}",
        "After Add 1": f"{max(example_add1_entry - float(stop_loss_points), example_base_entry):,.2f}",
        "After Add 2": f"{example_add1_entry:,.2f}",
        "Meaning": "Starts with its own stop, then is protected around the base entry, then moves to its own breakeven after Add 2.",
    },
    {
        "Tier": "Add 2",
        "Entry Example": f"{example_add2_entry:,.2f}",
        "Initial Stop": f"{example_add2_entry - float(stop_loss_points):,.2f}",
        "After Add 1": "N/A",
        "After Add 2": f"{max(example_add2_entry - float(stop_loss_points), example_add1_entry):,.2f}",
        "Meaning": f"Starts with its own stop, then after {int(add2_tighten_delay_bars)} bar(s) from Add 2 entry it tightens toward the Add 1 entry area.",
    },
]

st.caption(
    "Illustration only. These example stop levels assume a base entry of 5000. "
    "Actual stops use the real entry prices on each trade."
)
st.dataframe(pd.DataFrame(guide_rows), use_container_width=True, hide_index=True)
st.caption(f"With the current setting, Add 2 stop tightening waits {int(add2_tighten_delay_bars)} five-minute bar(s) after Add 2 entry.")

stop_flow_lines = [
    f"Base: {example_base_entry:,.2f} -> {example_base_entry - float(stop_loss_points):,.2f} initial stop -> {example_base_entry:,.2f} after Add 1 -> {example_base_entry + float(profit_lock_points_after_add2):,.2f} after Add 2",
    f"Add 1: {example_add1_entry:,.2f} -> {example_add1_entry - float(stop_loss_points):,.2f} initial stop -> {max(example_add1_entry - float(stop_loss_points), example_base_entry):,.2f} after Add 1 -> {example_add1_entry:,.2f} after Add 2",
    f"Add 2: {example_add2_entry:,.2f} -> {example_add2_entry - float(stop_loss_points):,.2f} initial stop -> wait {int(add2_tighten_delay_bars)} bar(s) -> {max(example_add2_entry - float(stop_loss_points), example_add1_entry):,.2f} tightened stop",
]
st.code("\n".join(stop_flow_lines), language="text")

df = load_prices(SYMBOL, start=str(start_date), end=str(end_date), bar_size=BAR_SIZE, source=SOURCE)
if df.empty:
    st.warning("No MES Databento data in the selected range.")
    st.stop()

result_key = f"mes_intraday_pyramid_{start_date}_{end_date}"
if st.button("▶ Run MES Pyramid Backtest", type="primary"):
    with st.spinner("Running MES intraday pyramid backtest…"):
        result = run_backtest(df, params=params, initial_capital=initial_capital)
    st.session_state[result_key] = result

result = st.session_state.get(result_key)
if result is None:
    st.info("Press **▶ Run MES Pyramid Backtest** to see results.")
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
c5.metric("Win Rate", f"{m['win_rate_pct']:.1f}%  ({m['num_trades']} exits)")

s1, s2, s3, s4 = st.columns(4)
s1.metric("Final Value", f"${result['final_value']:,.2f}")
s2.metric("Campaign Days", f"{summary['campaign_days']}")
s3.metric("Add 1 Hit Rate", f"{summary['add1_hit_rate_pct']:.1f}%")
s4.metric("Add 2 Hit Rate", f"{summary['add2_hit_rate_pct']:.1f}%")

t1, t2 = st.columns(2)
t1.metric("Avg Contracts / Campaign", f"{summary['avg_contracts_per_campaign']:.2f}")
t2.metric("Avg Hold (bars)", f"{summary['avg_hold_bars']:.1f}")

st.subheader("MES Price, VWAP, and Events")
price_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], subplot_titles=("Price / VWAP / PDH", "Volume"))
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["close"], line=dict(color="#90caf9", width=1.2), name="Close"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["vwap"], line=dict(color="#f39c12", width=1.0), name="VWAP"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["pdh"], line=dict(color="#ef5350", width=1.0, dash="dot"), name="PDH"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["session_open"], line=dict(color="#d4e157", width=1.0, dash="dash"), name="Open"), row=1, col=1)

if not signals_df.empty:
    for signal, marker_symbol, color in [
        ("BUY", "triangle-up", "#26a69a"),
        ("ADD", "circle", "#ffee58"),
        ("SELL", "triangle-down", "#ef5350"),
    ]:
        signal_df = signals_df[signals_df["signal"] == signal]
        if not signal_df.empty:
            price_fig.add_trace(
                go.Scatter(
                    x=signal_df["date"],
                    y=signal_df["price"],
                    mode="markers",
                    marker=dict(symbol=marker_symbol, color=color, size=9),
                    name=signal,
                    text=signal_df["detail"],
                ),
                row=1,
                col=1,
            )

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
    exit_fig.update_layout(height=260, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="Exits")
    st.plotly_chart(exit_fig, use_container_width=True)
else:
    st.info("No exits to summarize.")

st.subheader("Year-by-Year MES vs QQQ")
st.caption("Uses the current MES pyramid settings on MES 5-minute Databento data, versus QQQ daily buy-and-hold from yfinance. 2026 is partial year through the latest available data.")
comparison_df = _build_yearly_comparison_table(initial_capital=initial_capital, params=params)
st.dataframe(comparison_df, use_container_width=True, hide_index=True)
