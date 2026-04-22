"""TQQQ intraday pyramid study page."""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backtest.intraday_pyramid import DEFAULT_PARAMS, run_backtest
from dashboard.shared import render_sidebar
from data.database import get_date_range, load_prices

PAGE_DEFAULTS = {
    "campaign_allocation_pct": DEFAULT_PARAMS.get("campaign_allocation_pct", 1.0),
    "tier2_trigger_pct": DEFAULT_PARAMS.get("tier2_trigger_pct", 0.35),
    "tier3_trigger_pct": DEFAULT_PARAMS.get("tier3_trigger_pct", 0.70),
    "initial_stop_cap_pct": DEFAULT_PARAMS.get("initial_stop_cap_pct", 0.60),
    "post_tier1_stop_cap_pct": DEFAULT_PARAMS.get("post_tier1_stop_cap_pct", 0.30),
    "reclaim_buffer_pct": DEFAULT_PARAMS.get("reclaim_buffer_pct", 0.10),
    "tier1_profit_lock_pct": DEFAULT_PARAMS.get("tier1_profit_lock_pct", 0.20),
}

st.set_page_config(page_title="TQQQ Intraday Pyramid", layout="wide", page_icon="📈")
st.title("📈 TQQQ Intraday Pyramid")
st.caption("Version 1: long only, 5-minute Polygon data, no entry before 10:00 AM ET, three confirmation bars above yesterday's high and VWAP.")

cfg = render_sidebar()
initial_capital = cfg["initial_capital"]
sidebar_start_date = cfg["start_date"]
sidebar_end_date = cfg["end_date"]

SYMBOL = "TQQQ"
BAR_SIZE = "5m"
SOURCE = "polygon"

st.info(f"Fixed settings for this study: **{SYMBOL}**, **{BAR_SIZE}**, **{SOURCE}**.")

data_start_str, data_end_str = get_date_range(SYMBOL, BAR_SIZE, SOURCE)
if not data_start_str or not data_end_str:
    st.warning("No TQQQ 5-minute Polygon data available yet. Refresh data first.")
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

if "tqqq_intraday_pyramid_preset" not in st.session_state:
    st.session_state["tqqq_intraday_pyramid_preset"] = "Quarter"
if "tqqq_intraday_pyramid_range" not in st.session_state:
    st.session_state["tqqq_intraday_pyramid_range"] = (default_start_date, available_end)
if "tqqq_intraday_pyramid_year" not in st.session_state:
    st.session_state["tqqq_intraday_pyramid_year"] = "None"


def _apply_preset(preset_label: str) -> None:
    st.session_state["tqqq_intraday_pyramid_preset"] = preset_label
    days = PRESET_DAYS[preset_label]
    if days is None:
        return
    start = max(pd.Timestamp(available_end) - pd.Timedelta(days=days), pd.Timestamp(available_start))
    st.session_state["tqqq_intraday_pyramid_range"] = (start.date(), available_end)


preset_cols = st.columns(len(PRESET_DAYS))
for idx, preset_label in enumerate(PRESET_DAYS):
    with preset_cols[idx]:
        if st.button(
            preset_label,
            key=f"pyramid_preset_{preset_label}",
            use_container_width=True,
            type="primary" if st.session_state["tqqq_intraday_pyramid_preset"] == preset_label else "secondary",
        ):
            _apply_preset(preset_label)

st.markdown("**Specific year**")
year_labels = ["None"] + [str(year) for year in available_years]
year_cols = st.columns(min(len(year_labels), 5))
for idx, year_label in enumerate(year_labels):
    with year_cols[idx % len(year_cols)]:
        if st.button(
            year_label,
            key=f"tqqq_year_{year_label}",
            use_container_width=True,
            type="primary" if st.session_state["tqqq_intraday_pyramid_year"] == year_label else "secondary",
        ):
            st.session_state["tqqq_intraday_pyramid_year"] = year_label
            if year_label != "None":
                selected_year = int(year_label)
                start = max(pd.Timestamp(f"{selected_year}-01-01"), pd.Timestamp(available_start))
                end = min(pd.Timestamp(f"{selected_year}-12-31"), pd.Timestamp(available_end))
                st.session_state["tqqq_intraday_pyramid_range"] = (start.date(), end.date())
                st.session_state["tqqq_intraday_pyramid_preset"] = "Custom"

range_value = st.date_input(
    "Date range",
    value=st.session_state["tqqq_intraday_pyramid_range"],
    min_value=available_start,
    max_value=available_end,
    key="tqqq_intraday_pyramid_range",
)
if isinstance(range_value, tuple) and len(range_value) == 2:
    start_date, end_date = range_value
else:
    start_date, end_date = sidebar_start_date, sidebar_end_date

if start_date > end_date:
    st.error("Simulation start date must be on or before the end date.")
    st.stop()

if st.session_state["tqqq_intraday_pyramid_preset"] != "Custom":
    preset_days = PRESET_DAYS[st.session_state["tqqq_intraday_pyramid_preset"]]
    expected_start = max(pd.Timestamp(available_end) - pd.Timedelta(days=preset_days), pd.Timestamp(available_start)).date()
    if (start_date, end_date) != (expected_start, available_end):
        st.session_state["tqqq_intraday_pyramid_preset"] = "Custom"

st.caption(f"Running the simulation from **{start_date}** to **{end_date}**.")

pc1, pc2, pc3 = st.columns(3)
with pc1:
    campaign_allocation_pct = st.slider(
        "Campaign allocation (% of portfolio)",
        10,
        100,
        int(PAGE_DEFAULTS["campaign_allocation_pct"] * 100),
        step=5,
    ) / 100
with pc2:
    tier2_trigger_pct = st.number_input(
        "Tier 2 trigger (%)",
        min_value=0.10,
        max_value=2.00,
        value=float(PAGE_DEFAULTS["tier2_trigger_pct"]),
        step=0.05,
    )
with pc3:
    tier3_trigger_pct = st.number_input(
        "Tier 3 trigger (%)",
        min_value=0.20,
        max_value=3.00,
        value=float(PAGE_DEFAULTS["tier3_trigger_pct"]),
        step=0.05,
    )

pc4, pc5, pc6, pc7 = st.columns(4)
with pc4:
    initial_stop_cap_pct = st.number_input(
        "Initial stop cap (%)",
        min_value=0.20,
        max_value=2.00,
        value=float(PAGE_DEFAULTS["initial_stop_cap_pct"]),
        step=0.05,
    )
with pc5:
    post_tier1_stop_cap_pct = st.number_input(
        "Post-Tier-1 stop cap (%)",
        min_value=0.05,
        max_value=1.00,
        value=float(PAGE_DEFAULTS["post_tier1_stop_cap_pct"]),
        step=0.05,
    )
with pc6:
    reclaim_buffer_pct = st.number_input(
        "Reclaim buffer (%)",
        min_value=0.05,
        max_value=1.00,
        value=float(PAGE_DEFAULTS["reclaim_buffer_pct"]),
        step=0.05,
    )
with pc7:
    tier1_profit_lock_pct = st.number_input(
        "Tier 1 profit lock (%)",
        min_value=0.05,
        max_value=1.00,
        value=float(PAGE_DEFAULTS["tier1_profit_lock_pct"]),
        step=0.05,
    )

params = {
    **DEFAULT_PARAMS,
    "campaign_allocation_pct": campaign_allocation_pct,
    "tier2_trigger_pct": tier2_trigger_pct,
    "tier3_trigger_pct": tier3_trigger_pct,
    "initial_stop_cap_pct": initial_stop_cap_pct,
    "post_tier1_stop_cap_pct": post_tier1_stop_cap_pct,
    "reclaim_buffer_pct": reclaim_buffer_pct,
    "tier1_profit_lock_pct": tier1_profit_lock_pct,
}

df = load_prices(
    SYMBOL,
    start=str(start_date),
    end=str(end_date),
    bar_size=BAR_SIZE,
    source=SOURCE,
)
if df.empty:
    st.warning("No TQQQ 5-minute Polygon data in the selected range. Refresh data first.")
    st.stop()

result_key = f"intraday_pyramid_{SYMBOL}_{start_date}_{end_date}"
if st.button("▶ Run Pyramid Backtest", type="primary"):
    with st.spinner("Running intraday pyramid backtest…"):
        result = run_backtest(df, params=params, initial_capital=initial_capital)
    st.session_state[result_key] = result

result = st.session_state.get(result_key)
if result is None:
    st.info("Press **▶ Run Pyramid Backtest** to see results.")
    st.stop()

m = result["metrics"]
summary = result["summary"]
price_df = result["price_df"]
signals_df = result["signals_df"]
equity_curve = result["equity_curve"]
campaigns = result["campaigns"]
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
s3.metric("Tier 2 Hit Rate", f"{summary['tier2_hit_rate_pct']:.1f}%")
s4.metric("Tier 3 Hit Rate", f"{summary['tier3_hit_rate_pct']:.1f}%")

st.subheader("TQQQ Price, VWAP, and Events")
price_fig = make_subplots(
    rows=2,
    cols=1,
    shared_xaxes=True,
    row_heights=[0.75, 0.25],
    subplot_titles=("Price / VWAP / Yesterday High", "Volume"),
)

price_fig.add_trace(
    go.Scatter(x=price_df["date"], y=price_df["close"], line=dict(color="#90caf9", width=1.2), name="Close"),
    row=1,
    col=1,
)
price_fig.add_trace(
    go.Scatter(x=price_df["date"], y=price_df["vwap"], line=dict(color="#f39c12", width=1.0), name="VWAP"),
    row=1,
    col=1,
)
price_fig.add_trace(
    go.Scatter(
        x=price_df["date"],
        y=price_df["yesterday_high"],
        line=dict(color="#ef5350", width=1.0, dash="dot"),
        name="Yesterday High",
    ),
    row=1,
    col=1,
)

if not signals_df.empty:
    buy_df = signals_df[signals_df["signal"] == "BUY"]
    add_df = signals_df[signals_df["signal"] == "ADD"]
    sell_df = signals_df[signals_df["signal"] == "SELL"]

    if not buy_df.empty:
        price_fig.add_trace(
            go.Scatter(
                x=buy_df["date"],
                y=buy_df["price"],
                mode="markers",
                marker=dict(symbol="triangle-up", color="#26a69a", size=10),
                name="Tier 1",
                text=buy_df["detail"],
            ),
            row=1,
            col=1,
        )
    if not add_df.empty:
        price_fig.add_trace(
            go.Scatter(
                x=add_df["date"],
                y=add_df["price"],
                mode="markers",
                marker=dict(symbol="circle", color="#d4e157", size=8),
                name="Adds",
                text=add_df["detail"],
            ),
            row=1,
            col=1,
        )
    if not sell_df.empty:
        price_fig.add_trace(
            go.Scatter(
                x=sell_df["date"],
                y=sell_df["price"],
                mode="markers",
                marker=dict(symbol="triangle-down", color="#ef5350", size=9),
                name="Exits",
                text=sell_df["detail"],
            ),
            row=1,
            col=1,
        )

price_fig.add_trace(
    go.Bar(x=price_df["date"], y=price_df["volume"], marker_color="#546e7a", showlegend=False),
    row=2,
    col=1,
)
price_fig.update_layout(height=560, template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(t=40, b=20))
st.plotly_chart(price_fig, use_container_width=True)

st.subheader("Equity Curve")
eq_fig = go.Figure()
eq_fig.add_trace(
    go.Scatter(
        x=equity_curve.index,
        y=equity_curve.values,
        line=dict(color="#90caf9", width=1.5),
        name="Portfolio",
        fill="tozeroy",
        fillcolor="rgba(144,202,249,0.08)",
    )
)
eq_fig.add_hline(y=initial_capital, line_dash="dash", line_color="#546e7a", annotation_text="Starting capital", annotation_position="bottom right")
eq_fig.update_layout(height=320, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="Portfolio Value ($)")
st.plotly_chart(eq_fig, use_container_width=True)

st.subheader("Campaign Summary")
if campaigns:
    cdf = pd.DataFrame(campaigns).copy()
    cdf["entry_time"] = pd.to_datetime(cdf["entry_time"]).dt.strftime("%Y-%m-%d %H:%M")
    cdf["exit_time"] = pd.to_datetime(cdf["exit_time"]).dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(cdf, use_container_width=True, hide_index=True)
else:
    st.info("No campaign days triggered.")

st.subheader("Trade History")
if trades:
    tdf = pd.DataFrame(trades).copy()
    tdf["pnl"] = tdf["pnl"].map(lambda x: f"${x:+,.2f}")
    tdf["return_pct"] = tdf["return_pct"].map(lambda x: f"{x:+.2f}%")
    tdf["entry_price"] = tdf["entry_price"].map(lambda x: f"${x:,.4f}")
    tdf["exit_price"] = tdf["exit_price"].map(lambda x: f"${x:,.4f}")
    tdf["shares"] = tdf["shares"].map(lambda x: f"{x:,.4f}")
    st.dataframe(tdf, use_container_width=True, hide_index=True)
else:
    st.info("No tier exits were recorded.")

st.subheader("Exit Mix")
if summary["exit_reason_counts"]:
    exit_df = pd.DataFrame(
        [{"Exit": key, "Count": value} for key, value in summary["exit_reason_counts"].items()]
    )
    exit_fig = go.Figure(go.Bar(x=exit_df["Exit"], y=exit_df["Count"], marker_color="#90caf9"))
    exit_fig.update_layout(height=260, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="Campaigns")
    st.plotly_chart(exit_fig, use_container_width=True)
else:
    st.info("No exits to summarize.")
