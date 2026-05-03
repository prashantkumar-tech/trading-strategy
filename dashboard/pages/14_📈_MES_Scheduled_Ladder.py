"""MES scheduled ladder study page."""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backtest.mes_scheduled_ladder import DEFAULT_PARAMS, run_backtest
from dashboard.shared import render_sidebar
from data.database import get_date_range, load_prices


def _time_label_to_minute(label: str) -> int:
    hour, minute = label.split(":")
    return int(hour) * 60 + int(minute)


def _build_yearly_table(initial_capital: float, params: dict, latest_date: pd.Timestamp) -> pd.DataFrame:
    qqq_df = load_prices("QQQ", start="2020-01-01", end=str(latest_date.date()), bar_size="1d", source="yfinance").sort_values("date").reset_index(drop=True)
    spy_df = load_prices("SPY", start="2020-01-01", end=str(latest_date.date()), bar_size="1d", source="yfinance").sort_values("date").reset_index(drop=True)

    rows = []
    for year in range(2020, latest_date.year + 1):
        year_start = pd.Timestamp(f"{year}-01-01")
        year_end = min(pd.Timestamp(f"{year}-12-31"), latest_date)
        if year_end < year_start:
            continue

        df = load_prices("MES", start=str(year_start.date()), end=str(year_end.date()), bar_size="5m", source="databento")
        result = run_backtest(df, params=params, initial_capital=initial_capital)
        m = result["metrics"]
        s = result["summary"]
        qqq_year = qqq_df[(qqq_df["date"] >= year_start) & (qqq_df["date"] <= year_end)].copy()
        spy_year = spy_df[(spy_df["date"] >= year_start) & (spy_df["date"] <= year_end)].copy()

        qqq_return = None
        if not qqq_year.empty:
            qqq_return = (float(qqq_year["close"].iloc[-1]) / float(qqq_year["close"].iloc[0]) - 1) * 100

        spy_return = None
        if not spy_year.empty:
            spy_return = (float(spy_year["close"].iloc[-1]) / float(spy_year["close"].iloc[0]) - 1) * 100

        mes_return = float(m["total_return_pct"])
        rows.append({
            "Year": str(year),
            "Period End": str(year_end.date()),
            "Final Value": f"${float(result['final_value']):,.2f}",
            "MES Return": f"{mes_return:+.2f}%",
            "Sharpe": f"{float(m['sharpe_ratio']):.3f}",
            "Max DD": f"{float(m['max_drawdown_pct']):.2f}%",
            "Trades": int(m["num_trades"]),
            "Win Rate": f"{float(m['win_rate_pct']):.2f}%",
            "Campaign Days": int(s["campaign_days"]),
            "Campaigns": int(s["campaigns_opened"]),
            "Target 1 Hit Rate": f"{float(s['target1_hit_rate_pct']):.2f}%",
            "Runner Win Rate": f"{float(s['runner_win_rate_pct']):.2f}%",
            "SPY Return": f"{spy_return:+.2f}%" if spy_return is not None else "N/A",
            "QQQ Return": f"{qqq_return:+.2f}%" if qqq_return is not None else "N/A",
            "MES - SPY": f"{mes_return - float(spy_return):+.2f}%" if spy_return is not None else "N/A",
            "MES - QQQ": f"{mes_return - float(qqq_return):+.2f}%" if qqq_return is not None else "N/A",
        })
    return pd.DataFrame(rows)


st.set_page_config(page_title="MES Scheduled Ladder", layout="wide", page_icon="📈")
st.title("📈 MES Scheduled Ladder")
st.caption("Version 1: scheduled long-only MES campaigns every 30 minutes when price is above the session open and VWAP.")

cfg = render_sidebar()
initial_capital = cfg["initial_capital"]

SYMBOL = "MES"
BAR_SIZE = "5m"
SOURCE = "databento"

data_start_str, data_end_str = get_date_range(SYMBOL, BAR_SIZE, SOURCE)
if not data_start_str or not data_end_str:
    st.warning("No MES 5-minute Databento data available yet. Use the sidebar to fetch MES from Databento first.")
    st.stop()

available_start = pd.Timestamp(data_start_str).date()
available_end = pd.Timestamp(data_end_str).date()

st.subheader("Simulation Range")
default_start = max(pd.Timestamp("2020-01-01"), pd.Timestamp(available_start))
default_start_date = default_start.date()

if "mes_scheduled_ladder_range" not in st.session_state:
    st.session_state["mes_scheduled_ladder_range"] = (default_start_date, available_end)

range_value = st.date_input(
    "Date range",
    value=st.session_state["mes_scheduled_ladder_range"],
    min_value=available_start,
    max_value=available_end,
    key="mes_scheduled_ladder_range",
)
if isinstance(range_value, tuple) and len(range_value) == 2:
    start_date, end_date = range_value
else:
    start_date, end_date = default_start_date, available_end

if start_date > end_date:
    st.error("Simulation start date must be on or before the end date.")
    st.stop()

st.caption(f"Running the simulation from **{start_date}** to **{end_date}**.")

time_options = ["9:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "13:00", "13:30", "14:00", "14:30", "15:00", "15:30", "15:55"]

pc1, pc2, pc3, pc4 = st.columns(4)
with pc1:
    contracts_per_campaign = st.number_input("Contracts per campaign", min_value=1, max_value=4, value=int(DEFAULT_PARAMS["contracts_per_campaign"]), step=1)
with pc2:
    target1_points = st.number_input("Target 1 (pts)", min_value=1.0, max_value=100.0, value=float(DEFAULT_PARAMS["target1_points"]), step=0.5)
with pc3:
    stop_loss_points = st.number_input("Stop loss (pts)", min_value=1.0, max_value=100.0, value=float(DEFAULT_PARAMS["stop_loss_points"]), step=0.5)
with pc4:
    entry_interval_minutes = st.selectbox("Entry interval", options=[15, 30, 60], index=[15, 30, 60].index(int(DEFAULT_PARAMS["entry_interval_minutes"])))

pc5, pc6, pc7, pc8 = st.columns(4)
with pc5:
    first_entry_time = st.selectbox("First entry after", options=time_options[:-1], index=time_options[:-1].index("9:30"))
with pc6:
    last_entry_time = st.selectbox("Last entry by", options=time_options[:-1], index=time_options[:-1].index("15:30"))
with pc7:
    final_exit_time = st.selectbox("Final exit", options=time_options, index=time_options.index("15:55"))
with pc8:
    runner_management = st.selectbox("Runner management", options=["breakeven_only", "trail_2bar_low"], index=["breakeven_only", "trail_2bar_low"].index(DEFAULT_PARAMS["runner_management"]))

pc9, pc10, pc11 = st.columns(3)
with pc9:
    require_above_open = st.checkbox("Require above session open", value=bool(DEFAULT_PARAMS["require_above_open"]))
with pc10:
    require_above_vwap = st.checkbox("Require above VWAP", value=bool(DEFAULT_PARAMS["require_above_vwap"]))
with pc11:
    breakeven_after_target1 = st.checkbox("Move runner to breakeven after Target 1", value=bool(DEFAULT_PARAMS["breakeven_after_target1"]))

params = {
    **DEFAULT_PARAMS,
    "contracts_per_campaign": int(contracts_per_campaign),
    "target1_points": float(target1_points),
    "stop_loss_points": float(stop_loss_points),
    "entry_interval_minutes": int(entry_interval_minutes),
    "first_entry_minute": _time_label_to_minute(first_entry_time),
    "last_entry_minute": _time_label_to_minute(last_entry_time),
    "final_exit_minute": _time_label_to_minute(final_exit_time),
    "runner_management": runner_management,
    "require_above_open": bool(require_above_open),
    "require_above_vwap": bool(require_above_vwap),
    "breakeven_after_target1": bool(breakeven_after_target1),
}

df = load_prices(SYMBOL, start=str(start_date), end=str(end_date), bar_size=BAR_SIZE, source=SOURCE)
if df.empty:
    st.warning("No MES Databento data in the selected range.")
    st.stop()

result_key = f"mes_scheduled_ladder_{start_date}_{end_date}"
if st.button("▶ Run MES Scheduled Ladder", type="primary"):
    with st.spinner("Running MES scheduled ladder backtest…"):
        result = run_backtest(df, params=params, initial_capital=initial_capital)
    st.session_state[result_key] = result

result = st.session_state.get(result_key)
if result is None:
    st.info("Press **▶ Run MES Scheduled Ladder** to see results.")
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
s3.metric("Campaigns Opened", f"{summary['campaigns_opened']}")
s4.metric("Avg Hold (bars)", f"{summary['avg_hold_bars']:.1f}")

t1, t2 = st.columns(2)
t1.metric("Target 1 Hit Rate", f"{summary['target1_hit_rate_pct']:.1f}%")
t2.metric("Runner Win Rate", f"{summary['runner_win_rate_pct']:.1f}%")

st.subheader("MES Price, VWAP, and Events")
price_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], subplot_titles=("Price / VWAP / Session Open", "Volume"))
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["close"], line=dict(color="#90caf9", width=1.2), name="Close"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["vwap"], line=dict(color="#f39c12", width=1.0), name="VWAP"), row=1, col=1)
price_fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["session_open"], line=dict(color="#d4e157", width=1.0, dash="dash"), name="Open"), row=1, col=1)
if not signals_df.empty:
    for signal, symbol, color in [("BUY", "triangle-up", "#26a69a"), ("SELL", "triangle-down", "#ef5350")]:
        sdf = signals_df[signals_df["signal"] == signal]
        if not sdf.empty:
            price_fig.add_trace(go.Scatter(x=sdf["date"], y=sdf["price"], mode="markers", marker=dict(symbol=symbol, color=color, size=9), name=signal, text=sdf["detail"]), row=1, col=1)
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

st.subheader("Year-by-Year Summary")
st.caption("Uses the current MES Scheduled Ladder settings across each calendar year from 2020 through the latest available MES date. 2026 is partial year through the latest available data.")
yearly_df = _build_yearly_table(initial_capital=initial_capital, params=params, latest_date=pd.Timestamp(data_end_str))
st.dataframe(yearly_df, use_container_width=True, hide_index=True)
