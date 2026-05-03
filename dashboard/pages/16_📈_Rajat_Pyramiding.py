"""Rajat's SPY intraday pyramiding study page."""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backtest.rajat_pyramiding import DEFAULT_PARAMS, build_yearly_summary, run_backtest
from data.database import get_date_range, init_db, load_prices

SYMBOL = "SPY"
BAR_SIZE = "5m"
SOURCE = "polygon"

EXIT_COLORS = {
    "Trailing stop":       "#ef4444",  # red   — time-ratcheted stop hit
    "End of day":          "#a78bfa",  # purple — forced EOD close
    "End of day fallback": "#a78bfa",
}


def _load_prices(start_date, end_date) -> pd.DataFrame:
    return load_prices(
        SYMBOL,
        start=str(start_date),
        end=str(end_date),
        bar_size=BAR_SIZE,
        source=SOURCE,
    )


def _format_metrics(result: dict) -> None:
    metrics = result["metrics"]
    summary = result["summary"]
    cols = st.columns(5)
    cols[0].metric("Final Value", f"${float(result['final_value']):,.2f}")
    cols[1].metric("Total Return", f"{float(metrics['total_return_pct']):+.2f}%")
    cols[2].metric("Max Drawdown", f"{float(metrics['max_drawdown_pct']):.2f}%")
    cols[3].metric("Win Rate", f"{float(metrics['win_rate_pct']):.2f}%")
    cols[4].metric("Trades", f"{int(metrics['num_trades']):,}")

    more = st.columns(5)
    more[0].metric("Sharpe", f"{float(metrics['sharpe_ratio']):.3f}")
    more[1].metric("Entries", f"{int(summary.get('entries', 0)):,}")
    more[2].metric("Traded Days", f"{int(summary.get('traded_days', 0)):,}")
    more[3].metric("Skipped Entries", f"{int(summary.get('skipped_entries', 0)):,}")
    more[4].metric("Avg Entries / Day", f"{float(summary.get('avg_entries_per_traded_day', 0.0)):.2f}")


def _plot_equity(result: dict) -> None:
    equity = result["equity_curve"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=equity.index,
        y=equity.values,
        name="Portfolio",
        line=dict(color="#38bdf8", width=1.6),
    ))
    fig.update_layout(
        height=330,
        template="plotly_dark",
        margin=dict(t=20, b=20),
        yaxis_title="Portfolio Value ($)",
    )
    st.plotly_chart(fig, use_container_width=True)


def _build_daily_summary(
    trades: list[dict],
    entries: list[dict],
    skipped_entry_events: list[dict],
) -> pd.DataFrame:
    if not trades and not entries and not skipped_entry_events:
        return pd.DataFrame()
    frames = []
    if trades:
        trades_df = pd.DataFrame(trades).copy()
        trades_df["entry_date"] = pd.to_datetime(trades_df["entry_date"])
        trades_df["exit_date"] = pd.to_datetime(trades_df["exit_date"])
        trades_df["day"] = trades_df["entry_date"].dt.strftime("%Y-%m-%d")
        trade_daily = trades_df.groupby("day", as_index=False).agg(
            share_exits=("entry_id", "count"),
            pnl=("pnl", "sum"),
            avg_exit=("exit_price", "mean"),
            last_exit=("exit_date", "max"),
            exits=("exit_rule", lambda s: "; ".join(f"{k} x{v}" for k, v in s.value_counts().items())),
        )
        frames.append(trade_daily.set_index("day"))

    if entries:
        entries_df = pd.DataFrame(entries).copy()
        entries_df["date"] = pd.to_datetime(entries_df["date"])
        entries_df["day"] = entries_df["date"].dt.strftime("%Y-%m-%d")
        entry_daily = entries_df.groupby("day", as_index=False).agg(
            entries=("entry_id", "nunique"),
            shares_bought=("shares", "sum"),
            avg_entry=("price", "mean"),
            first_entry=("date", "min"),
        )
        frames.append(entry_daily.set_index("day"))

    if skipped_entry_events:
        skipped_df = pd.DataFrame(skipped_entry_events).copy()
        skipped_df["date"] = pd.to_datetime(skipped_df["date"])
        skipped_df["day"] = skipped_df["date"].dt.strftime("%Y-%m-%d")
        skipped_daily = skipped_df.groupby("day", as_index=False).agg(
            skipped_entries=("date", "count"),
            skip_reasons=("reason", lambda s: "; ".join(f"{k} x{v}" for k, v in s.value_counts().items())),
        )
        frames.append(skipped_daily.set_index("day"))

    daily = pd.concat(frames, axis=1).reset_index().rename(columns={"index": "day"})
    daily = daily.sort_values("day", ascending=False)
    for col in ["share_exits", "entries", "shares_bought", "skipped_entries"]:
        if col in daily.columns:
            daily[col] = daily[col].fillna(0).astype(int)
    for col in ["pnl", "avg_entry", "avg_exit"]:
        if col in daily.columns:
            daily[col] = daily[col].round(4 if col != "pnl" else 2)
    return daily


def _plot_day_detail(
    price_df: pd.DataFrame,
    entries: pd.DataFrame,
    trades: pd.DataFrame,
    skipped: pd.DataFrame,
    day: str,
) -> None:
    day_prices = price_df[pd.to_datetime(price_df["date"]).dt.strftime("%Y-%m-%d") == day].copy()
    if day_prices.empty:
        st.caption("No price bars found for the selected day.")
        return

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=day_prices["date"],
        open=day_prices["open"],
        high=day_prices["high"],
        low=day_prices["low"],
        close=day_prices["close"],
        name="SPY",
        increasing_line_color="#22c55e",
        decreasing_line_color="#ef4444",
    ))

    if not entries.empty:
        day_entries = entries[entries["day"] == day]
        if not day_entries.empty:
            fig.add_trace(go.Scatter(
                x=day_entries["date"],
                y=day_entries["price"],
                mode="markers+text",
                marker=dict(symbol="triangle-up", size=13, color="#38bdf8"),
                text=day_entries["shares"].map(lambda v: f"{v}sh"),
                textposition="bottom center",
                name="Buy",
            ))

    if not trades.empty:
        day_trades = trades[trades["day"] == day]
        if not day_trades.empty:
            fig.add_trace(go.Scatter(
                x=day_trades["exit_date"],
                y=day_trades["exit_price"],
                mode="markers",
                marker=dict(
                    symbol="x",
                    size=11,
                    color=[EXIT_COLORS.get(rule, "#eab308") for rule in day_trades["exit_rule"]],
                ),
                name="Exit",
                text=day_trades.apply(
                    lambda r: f"{r['exit_rule']} | pnl {'+' if r['pnl'] >= 0 else ''}{r['pnl']:.2f}",
                    axis=1,
                ),
            ))

    if not skipped.empty:
        day_skipped = skipped[skipped["day"] == day]
        if not day_skipped.empty:
            fig.add_trace(go.Scatter(
                x=day_skipped["date"],
                y=day_skipped["price"],
                mode="markers",
                marker=dict(symbol="circle-open", size=10, color="#facc15", line=dict(width=2)),
                name="Skipped",
                text=day_skipped["reason"],
            ))

    # Legend annotations for exit colours
    fig.update_layout(
        height=500,
        template="plotly_dark",
        margin=dict(t=30, b=20),
        title=f"SPY 5-minute — {day}",
        yaxis_title="SPY Price",
        xaxis_rangeslider_visible=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Colour legend
    legend_cols = st.columns(len(EXIT_COLORS))
    for i, (label, color) in enumerate(EXIT_COLORS.items()):
        legend_cols[i].markdown(
            f"<span style='color:{color}'>■</span> {label}",
            unsafe_allow_html=True,
        )


def _plot_exit_breakdown(trades: list[dict]) -> None:
    if not trades:
        return
    df = pd.DataFrame(trades)
    counts = df["exit_rule"].value_counts().reset_index()
    counts.columns = ["exit_rule", "count"]
    colors = [EXIT_COLORS.get(r, "#eab308") for r in counts["exit_rule"]]
    fig = go.Figure(go.Bar(
        x=counts["exit_rule"],
        y=counts["count"],
        marker_color=colors,
        text=counts["count"],
        textposition="outside",
    ))
    fig.update_layout(
        height=280,
        template="plotly_dark",
        margin=dict(t=10, b=10),
        yaxis_title="Count",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def _plot_pnl_by_exit_rule(trades: list[dict]) -> None:
    if not trades:
        return
    df = pd.DataFrame(trades)
    pnl = df.groupby("exit_rule")["pnl"].sum().reset_index()
    colors = [EXIT_COLORS.get(r, "#eab308") for r in pnl["exit_rule"]]
    fig = go.Figure(go.Bar(
        x=pnl["exit_rule"],
        y=pnl["pnl"],
        marker_color=colors,
        text=pnl["pnl"].map(lambda v: f"${v:+.2f}"),
        textposition="outside",
    ))
    fig.update_layout(
        height=280,
        template="plotly_dark",
        margin=dict(t=10, b=10),
        yaxis_title="Total P&L ($)",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def _run_parameter_sweep(
    df: pd.DataFrame,
    initial_capital: float,
    trail_values: list[float],
    ratchet_values: list[float],
    entry_price_col: str,
) -> pd.DataFrame:
    rows = []
    for trail_pct in trail_values:
        for ratchet_pct in ratchet_values:
            params = {
                **DEFAULT_PARAMS,
                "trail_pct": trail_pct,
                "hourly_ratchet_pct": ratchet_pct,
                "entry_price_col": entry_price_col,
            }
            result = run_backtest(df, params=params, initial_capital=initial_capital)
            metrics = result["metrics"]
            summary = result["summary"]
            rows.append({
                "trail_%": trail_pct,
                "ratchet_%/hr": ratchet_pct,
                "final_value": round(float(result["final_value"]), 2),
                "return_%": float(metrics["total_return_pct"]),
                "max_drawdown_%": float(metrics["max_drawdown_pct"]),
                "sharpe": float(metrics["sharpe_ratio"]),
                "trades": int(metrics["num_trades"]),
                "win_rate_%": float(metrics["win_rate_pct"]),
                "entries": int(summary.get("entries", 0)),
                "skipped_entries": int(summary.get("skipped_entries", 0)),
            })
    return pd.DataFrame(rows).sort_values(["return_%", "sharpe"], ascending=False).reset_index(drop=True)


def _build_period_summary(
    df: pd.DataFrame,
    params: dict,
    initial_capital: float,
    freq: str,
) -> pd.DataFrame:
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])
    data["period"] = data["date"].dt.to_period(freq)
    rows = []
    for period in sorted(data["period"].unique()):
        period_df = data[data["period"] == period].copy()
        if period_df.empty:
            continue
        result = run_backtest(period_df, params=params, initial_capital=initial_capital)
        metrics = result["metrics"]
        summary = result["summary"]
        final_value = float(result["final_value"])
        rows.append({
            "period": str(period),
            "start": str(period_df["date"].min().date()),
            "end": str(period_df["date"].max().date()),
            "final_value": round(final_value, 2),
            "profit": round(final_value - initial_capital, 2),
            "return_%": float(metrics["total_return_pct"]),
            "max_drawdown_%": float(metrics["max_drawdown_pct"]),
            "sharpe": float(metrics["sharpe_ratio"]),
            "trades": int(metrics["num_trades"]),
            "win_rate_%": float(metrics["win_rate_pct"]),
            "entries": int(summary.get("entries", 0)),
            "traded_days": int(summary.get("traded_days", 0)),
            "skipped_entries": int(summary.get("skipped_entries", 0)),
        })
    return pd.DataFrame(rows)


# ── Page setup ────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Rajat Pyramiding", layout="wide", page_icon="📈")
init_db()

st.title("📈 Rajat's Pyramiding Strategy")
st.caption(
    "SPY 5-minute intraday backtest. "
    "Entries start at 09:35 ET, repeat every hour, last slot 15:25 ET. "
    "Lot ladder: 4 → 3 → 2 → 1 → 1 shares per entry. "
    "Stop logic: initial stop ratchets up 0.25%% of entry price every 1 hour. "
    "All positions close at 15:55 ET."
)

data_start_str, data_end_str = get_date_range(SYMBOL, BAR_SIZE, SOURCE)
if not data_start_str or not data_end_str:
    st.error(f"No {SYMBOL} {BAR_SIZE} {SOURCE} data is available.")
    st.stop()

available_start = pd.Timestamp(data_start_str).date()
available_end = pd.Timestamp(data_end_str).date()
selectable_start = max(available_start, pd.Timestamp("2025-01-01").date())
default_start = selectable_start

WINDOW_STATE_VERSION = "hourly_lot_ladder_time_ratchet_v2"
PRESET_DAYS = {
    "1 Month": 31,
    "3 Months": 92,
    "6 Months": 183,
    "1 Year": 365,
    "5 Years": 365 * 5,
    "All": None,
}

if st.session_state.get("rajat_pyramiding_window_version") != WINDOW_STATE_VERSION:
    st.session_state["rajat_pyramiding_window"] = (default_start, available_end)
    st.session_state["rajat_pyramiding_preset"] = "Custom"
    st.session_state["rajat_pyramiding_window_version"] = WINDOW_STATE_VERSION
if "rajat_pyramiding_window" not in st.session_state:
    st.session_state["rajat_pyramiding_window"] = (default_start, available_end)
if "rajat_pyramiding_preset" not in st.session_state:
    st.session_state["rajat_pyramiding_preset"] = "Custom"

current_window = st.session_state.get("rajat_pyramiding_window")
if isinstance(current_window, tuple) and len(current_window) == 2:
    current_start, current_end = current_window
    if current_start < selectable_start or current_end > available_end:
        st.session_state["rajat_pyramiding_window"] = (
            max(current_start, selectable_start),
            min(current_end, available_end),
        )


def _apply_window_preset(label: str) -> None:
    st.session_state["rajat_pyramiding_preset"] = label
    days = PRESET_DAYS[label]
    if days is None:
        start = selectable_start
    else:
        start = max(selectable_start, (pd.Timestamp(available_end) - pd.Timedelta(days=days)).date())
    st.session_state["rajat_pyramiding_window"] = (start, available_end)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Study Scope")
    initial_capital = st.number_input(
        "Starting Capital ($)",
        min_value=1000,
        value=int(DEFAULT_PARAMS["initial_capital"]),
        step=500,
    )
    st.info(f"Fixed data: `{SYMBOL}` `{BAR_SIZE}` from `{SOURCE}`.")

    st.divider()
    st.header("Entry")
    entry_price_col = st.selectbox("Entry Price", options=["close", "open"], index=0)
    st.caption(
        "Lot ladder: **4 → 3 → 2 → 1 → 1** shares per consecutive hourly entry. "
        "Entries skipped when price is below previous entry price."
    )

    st.divider()
    st.header("Stop Logic")
    trail_pct = st.number_input(
        "Initial Trail Stop (%)",
        min_value=0.05, max_value=5.0,
        value=float(DEFAULT_PARAMS["trail_pct"]),
        step=0.05,
        help="Stop placed this % below entry price at the moment of entry.",
    )
    hourly_ratchet_pct = st.number_input(
        "Hourly Ratchet (%)",
        min_value=0.05, max_value=5.0,
        value=float(DEFAULT_PARAMS["hourly_ratchet_pct"]),
        step=0.05,
        help="Every full hour after entry, the stop rises by this % of the entry price. Stop only moves up.",
    )
    st.caption(
        "Stop = entry − trail% at entry, then +ratchet% of entry price every hour. "
        "Stop only moves up, never down."
    )


# ── Study window ──────────────────────────────────────────────────────────────

st.subheader("Study Window")
st.caption(
    f"Available {SYMBOL} {BAR_SIZE} {SOURCE} data: **{available_start}** to **{available_end}**. "
    f"Custom backtests on this page start at **{selectable_start}** or later."
)
preset_cols = st.columns(len(PRESET_DAYS))
for idx, preset_label in enumerate(PRESET_DAYS):
    with preset_cols[idx]:
        if st.button(
            preset_label,
            key=f"rajat_window_preset_{preset_label}",
            use_container_width=True,
            type="primary" if st.session_state["rajat_pyramiding_preset"] == preset_label else "secondary",
        ):
            _apply_window_preset(preset_label)

date_range = st.date_input(
    "Custom date range",
    min_value=selectable_start,
    max_value=available_end,
    key="rajat_pyramiding_window",
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, available_end

if start_date > end_date:
    st.error("Study window start must be on or before the end date.")
    st.stop()
if start_date < selectable_start:
    st.error(f"Study window start must be on or after {selectable_start}.")
    st.stop()

st.caption(f"Selected window: **{start_date}** to **{end_date}**.")

df = _load_prices(start_date, end_date)
if df.empty:
    st.error(f"No {SYMBOL} {BAR_SIZE} {SOURCE} bars found in the selected study window.")
    st.stop()

st.success(f"Loaded **{len(df):,}** bars from **{df['date'].min()}** to **{df['date'].max()}**.")

params = {
    **DEFAULT_PARAMS,
    "trail_pct": trail_pct,
    "hourly_ratchet_pct": hourly_ratchet_pct,
    "entry_price_col": entry_price_col,
}
window_key = f"{start_date}_{end_date}_{initial_capital}_{entry_price_col}_{trail_pct}_{hourly_ratchet_pct}"

# ── Single Backtest ───────────────────────────────────────────────────────────

st.subheader("Single Backtest")
run_single = st.button("▶ Run Rajat Backtest", type="primary", key="rajat_run_single")
result_key = f"rajat_result_{window_key}"
if run_single or result_key not in st.session_state:
    with st.spinner("Running Rajat pyramiding backtest..."):
        st.session_state[result_key] = run_backtest(
            df,
            params=params,
            initial_capital=initial_capital,
        )

result = st.session_state.get(result_key)
if result:
    _format_metrics(result)
    _plot_equity(result)

    yc1, yc2 = st.columns(2)
    with yc1:
        st.markdown("**Exit Count by Rule**")
        _plot_exit_breakdown(result["trades"])
    with yc2:
        st.markdown("**Total P&L by Exit Rule**")
        _plot_pnl_by_exit_rule(result["trades"])

    yb1, yb2 = st.columns(2)
    with yb1:
        st.markdown("**Yearly Breakdown**")
        yearly = build_yearly_summary(df, params=params, initial_capital=initial_capital)
        st.dataframe(yearly, use_container_width=True, hide_index=True)
    with yb2:
        st.markdown("**Exit Reason Summary**")
        exit_counts = pd.DataFrame(
            [{"Exit Rule": k, "Count": v} for k, v in result["summary"].get("exit_reason_counts", {}).items()]
        )
        if exit_counts.empty:
            st.caption("No exits.")
        else:
            st.dataframe(exit_counts, use_container_width=True, hide_index=True)

    # ── Day Drilldown ─────────────────────────────────────────────────────────

    daily = _build_daily_summary(
        result["trades"],
        result["entries"],
        result.get("skipped_entry_events", []),
    )
    if daily.empty:
        st.caption("No trades in this window.")
    else:
        st.markdown("**Day Drilldown**")
        selected_day = st.selectbox(
            "Select a day to inspect",
            options=daily["day"].tolist(),
            index=0,
            key=f"rajat_day_drilldown_{window_key}",
        )

        entries_df = pd.DataFrame(result["entries"])
        if not entries_df.empty:
            entries_df["date"] = pd.to_datetime(entries_df["date"])
            entries_df["day"] = entries_df["date"].dt.strftime("%Y-%m-%d")

        trades_df = pd.DataFrame(result["trades"])
        if not trades_df.empty:
            trades_df["entry_date"] = pd.to_datetime(trades_df["entry_date"])
            trades_df["exit_date"] = pd.to_datetime(trades_df["exit_date"])
            trades_df["day"] = trades_df["entry_date"].dt.strftime("%Y-%m-%d")

        skipped_df = pd.DataFrame(result.get("skipped_entry_events", []))
        if not skipped_df.empty:
            skipped_df["date"] = pd.to_datetime(skipped_df["date"])
            skipped_df["day"] = skipped_df["date"].dt.strftime("%Y-%m-%d")

        _plot_day_detail(result["price_df"], entries_df, trades_df, skipped_df, selected_day)

        selected_summary = daily[daily["day"] == selected_day].iloc[0].to_dict()
        selected_entries = int(selected_summary.get("entries", 0) or 0)
        selected_share_exits = int(selected_summary.get("share_exits", 0) or 0)
        selected_skips = int(selected_summary.get("skipped_entries", 0) or 0)
        skip_reasons = selected_summary.get("skip_reasons", "")
        st.info(
            f"On {selected_day}: **{selected_entries}** scheduled entries, "
            f"**{selected_share_exits}** share exits, "
            f"**{selected_skips}** skipped"
            + (f" ({skip_reasons})" if isinstance(skip_reasons, str) and skip_reasons else "") + "."
        )

        dc1, dc2 = st.columns(2)
        with dc1:
            st.markdown("**Selected-Day Entries**")
            day_entries = entries_df[entries_df["day"] == selected_day] if not entries_df.empty else pd.DataFrame()
            st.dataframe(day_entries, use_container_width=True, hide_index=True)
        with dc2:
            st.markdown("**Selected-Day Skipped Buys**")
            day_skipped = skipped_df[skipped_df["day"] == selected_day] if not skipped_df.empty else pd.DataFrame()
            st.dataframe(day_skipped, use_container_width=True, hide_index=True)

        st.markdown("**Daily Summary**")
        st.dataframe(daily, use_container_width=True, hide_index=True, height=320)

    detail_tab, entry_tab = st.tabs(["Trade Detail", "Entry Detail"])
    with detail_tab:
        st.dataframe(pd.DataFrame(result["trades"]), use_container_width=True, hide_index=True)
    with entry_tab:
        st.dataframe(pd.DataFrame(result["entries"]), use_container_width=True, hide_index=True)

st.divider()

# ── Period Backtests ──────────────────────────────────────────────────────────

st.subheader("Period Backtests")
st.caption("Runs the strategy independently for each year or month, resetting capital at the start of each period.")

period_cols = st.columns([1, 1, 2])
with period_cols[0]:
    period_granularity = st.selectbox(
        "Breakdown",
        options=["Yearly", "Monthly"],
        index=0,
        key="rajat_period_granularity",
    )
with period_cols[1]:
    run_periods = st.button("Run Period Backtests", type="primary", use_container_width=True)

period_freq = "Y" if period_granularity == "Yearly" else "M"
period_key = f"rajat_periods_{window_key}_{period_freq}"
if run_periods:
    with st.spinner(f"Running {period_granularity.lower()} Rajat backtests..."):
        st.session_state[period_key] = _build_period_summary(
            df,
            params=params,
            initial_capital=initial_capital,
            freq=period_freq,
        )

period_summary = st.session_state.get(period_key)
if period_summary is not None:
    if period_summary.empty:
        st.caption("No periods found in the selected window.")
    else:
        st.dataframe(
            period_summary,
            use_container_width=True,
            hide_index=True,
            column_config={
                "final_value": st.column_config.NumberColumn("final_value", format="$%.2f"),
                "profit": st.column_config.NumberColumn("profit", format="$%.2f"),
                "return_%": st.column_config.NumberColumn("return_%", format="%.2f%%"),
                "max_drawdown_%": st.column_config.NumberColumn("max_drawdown_%", format="%.2f%%"),
                "sharpe": st.column_config.NumberColumn("sharpe", format="%.3f"),
                "win_rate_%": st.column_config.NumberColumn("win_rate_%", format="%.2f%%"),
            },
        )

st.divider()

# ── Parameter Sweep ───────────────────────────────────────────────────────────

st.subheader("Parameter Sweep")
st.caption("Runs independent backtests across initial trail stop and profit lock trigger values.")

sw1, sw2 = st.columns(2)
with sw1:
    trail_values = st.multiselect(
        "Initial Trail Stop Values (%)",
        options=[0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50],
        default=[0.20, 0.25, 0.30],
    )
with sw2:
    ratchet_values = st.multiselect(
        "Hourly Ratchet Values (%/hr)",
        options=[0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50],
        default=[0.25],
    )

combo_count = len(trail_values) * len(ratchet_values)
sweep_key = f"rajat_sweep_{window_key}_{tuple(trail_values)}_{tuple(ratchet_values)}"
st.caption(f"{combo_count} combinations selected.")

if st.button("▶ Run Parameter Sweep", type="primary", key="rajat_run_sweep"):
    if combo_count == 0:
        st.error("Choose at least one value in each sweep control.")
    elif combo_count > 100:
        st.error("Please keep the sweep to 100 combinations or fewer.")
    else:
        with st.spinner(f"Running {combo_count} parameter combinations..."):
            st.session_state[sweep_key] = _run_parameter_sweep(
                df,
                initial_capital=initial_capital,
                trail_values=trail_values,
                ratchet_values=ratchet_values,
                entry_price_col=entry_price_col,
            )

sweep = st.session_state.get(sweep_key)
if sweep is not None:
    st.dataframe(sweep, use_container_width=True, hide_index=True)

    fig = go.Figure()
    for tval in sweep["trail_%"].unique():
        sub = sweep[sweep["trail_%"] == tval]
        fig.add_trace(go.Scatter(
            x=sub["max_drawdown_%"],
            y=sub["return_%"],
            mode="markers+text",
            text=sub["ratchet_%/hr"].map(lambda v: f"ratchet {v}%"),
            textposition="top center",
            marker=dict(size=10),
            name=f"trail {tval}%",
        ))
    fig.update_layout(
        height=360,
        template="plotly_dark",
        margin=dict(t=20, b=20),
        xaxis_title="Max Drawdown (%)",
        yaxis_title="Total Return (%)",
    )
    st.plotly_chart(fig, use_container_width=True)
