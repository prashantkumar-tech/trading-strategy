"""Shared fixed-window study renderer for symbol-specific pages."""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.database import init_db, load_prices
from backtest.fixed_window import run_fixed_window_backtest, run_fixed_window_optimization

WINDOW_START = "2025-10-02"
WINDOW_END = "2025-12-19"
BAR_SIZE = "5m"
REGULAR_OPEN_MINUTE = 9 * 60 + 30
REGULAR_CLOSE_MINUTE = 15 * 60 + 55
LOOKBACK_START = (pd.Timestamp(WINDOW_START) - pd.Timedelta(days=120)).strftime("%Y-%m-%d")
LADDER_MODES = {
    "Ladder 0.3% to 3.0%": (0.3, 3.0),
    "Ladder 0.4% to 4.0%": (0.4, 4.0),
    "Ladder 0.5% to 5.0%": (0.5, 5.0),
    "Ladder 0.6% to 6.0%": (0.6, 6.0),
}


def _build_fixed_window_rules(
    pos_pct_above: float,
    pos_pct_below: float,
    profit_target: float,
    time_stop_days: int,
) -> list:
    return [
        {
            "type": "entry", "label": "Negative Premarket Above MA50", "combinator": "AND",
            "position_pct": pos_pct_above,
            "conditions": [
                {"left": "signal_830_price", "op": "<", "right": "prev_day_close"},
                {"left": "signal_day_open", "op": ">", "right": "ma50"},
                {"left": "bar_minutes", "op": "==", "right": "510"},
            ],
        },
        {
            "type": "entry", "label": "Negative Premarket Below MA50", "combinator": "AND",
            "position_pct": pos_pct_below,
            "conditions": [
                {"left": "signal_830_price", "op": "<", "right": "prev_day_close"},
                {"left": "signal_day_open", "op": "<=", "right": "ma50"},
                {"left": "bar_minutes", "op": "==", "right": "510"},
            ],
        },
        {
            "type": "entry", "label": "Positive Open Above MA50", "combinator": "AND",
            "position_pct": pos_pct_above,
            "conditions": [
                {"left": "signal_day_open", "op": ">", "right": "prev_day_close"},
                {"left": "signal_day_open", "op": ">", "right": "ma50"},
                {"left": "bar_minutes", "op": "==", "right": "585"},
            ],
        },
        {
            "type": "entry", "label": "Positive Open Below MA50", "combinator": "AND",
            "position_pct": pos_pct_below,
            "conditions": [
                {"left": "signal_day_open", "op": ">", "right": "prev_day_close"},
                {"left": "signal_day_open", "op": "<=", "right": "ma50"},
                {"left": "bar_minutes", "op": "==", "right": "585"},
            ],
        },
        {
            "type": "exit", "label": "Profit target", "combinator": "AND",
            "conditions": [{"left": "position_return_pct", "op": ">=", "right": str(profit_target)}],
        },
        {
            "type": "exit", "label": "Time stop", "combinator": "AND",
            "conditions": [{"left": "days_held", "op": ">=", "right": str(time_stop_days)}],
        },
    ]


def _build_sell_matrix(trades: list, price_df: pd.DataFrame) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()

    df_trades = pd.DataFrame(trades).copy()
    df_trades["entry_date"] = pd.to_datetime(df_trades["entry_date"])
    df_trades["exit_date"] = pd.to_datetime(df_trades["exit_date"])
    df_trades["date"] = df_trades["entry_date"].dt.strftime("%Y-%m-%d")
    df_trades["buy_amount"] = df_trades["entry_price"] * df_trades["shares"]
    df_trades["sell_amount"] = df_trades["exit_price"] * df_trades["shares"]
    market_lookup = price_df.set_index("date")[["close", "ma50"]]
    daily_reference = (
        price_df.assign(_trade_date=pd.to_datetime(price_df["date"]).dt.strftime("%Y-%m-%d"))
        .groupby("_trade_date", as_index=False)
        .agg(
            reference_open=("close", "first"),
            reference_close=("signal_day_close", "first"),
            reference_ma50=("ma50", "first"),
            ma50_source=("ma50_source", "first"),
        )
        .rename(columns={"_trade_date": "date"})
    )
    reference_lookup = daily_reference.set_index("date")

    rows = []
    max_sells = 0
    grouped = df_trades.groupby("date", sort=True)
    for date_str, group in grouped:
        group = group.sort_values(["exit_date", "exit_price"]).reset_index(drop=True)
        total_shares = group["shares"].sum()
        buy_amount = group["buy_amount"].sum()
        sell_amount = group["sell_amount"].sum()
        total_pnl = group["pnl"].sum()
        pnl_pct = (total_pnl / buy_amount * 100) if buy_amount else 0.0
        avg_buy_price = (buy_amount / total_shares) if total_shares else 0.0
        exit_reason_counts = group["exit_rule"].fillna("").replace("", "Unknown").value_counts()
        note = "; ".join(f"{label} x{count}" for label, count in exit_reason_counts.items())

        above_flags = []
        for ts in group["entry_date"]:
            if ts in market_lookup.index:
                row = market_lookup.loc[ts]
                above_flags.append(bool(row["close"] > row["ma50"]) if pd.notna(row["ma50"]) else False)
        if above_flags and all(above_flags):
            above_ma50 = "Yes"
        elif above_flags and not any(above_flags):
            above_ma50 = "No"
        else:
            above_ma50 = "Mixed"

        ref_row = reference_lookup.loc[date_str] if date_str in reference_lookup.index else None
        sell_prices = [round(v, 2) for v in group["exit_price"].tolist()]
        max_sells = max(max_sells, len(sell_prices))
        row = {
            "Date": date_str,
            "Avg": round(avg_buy_price, 2),
            "QTY": round(total_shares, 2),
            "Buy Amt": round(buy_amount, 2),
            "Above 50DMA": above_ma50,
            "Ref Open": round(float(ref_row["reference_open"]), 2) if ref_row is not None and pd.notna(ref_row["reference_open"]) else None,
            "Ref Close": round(float(ref_row["reference_close"]), 2) if ref_row is not None and pd.notna(ref_row["reference_close"]) else None,
            "Ref MA50": round(float(ref_row["reference_ma50"]), 2) if ref_row is not None and pd.notna(ref_row["reference_ma50"]) else None,
            "Note": note,
            "PnL $": round(total_pnl, 2),
            "PnL %": round(pnl_pct, 2),
            "_sell_prices": sell_prices,
        }
        rows.append(row)

    matrix = pd.DataFrame(rows)
    max_sells = max(max_sells, 10)
    for idx in range(max_sells):
        matrix[f"Sell {idx + 1}"] = matrix["_sell_prices"].apply(
            lambda values, i=idx: values[i] if i < len(values) else None
        )

    sell_cols = [f"Sell {i}" for i in range(1, max_sells + 1)]
    ordered = ["Date", "Avg", "QTY", "Buy Amt", "Above 50DMA", "Ref Open", "Ref Close", "Ref MA50"] + sell_cols + ["Note", "PnL $", "PnL %"]
    matrix = matrix.drop(columns=["_sell_prices"])
    return matrix[ordered]


def _regular_session_reference(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ts = pd.to_datetime(df["date"])
    minutes = ts.dt.hour * 60 + ts.dt.minute
    df["_date_only"] = ts.dt.date
    df["_minutes"] = minutes

    regular = df[(df["_minutes"] >= REGULAR_OPEN_MINUTE) & (df["_minutes"] <= REGULAR_CLOSE_MINUTE)].copy()
    if regular.empty:
        raise ValueError("No regular-session bars available to build daily open/close reference series.")

    daily = (
        regular.groupby("_date_only", sort=True)
        .agg(
            reference_day_open=("open", "first"),
            reference_day_close=("close", "last"),
        )
        .reset_index()
    )
    premarket_830 = (
        df[df["_minutes"] == 8 * 60 + 30]
        .groupby("_date_only", sort=True)
        .agg(signal_830_price=("close", "first"))
        .reset_index()
    )
    daily["reference_day_ma50"] = (
        daily["reference_day_close"]
        .rolling(window=50, min_periods=50)
        .mean()
    )
    daily = daily.merge(premarket_830, on="_date_only", how="left")

    merged = df.merge(daily[["_date_only", "reference_day_open", "reference_day_ma50"]], on="_date_only", how="left")
    merged = merged.merge(daily[["_date_only", "reference_day_close", "signal_830_price"]], on="_date_only", how="left")
    return merged.drop(columns=["_date_only", "_minutes"])


def _load_study_prices(symbol: str, reference_symbol: str) -> pd.DataFrame:
    base_df = load_prices(symbol, start=LOOKBACK_START, end=WINDOW_END, bar_size=BAR_SIZE).copy()
    if base_df.empty:
        return base_df

    base_df["exec_close"] = base_df["close"]
    base_df["symbol_close"] = base_df["close"]
    base_df["symbol_ma50"] = base_df["ma50"]
    base_df["ma50_source"] = symbol

    if reference_symbol == symbol:
        aligned = _regular_session_reference(base_df)
        aligned["close"] = aligned["reference_day_open"]
        aligned["signal_day_open"] = aligned["reference_day_open"]
        aligned["signal_day_close"] = aligned["reference_day_close"]
        aligned["ma50"] = aligned["reference_day_ma50"]
        aligned = aligned.drop(columns=["reference_day_open", "reference_day_close", "reference_day_ma50"])
        return aligned[(aligned["date"] >= WINDOW_START) & (aligned["date"] <= f"{WINDOW_END} 23:59:59")].reset_index(drop=True)

    ref_df = load_prices(reference_symbol, start=LOOKBACK_START, end=WINDOW_END, bar_size=BAR_SIZE)
    if ref_df.empty:
        raise ValueError(
            f"No {BAR_SIZE} data for reference symbol {reference_symbol} in the fixed study window."
        )

    ref_daily = _regular_session_reference(ref_df)

    merged = base_df.merge(
        ref_daily[["date", "reference_day_open", "reference_day_close", "reference_day_ma50", "signal_830_price"]],
        on="date",
        how="left",
    )
    merged["close"] = merged["reference_day_open"]
    merged["signal_day_open"] = merged["reference_day_open"]
    merged["signal_day_close"] = merged["reference_day_close"]
    merged["ma50"] = merged["reference_day_ma50"]
    merged["ma50_source"] = reference_symbol
    merged = merged[(merged["date"] >= WINDOW_START) & (merged["date"] <= f"{WINDOW_END} 23:59:59")].reset_index(drop=True)
    return merged


def render_fixed_window_study(symbol: str, title: str, default_reference_symbol: str) -> None:
    init_db()

    st.title(title)
    st.caption(
        "Runs both a single backtest and a parameter sweep on a fixed 5-minute window "
        f"from **{WINDOW_START}** to **{WINDOW_END}** for **{symbol}**."
    )
    st.info(
        "Entry logic is fixed for this study: buy at 08:30 ET if premarket is below the previous day's close, "
        "otherwise buy at 09:45 ET when the regular-session open is above the previous day's close. "
        "Allocation is 10% when the reference open is above the reference 50DMA, otherwise 5%."
    )

    with st.sidebar:
        st.header("Study Scope")
        initial_capital = st.number_input("Starting Capital ($)", min_value=1000, value=10_000, step=500)
        st.info(f"Symbol is fixed to `{symbol}`.")
        reference_symbol = st.text_input("Reference MA50 Symbol", value=default_reference_symbol).upper().strip()
        exit_mode = st.selectbox(
            "Exit Strategy",
            options=["Standard", "Ladder 0.3% to 3.0%", "Ladder 0.4% to 4.0%", "Ladder 0.5% to 5.0%", "Ladder 0.6% to 6.0%"],
            index=0,
        )
        st.info(f"Bar size is fixed to `{BAR_SIZE}`.")
        st.info(f"Date window is fixed to `{WINDOW_START}` → `{WINDOW_END}`.")

    use_ladder_exits = exit_mode != "Standard"
    ladder_step_pct, ladder_max_pct = LADDER_MODES.get(exit_mode, (0.4, 4.0))

    try:
        df = _load_study_prices(symbol, reference_symbol or symbol)
    except ValueError as e:
        st.error(str(e))
        st.info(f"Fetch `{reference_symbol}` {BAR_SIZE} data first, then reload this page.")
        st.stop()

    if df.empty:
        st.error(f"No {BAR_SIZE} data for {symbol} in the fixed study window.")
        st.stop()

    st.success(
        f"Loaded **{len(df):,}** bars for **{symbol}** from **{df['date'].min()}** to **{df['date'].max()}**."
    )
    st.info(f"`ma50` is sourced from **{reference_symbol or symbol}** for this study.")

    st.divider()

    st.subheader("Single Backtest")
    bt_col1, bt_col2, bt_col3 = st.columns(3)
    with bt_col1:
        bt_pos_above = st.number_input("Above MA50 Position Size (%)", 1, 50, 10, step=1, key=f"{symbol}_fw_bt_pos_above")
    with bt_col2:
        bt_profit_target = st.number_input(
            "Profit Target (%)",
            0.5,
            20.0,
            2.0,
            step=0.5,
            key=f"{symbol}_fw_bt_profit",
            disabled=exit_mode != "Standard",
        )
    with bt_col3:
        bt_time_stop = st.number_input("Time Stop (days)", 1, 20, 3, step=1, key=f"{symbol}_fw_bt_time_stop")

    if st.button("▶ Run Fixed-Window Backtest", type="primary", key=f"{symbol}_fw_run_bt"):
        rules = _build_fixed_window_rules(
            bt_pos_above / 100,
            (bt_pos_above / 2) / 100,
            bt_profit_target,
            int(bt_time_stop),
        )
        with st.spinner("Running fixed-window backtest…"):
            result = run_fixed_window_backtest(
                df,
                rules,
                initial_capital=initial_capital,
                use_ladder_exits=use_ladder_exits,
                ladder_step_pct=ladder_step_pct,
                ladder_max_pct=ladder_max_pct,
            )
        st.session_state[f"fixed_window_backtest_{symbol}"] = result

    bt_result = st.session_state.get(f"fixed_window_backtest_{symbol}")
    if bt_result:
        metrics = bt_result["metrics"]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Return", f"{metrics['total_return_pct']:.1f}%")
        c2.metric("Ann. Return", f"{metrics['annualized_return_pct']:.1f}%")
        c3.metric("Sharpe", f"{metrics['sharpe_ratio']:.2f}")
        c4.metric("Max Drawdown", f"{metrics['max_drawdown_pct']:.1f}%")
        c5.metric("Win Rate", f"{metrics['win_rate_pct']:.1f}% ({metrics['num_trades']} trades)")

        st.metric("Final Value", f"${bt_result['final_value']:,.2f}")

        eq_fig = go.Figure()
        eq_fig.add_trace(go.Scatter(
            x=bt_result["equity_curve"].index,
            y=bt_result["equity_curve"].values,
            line=dict(color="#90caf9", width=1.5),
            name="Portfolio",
        ))
        eq_fig.update_layout(
            height=320,
            template="plotly_dark",
            margin=dict(t=20, b=20),
            yaxis_title="Portfolio Value ($)",
            title=f"{symbol} Equity Curve",
        )
        st.plotly_chart(eq_fig, use_container_width=True)

        if bt_result["trades"]:
            st.subheader("Daily Sell Matrix")
            sell_matrix = _build_sell_matrix(bt_result["trades"], df)
            if not sell_matrix.empty:
                st.dataframe(sell_matrix, use_container_width=True, hide_index=True)

            st.subheader("Trade Detail")
            st.dataframe(pd.DataFrame(bt_result["trades"]), use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("Fixed-Window Optimizer")
    opt_col1, opt_col2, opt_col3 = st.columns(3)
    with opt_col1:
        st.markdown("**Position size above MA50 (%)**")
        pos_above_min = st.number_input("Min", 5, 30, 8, step=1, key=f"{symbol}_fw_pa_min")
        pos_above_max = st.number_input("Max", 5, 50, 12, step=1, key=f"{symbol}_fw_pa_max")
        pos_above_step = st.number_input("Step", 1, 10, 2, step=1, key=f"{symbol}_fw_pa_step")
    with opt_col2:
        st.markdown("**Profit target (%)**")
        pt_min = st.number_input("Min", 0.5, 10.0, 1.5, step=0.5, key=f"{symbol}_fw_pt_min", disabled=exit_mode != "Standard")
        pt_max = st.number_input("Max", 0.5, 20.0, 2.5, step=0.5, key=f"{symbol}_fw_pt_max", disabled=exit_mode != "Standard")
        pt_step = st.number_input("Step", 0.5, 5.0, 0.5, step=0.5, key=f"{symbol}_fw_pt_step", disabled=exit_mode != "Standard")
    with opt_col3:
        st.markdown("**Time stop (days)**")
        ts_min = st.number_input("Min", 1, 20, 2, step=1, key=f"{symbol}_fw_ts_min")
        ts_max = st.number_input("Max", 1, 20, 4, step=1, key=f"{symbol}_fw_ts_max")
        ts_step = st.number_input("Step", 1, 10, 1, step=1, key=f"{symbol}_fw_ts_step")

    pos_vals = list(np.arange(pos_above_min, pos_above_max + pos_above_step, pos_above_step) / 100)
    pt_vals = [ladder_max_pct] if use_ladder_exits else list(np.arange(pt_min, pt_max + pt_step, pt_step))
    ts_vals = list(range(int(ts_min), int(ts_max) + 1, int(ts_step)))
    n_combos = len(pos_vals) * len(pt_vals) * len(ts_vals)
    st.caption(f"**{n_combos} combinations** on the fixed study window for **{symbol}**.")
    if use_ladder_exits:
        st.info(
            f"Ladder mode sells 10% of the original position at each +{ladder_step_pct:.1f}% step "
            f"from +{ladder_step_pct:.1f}% to +{ladder_max_pct:.1f}%. Profit target inputs are ignored in this mode."
        )

    if st.button("▶ Run Fixed-Window Optimization", type="primary", key=f"{symbol}_fw_run_opt"):
        with st.spinner(f"Optimizing {symbol} on the fixed window ({n_combos} combos)…"):
            result = run_fixed_window_optimization(
                df,
                pos_vals,
                pt_vals,
                ts_vals,
                initial_capital,
                use_ladder_exits=use_ladder_exits,
                ladder_step_pct=ladder_step_pct,
                ladder_max_pct=ladder_max_pct,
                rule_builder=_build_fixed_window_rules,
            )
        st.session_state[f"fixed_window_opt_{symbol}"] = result

    opt_result = st.session_state.get(f"fixed_window_opt_{symbol}")
    if opt_result is not None:
        st.dataframe(opt_result, use_container_width=True)

        if not opt_result.empty:
            sc1, sc2 = st.columns(2)
            with sc1:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=opt_result["profit_target_%"],
                    y=opt_result["sharpe"],
                    mode="markers",
                    marker=dict(
                        size=8,
                        color=opt_result["above_MA50_%"],
                        colorscale="Blues",
                        showscale=True,
                        colorbar=dict(title="Above MA50 %"),
                    ),
                    text=opt_result["num_trades"],
                ))
                fig.update_layout(template="plotly_dark", title="Sharpe vs Profit Target")
                st.plotly_chart(fig, use_container_width=True)

            with sc2:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=opt_result["time_stop_days"],
                    y=opt_result["total_return_%"],
                    mode="markers",
                    marker=dict(
                        size=8,
                        color=opt_result["above_MA50_%"],
                        colorscale="Blues",
                        showscale=True,
                        colorbar=dict(title="Above MA50 %"),
                    ),
                    text=opt_result["num_trades"],
                ))
                fig2.update_layout(template="plotly_dark", title="Total Return vs Time Stop")
                st.plotly_chart(fig2, use_container_width=True)
