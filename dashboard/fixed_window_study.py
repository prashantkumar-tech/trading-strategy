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

BAR_SIZE = "5m"
REGULAR_OPEN_MINUTE = 9 * 60 + 30
REGULAR_CLOSE_MINUTE = 15 * 60 + 55
LADDER_MODES = {
    "Ladder 0.3% to 3.0%": (0.3, 3.0),
    "Ladder 0.4% to 4.0%": (0.4, 4.0),
    "Ladder 0.5% to 5.0%": (0.5, 5.0),
    "Ladder 0.6% to 6.0%": (0.6, 6.0),
}
TIME_STOP_SCHEDULES = {
    "Day 3": [3],
    "Days 3 and 5": [3, 5],
    "Days 3, 4, and 5": [3, 4, 5],
}
TIME_STOP_SELL_TIMES = {
    "9:45 AM ET": 9 * 60 + 45,
    "1:00 PM ET": 13 * 60,
    "3:40 PM ET": 15 * 60 + 40,
}


def _build_fixed_window_rules(
    pos_pct_above: float,
    pos_pct_below: float,
    profit_target: float,
    time_stop_schedule: str,
    time_stop_sell_time: str = "3:40 PM ET",
    **_: dict,
) -> list:
    schedule_days = TIME_STOP_SCHEDULES[time_stop_schedule]
    return [
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
            "conditions": [{"left": "days_held", "op": ">=", "right": str(schedule_days[0])}],
            "schedule_days": schedule_days,
            "schedule_label": time_stop_schedule,
            "schedule_sell_minute": TIME_STOP_SELL_TIMES[time_stop_sell_time],
            "schedule_sell_time_label": time_stop_sell_time,
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
    matrix = matrix[ordered]

    total_buy_amt = pd.to_numeric(matrix["Buy Amt"], errors="coerce").sum()
    total_pnl = pd.to_numeric(matrix["PnL $"], errors="coerce").sum()
    total_qty = pd.to_numeric(matrix["QTY"], errors="coerce").sum()
    total_pnl_pct = (total_pnl / total_buy_amt * 100) if total_buy_amt else 0.0

    total_row = {col: None for col in matrix.columns}
    total_row["Date"] = "TOTAL"
    total_row["QTY"] = round(total_qty, 2)
    total_row["Buy Amt"] = round(total_buy_amt, 2)
    total_row["Note"] = "Aggregate realized/unrealized exits"
    total_row["PnL $"] = round(total_pnl, 2)
    total_row["PnL %"] = round(total_pnl_pct, 2)

    return pd.concat([matrix, pd.DataFrame([total_row])], ignore_index=True)


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


def _add_prev_day_close(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach prev_day_close to every bar using the full (including lookback) dataset
    so the first bar of the study window has a valid previous-day close.
    """
    df = df.copy()
    ts = pd.to_datetime(df["date"])
    date_only = ts.dt.date
    signal_close_source = "signal_day_close" if "signal_day_close" in df.columns else "close"
    daily_close = (
        df.assign(_d=date_only)
        .groupby("_d", sort=True)[signal_close_source]
        .first()
    )
    prev_close_map = daily_close.shift(1)
    df["prev_day_close"] = date_only.map(prev_close_map)
    return df


def _load_study_prices(
    symbol: str,
    reference_symbol: str,
    window_start: str,
    window_end: str,
    data_source: str,
) -> pd.DataFrame:
    lookback_start = (pd.Timestamp(window_start) - pd.Timedelta(days=120)).strftime("%Y-%m-%d")
    base_df = load_prices(
        symbol,
        start=lookback_start,
        end=window_end,
        bar_size=BAR_SIZE,
        source=data_source,
    ).copy()
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
        aligned = _add_prev_day_close(aligned)
        return aligned[(aligned["date"] >= window_start) & (aligned["date"] <= f"{window_end} 23:59:59")].reset_index(drop=True)

    ref_df = load_prices(
        reference_symbol,
        start=lookback_start,
        end=window_end,
        bar_size=BAR_SIZE,
        source=data_source,
    )
    if ref_df.empty:
        raise ValueError(
            f"No {BAR_SIZE} {data_source} data for reference symbol {reference_symbol} in the selected study window."
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
    merged = _add_prev_day_close(merged)
    merged = merged[(merged["date"] >= window_start) & (merged["date"] <= f"{window_end} 23:59:59")].reset_index(drop=True)
    return merged


def _build_source_comparison(
    symbol: str,
    reference_symbol: str,
    window_start: str,
    window_end: str,
    rules: list,
    initial_capital: float,
    use_ladder_exits: bool,
    ladder_step_pct: float,
    ladder_max_pct: float,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    outputs = {}
    for source in ["polygon", "twelve_data"]:
        df = _load_study_prices(symbol, reference_symbol, window_start, window_end, source)
        result = run_fixed_window_backtest(
            df,
            rules,
            initial_capital=initial_capital,
            use_ladder_exits=use_ladder_exits,
            ladder_step_pct=ladder_step_pct,
            ladder_max_pct=ladder_max_pct,
        )
        trades = pd.DataFrame(result["trades"])
        if trades.empty:
            daily = pd.DataFrame(columns=["entry_day", "fills", "pnl", "entry_price", "last_exit", "exit_rules"])
        else:
            trades["entry_day"] = pd.to_datetime(trades["entry_date"]).dt.strftime("%Y-%m-%d")
            daily = trades.groupby("entry_day", as_index=False).agg(
                fills=("exit_rule", "count"),
                pnl=("pnl", "sum"),
                entry_price=("entry_price", "first"),
                last_exit=("exit_date", "max"),
                exit_rules=("exit_rule", lambda s: "; ".join(s.astype(str).tolist()[:6]) + (" ..." if len(s) > 6 else "")),
            )
        outputs[source] = {"df": df, "result": result, "trades": trades, "daily": daily}

    poly = outputs["polygon"]["daily"].rename(
        columns={"fills": "poly_fills", "pnl": "poly_pnl", "entry_price": "poly_entry", "last_exit": "poly_last_exit", "exit_rules": "poly_rules"}
    )
    td = outputs["twelve_data"]["daily"].rename(
        columns={"fills": "td_fills", "pnl": "td_pnl", "entry_price": "td_entry", "last_exit": "td_last_exit", "exit_rules": "td_rules"}
    )
    day_compare = poly.merge(td, on="entry_day", how="outer", indicator=True).sort_values("entry_day").reset_index(drop=True)
    day_compare["pnl_diff"] = day_compare["td_pnl"].fillna(0) - day_compare["poly_pnl"].fillna(0)

    poly_days = sorted(pd.to_datetime(outputs["polygon"]["df"]["date"]).dt.strftime("%Y-%m-%d").unique())
    td_days = sorted(pd.to_datetime(outputs["twelve_data"]["df"]["date"]).dt.strftime("%Y-%m-%d").unique())
    common_days = sorted(set(poly_days) & set(td_days))
    fill_compare = pd.DataFrame({"day": common_days})
    fill_compare["polygon_filled"] = fill_compare["day"].isin(set(outputs["polygon"]["daily"]["entry_day"]))
    fill_compare["twelve_data_filled"] = fill_compare["day"].isin(set(outputs["twelve_data"]["daily"]["entry_day"]))

    return outputs, day_compare, fill_compare


def render_fixed_window_study(
    symbol: str,
    title: str,
    default_reference_symbol: str,
    data_source: str = "polygon",
) -> None:
    init_db()

    default_window_start = pd.Timestamp("2025-10-02").date()
    default_window_end = pd.Timestamp("2025-12-19").date()
    max_window_end = pd.Timestamp.today().date()
    min_window_start = (pd.Timestamp.today() - pd.Timedelta(days=365 * 5)).date()

    st.title(title)
    st.caption("Runs both a single backtest and a parameter sweep on a user-selected 5-minute window.")
    st.caption(f"Data source: **{data_source}**")
    st.info(
        "Entry logic is fixed for this study: buy at 09:45 ET when the regular-session open is above the previous day's close. "
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
        window_range = st.date_input(
            "Study Window",
            value=(default_window_start, default_window_end),
            min_value=min_window_start,
            max_value=max_window_end,
            key=f"{symbol}_fw_window_range",
        )
        st.info(f"Bar size is fixed to `{BAR_SIZE}`.")

    if isinstance(window_range, tuple) and len(window_range) == 2:
        window_start_date, window_end_date = window_range
    else:
        window_start_date, window_end_date = default_window_start, default_window_end

    if window_start_date > window_end_date:
        st.error("Study window start must be on or before the end date.")
        st.stop()

    if window_start_date < min_window_start or window_end_date > max_window_end:
        st.error(f"Study window must stay within the last five years: {min_window_start} to {max_window_end}.")
        st.stop()

    window_start = str(window_start_date)
    window_end = str(window_end_date)
    window_key = f"{symbol}_{data_source}_{window_start}_{window_end}"

    use_ladder_exits = exit_mode != "Standard"
    ladder_step_pct, ladder_max_pct = LADDER_MODES.get(exit_mode, (0.4, 4.0))

    try:
        df = _load_study_prices(symbol, reference_symbol or symbol, window_start, window_end, data_source)
    except ValueError as e:
        st.error(str(e))
        st.info(f"Fetch `{reference_symbol}` {BAR_SIZE} data from `{data_source}` first, then reload this page.")
        st.stop()

    if df.empty:
        st.error(f"No {BAR_SIZE} {data_source} data for {symbol} in the selected study window.")
        st.stop()

    st.success(
        f"Loaded **{len(df):,}** bars for **{symbol}** from **{df['date'].min()}** to **{df['date'].max()}**."
    )
    st.info(f"`ma50` is sourced from **{reference_symbol or symbol}** for this study.")
    st.caption(f"Window: **{window_start}** to **{window_end}**")

    st.divider()

    st.subheader("Single Backtest")
    bt_col1, bt_col2, bt_col3, bt_col4 = st.columns(4)
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
        bt_time_stop = st.selectbox(
            "Time Stop Schedule",
            options=list(TIME_STOP_SCHEDULES.keys()),
            index=0,
            key=f"{symbol}_fw_bt_time_stop",
        )
    with bt_col4:
        bt_time_stop_sell_time = st.selectbox(
            "Sell Time",
            options=list(TIME_STOP_SELL_TIMES.keys()),
            index=1,
            key=f"{symbol}_fw_bt_time_stop_sell_time",
        )

    if st.button("▶ Run Fixed-Window Backtest", type="primary", key=f"{symbol}_fw_run_bt"):
        rules = _build_fixed_window_rules(
            bt_pos_above / 100,
            (bt_pos_above / 2) / 100,
            bt_profit_target,
            bt_time_stop,
            bt_time_stop_sell_time,
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
        st.session_state[f"fixed_window_backtest_{window_key}"] = result

    bt_result = st.session_state.get(f"fixed_window_backtest_{window_key}")
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

    st.subheader("Source Comparison")
    st.caption("Runs the same fixed-window strategy on both Polygon and Twelve Data and compares day-wise fills and P&L.")

    if st.button("▶ Run Source Comparison", key=f"{symbol}_fw_run_compare"):
        compare_rules = _build_fixed_window_rules(
            bt_pos_above / 100,
            (bt_pos_above / 2) / 100,
            bt_profit_target,
            bt_time_stop,
            bt_time_stop_sell_time,
        )
        try:
            with st.spinner("Running Polygon vs Twelve Data comparison…"):
                compare_outputs, day_compare, fill_compare = _build_source_comparison(
                    symbol=symbol,
                    reference_symbol=reference_symbol or symbol,
                    window_start=window_start,
                    window_end=window_end,
                    rules=compare_rules,
                    initial_capital=initial_capital,
                    use_ladder_exits=use_ladder_exits,
                    ladder_step_pct=ladder_step_pct,
                    ladder_max_pct=ladder_max_pct,
                )
            st.session_state[f"fixed_window_compare_{window_key}"] = (compare_outputs, day_compare, fill_compare)
        except ValueError as e:
            st.error(str(e))

    compare_state = st.session_state.get(f"fixed_window_compare_{window_key}")
    if compare_state:
        compare_outputs, day_compare, fill_compare = compare_state
        c1, c2, c3, c4 = st.columns(4)
        poly_metrics = compare_outputs["polygon"]["result"]["metrics"]
        td_metrics = compare_outputs["twelve_data"]["result"]["metrics"]
        c1.metric("Polygon Return", f"{poly_metrics['total_return_pct']:.2f}%")
        c2.metric("Twelve Data Return", f"{td_metrics['total_return_pct']:.2f}%")
        c3.metric("Polygon Trades", f"{poly_metrics['num_trades']}")
        c4.metric("Twelve Data Trades", f"{td_metrics['num_trades']}")

        eq_fig = go.Figure()
        poly_ec = compare_outputs["polygon"]["result"]["equity_curve"]
        td_ec = compare_outputs["twelve_data"]["result"]["equity_curve"]
        eq_fig.add_trace(go.Scatter(x=poly_ec.index, y=poly_ec.values, name="Polygon", line=dict(color="#38bdf8", width=1.6)))
        eq_fig.add_trace(go.Scatter(x=td_ec.index, y=td_ec.values, name="Twelve Data", line=dict(color="#22c55e", width=1.6)))
        eq_fig.update_layout(height=320, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="Portfolio Value ($)")
        st.plotly_chart(eq_fig, use_container_width=True)

        fill_summary = (
            fill_compare.groupby(["polygon_filled", "twelve_data_filled"])
            .size()
            .reset_index(name="days")
            .sort_values(["polygon_filled", "twelve_data_filled"])
        )
        fs_col1, fs_col2 = st.columns(2)
        with fs_col1:
            st.markdown("**Fill Status Summary**")
            st.dataframe(fill_summary, use_container_width=True, hide_index=True)
        with fs_col2:
            st.markdown("**Day-wise P&L Differences**")
            both = day_compare[day_compare["_merge"] == "both"].copy()
            if not both.empty:
                pnl_fig = go.Figure()
                pnl_fig.add_trace(go.Bar(x=both["entry_day"], y=both["pnl_diff"], marker_color="#f59e0b", name="TD - Polygon"))
                pnl_fig.update_layout(height=260, template="plotly_dark", margin=dict(t=20, b=20), yaxis_title="PnL Diff ($)")
                st.plotly_chart(pnl_fig, use_container_width=True)

        st.markdown("**Day-wise Comparison Table**")
        display = day_compare.copy()
        st.dataframe(
            display[
                [
                    "entry_day",
                    "_merge",
                    "poly_fills",
                    "td_fills",
                    "poly_pnl",
                    "td_pnl",
                    "pnl_diff",
                    "poly_entry",
                    "td_entry",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    st.subheader("Fixed-Window Optimizer")
    opt_col1, opt_col2, opt_col3, opt_col4 = st.columns(4)
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
        st.markdown("**Time stop schedules**")
        ts_vals = st.multiselect(
            "Schedules",
            options=list(TIME_STOP_SCHEDULES.keys()),
            default=list(TIME_STOP_SCHEDULES.keys()),
            key=f"{symbol}_fw_ts_vals",
        )
    with opt_col4:
        st.markdown("**Sell time**")
        ts_sell_times = st.multiselect(
            "Times",
            options=list(TIME_STOP_SELL_TIMES.keys()),
            default=list(TIME_STOP_SELL_TIMES.keys()),
            key=f"{symbol}_fw_ts_sell_times",
        )

    pos_vals = list(np.arange(pos_above_min, pos_above_max + pos_above_step, pos_above_step) / 100)
    pt_vals = [ladder_max_pct] if use_ladder_exits else list(np.arange(pt_min, pt_max + pt_step, pt_step))
    time_stop_options = [(schedule, sell_time) for schedule in ts_vals for sell_time in ts_sell_times]
    n_combos = len(pos_vals) * len(pt_vals) * len(time_stop_options)
    st.caption(f"**{n_combos} combinations** on the selected study window for **{symbol}**.")
    if use_ladder_exits:
        st.info(
            f"Ladder mode sells 10% of the original position at each +{ladder_step_pct:.1f}% step "
            f"from +{ladder_step_pct:.1f}% to +{ladder_max_pct:.1f}%. Profit target inputs are ignored in this mode."
        )

    if st.button("▶ Run Fixed-Window Optimization", type="primary", key=f"{symbol}_fw_run_opt"):
        with st.spinner(f"Optimizing {symbol} on the fixed window ({n_combos} combos)…"):
            result = run_fixed_window_optimization(
                df=df,
                pos_pct_above_values=pos_vals,
                profit_target_values=pt_vals,
                time_stop_values=ts_vals,
                initial_capital=initial_capital,
                time_stop_sell_times=time_stop_options,
                use_ladder_exits=use_ladder_exits,
                ladder_step_pct=ladder_step_pct,
                ladder_max_pct=ladder_max_pct,
                rule_builder=_build_fixed_window_rules,
            )
        st.session_state[f"fixed_window_opt_{window_key}"] = result

    opt_result = st.session_state.get(f"fixed_window_opt_{window_key}")
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
