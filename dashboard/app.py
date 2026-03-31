"""Streamlit dashboard: rule builder + backtesting + results visualization."""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.fetcher import fetch_and_store, DEFAULT_SOURCE
from data.database import load_prices, list_symbols, list_bar_sizes, init_db
from backtest.simulator import run_backtest
from backtest.optimizer import run_optimization

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Trading Strategy Backtester", layout="wide")
st.title("Trading Strategy Backtester")

ENTRY_FIELDS = [
    "close", "open", "high", "low", "ma50", "ma200", "volume",
    "close_prev", "open_prev", "ma50_prev", "ma200_prev",
]
EXIT_FIELDS = ENTRY_FIELDS + ["position_return_pct", "days_held"]
OPS = [">", "<", ">=", "<=", "==", "crosses_above", "crosses_below"]

# ── Session state ─────────────────────────────────────────────────────────────
if "rules_by_symbol" not in st.session_state:
    st.session_state.rules_by_symbol = {}


def get_rules(sym: str) -> list:
    return st.session_state.rules_by_symbol.get(sym, [])

def set_rules(sym: str, rules: list):
    st.session_state.rules_by_symbol[sym] = rules


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Symbols")
    init_db()
    available = list_symbols()

    if available:
        symbol = st.selectbox("Active symbol", available,
                              index=available.index("SPY") if "SPY" in available else 0)
    else:
        symbol = "SPY"

    # Bar size selector — shows only sizes stored for this symbol
    stored_bar_sizes = list_bar_sizes(symbol) if available else ["1d"]
    bar_size = st.selectbox(
        "Bar size",
        options=["1d", "5m", "15m", "1h"],
        index=0 if "1d" in stored_bar_sizes else 0,
        help="Daily bars come from Yahoo Finance. Intraday bars require Polygon.",
    )
    if bar_size not in stored_bar_sizes:
        st.caption(f"No {bar_size} data for {symbol} yet — fetch it below.")

    st.divider()
    new_sym = st.text_input("Add / refresh symbol", placeholder="e.g. TQQQ, QQQ")
    fetch_source = st.selectbox(
        "Data source",
        ["yfinance (daily)", "polygon (intraday)"],
        help="yfinance for daily bars (free). Polygon for intraday (API key required).",
    )
    source_key = "yfinance" if "yfinance" in fetch_source else "polygon"

    if st.button("Fetch / Refresh", use_container_width=True):
        target = new_sym.upper().strip() if new_sym.strip() else symbol
        with st.spinner(f"Downloading {target} ({bar_size}) via {source_key}..."):
            try:
                fetch_and_store(target, bar_size=bar_size, source=source_key)
                st.success(f"{target} {bar_size} data updated.")
            except Exception as e:
                st.error(str(e))
        st.rerun()

    if available:
        st.caption(f"Stored: {', '.join(available)}")

    st.divider()
    st.header("Portfolio Settings")
    initial_capital = st.number_input("Starting Capital ($)", value=10_000, step=500)

    st.divider()
    st.header("Position Sizing")
    default_pos_pct = st.slider(
        "Default allocation per day (%)", min_value=1, max_value=50, value=10,
        help="% of total portfolio value deployed per entry signal. "
             "Scales automatically as your portfolio grows.",
    )
    daily_deploy = initial_capital * default_pos_pct / 100
    st.caption(
        f"At **${initial_capital:,.0f}** portfolio → **${daily_deploy:,.0f}** per trade today.\n\n"
        f"At **$15,000** → **${15_000 * default_pos_pct / 100:,.0f}** per trade."
    )

    st.divider()
    st.header("Date Range")
    start_date = st.date_input("From", value=pd.Timestamp("2010-01-01"))
    end_date   = st.date_input("To",   value=pd.Timestamp.today())


# ── Index Performance ─────────────────────────────────────────────────────────
st.header(f"{symbol} Performance ({bar_size})")

_df_index = load_prices(symbol, start=str(start_date), end=str(end_date), bar_size=bar_size)

if _df_index.empty:
    st.info(f"No {bar_size} data for {symbol}. Select the bar size and source above, then click Fetch.")
else:
    _first    = _df_index["close"].iloc[0]
    _last     = _df_index["close"].iloc[-1]
    _n_years  = len(_df_index) / 252
    _total_ret = (_last / _first - 1) * 100
    _ann_ret   = ((1 + _total_ret / 100) ** (1 / _n_years) - 1) * 100 if _n_years > 0 else 0
    _daily_ret = _df_index["close"].pct_change().dropna()
    _sharpe    = (_daily_ret.mean() / _daily_ret.std()) * np.sqrt(252) if _daily_ret.std() > 0 else 0
    _roll_max  = _df_index["close"].cummax()
    _max_dd    = ((_df_index["close"] - _roll_max) / _roll_max).min() * 100

    ic1, ic2, ic3, ic4 = st.columns(4)
    ic1.metric("Total Return",  f"{_total_ret:.1f}%")
    ic2.metric("Ann. Return",   f"{_ann_ret:.1f}%")
    ic3.metric("Sharpe Ratio",  f"{_sharpe:.2f}")
    ic4.metric("Max Drawdown",  f"{_max_dd:.1f}%")

    _fig_idx = make_subplots(rows=2, cols=1, shared_xaxes=True,
                              row_heights=[0.75, 0.25],
                              subplot_titles=(f"{symbol} Price", "Volume"))
    _fig_idx.add_trace(go.Scatter(
        x=_df_index["date"], y=_df_index["close"],
        line=dict(color="#90caf9", width=1.5), name="Close",
        fill="tozeroy", fillcolor="rgba(144,202,249,0.08)",
    ), row=1, col=1)
    _fig_idx.add_trace(go.Scatter(
        x=_df_index["date"], y=_df_index["ma50"],
        line=dict(color="#f39c12", width=1.5), name="MA50",
    ), row=1, col=1)
    _fig_idx.add_trace(go.Scatter(
        x=_df_index["date"], y=_df_index["ma200"],
        line=dict(color="#3498db", width=1.5), name="MA200",
    ), row=1, col=1)
    _fig_idx.add_trace(go.Bar(
        x=_df_index["date"], y=_df_index["volume"],
        marker_color="#546e7a", name="Volume", showlegend=False,
    ), row=2, col=1)
    _fig_idx.update_layout(height=480, template="plotly_dark",
                            xaxis_rangeslider_visible=False,
                            margin=dict(t=40, b=20))
    st.plotly_chart(_fig_idx, use_container_width=True)

st.divider()


# ── Helper: render one condition row ─────────────────────────────────────────
def condition_row(idx: int, rule_key: str, available_fields: list) -> dict:
    c1, c2, c3 = st.columns(3)
    with c1:
        left = st.selectbox(
            f"Left [{idx+1}]", available_fields,
            index=available_fields.index("close") if "close" in available_fields else 0,
            key=f"{rule_key}_left_{idx}",
        )
    with c2:
        op = st.selectbox(f"Operator [{idx+1}]", OPS, key=f"{rule_key}_op_{idx}")
    with c3:
        right_opts = available_fields + ["(enter value)"]
        right_sel = st.selectbox(
            f"Right [{idx+1}]", right_opts,
            index=available_fields.index("ma50") if "ma50" in available_fields else 0,
            key=f"{rule_key}_right_sel_{idx}",
        )
        if right_sel == "(enter value)":
            right = st.text_input(f"Value [{idx+1}]", value="0",
                                   key=f"{rule_key}_right_val_{idx}")
        else:
            right = right_sel
    return {"left": left, "op": op, "right": right}


# ── Rule Builder ──────────────────────────────────────────────────────────────
st.header(f"Rule Builder — {symbol}")
st.caption("Rules are saved per symbol. Switching symbols gives you a clean slate.")

tabs = st.tabs(["Add Entry Rule", "Add Exit Rule", "Presets"])

# Tab 1: Entry rule
with tabs[0]:
    st.markdown(
        "**Entry rules** decide when to open a new position each day. "
        "The first matching entry rule fires; use mutually exclusive conditions "
        "to handle different market states with different position sizes."
    )
    col1, col2 = st.columns(2)
    with col1:
        entry_label      = st.text_input("Rule name", value="Entry Rule", key="entry_label")
        entry_pos_pct    = st.slider("Position size (% of portfolio)", 1, 50, default_pos_pct,
                                      key="entry_pos_pct",
                                      help="Defaults to the sidebar value. Override here for this rule.")
        st.caption(f"= **${initial_capital * entry_pos_pct / 100:,.0f}** at current starting capital")
        entry_combinator = st.selectbox("Combine conditions with", ["AND", "OR"], key="entry_combinator")
    with col2:
        entry_num_conds = st.number_input("Number of conditions", 1, 5, 1, key="entry_num_conds")

    entry_conditions = [condition_row(i, "entry", ENTRY_FIELDS) for i in range(int(entry_num_conds))]

    if st.button("Add Entry Rule", key="btn_add_entry"):
        rules = get_rules(symbol)
        rules.append({
            "type": "entry", "label": entry_label,
            "combinator": entry_combinator,
            "position_pct": entry_pos_pct / 100,
            "conditions": entry_conditions,
        })
        set_rules(symbol, rules)
        st.success(f"Entry rule '{entry_label}' added to {symbol}.")

# Tab 2: Exit rule
with tabs[1]:
    st.markdown(
        "**Exit rules** are checked for every open position each day. "
        "A position closes as soon as any exit rule is satisfied. "
        "Use `position_return_pct` for profit/loss targets and `days_held` for time stops."
    )
    col1, col2 = st.columns(2)
    with col1:
        exit_label       = st.text_input("Rule name", value="Exit Rule", key="exit_label")
        exit_combinator  = st.selectbox("Combine conditions with", ["AND", "OR"], key="exit_combinator")
    with col2:
        exit_num_conds = st.number_input("Number of conditions", 1, 5, 1, key="exit_num_conds")

    exit_conditions = [condition_row(i, "exit", EXIT_FIELDS) for i in range(int(exit_num_conds))]

    if st.button("Add Exit Rule", key="btn_add_exit"):
        rules = get_rules(symbol)
        rules.append({
            "type": "exit", "label": exit_label,
            "combinator": exit_combinator,
            "conditions": exit_conditions,
        })
        set_rules(symbol, rules)
        st.success(f"Exit rule '{exit_label}' added to {symbol}.")

# Tab 3: Presets
with tabs[2]:
    st.markdown(f"Load a complete pre-built strategy for **{symbol}**. Replaces existing rules.")

    pcol1, pcol2 = st.columns(2)
    with pcol1:
        if st.button("MA50 Momentum", use_container_width=True):
            set_rules(symbol, [
                {"type": "entry", "label": "Above MA50 — 10%", "combinator": "AND",
                 "position_pct": 0.10,
                 "conditions": [{"left": "close", "op": ">", "right": "ma50"}]},
                {"type": "entry", "label": "Below MA50 — 5%", "combinator": "AND",
                 "position_pct": 0.05,
                 "conditions": [{"left": "close", "op": "<=", "right": "ma50"}]},
                {"type": "exit", "label": "2% profit target", "combinator": "AND",
                 "conditions": [{"left": "position_return_pct", "op": ">=", "right": "2"}]},
                {"type": "exit", "label": "3-day time stop", "combinator": "AND",
                 "conditions": [{"left": "days_held", "op": ">=", "right": "3"}]},
            ])
            st.success(f"MA50 Momentum loaded for {symbol}.")
            st.rerun()

    with pcol2:
        if st.button("Golden Cross / Death Cross", use_container_width=True):
            set_rules(symbol, [
                {"type": "entry", "label": "Golden Cross entry", "combinator": "AND",
                 "position_pct": 0.10,
                 "conditions": [{"left": "ma50", "op": "crosses_above", "right": "ma200"}]},
                {"type": "exit", "label": "Death Cross exit", "combinator": "AND",
                 "conditions": [{"left": "ma50", "op": "crosses_below", "right": "ma200"}]},
            ])
            st.success(f"Golden Cross loaded for {symbol}.")
            st.rerun()

    if st.button("Clear All Rules for this Symbol", type="secondary"):
        set_rules(symbol, [])
        st.rerun()


# ── Active rules display ──────────────────────────────────────────────────────
st.divider()
current_rules = get_rules(symbol)

if current_rules:
    st.subheader(f"Active Rules — {symbol}")
    entry_rules = [r for r in current_rules if r.get("type") == "entry"]
    exit_rules  = [r for r in current_rules if r.get("type") == "exit"]

    for section_label, section_rules, badge in [
        ("Entry Rules", entry_rules, "🟢"),
        ("Exit Rules",  exit_rules,  "🔴"),
    ]:
        if section_rules:
            st.markdown(f"**{section_label}**")
            for rule in section_rules:
                i = current_rules.index(rule)
                is_entry = rule.get("type") == "entry"

                cond_strs = [
                    f"`{c['left']}` **{c['op']}** `{c['right']}`"
                    for c in rule["conditions"]
                ]
                joined = f" _{rule['combinator']}_ ".join(cond_strs)

                if is_entry:
                    pct = rule["position_pct"]
                    usd = initial_capital * pct
                    col_r, col_edit, col_del = st.columns([5, 1, 1])
                    with col_r:
                        st.markdown(
                            f"{badge} **{rule['label']}** — "
                            f"**{pct*100:.0f}% = ${usd:,.0f}** per trade — {joined}"
                        )
                    with col_edit:
                        if st.button("Edit %", key=f"edit_{symbol}_{i}"):
                            st.session_state[f"editing_pct_{symbol}_{i}"] = True
                    with col_del:
                        if st.button("Remove", key=f"del_{symbol}_{i}"):
                            current_rules.pop(i)
                            set_rules(symbol, current_rules)
                            st.rerun()

                    # Inline % editor (shown when Edit % is clicked)
                    if st.session_state.get(f"editing_pct_{symbol}_{i}"):
                        new_pct = st.slider(
                            f"New position size for '{rule['label']}'",
                            min_value=1, max_value=50,
                            value=int(pct * 100),
                            key=f"new_pct_{symbol}_{i}",
                        )
                        st.caption(f"= **${initial_capital * new_pct / 100:,.0f}** at current capital · "
                                   f"**${15_000 * new_pct / 100:,.0f}** at $15,000")
                        sc1, sc2 = st.columns([1, 5])
                        with sc1:
                            if st.button("Save", key=f"save_pct_{symbol}_{i}", type="primary"):
                                current_rules[i]["position_pct"] = new_pct / 100
                                set_rules(symbol, current_rules)
                                del st.session_state[f"editing_pct_{symbol}_{i}"]
                                st.rerun()
                        with sc2:
                            if st.button("Cancel", key=f"cancel_pct_{symbol}_{i}"):
                                del st.session_state[f"editing_pct_{symbol}_{i}"]
                                st.rerun()
                else:
                    col_r, col_del = st.columns([6, 1])
                    with col_r:
                        st.markdown(f"{badge} **{rule['label']}** — {joined}")
                    with col_del:
                        if st.button("Remove", key=f"del_{symbol}_{i}"):
                            current_rules.pop(i)
                            set_rules(symbol, current_rules)
                            st.rerun()
else:
    st.info(f"No rules for {symbol} yet. Add rules above or load a preset.")


# ── Run Backtest ──────────────────────────────────────────────────────────────
st.divider()
run_col, _ = st.columns([1, 3])
with run_col:
    run_btn = st.button(f"Run Backtest — {symbol}", type="primary", use_container_width=True)

if run_btn:
    rules_to_run = get_rules(symbol)
    entry_rules  = [r for r in rules_to_run if r.get("type") == "entry"]
    exit_rules   = [r for r in rules_to_run if r.get("type") == "exit"]

    if not entry_rules:
        st.error("Add at least one Entry rule before running.")
    elif not exit_rules:
        st.error("Add at least one Exit rule before running.")
    else:
        df = load_prices(symbol, start=str(start_date), end=str(end_date), bar_size=bar_size)
        if df.empty:
            st.error(f"No {bar_size} data for {symbol}. Fetch it first.")
        else:
            with st.spinner(f"Running backtest for {symbol}..."):
                result = run_backtest(df, rules_to_run, initial_capital)

            m          = result["metrics"]
            equity     = result["equity_curve"]
            trades     = result["trades"]
            signals_df = result["signals_df"]

            # ── Metrics (with delta vs buy-and-hold) ──────────────────────
            st.subheader(f"Performance Summary — {symbol}")

            _ix_total = round(_total_ret, 2) if not _df_index.empty else None
            _ix_ann   = round(_ann_ret,   2) if not _df_index.empty else None
            _ix_dd    = round(_max_dd,    2) if not _df_index.empty else None

            mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
            mc1.metric("Total Return",  f"{m['total_return_pct']}%",
                       delta=f"{m['total_return_pct'] - _ix_total:.1f}% vs B&H" if _ix_total else None)
            mc2.metric("Ann. Return",   f"{m['annualized_return_pct']}%",
                       delta=f"{m['annualized_return_pct'] - _ix_ann:.1f}% vs B&H" if _ix_ann else None)
            mc3.metric("Sharpe Ratio",  m["sharpe_ratio"])
            mc4.metric("Max Drawdown",  f"{m['max_drawdown_pct']}%",
                       delta=f"{m['max_drawdown_pct'] - _ix_dd:.1f}% vs B&H" if _ix_dd else None,
                       delta_color="inverse")
            mc5.metric("# Trades",      m["num_trades"])
            mc6.metric("Win Rate",      f"{m['win_rate_pct']}%")

            # ── Price chart with signals ───────────────────────────────────
            st.subheader("Price Chart with Signals")
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                row_heights=[0.7, 0.3],
                                subplot_titles=("Price + MAs", "Volume"))

            fig.add_trace(go.Candlestick(
                x=df["date"], open=df["open"], high=df["high"],
                low=df["low"], close=df["close"], name="Price",
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
            ), row=1, col=1)
            fig.add_trace(go.Scatter(x=df["date"], y=df["ma50"],
                                     line=dict(color="#f39c12", width=1.5), name="MA50"), row=1, col=1)
            fig.add_trace(go.Scatter(x=df["date"], y=df["ma200"],
                                     line=dict(color="#3498db", width=1.5), name="MA200"), row=1, col=1)

            date_col   = df["date"].astype(str)
            buy_dates  = signals_df[signals_df["signal"].isin(["BUY", "BUY+SELL"])]["date"].astype(str)
            sell_dates = signals_df[signals_df["signal"].isin(["SELL", "BUY+SELL"])]["date"].astype(str)

            if not buy_dates.empty:
                buy_prices = df.loc[date_col.isin(buy_dates), "low"]
                fig.add_trace(go.Scatter(
                    x=buy_dates, y=buy_prices.values, mode="markers",
                    marker=dict(symbol="triangle-up", size=10, color="#00e676"), name="BUY",
                ), row=1, col=1)
            if not sell_dates.empty:
                sell_prices = df.loc[date_col.isin(sell_dates), "high"]
                fig.add_trace(go.Scatter(
                    x=sell_dates, y=sell_prices.values, mode="markers",
                    marker=dict(symbol="triangle-down", size=10, color="#ff1744"), name="SELL",
                ), row=1, col=1)

            fig.add_trace(go.Bar(x=df["date"], y=df["volume"],
                                  marker_color="#546e7a", showlegend=False), row=2, col=1)
            fig.update_layout(height=650, xaxis_rangeslider_visible=False,
                               template="plotly_dark", margin=dict(t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

            # ── Equity curve vs buy & hold (+ SPY benchmark) ──────────────
            st.subheader(f"Equity Curve vs Buy & Hold — {symbol}")
            bh_values = df["close"] * (initial_capital / df["close"].iloc[0])

            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=equity.index, y=equity.values,
                                      line=dict(color="#00e676", width=2), name="Strategy"))
            fig2.add_trace(go.Scatter(x=df["date"], y=bh_values.values,
                                      line=dict(color="#3498db", width=1.5, dash="dot"),
                                      name=f"{symbol} Buy & Hold"))

            # SPY benchmark (only add if the active symbol is not SPY)
            if symbol != "SPY":
                _df_spy = load_prices("SPY", start=str(start_date), end=str(end_date), bar_size=bar_size)
                if not _df_spy.empty:
                    spy_values = _df_spy["close"] * (initial_capital / _df_spy["close"].iloc[0])
                    fig2.add_trace(go.Scatter(
                        x=_df_spy["date"], y=spy_values.values,
                        line=dict(color="#ff9800", width=1.5, dash="dash"),
                        name="SPY (benchmark)",
                    ))

            fig2.update_layout(height=380, template="plotly_dark",
                                yaxis_title="Portfolio Value ($)", margin=dict(t=20, b=20))
            st.plotly_chart(fig2, use_container_width=True)

            # ── Trade table ───────────────────────────────────────────────
            if trades:
                st.subheader(f"Trade History ({len(trades)} trades)")
                trades_df = pd.DataFrame(trades)
                trades_df["pnl"]              = trades_df["pnl"].map(lambda x: f"${x:+.2f}")
                trades_df["return_pct"]       = trades_df["return_pct"].map(lambda x: f"{x:+.2f}%")
                trades_df["unallocated_capital"] = trades_df["unallocated_capital"].map(lambda x: f"${x:,.2f}")
                trades_df = trades_df.rename(columns={"unallocated_capital": "unallocated capital"})
                st.dataframe(trades_df, use_container_width=True, hide_index=True)
            else:
                st.info("No completed trades in this period.")


# ── Data Explorer ─────────────────────────────────────────────────────────────
st.divider()
st.header("Data Explorer")

with st.expander("Browse raw price data", expanded=False):
    from data.database import get_connection

    # Summary table across all 5-min symbols
    st.subheader("5-min Data Inventory")
    with get_connection() as _conn:
        _inv = pd.read_sql_query("""
            SELECT symbol,
                   COUNT(*)                          AS bars,
                   MIN(substr(date,1,10))            AS from_date,
                   MAX(substr(date,1,10))            AS to_date
            FROM prices
            WHERE bar_size = '5m'
            GROUP BY symbol
            ORDER BY symbol
        """, _conn)
    if _inv.empty:
        st.info("No 5-min data yet. Fetch symbols using Polygon in the sidebar.")
    else:
        _inv["bars"] = _inv["bars"].map(lambda x: f"{x:,}")
        st.dataframe(_inv, use_container_width=True, hide_index=True)

    st.divider()

    # Interactive browser
    st.subheader("Browse Bars")
    de_col1, de_col2, de_col3 = st.columns(3)
    with de_col1:
        de_sym = st.selectbox("Symbol", list_symbols(), key="de_sym")
    with de_col2:
        de_bar = st.selectbox("Bar size", list_bar_sizes(de_sym) if de_sym else ["1d"], key="de_bar")
    with de_col3:
        de_rows = st.selectbox("Rows to show", [100, 500, 1000, 5000, "All"], key="de_rows")

    de_start = st.date_input("From", value=pd.Timestamp("2024-01-01"), key="de_start")
    de_end   = st.date_input("To",   value=pd.Timestamp.today(),       key="de_end")

    if st.button("Load Data", key="de_load"):
        de_df = load_prices(de_sym, start=str(de_start), end=str(de_end), bar_size=de_bar)
        if de_df.empty:
            st.warning(f"No {de_bar} data for {de_sym} in that range.")
        else:
            st.caption(f"{len(de_df):,} bars loaded — {str(de_df['date'].iloc[0])[:16]} → {str(de_df['date'].iloc[-1])[:16]}")

            # Candlestick chart
            de_fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                    row_heights=[0.75, 0.25],
                                    subplot_titles=(f"{de_sym} ({de_bar})", "Volume"))
            de_fig.add_trace(go.Candlestick(
                x=de_df["date"], open=de_df["open"], high=de_df["high"],
                low=de_df["low"], close=de_df["close"], name="Price",
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
            ), row=1, col=1)
            if de_df["ma50"].notna().any():
                de_fig.add_trace(go.Scatter(x=de_df["date"], y=de_df["ma50"],
                                             line=dict(color="#f39c12", width=1), name="MA50"), row=1, col=1)
            if de_df["ma200"].notna().any():
                de_fig.add_trace(go.Scatter(x=de_df["date"], y=de_df["ma200"],
                                             line=dict(color="#3498db", width=1), name="MA200"), row=1, col=1)
            de_fig.add_trace(go.Bar(x=de_df["date"], y=de_df["volume"],
                                     marker_color="#546e7a", showlegend=False), row=2, col=1)
            de_fig.update_layout(height=500, template="plotly_dark",
                                  xaxis_rangeslider_visible=False, margin=dict(t=30, b=20))
            st.plotly_chart(de_fig, use_container_width=True)

            # Raw table
            show_n = len(de_df) if de_rows == "All" else int(de_rows)
            display_df = de_df.head(show_n).copy()
            display_df["date"] = display_df["date"].astype(str).str[:19]
            for col in ["open", "high", "low", "close"]:
                display_df[col] = display_df[col].map(lambda x: f"{x:.2f}")
            display_df["volume"] = display_df["volume"].map(lambda x: f"{int(x):,}")
            st.dataframe(display_df[["date","open","high","low","close","volume","ma50","ma200"]],
                         use_container_width=True, hide_index=True)


# ── Strategy Optimizer ────────────────────────────────────────────────────────
st.divider()
st.header("Strategy Optimizer")
st.caption(
    "Sweeps combinations of position size, profit target, and time stop for the "
    "MA50 Momentum strategy across both SPY and SSO. Ranks results by Sharpe ratio."
)

with st.expander("Configure Parameter Ranges", expanded=True):
    oc1, oc2, oc3 = st.columns(3)

    with oc1:
        st.markdown("**Above-MA50 position size (%)**")
        above_min  = st.number_input("Min",  value=8,  step=1, key="opt_above_min")
        above_max  = st.number_input("Max",  value=15, step=1, key="opt_above_max")
        above_step = st.number_input("Step", value=2,  step=1, key="opt_above_step")

    with oc2:
        st.markdown("**Profit target (%)**")
        profit_min  = st.number_input("Min",  value=1.5, step=0.5, key="opt_profit_min")
        profit_max  = st.number_input("Max",  value=3.5, step=0.5, key="opt_profit_max")
        profit_step = st.number_input("Step", value=0.5, step=0.5, key="opt_profit_step")

    with oc3:
        st.markdown("**Time stop (days)**")
        days_min  = st.number_input("Min",  value=2, step=1, key="opt_days_min")
        days_max  = st.number_input("Max",  value=5, step=1, key="opt_days_max")
        days_step = st.number_input("Step", value=1, step=1, key="opt_days_step")

    all_stored = list_symbols(bar_size=bar_size)
    opt_symbols = st.multiselect(
        "Symbols to optimise",
        options=all_stored,
        default=[s for s in ["SPY", "SSO", "SPXL"] if s in all_stored],
        help="Only symbols with data for the selected bar size are shown.",
    )

    st.caption("Below-MA50 position size is automatically set to half the above-MA50 value.")

    # Preview combo count
    _above_vals  = list(np.arange(above_min,  above_max  + above_step  * 0.01, above_step))
    _profit_vals = list(np.arange(profit_min, profit_max + profit_step * 0.01, profit_step))
    _days_vals   = list(range(int(days_min), int(days_max) + 1, max(1, int(days_step))))
    _n_combos    = len(_above_vals) * len(_profit_vals) * len(_days_vals)
    _n_syms      = len(opt_symbols)
    st.info(f"**{_n_combos} combinations** × **{_n_syms} symbol{'s' if _n_syms != 1 else ''}** = **{_n_combos * _n_syms} backtests**")

opt_col, _ = st.columns([1, 3])
with opt_col:
    opt_btn = st.button("Run Optimization", type="primary", use_container_width=True)

if opt_btn:
    above_vals  = [round(v / 100, 4) for v in
                   np.arange(above_min, above_max + above_step * 0.01, above_step)]
    profit_vals = [round(v, 2) for v in
                   np.arange(profit_min, profit_max + profit_step * 0.01, profit_step)]
    days_vals   = list(range(int(days_min), int(days_max) + 1, max(1, int(days_step))))

    n_combos = len(above_vals) * len(profit_vals) * len(days_vals)
    if not opt_symbols:
        st.error("Select at least one symbol to optimise.")
    elif n_combos < 10:
        st.warning(f"Only {n_combos} combinations — widen the ranges to get at least 10.")
    else:
        results = {}
        for sym in opt_symbols:
            df_sym = load_prices(sym, start=str(start_date), end=str(end_date), bar_size=bar_size)
            if df_sym.empty:
                st.warning(f"No data for {sym} — skipping.")
                continue
            with st.spinner(f"Running {n_combos} backtests for {sym}..."):
                results[sym] = run_optimization(
                    df_sym, above_vals, profit_vals, days_vals, initial_capital
                )

        if not results:
            st.error("No data available for any symbol.")
        else:
            # ── Results tabs per symbol ───────────────────────────────────
            sym_tabs = st.tabs(list(results.keys()) + ["Head-to-Head"])

            for tab, (sym, df_res) in zip(sym_tabs, results.items()):
                with tab:
                    st.subheader(f"Top 10 — {sym}")

                    # Highlight best row per metric
                    best_sharpe  = df_res["sharpe"].idxmax()
                    best_return  = df_res["total_return_%"].idxmax()
                    best_dd      = df_res["max_drawdown_%"].idxmax()  # least negative = max

                    st.markdown(
                        f"Best Sharpe: row **#{best_sharpe}** &nbsp;|&nbsp; "
                        f"Best Total Return: row **#{best_return}** &nbsp;|&nbsp; "
                        f"Smallest Drawdown: row **#{best_dd}**"
                    )

                    # Display top 10 by Sharpe (already sorted)
                    top10 = df_res.head(10).copy()
                    top10["final_value"] = top10["final_value"].map(lambda x: f"${x:,.0f}")
                    top10["max_drawdown_%"] = top10["max_drawdown_%"].map(lambda x: f"{x:.1f}%")
                    top10["total_return_%"] = top10["total_return_%"].map(lambda x: f"{x:.1f}%")
                    top10["ann_return_%"]   = top10["ann_return_%"].map(lambda x: f"{x:.1f}%")
                    top10["win_rate_%"]     = top10["win_rate_%"].map(lambda x: f"{x:.1f}%")
                    st.dataframe(top10, use_container_width=True)

                    st.subheader("Sharpe vs Total Return (all scenarios)")
                    fig_scatter = go.Figure()
                    fig_scatter.add_trace(go.Scatter(
                        x=df_res["total_return_%"],
                        y=df_res["sharpe"],
                        mode="markers",
                        marker=dict(
                            size=8,
                            color=df_res["profit_target_%"],
                            colorscale="Viridis",
                            showscale=True,
                            colorbar=dict(title="Profit target %"),
                        ),
                        text=[
                            f"above={r['above_MA50_%']}% | profit={r['profit_target_%']}% | days={r['time_stop_days']}"
                            for _, r in df_res.iterrows()
                        ],
                        hoverinfo="text+x+y",
                    ))
                    # Mark best Sharpe
                    br = df_res.loc[best_sharpe]
                    fig_scatter.add_trace(go.Scatter(
                        x=[br["total_return_%"]], y=[br["sharpe"]],
                        mode="markers",
                        marker=dict(size=14, color="#ff1744", symbol="star"),
                        name="Best Sharpe",
                    ))
                    fig_scatter.update_layout(
                        height=400, template="plotly_dark",
                        xaxis_title="Total Return (%)",
                        yaxis_title="Sharpe Ratio",
                        margin=dict(t=20, b=40),
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)

            # ── Head-to-Head tab ──────────────────────────────────────────
            with sym_tabs[-1]:
                if len(results) < 2:
                    st.info("Need at least 2 symbols to show head-to-head comparison.")
                else:
                    st.subheader("Best scenario per metric — " + " vs ".join(results.keys()))

                    metric_defs = [
                        ("Highest Sharpe",      "sharpe",         "idxmax"),
                        ("Highest Total Return", "total_return_%", "idxmax"),
                        ("Smallest Drawdown",    "max_drawdown_%", "idxmax"),
                        ("Best Win Rate",        "win_rate_%",     "idxmax"),
                    ]

                    for metric_label, col, agg in metric_defs:
                        st.markdown(f"**{metric_label}**")
                        cols = st.columns(len(results))
                        for (sym, df_res), col_widget in zip(results.items(), cols):
                            idx = getattr(df_res[col], agg)()
                            row = df_res.loc[idx]
                            with col_widget:
                                st.markdown(f"**{sym}**")
                                st.json({
                                    "above_MA50_%":    int(row["above_MA50_%"]),
                                    "below_MA50_%":    int(row["below_MA50_%"]),
                                    "profit_target_%": row["profit_target_%"],
                                    "time_stop_days":  int(row["time_stop_days"]),
                                    col:               round(row[col], 3),
                                })
                        st.divider()
