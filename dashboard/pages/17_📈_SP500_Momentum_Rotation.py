"""S&P 500 Momentum Rotation — Streamlit page."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.database import list_symbols, load_prices
from data.sp500_universe import get_sp500_tickers, get_sp500_meta
from backtest.momentum_rotation import run_momentum_rotation, momentum_leaderboard
from backtest.metrics import yearly_performance

st.set_page_config(page_title="S&P 500 Momentum Rotation", page_icon="📈", layout="wide")
st.title("📈 S&P 500 Momentum Rotation")

# ── Sidebar controls ──────────────────────────────────────────────────────
st.sidebar.header("Strategy settings")
top_n = st.sidebar.number_input("Basket size (top N)", 1, 50, 10)
buffer_rank = st.sidebar.number_input("Sell buffer (exit when rank >)", min_value=int(top_n), max_value=100, value=max(20, int(top_n)))
lookback_days = st.sidebar.number_input("Lookback (trading days)", 20, 400, 252)
skip_days = st.sidebar.number_input("Skip recent (trading days)", 0, 60, 21)
initial_capital = st.sidebar.number_input("Starting capital ($)", 1000, 10_000_000, 10_000, step=1000)
start = st.sidebar.text_input("Start date (YYYY-MM-DD)", "2015-01-01")
end = st.sidebar.text_input("End date (YYYY-MM-DD)", "")

available = list_symbols(bar_size="1d", source="yfinance")
try:
    constituents = set(get_sp500_tickers())
    symbols = sorted((set(available) & constituents) - {"SPY"})
except Exception as e:
    st.warning(f"Could not load the S&P 500 constituent list ({e}); "
               f"falling back to all daily symbols in the database (may include non-constituents).")
    symbols = sorted(set(available) - {"SPY"})
st.caption(f"{len(symbols)} daily symbols available in the database.")

# ── Momentum leaderboard ──────────────────────────────────────────────────
st.subheader("📊 Momentum leaderboard")
st.caption("Top names ranked by 12-1 momentum as of the latest available date. "
           "`above_200ma` = passes the strategy's trend filter (only these are eligible to be held).")
leaderboard_n = st.number_input("Show top N", 5, 100, 20, key="leaderboard_n")
if st.button("Show leaderboard"):
    if not symbols:
        st.error("No symbols in the DB — fetch the S&P 500 universe first "
                 "(`python3 -m data.fetch_universe`).")
    else:
        with st.spinner(f"Ranking {len(symbols)} symbols..."):
            try:
                try:
                    meta = get_sp500_meta()
                except Exception:
                    meta = None
                board = momentum_leaderboard(
                    symbols, start=start or None, end=end or None,
                    top_n=int(leaderboard_n),
                    lookback_days=int(lookback_days), skip_days=int(skip_days),
                    meta=meta,
                )
            except ValueError as e:
                st.error(str(e))
                st.stop()
        eligible_count = int(board["above_200ma"].sum())
        st.caption(f"{eligible_count} of the top {len(board)} pass the 200-day MA filter.")
        st.dataframe(board, use_container_width=True, hide_index=True)

st.divider()

if st.button("Run backtest", type="primary"):
    if len(symbols) < top_n:
        st.error(f"Only {len(symbols)} symbols in the DB — fetch the S&P 500 universe first "
                 f"(`python3 -m data.fetch_universe`).")
        st.stop()

    with st.spinner(f"Ranking {len(symbols)} symbols..."):
        try:
            result = run_momentum_rotation(
                symbols, start=start or None, end=end or None,
                top_n=int(top_n), buffer_rank=int(buffer_rank),
                lookback_days=int(lookback_days), skip_days=int(skip_days),
                initial_capital=float(initial_capital),
            )
        except ValueError as e:
            st.error(str(e))
            st.stop()

    m = result["metrics"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total return", f"{m['total_return_pct']}%")
    c2.metric("Annualized", f"{m['annualized_return_pct']}%")
    c3.metric("Sharpe", m["sharpe_ratio"])
    c4.metric("Max drawdown", f"{m['max_drawdown_pct']}%")

    # SPY buy & hold, normalized to the same starting capital (reused below).
    eq = result["equity_curve"]
    spy = load_prices("SPY", start=start or None, end=end or None, bar_size="1d", source="yfinance")
    spy_bh = None
    if not spy.empty:
        spy_close = spy.set_index("date")["close"]
        spy_bh = spy_close / spy_close.iloc[0] * float(initial_capital)

    # Equity curve vs SPY buy & hold
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=eq.index, y=eq.values, name="Strategy"))
    if spy_bh is not None:
        fig.add_trace(go.Scatter(x=spy_bh.index, y=spy_bh.values, name="SPY buy & hold"))
    fig.update_layout(title="Equity curve", xaxis_title="Date", yaxis_title="Portfolio value ($)")
    st.plotly_chart(fig, use_container_width=True)

    # ── Yearly performance vs S&P 500 ──────────────────────────────────────
    st.subheader("Yearly performance vs S&P 500")
    strat_yp = yearly_performance(eq).rename(
        columns={"return_pct": "strategy_pct", "pnl": "strategy_pnl"})
    comp = strat_yp[["year", "strategy_pct", "strategy_pnl"]].copy()
    if spy_bh is not None:
        spy_yp = yearly_performance(spy_bh).rename(
            columns={"return_pct": "spy_pct", "pnl": "spy_pnl"})
        comp = comp.merge(spy_yp[["year", "spy_pct", "spy_pnl"]], on="year", how="left")
        comp["outperformance_pp"] = (comp["strategy_pct"] - comp["spy_pct"]).round(2)

    if comp.empty:
        st.info("Not enough history to compute yearly performance for this window.")
    else:
        ybar = go.Figure()
        ybar.add_trace(go.Bar(x=comp["year"], y=comp["strategy_pct"], name="Strategy"))
        if "spy_pct" in comp.columns:
            ybar.add_trace(go.Bar(x=comp["year"], y=comp["spy_pct"], name="SPY buy & hold"))
        ybar.update_layout(barmode="group", title="Annual return by year",
                           xaxis_title="Year", yaxis_title="Return (%)")
        ybar.update_xaxes(type="category")
        st.plotly_chart(ybar, use_container_width=True)

        rename = {
            "year": "Year",
            "strategy_pct": "Strategy %",
            "spy_pct": "SPY %",
            "outperformance_pp": "Outperformance (pp)",
            "strategy_pnl": "Strategy P&L ($)",
            "spy_pnl": "SPY P&L ($)",
        }
        order = [c for c in ["year", "strategy_pct", "spy_pct", "outperformance_pp",
                             "strategy_pnl", "spy_pnl"] if c in comp.columns]
        st.dataframe(comp[order].rename(columns=rename),
                     use_container_width=True, hide_index=True)

    st.subheader("Current basket")
    st.write(result["holdings"])

    st.subheader("Rotation / trade log")
    trades_df = pd.DataFrame(result["trades"])
    if trades_df.empty:
        st.info("No completed round-trip trades in this window.")
    else:
        st.dataframe(trades_df, use_container_width=True)
else:
    st.info("Set parameters in the sidebar and click **Run backtest**.")
