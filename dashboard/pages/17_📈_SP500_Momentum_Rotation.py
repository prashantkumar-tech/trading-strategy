"""S&P 500 Momentum Rotation — Streamlit page."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.database import list_symbols, load_prices
from backtest.momentum_rotation import run_momentum_rotation

st.set_page_config(page_title="S&P 500 Momentum Rotation", page_icon="📈", layout="wide")
st.title("📈 S&P 500 Momentum Rotation")

# ── Sidebar controls ──────────────────────────────────────────────────────
st.sidebar.header("Strategy settings")
top_n = st.sidebar.number_input("Basket size (top N)", 1, 50, 10)
buffer_rank = st.sidebar.number_input("Sell buffer (exit when rank >)", top_n, 100, 20)
lookback_days = st.sidebar.number_input("Lookback (trading days)", 20, 400, 252)
skip_days = st.sidebar.number_input("Skip recent (trading days)", 0, 60, 21)
initial_capital = st.sidebar.number_input("Starting capital ($)", 1000, 10_000_000, 10_000, step=1000)
start = st.sidebar.text_input("Start date (YYYY-MM-DD)", "2015-01-01")
end = st.sidebar.text_input("End date (YYYY-MM-DD)", "")

symbols = list_symbols(bar_size="1d", source="yfinance")
st.caption(f"{len(symbols)} daily symbols available in the database.")

if st.button("Run backtest", type="primary"):
    if len(symbols) < top_n:
        st.error(f"Only {len(symbols)} symbols in the DB — fetch the S&P 500 universe first "
                 f"(`python -m data.fetch_universe`).")
        st.stop()

    with st.spinner(f"Ranking {len(symbols)} symbols..."):
        result = run_momentum_rotation(
            symbols, start=start or None, end=end or None,
            top_n=int(top_n), buffer_rank=int(buffer_rank),
            lookback_days=int(lookback_days), skip_days=int(skip_days),
            initial_capital=float(initial_capital),
        )

    m = result["metrics"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total return", f"{m['total_return_pct']}%")
    c2.metric("Annualized", f"{m['annualized_return_pct']}%")
    c3.metric("Sharpe", m["sharpe_ratio"])
    c4.metric("Max drawdown", f"{m['max_drawdown_pct']}%")

    # Equity curve vs SPY buy & hold
    fig = go.Figure()
    eq = result["equity_curve"]
    fig.add_trace(go.Scatter(x=eq.index, y=eq.values, name="Strategy"))
    spy = load_prices("SPY", start=start or None, end=end or None)
    if not spy.empty:
        spy = spy.set_index("date")["close"]
        spy_bh = spy / spy.iloc[0] * float(initial_capital)
        fig.add_trace(go.Scatter(x=spy_bh.index, y=spy_bh.values, name="SPY buy & hold"))
    fig.update_layout(title="Equity curve", xaxis_title="Date", yaxis_title="Portfolio value ($)")
    st.plotly_chart(fig, use_container_width=True)

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
