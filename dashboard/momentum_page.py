"""Shared renderer for a momentum-rotation dashboard page.

Both the S&P 500 page and the broad-universe page are thin wrappers around
render_momentum_page(); they differ only in the universe source, the page
title, and the session/cache key prefix. The buy-&-hold benchmark is SPY on
every page so results line up across universes.
"""

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.database import list_symbols, load_prices
from backtest.momentum_rotation import run_momentum_rotation, momentum_leaderboard
from backtest.metrics import yearly_performance, compute_metrics
from dashboard import backtest_cache


def _render_leaderboard(board):
    eligible_count = int(board["above_200ma"].sum())
    st.caption(f"{eligible_count} of the top {len(board)} pass the 200-day MA filter.")
    st.dataframe(board, use_container_width=True, hide_index=True)


def _build_comparison(eq, spy_bh, invested_curve=None):
    strat_yp = yearly_performance(eq).rename(columns={
        "return_pct": "strategy_pct", "pnl": "strategy_pnl",
        "max_drawdown_pct": "strategy_maxdd", "end_value": "strategy_value"})
    comp = strat_yp[["year", "strategy_pct", "strategy_maxdd",
                     "strategy_pnl", "strategy_value"]].copy()
    if spy_bh is not None:
        spy_yp = yearly_performance(spy_bh).rename(columns={
            "return_pct": "spy_pct", "pnl": "spy_pnl",
            "max_drawdown_pct": "spy_maxdd", "end_value": "spy_value"})
        comp = comp.merge(spy_yp[["year", "spy_pct", "spy_maxdd", "spy_pnl", "spy_value"]],
                          on="year", how="left")
        comp["outperformance_pp"] = (comp["strategy_pct"] - comp["spy_pct"]).round(2)
    if invested_curve is not None and not invested_curve.empty:
        inv = invested_curve.copy()
        inv.index = pd.to_datetime(inv.index)
        inv_by_year = inv.groupby(inv.index.year).last()
        comp["strategy_invested"] = comp["year"].map(inv_by_year).round(2)
        comp["strategy_cash"] = (comp["strategy_value"] - comp["strategy_invested"]).clip(lower=0).round(2)
    return comp


def _render_backtest(bt):
    result, eq, spy_bh, comp = bt["result"], bt["eq"], bt["spy_bh"], bt["comp"]
    spy_metrics = bt.get("spy_metrics")
    if bt.get("ran_at"):
        st.caption(f"Last run: {bt['ran_at']}")

    m = result["metrics"]
    st.markdown("###### Strategy")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total return", f"{m['total_return_pct']}%")
    c2.metric("Annualized", f"{m['annualized_return_pct']}%")
    c3.metric("Sharpe", m["sharpe_ratio"])
    c4.metric("Max drawdown", f"{m['max_drawdown_pct']}%")

    if spy_metrics is not None:
        st.markdown("###### S&P 500 (buy & hold)")
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Total return", f"{spy_metrics['total_return_pct']}%")
        d2.metric("Annualized", f"{spy_metrics['annualized_return_pct']}%")
        d3.metric("Sharpe", spy_metrics["sharpe_ratio"])
        d4.metric("Max drawdown", f"{spy_metrics['max_drawdown_pct']}%")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=eq.index, y=eq.values, name="Strategy"))
    if spy_bh is not None:
        fig.add_trace(go.Scatter(x=spy_bh.index, y=spy_bh.values, name="SPY buy & hold"))
    fig.update_layout(title="Equity curve", xaxis_title="Date", yaxis_title="Portfolio value ($)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Yearly performance vs S&P 500")
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
            "year": "Year", "strategy_pct": "Strategy %", "spy_pct": "SPY %",
            "outperformance_pp": "Outperformance (pp)",
            "strategy_maxdd": "Strategy Max DD %", "spy_maxdd": "SPY Max DD %",
            "strategy_value": "Strategy Value ($)", "spy_value": "SPY Value ($)",
            "strategy_invested": "Strategy Invested ($)", "strategy_cash": "Strategy Cash ($)",
            "strategy_pnl": "Strategy P&L ($)", "spy_pnl": "SPY P&L ($)",
        }
        order = [c for c in ["year", "strategy_pct", "spy_pct", "outperformance_pp",
                             "strategy_maxdd", "spy_maxdd",
                             "strategy_value", "strategy_invested", "strategy_cash", "spy_value",
                             "strategy_pnl", "spy_pnl"] if c in comp.columns]
        st.dataframe(comp[order].rename(columns=rename),
                     use_container_width=True, hide_index=True)

    st.subheader("Open positions")
    st.caption("Current holdings — the purchases still open, valued at the latest price.")
    open_df = pd.DataFrame(result.get("open_positions", []))
    if open_df.empty:
        st.info("No open positions (fully in cash).")
    else:
        rename = {
            "symbol": "Symbol", "entry_date": "Entry date", "entry_price": "Entry price",
            "shares": "Shares", "current_price": "Current price",
            "market_value": "Market value ($)", "return_since_entry_pct": "Return since entry %",
        }
        st.dataframe(open_df.rename(columns=rename), use_container_width=True, hide_index=True)

    st.subheader("Rotation / trade log (closed round-trips)")
    st.caption("Each row is a position that was bought and later sold (entry → exit).")
    trades_df = pd.DataFrame(result["trades"])
    if trades_df.empty:
        st.info("No completed round-trip trades in this window.")
    else:
        st.dataframe(trades_df, use_container_width=True)


def render_momentum_page(*, page_title, page_icon, universe_label,
                         get_tickers, get_meta, cache_prefix, fetch_hint):
    """Render a full momentum-rotation page for a given universe.

    get_tickers() -> iterable of constituent tickers for this universe.
    get_meta()    -> {ticker: {"name":.., "sector":..}} or None (may raise).
    cache_prefix  -> namespaces the session_state / on-disk cache keys per page.
    fetch_hint    -> shell command shown when the universe has no local data.
    """
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout="wide")

    lb_key, bt_key = f"{cache_prefix}_leaderboard", f"{cache_prefix}_backtest"
    for key in (lb_key, bt_key):
        if key not in st.session_state:
            saved = backtest_cache.load(key)
            if saved is not None:
                st.session_state[key] = saved

    st.title(f"{page_icon} {page_title}")

    # ── Sidebar controls ──────────────────────────────────────────────────
    st.sidebar.header("Strategy settings")
    top_n = st.sidebar.number_input("Basket size (top N)", 1, 50, 10)
    buffer_rank = st.sidebar.number_input("Sell buffer (exit when rank >)",
                                          min_value=int(top_n), max_value=100, value=max(20, int(top_n)))
    lookback_days = st.sidebar.number_input("Lookback (trading days)", 20, 400, 252)
    skip_days = st.sidebar.number_input("Skip recent (trading days)", 0, 60, 21)
    max_per_sector = st.sidebar.number_input(
        "Max holdings per sector (0 = no cap)", 0, int(top_n), 0,
        help="Limit how many of the basket may come from one sector, to reduce concentration.")
    use_regime = st.sidebar.checkbox(
        "Cash out when SPY < 200-day MA", value=False,
        help="Market-regime filter: hold cash whenever SPY is below its 200-day moving average.")
    initial_capital = st.sidebar.number_input("Starting capital ($)", 1000, 10_000_000, 10_000, step=1000)
    start = st.sidebar.text_input("Start date (YYYY-MM-DD)", "2015-01-01")
    end = st.sidebar.text_input("End date (YYYY-MM-DD)", "")

    available = set(list_symbols(bar_size="1d", source="yfinance"))
    try:
        symbols = sorted((available & set(get_tickers())) - {"SPY"})
    except Exception as e:
        st.warning(f"Could not load the {universe_label} list ({e}); "
                   f"falling back to all daily symbols in the database.")
        symbols = sorted(available - {"SPY"})
    st.caption(f"{len(symbols)} {universe_label} symbols with data in the database.")

    def _meta_or_none():
        if get_meta is None:
            return None
        try:
            return get_meta()
        except Exception:
            return None

    # ── Momentum leaderboard ──────────────────────────────────────────────
    st.subheader("📊 Momentum leaderboard")
    st.caption("Top names ranked by 12-1 momentum as of the latest available date. "
               "`above_200ma` = passes the strategy's trend filter (only these are eligible to be held).")
    leaderboard_n = st.number_input("Show top N", 5, 100, 20, key=f"{cache_prefix}_leaderboard_n")
    if st.button("Show leaderboard"):
        if not symbols:
            st.session_state.pop(lb_key, None)
            st.error(f"No {universe_label} symbols in the DB — fetch data first (`{fetch_hint}`).")
        else:
            with st.spinner(f"Ranking {len(symbols)} symbols..."):
                try:
                    st.session_state[lb_key] = momentum_leaderboard(
                        symbols, start=start or None, end=end or None,
                        top_n=int(leaderboard_n),
                        lookback_days=int(lookback_days), skip_days=int(skip_days),
                        meta=_meta_or_none(),
                    )
                    backtest_cache.save(lb_key, st.session_state[lb_key])
                except ValueError as e:
                    st.session_state.pop(lb_key, None)
                    st.error(str(e))

    if lb_key in st.session_state:
        _render_leaderboard(st.session_state[lb_key])

    st.divider()

    # ── Backtest ──────────────────────────────────────────────────────────
    if st.button("Run backtest", type="primary"):
        if len(symbols) < top_n:
            st.session_state.pop(bt_key, None)
            st.error(f"Only {len(symbols)} {universe_label} symbols in the DB — fetch data first "
                     f"(`{fetch_hint}`).")
        else:
            sectors, cap = None, None
            if int(max_per_sector) > 0:
                cap = int(max_per_sector)
                meta = _meta_or_none()
                if meta:
                    sectors = {s: meta[s]["sector"] for s in symbols
                               if s in meta and meta[s].get("sector")}
                if not sectors:
                    st.warning("No sector data for this universe; running without the sector cap.")
                    cap = None

            regime = None
            if use_regime:
                spy_df = load_prices("SPY", start=start or None, end=end or None,
                                     bar_size="1d", source="yfinance")
                if spy_df.empty:
                    st.warning("No SPY data available; running without the market-regime filter.")
                else:
                    spy_df = spy_df.set_index("date")
                    regime = (spy_df["close"] > spy_df["ma200"]).where(spy_df["ma200"].notna(), True)

            with st.spinner(f"Ranking {len(symbols)} symbols..."):
                try:
                    result = run_momentum_rotation(
                        symbols, start=start or None, end=end or None,
                        top_n=int(top_n), buffer_rank=int(buffer_rank),
                        lookback_days=int(lookback_days), skip_days=int(skip_days),
                        initial_capital=float(initial_capital),
                        sectors=sectors, max_per_sector=cap, regime=regime,
                    )
                except ValueError as e:
                    st.session_state.pop(bt_key, None)
                    st.error(str(e))
                    result = None

            if result is not None:
                eq = result["equity_curve"]
                spy = load_prices("SPY", start=start or None, end=end or None,
                                  bar_size="1d", source="yfinance")
                spy_bh, spy_metrics = None, None
                if not spy.empty:
                    spy_close = spy.set_index("date")["close"]
                    spy_bh = spy_close / spy_close.iloc[0] * float(initial_capital)
                    spy_metrics = compute_metrics(spy_bh, [])
                st.session_state[bt_key] = {
                    "result": result, "eq": eq, "spy_bh": spy_bh, "spy_metrics": spy_metrics,
                    "comp": _build_comparison(eq, spy_bh, result["invested_curve"]),
                    "ran_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
                backtest_cache.save(bt_key, st.session_state[bt_key])

    if bt_key in st.session_state:
        _render_backtest(st.session_state[bt_key])
    else:
        st.info("Set parameters in the sidebar and click **Run backtest**.")
