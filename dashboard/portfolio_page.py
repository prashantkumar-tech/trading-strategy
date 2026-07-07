"""Live Portfolio page — operate the momentum rotation strategy forward.

Wraps the ``portfolio`` engine (state/store/engine) around the existing momentum
ranking. Lets you create a fresh paper portfolio, refresh prices, review the
proposed basket (keep / veto / promote = the approve flow), apply the rebalance,
and track performance vs SPY. Portfolio state persists as JSON via ``store``.
"""

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.database import init_db, list_symbols, load_prices
from data.sp500_universe import get_sp500_tickers, get_sp500_meta
from data.broad_universe import get_broad_universe, get_broad_meta
from data.update_prices import refresh_universe, universe_as_of

from portfolio import store
from portfolio.state import new_portfolio
from portfolio.engine import (
    latest_ranks, propose_actions, apply_rebalance, check_status, value_portfolio,
)

_BASKET_ACTIONS = {"BUY", "HOLD", "TRIM"}


# ── Universe resolution ──────────────────────────────────────────────────────

def _resolve_universe(config: dict):
    """Return (symbols_with_data, meta) for the portfolio's configured universe."""
    if config.get("universe") == "broad":
        floor = float(config.get("cap_floor_bn", 2.0)) * 1e9
        tickers = get_broad_universe(min_market_cap=floor)
        try:
            meta = get_broad_meta()
        except Exception:
            meta = None
    else:
        tickers = get_sp500_tickers()
        try:
            meta = get_sp500_meta()
        except Exception:
            meta = None
    available = set(list_symbols(bar_size="1d", source="yfinance"))
    symbols = sorted((available & set(tickers)) - {"SPY"})
    return symbols, meta


def _sectors_for(config: dict, meta, symbols):
    if not config.get("max_per_sector") or not meta:
        return None
    sectors = {s: meta[s]["sector"] for s in symbols
               if s in meta and meta[s].get("sector")}
    return sectors or None


# ── Create-portfolio form ────────────────────────────────────────────────────

def _render_create_form():
    st.subheader("Create a portfolio")
    name = st.text_input("Name", "my-momentum")
    universe = st.selectbox("Universe", ["S&P 500", "Broad market (≥ cap floor)"])
    cap_floor = None
    if universe.startswith("Broad"):
        cap_floor = st.number_input("Market cap floor ($B)", 1.0, 100.0, 2.0, step=0.5)
    c1, c2, c3 = st.columns(3)
    top_n = c1.number_input("Basket size (top N)", 1, 50, 10)
    buffer_rank = c2.number_input("Sell buffer (exit when rank >)",
                                  int(top_n), 100, max(20, int(top_n)))
    initial_capital = c3.number_input("Starting capital ($)", 1000, 10_000_000, 10_000, step=1000)
    c4, c5, c6 = st.columns(3)
    lookback = c4.number_input("Lookback (trading days)", 20, 400, 252)
    skip = c5.number_input("Skip recent (trading days)", 0, 60, 21)
    max_per_sector = c6.number_input("Max per sector (0 = no cap)", 0, int(top_n), 0)

    if st.button("Create portfolio", type="primary"):
        if store.exists(name):
            st.error(f"A portfolio named '{name}' already exists.")
            return
        config = {
            "universe": "broad" if universe.startswith("Broad") else "sp500",
            "cap_floor_bn": cap_floor,
            "top_n": int(top_n),
            "buffer_rank": int(buffer_rank),
            "lookback_days": int(lookback),
            "skip_days": int(skip),
            "max_per_sector": int(max_per_sector),
        }
        p = new_portfolio(name, config, float(initial_capital), date.today().isoformat())
        store.save(p)
        st.session_state["live_portfolio"] = name
        st.success(f"Created '{name}'. Scroll up to select it.")
        st.rerun()


# ── Sections ─────────────────────────────────────────────────────────────────

def _render_header(p, val):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Portfolio value", f"${val['current_value']:,.0f}")
    c2.metric("Cash", f"${val['cash']:,.0f}")
    c3.metric("Invested", f"${val['holdings_value']:,.0f}")
    c4.metric("Return since inception", f"{val['total_return_pct']:.2f}%")
    st.caption(f"Inception {p.inception_date} · "
               f"last rebalance {p.last_rebalance_date or '—'} · "
               f"config: {p.config.get('universe')} top-{p.config.get('top_n')}")


def _render_status_banner(status):
    if status["stale"]:
        st.error(f"⚠ Prices are stale (as of {status['prices_as_of']}). "
                 f"Refresh before rebalancing — signals may be out of date.")
    elif status["rebalance_due"]:
        st.warning(f"⏰ Rebalance due (prices as of {status['prices_as_of']}).")
    else:
        st.success(f"✓ Up to date (prices as of {status['prices_as_of']}).")
    for a in status["alerts"]:
        st.info(f"⚠ {a['message']}")


def _leaderboard_df(ranks, meta, limit=30):
    rows = []
    for s, r in sorted(ranks.rank.items(), key=lambda kv: kv[1])[:limit]:
        rows.append({
            "rank": r, "symbol": s,
            "name": (meta.get(s, {}).get("name", "") if meta else ""),
            "momentum_%": round(float(ranks.momentum.get(s, float("nan")) * 100), 2),
            "price": ranks.price.get(s),
            "above_200ma": s in ranks.eligible,
        })
    return pd.DataFrame(rows)


def _render_rebalance(p, ranks, status, meta, name):
    st.subheader("Rebalance — review & approve")
    sectors = _sectors_for(p.config, meta, list(ranks.price.keys()))
    actions = propose_actions(p, ranks, sectors=sectors)
    proposed = [a["symbol"] for a in actions if a["action"] in _BASKET_ACTIONS]

    st.caption("Suggested actions this rebalance:")
    st.dataframe(pd.DataFrame(actions), use_container_width=True, hide_index=True)

    st.markdown("**Approve the basket** — untick to veto a name; promote extras below.")
    editor = pd.DataFrame({"symbol": proposed, "keep": [True] * len(proposed)})
    edited = st.data_editor(editor, use_container_width=True, hide_index=True,
                            disabled=["symbol"], key=f"approve_{name}")
    kept = [row["symbol"] for _, row in edited.iterrows() if row["keep"]]

    promotable = [s for s in _leaderboard_df(ranks, meta)["symbol"]
                  if s in ranks.eligible and s not in proposed]
    promoted = st.multiselect("Promote additional names (from the leaderboard)", promotable)
    approved = kept + [s for s in promoted if s not in kept]

    st.caption(f"Approved basket ({len(approved)}): {', '.join(approved) or '—'}")
    disabled = not status["can_rebalance"] or not approved
    if st.button("✅ Approve & apply", type="primary", disabled=disabled):
        apply_rebalance(p, approved, ranks, date.today().isoformat())
        store.save(p)
        st.success("Rebalance applied and saved.")
        st.rerun()
    if not status["can_rebalance"]:
        st.caption("Rebalance is blocked while prices are stale.")


def _render_holdings(p, ranks):
    st.subheader("Holdings")
    if not p.positions:
        st.info("No positions yet — approve a rebalance to build the basket.")
        return
    rows = []
    for s, pos in p.positions.items():
        price = ranks.price.get(s)
        mv = pos.shares * price if price else None
        rows.append({
            "symbol": s, "shares": round(pos.shares, 4),
            "entry_price": round(pos.entry_price, 2), "price": price,
            "market_value": round(mv, 2) if mv else None,
            "return_%": round((price / pos.entry_price - 1) * 100, 2) if price else None,
            "rank": ranks.rank.get(s),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_transactions(p):
    st.subheader("Transaction log")
    if not p.transactions:
        st.info("No transactions yet.")
        return
    df = pd.DataFrame([{
        "date": t.date, "action": t.action, "symbol": t.symbol,
        "shares": round(t.shares, 4), "price": round(t.price, 2),
        "amount": t.amount, "reason": t.reason,
    } for t in reversed(p.transactions)])
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_equity_curve(val):
    st.subheader("Equity curve vs SPY")
    eq = val["equity_curve"]
    if eq is None or eq.empty:
        st.info("The curve fills in once the portfolio holds positions across days.")
        return
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=eq.index, y=eq.values, name="Portfolio"))
    spy = val.get("spy_curve")
    if spy is not None and not spy.empty:
        fig.add_trace(go.Scatter(x=spy.index, y=spy.values, name="SPY buy & hold"))
    fig.update_layout(xaxis_title="Date", yaxis_title="Value ($)")
    st.plotly_chart(fig, use_container_width=True)


# ── Entry point ──────────────────────────────────────────────────────────────

def render_portfolio_page():
    st.set_page_config(page_title="Live Portfolio", page_icon="💼", layout="wide")
    init_db()
    st.title("💼 Live Portfolio")
    st.caption("Operate the momentum rotation strategy forward: suggested buys/sells, "
               "an approve flow, and forward performance vs SPY. "
               "Paper portfolio — mirror trades in your real broker. Ignores costs/taxes.")

    names = store.list_portfolios()
    options = ["➕ Create new…"] + names
    default = st.session_state.get("live_portfolio")
    idx = options.index(default) if default in options else 0
    choice = st.selectbox("Portfolio", options, index=idx)

    if choice == "➕ Create new…":
        _render_create_form()
        return

    st.session_state["live_portfolio"] = choice
    p = store.load(choice)
    if p is None:
        st.error("Could not load that portfolio.")
        return

    with st.spinner("Resolving universe…"):
        symbols, meta = _resolve_universe(p.config)
    if len(symbols) < p.config["top_n"]:
        st.error(f"Only {len(symbols)} symbols with data for this universe — "
                 f"fetch data first (`python3 -m data.fetch_universe`).")
        return

    as_of = universe_as_of(symbols)
    fresh_col, btn_col = st.columns([3, 1])
    fresh_col.caption(f"Prices as of **{as_of}** across {len(symbols)} symbols.")
    if btn_col.button("🔄 Refresh prices now"):
        with st.spinner(f"Refreshing {len(symbols)} symbols…"):
            summary = refresh_universe(symbols)
        st.success(f"Refreshed. ok={len(summary['ok'])} failed={len(summary['failed'])}")
        st.rerun()

    with st.spinner(f"Ranking {len(symbols)} symbols…"):
        ranks = latest_ranks(symbols, lookback_days=p.config["lookback_days"],
                             skip_days=p.config["skip_days"])

    today = date.today().isoformat()
    status = check_status(p, ranks, today, prices_as_of=as_of)
    val = value_portfolio(p, spy_loader=load_prices, today=today)

    _render_header(p, val)
    _render_status_banner(status)
    st.divider()
    _render_rebalance(p, ranks, status, meta, choice)
    st.divider()
    _render_holdings(p, ranks)
    _render_transactions(p)
    _render_equity_curve(val)
