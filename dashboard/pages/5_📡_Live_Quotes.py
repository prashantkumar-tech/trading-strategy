"""Live Quotes — real-time bid/ask/last from IB Gateway (paper account)."""

import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar
from data.database import list_symbols
from ibkr.quotes import fetch_quotes, PAPER_PORT, LIVE_PORT

st.set_page_config(page_title="Live Quotes", layout="wide", page_icon="📡")
st.title("📡 Live Quotes")

render_sidebar()

# ── Connection settings ───────────────────────────────────────────────────────
st.subheader("IB Gateway Connection")
col1, col2 = st.columns([1, 3])
with col1:
    account_type = st.radio("Account", ["Paper", "Live"], index=0, horizontal=True)
port = PAPER_PORT if account_type == "Paper" else LIVE_PORT

st.caption(
    f"Connects to **IB Gateway** on `127.0.0.1:{port}`. "
    f"Make sure IB Gateway is open, logged into your **{'paper' if port == PAPER_PORT else 'live'}** account, "
    f"and **API access is enabled** (File → Global Configuration → API → Settings → Enable ActiveX and Socket Clients)."
)

# ── Symbol selection ──────────────────────────────────────────────────────────
st.subheader("Symbols")
stored = list_symbols()
default_syms = stored if stored else ["SPY"]

selected = st.multiselect(
    "Symbols to quote",
    options=stored if stored else ["SPY", "SSO", "SPXL", "TQQQ", "SPXU", "SQQQ"],
    default=default_syms,
)

extra = st.text_input("Add extra symbols (comma-separated)", placeholder="e.g. AAPL, MSFT")
if extra.strip():
    selected = selected + [s.strip().upper() for s in extra.split(",") if s.strip()]

# ── Fetch quotes ──────────────────────────────────────────────────────────────
if st.button("🔄 Fetch Quotes", type="primary"):
    if not selected:
        st.error("Select at least one symbol.")
    else:
        with st.spinner(f"Connecting to IB Gateway on port {port}…"):
            df = fetch_quotes(selected, port=port)

        if "error" in df.attrs:
            err = df.attrs["error"]
            st.error(f"Could not connect to IB Gateway: `{err}`")
            st.info(
                "**Setup checklist:**\n"
                "1. Download & install **IB Gateway** from interactivebrokers.com\n"
                "2. Log in with your **paper trading** credentials\n"
                "3. Go to **File → Global Configuration → API → Settings**\n"
                "   - Check **Enable ActiveX and Socket Clients**\n"
                f"  - Confirm Socket port is **{port}**\n"
                "4. Click **Apply / OK** and retry"
            )
        elif df.empty:
            st.warning("Connected but received no data. Markets may be closed.")
        else:
            st.success(f"Fetched quotes for {len(df)} symbol(s).")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.session_state["last_quotes"] = df

# ── Show last fetched quotes ──────────────────────────────────────────────────
elif "last_quotes" in st.session_state:
    st.caption("Showing last fetched quotes — press **Fetch Quotes** to refresh.")
    st.dataframe(st.session_state["last_quotes"], use_container_width=True, hide_index=True)
else:
    st.info("Press **Fetch Quotes** to connect to IB Gateway and pull live prices.")
