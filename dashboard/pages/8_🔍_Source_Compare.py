"""Source Compare — visual comparison of Polygon vs Twelve Data intraday bars."""

from datetime import timedelta
from pathlib import Path
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar
from data.sources import polygon as poly_src
from data.sources import twelve_data as td_src


st.set_page_config(page_title="Source Compare", layout="wide", page_icon="🔍")
st.title("🔍 Source Compare")
st.caption("Compare intraday bars from Polygon and Twelve Data.")

render_sidebar()


@st.cache_data(ttl=300, show_spinner=False)
def load_source_bars(symbol: str, start: str, end: str, include_extended: bool, include_close_bar: bool):
    td = td_src.fetch(symbol, bar_size="5m", start=start, end=end)
    poly = poly_src.fetch(symbol, bar_size="5m", start=start, end=end)

    td["date"] = pd.to_datetime(td["date"])
    poly["date"] = pd.to_datetime(poly["date"])

    td = _filter_session(td, include_extended=include_extended, include_close_bar=include_close_bar)
    poly = _filter_session(poly, include_extended=include_extended, include_close_bar=include_close_bar)

    td = td[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    poly = poly[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
    return td, poly


def _filter_session(df: pd.DataFrame, include_extended: bool, include_close_bar: bool) -> pd.DataFrame:
    if include_extended:
        return df.copy()

    end_time = "16:00:00" if include_close_bar else "15:55:00"
    return df[df["date"].dt.strftime("%H:%M:%S").between("09:30:00", end_time)].copy()


def build_comparison(td: pd.DataFrame, poly: pd.DataFrame) -> pd.DataFrame:
    td = td.rename(columns={c: f"td_{c}" for c in ["open", "high", "low", "close", "volume"]})
    poly = poly.rename(columns={c: f"poly_{c}" for c in ["open", "high", "low", "close", "volume"]})

    df = td.merge(poly, on="date", how="outer").sort_values("date").reset_index(drop=True)
    for field in ["open", "high", "low", "close"]:
        df[f"d_{field}"] = (df[f"td_{field}"] - df[f"poly_{field}"]).round(4)
    df["session_day"] = df["date"].dt.date
    return df


today = pd.Timestamp.today().date()
default_start = today - timedelta(days=31)

ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([1.1, 1.1, 0.8, 0.8])
with ctrl1:
    symbol = st.text_input("Symbol", value="TQQQ").upper().strip()
with ctrl2:
    date_range = st.date_input("Range", value=(default_start, today))
with ctrl3:
    include_extended = st.checkbox("Extended Hours", value=False)
with ctrl4:
    include_close_bar = st.checkbox("Include 16:00 Bar", value=False, disabled=include_extended)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, today

run_compare = st.button("Fetch Comparison", type="primary", use_container_width=True)

if not run_compare:
    st.info("Choose a symbol and date range, then click `Fetch Comparison`.")
    st.stop()

if not symbol:
    st.error("Enter a symbol.")
    st.stop()

if start_date > end_date:
    st.error("Start date must be on or before end date.")
    st.stop()

try:
    with st.spinner(f"Fetching {symbol} 5-minute bars from Twelve Data and Polygon..."):
        td_df, poly_df = load_source_bars(
            symbol=symbol,
            start=str(start_date),
            end=str(end_date),
            include_extended=include_extended,
            include_close_bar=include_close_bar,
        )
except Exception as exc:
    st.error(str(exc))
    st.stop()

df = build_comparison(td_df, poly_df)
matched = df[df["td_open"].notna() & df["poly_open"].notna()].copy()
only_td = df[df["poly_open"].isna()].copy()
only_poly = df[df["td_open"].isna()].copy()

st.subheader(f"{symbol} Summary")
metric1, metric2, metric3, metric4, metric5 = st.columns(5)
metric1.metric("Matched Bars", f"{len(matched):,}")
metric2.metric("Only Twelve Data", f"{len(only_td):,}")
metric3.metric("Only Polygon", f"{len(only_poly):,}")
metric4.metric("Avg |Close Diff|", f"{matched['d_close'].abs().mean():.4f}" if not matched.empty else "n/a")
metric5.metric("Max |Close Diff|", f"{matched['d_close'].abs().max():.4f}" if not matched.empty else "n/a")

st.caption(
    f"Twelve Data: {len(td_df):,} bars  |  Polygon: {len(poly_df):,} bars  |  "
    f"Window: {start_date} to {end_date}"
)

st.divider()

price_fig = make_subplots(
    rows=2,
    cols=1,
    shared_xaxes=True,
    row_heights=[0.72, 0.28],
    subplot_titles=(f"{symbol} Close Overlay", "Close Difference (Twelve Data - Polygon)"),
)
price_fig.add_trace(
    go.Scatter(
        x=matched["date"],
        y=matched["td_close"],
        name="Twelve Data Close",
        line=dict(color="#22c55e", width=1.5),
    ),
    row=1,
    col=1,
)
price_fig.add_trace(
    go.Scatter(
        x=matched["date"],
        y=matched["poly_close"],
        name="Polygon Close",
        line=dict(color="#38bdf8", width=1.5),
    ),
    row=1,
    col=1,
)
price_fig.add_trace(
    go.Bar(
        x=matched["date"],
        y=matched["d_close"],
        name="Close Diff",
        marker_color="#f59e0b",
    ),
    row=2,
    col=1,
)
price_fig.update_layout(height=620, template="plotly_dark", margin=dict(t=40, b=20))
st.plotly_chart(price_fig, use_container_width=True)

hist_col, daily_col = st.columns([1, 1])

with hist_col:
    st.subheader("Diff Distribution")
    hist_fig = go.Figure()
    hist_fig.add_trace(
        go.Histogram(
            x=matched["d_close"],
            nbinsx=50,
            marker_color="#f97316",
            name="Close Diff",
        )
    )
    hist_fig.update_layout(
        height=340,
        template="plotly_dark",
        margin=dict(t=30, b=20),
        xaxis_title="Close Diff",
        yaxis_title="Bars",
    )
    st.plotly_chart(hist_fig, use_container_width=True)

with daily_col:
    st.subheader("Daily Difference Summary")
    daily = (
        df.groupby("session_day", dropna=False)
        .agg(
            mean_abs_close_diff=("d_close", lambda s: s.abs().mean()),
            max_abs_close_diff=("d_close", lambda s: s.abs().max()),
            only_twelve=("poly_open", lambda s: int(s.isna().sum())),
            only_polygon=("td_open", lambda s: int(s.isna().sum())),
        )
        .reset_index()
    )
    daily_fig = go.Figure()
    daily_fig.add_trace(
        go.Bar(
            x=daily["session_day"],
            y=daily["mean_abs_close_diff"],
            name="Mean |Close Diff|",
            marker_color="#fb7185",
        )
    )
    daily_fig.add_trace(
        go.Scatter(
            x=daily["session_day"],
            y=daily["max_abs_close_diff"],
            name="Max |Close Diff|",
            line=dict(color="#facc15", width=2),
        )
    )
    daily_fig.update_layout(height=340, template="plotly_dark", margin=dict(t=30, b=20))
    st.plotly_chart(daily_fig, use_container_width=True)

st.divider()

table_col1, table_col2 = st.columns([1.1, 0.9])
with table_col1:
    st.subheader("Largest Close Divergences")
    top_diff = matched.reindex(matched["d_close"].abs().sort_values(ascending=False).index).head(50).copy()
    top_diff["date"] = top_diff["date"].astype(str).str[:19]
    st.dataframe(
        top_diff[["date", "td_open", "poly_open", "td_close", "poly_close", "d_close"]],
        use_container_width=True,
        hide_index=True,
    )

with table_col2:
    st.subheader("Unmatched Bars")
    unmatched = pd.concat(
        [
            only_td.assign(source="Twelve Data")[["date", "source", "td_open", "td_close"]],
            only_poly.assign(source="Polygon")[["date", "source", "poly_open", "poly_close"]],
        ],
        ignore_index=True,
    ).sort_values("date")
    if unmatched.empty:
        st.success("No unmatched bars in the selected window.")
    else:
        unmatched["date"] = unmatched["date"].astype(str).str[:19]
        st.dataframe(unmatched, use_container_width=True, hide_index=True)
