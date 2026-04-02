"""SPXL fixed-window intraday study page backed by Twelve Data."""

import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.fixed_window_study import render_fixed_window_study

st.set_page_config(page_title="SPXL Fixed Window Twelve Data", layout="wide", page_icon="🗓️")
render_fixed_window_study(
    "SPXL",
    "🗓️ SPXL Fixed Window Study — Twelve Data",
    default_reference_symbol="SPY",
    data_source="twelve_data",
)
