"""SPXL fixed-window intraday study page."""

import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.fixed_window_study import render_fixed_window_study

st.set_page_config(page_title="SPXL Fixed Window", layout="wide", page_icon="🗓️")
render_fixed_window_study("SPXL", "🗓️ SPXL Fixed Window Study", default_reference_symbol="SPY")
