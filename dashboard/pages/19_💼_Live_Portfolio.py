"""Live Portfolio — Streamlit page (thin wrapper around the shared renderer)."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.portfolio_page import render_portfolio_page

render_portfolio_page()
