---
name: 5-min strategy system architecture
description: New bar-by-bar 5-min backtest system built on top of existing codebase
type: project
---
A complete 5-min strategy optimization system was built in April 2026. Files created:
- backtest/metrics_v2.py — extended metrics (CAGR, Sortino, Calmar, profit factor, holding time)
- backtest/signals_5min.py — vectorised technical indicators (EMA, VWAP, RSI, BB, ATR, Z-score, OR)
- backtest/engine_5min.py — bar-by-bar engine: $100k capital, 50% max deployed, next-bar fill, stop/TP/trailing, partial exits
- backtest/strategies_5min.py — 7 strategies: EMACrossover, VWAPReversion, RSIMeanReversion, BollingerBandStrategy, ATRMomentum, OpeningRangeBreakout, MultiDayTrend
- backtest/optimizer_5min.py — grid_search / random_search / staged_search with ProcessPoolExecutor across all 28 CPUs
- backtest/validation.py — walk-forward (n_windows), Monte Carlo (resample trades), sensitivity analysis, is_robust() filter
- run_optimization_5min.py — CLI entrypoint: python run_optimization_5min.py [--quick] [--search staged] [--workers 28]

Data: polygon 5-min TQQQ (239k bars 2021–2026), SPXL also available.

**Why:** User wants large-scale parallel optimization of 5-min strategies with proper multi-day holding, partial exits, 50% capital constraint.

**How to apply:** When user asks to run optimization or improve strategies, use these modules. Entry point is run_optimization_5min.py.
