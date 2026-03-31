# Trading Strategy — Progress Tracker

**Repository:** https://github.com/prashantkumar-tech/trading-strategy
**Started:** 2026-03-30
**Last updated:** 2026-03-31

---

## Completed

### Phase 1 — Data Pipeline
- [x] Fetch daily OHLCV data via yfinance (20-year history)
- [x] Compute MA50 and MA200 on ingestion
- [x] Store in SQLite (`db/trading.db`) with `(symbol, date)` primary key
- [x] Fixed upsert bug — `if_exists="replace"` was wiping all symbols on each fetch; changed to delete-then-insert per symbol
- [x] Symbols stored: **SPY** (5,030 bars), **SSO** (4,974 bars), **SPXL** (4,375 bars)

### Phase 2 — Backtesting Engine
- [x] Multi-position simulator — opens a new position every day conditions are met, each tracked independently
- [x] Entry rules: condition-based, first match fires, per-rule position sizing
- [x] Exit rules: evaluated per open position each day; supports price-based (`position_return_pct`) and time-based (`days_held`) exits
- [x] Position sizing scales with current portfolio value (10% of $15k = $1,500 automatically)
- [x] Fixed `unallocated_capital` bug — was stamped mid-day after sell but before buy; corrected to end-of-day cash
- [x] Performance metrics: total return, annualized return, Sharpe ratio, max drawdown, win rate, avg win/loss
- [x] Max drawdown is **portfolio-level** across the full backtest period (not per-position)

### Phase 3 — Streamlit Dashboard
- [x] Symbol selector dropdown (SPY / SSO / SPXL); rules saved independently per symbol
- [x] Add new symbols via sidebar fetch button
- [x] **Index Performance** section: always-visible price chart (close + MA50 + MA200 + volume), 4 key stats
- [x] **Rule Builder UI** with 3 tabs:
  - Add Entry Rule (conditions + position size %)
  - Add Exit Rule (supports `position_return_pct`, `days_held`, market fields)
  - Presets (MA50 Momentum, Golden Cross / Death Cross)
- [x] Position Sizing panel in sidebar — % slider with live $ preview at current and $15k capital
- [x] Inline **Edit %** button on each active entry rule (no need to remove and re-add)
- [x] Run Backtest button — price chart with BUY/SELL signals, equity curve, trade history table
- [x] Equity curve shows 3 lines: Strategy / Symbol Buy & Hold / SPY benchmark (when symbol ≠ SPY)
- [x] Trade history table includes `unallocated capital` column (end-of-day cash after all transactions)
- [x] Backtest metrics show delta vs buy-and-hold (e.g. "+12% vs B&H")

### Phase 4 — Strategy Optimizer
- [x] Parameter sweep across: above-MA50 %, profit target %, time stop days
- [x] Below-MA50 % auto-set to half of above-MA50 %
- [x] Runs all combinations against SPY, SSO, SPXL in one pass (64 combos × 3 symbols = 192 backtests)
- [x] Results ranked by Sharpe ratio; top 10 table per symbol
- [x] Scatter plot: Sharpe vs Total Return, colored by profit target
- [x] Head-to-Head tab: best settings per metric across all symbols side by side
- [x] Configurable parameter ranges (min / max / step) with live combo count preview

### Phase 5 — GitHub
- [x] Repository created: https://github.com/prashantkumar-tech/trading-strategy
- [x] `.gitignore` excludes `db/` (SQLite data) and `.claude/` (local config)
- [x] 2 commits pushed (initial + SPXL)

---

## Optimizer Findings (2010–present, $10k starting capital)

| Symbol | Best Sharpe Config | Sharpe | Total Return | Max Drawdown |
|---|---|---|---|---|
| SPY | 10% above MA50, 2% profit target, 5-day stop | 0.900 | +127% | -8.9% |
| SSO | 10% above MA50, 2% profit target, 2-day stop | 0.861 | +87% | -6.9% |
| SPXL | 12% above MA50, 3% profit target, 2-day stop | 0.871 | +199% | -11.4% |

**Key insight:** Returns are calculated on full $10k (including ~70% idle cash). Return on deployed capital only (~$3k) is roughly 3× higher. Longer time stops increase total return by keeping more capital working.

---

## Known Limitations

- [ ] Returns shown on full capital — no "return on deployed capital" metric yet
- [ ] No interest earned on idle cash (real money markets earn ~5%)
- [ ] Daily bars only — no intraday data yet
- [ ] No live trading integration (IBKR deferred)
- [ ] No inline rule editing (only Edit % is inline; changing conditions requires remove + re-add)

---

## Up Next

### Intraday Data (5-min bars) — Planned
- **Decision:** Use Polygon.io (verify plan includes 5-min data at polygon.io/pricing)
- **Symbols:** SPY, SSO, SPXL, TQQQ
- **Architecture changes needed:**
  - `data/sources/yfinance.py` — extract current Yahoo logic
  - `data/sources/polygon.py` — new Polygon API fetcher
  - `data/fetcher.py` — thin orchestrator, picks source by argument
  - `data/database.py` — add `bar_size` column, new PK: `(symbol, date, bar_size)`
  - `dashboard/app.py` — add source + bar size selectors in sidebar
  - Everything else (simulator, optimizer, metrics) unchanged
- **Blocker:** Need Polygon API key

### Other Backlog
- [ ] Return on deployed capital metric
- [ ] Inline rule condition editing (not just % editing)
- [ ] IBKR live trading integration (paper trading first)
- [ ] TQQQ data (add once intraday pipeline is ready; daily available now via yfinance)
- [ ] Stop-loss rule support (exit when `position_return_pct <= -X`)
