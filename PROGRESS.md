# Trading Strategy — Progress Tracker

**Repository:** https://github.com/prashantkumar-tech/trading-strategy
**Started:** 2026-03-30
**Last updated:** 2026-07-06

---

## Momentum Rotation System (current focus, added 2026-07)

A cross-sectional momentum rotation strategy, separate from the rule-builder/optimizer
work below. Design + plan in `docs/superpowers/`.

**Strategy:** each month, rank a universe by 12-1 momentum (return over ~252 trading days
skipping the most recent ~21), hold an equal-weight top-10 basket of names above their own
200-day MA, and rotate a holding out only once it falls past rank 20 (hold buffer).

**Key files:**
- `backtest/momentum_rotation.py` — the engine: `run_momentum_rotation()`, `momentum_leaderboard()`,
  `select_basket()` (with optional per-sector cap), `build_price_panel()`, `compute_momentum()`.
  Optional knobs: `sectors`+`max_per_sector` (sector cap), `regime` (SPY 200-MA cash filter).
  Returns equity_curve, invested_curve, trades (closed round-trips), open_positions, metrics, holdings.
- `backtest/metrics.py` — `yearly_performance()` (per-year P&L, return, intra-year max drawdown).
- `data/sp500_universe.py` — S&P 500 list + name/sector from Wikipedia (browser UA; 503 names).
- `data/broad_universe.py` — broad universe from NASDAQ Trader symbol directories
  (`nasdaqlisted.txt`/`otherlisted.txt`), common-stock filtered, market-cap floor via the
  NASDAQ screener bulk API. `get_broad_universe(min_market_cap)`.
- `data/fetch_universe.py` / `data/fetch_broad_universe.py` — bulk price fetch (per-symbol,
  skips symbols already in DB). Run: `python3 -m data.fetch_broad_universe`.
- `dashboard/momentum_page.py` — shared page renderer; pages 17 (S&P 500) and 18 (Broad Market)
  are thin wrappers, per-page session/cache key prefixes.
- `dashboard/pages/17_📈_SP500_Momentum_Rotation.py`, `18_🌐_Broad_Market_Momentum_Rotation.py`.
- `dashboard/backtest_cache.py` — pickles last leaderboard/backtest to gitignored
  `db/dashboard_cache/` so results survive a Streamlit restart.

**Dashboard features (both pages):** momentum leaderboard (rank/name/[sector]/momentum/200-MA flag),
backtest with Strategy + S&P 500 metric rows, equity curve vs SPY, yearly table
(return, max DD, end-of-year value, invested vs cash, P&L for strategy & SPY), open-positions table,
closed-trade log, sector cap + regime-filter controls. Broad page adds a market-cap floor control
(default $2B). Results persist across restarts.

**Data in DB (`db/trading.db`, gitignored):** ~2,025 daily symbols — S&P 500 (503) plus broad
universe ≥$1B (~1,957 with data). Universe caches (gitignored): `data/broad_tickers.tsv`,
`data/broad_marketcap.tsv`, `data/sp500_*.{txt,tsv}`.

**Experiment findings (2015→now, top-10, monthly, 200-MA filter, vs SPY B&H 13.5% ann / 0.80 Sharpe):**
| Universe | Ann % | Sharpe | Max DD % |
|---|---|---|---|
| S&P 500 (503) | 48.0 | 1.34 | −41.4 |
| Broad ≥$1B (2025) | 57.6 | 1.18 | −49.5 |
| Broad ≥$2B (1624) | 65.1 | 1.29 | −47.7 |
| Broad ≥$5B (1095) | 61.9 | 1.37 | −54.1 |

Broader universe = higher headline return but **not** better risk-adjusted (deeper drawdowns).
The edge comes from concentrated bets on a few survivorship-inflated small/mid-cap names; the
market-cap floor is second-order noise (non-monotonic sweep); widening the basket to top-30 cut
returns (~65%→47% ann) without reducing drawdown (systematic, correlated, not idiosyncratic risk).
**All backtests ignore trading costs/taxes and use today's constituent lists (survivorship-biased),
so results are optimistic — treat the broad-universe edge as an upper bound.**

**Tests:** `python3 -m pytest -q` (35 passing). **Run app:** `python3 -m streamlit run dashboard/app.py`.

**Possible next steps:** min-price/dollar-volume filter to strip micro-cap artifacts (e.g. AXTI-style
5000% momentum); persist sidebar settings alongside cached results; point-in-time constituent lists
to reduce survivorship bias; transaction-cost modeling; retry the ~68 rate-limited broad symbols.

---

## Live Portfolio Operations Layer (current focus, added 2026-07)

Turns the momentum research tool into a tool to **operate** the strategy forward: a persistent
paper portfolio that suggests buys/sells, runs an approve flow (keep/veto/promote), applies the
rebalance, and tracks performance vs SPY. Phase 1 of a 3-phase vision (2: product-grade UX,
3: multi-user/externalized). Design + plan: `docs/superpowers/` / plan file.

**Decisions:** decision-support + virtual paper tracker (one engine); *assisted approve flow* for
selection; fixed-monthly rebalance + interim breach alerts; fresh $10k start; extend Streamlit.

**Key files (new):**
- `portfolio/state.py` — `Portfolio`/`Position`/`Transaction` dataclasses, JSON-serializable;
  `new_portfolio()` (also stashes `initial_capital` in config for curve reconstruction).
- `portfolio/store.py` — JSON persistence to `db/portfolio/<name>.json` (name-keyed; `db/` gitignored).
- `portfolio/engine.py` — `Ranks` (derives rank/eligible/eligible_rank), `latest_ranks()`,
  `propose_actions()` (diffs target basket via reused `select_basket`), `apply_rebalance()`
  (equal-weight, trims-before-buys, logs one txn per fill, conserves value), `check_status()`
  (monthly rebalance-due + 200-MA/rank-buffer alerts + stale-price guard),
  `value_portfolio()` (equity curve reconstructed from txns; snapshot values holdings at latest
  close so day-zero header is right), `previous_trading_day()`/`is_stale()`.
- `data/update_prices.py` — thin wrapper over the **existing** incremental fetch
  (`fetch_universe(refresh_mode="incremental")` in `data/fetcher.py` already does since-last-date
  fetch + MA recompute + upsert); adds `universe_as_of()` freshness read + CLI entry point.
- `dashboard/portfolio_page.py` + `dashboard/pages/19_💼_Live_Portfolio.py` — create form, freshness
  banner + Refresh button, status banner, approve-flow editor, holdings, txn log, equity curve.
- `deploy/com.tradingstrategy.updateprices.plist` + `deploy/README.md` — macOS `launchd` job that
  runs the daily incremental refresh after the close (cron fallback documented).

**Tests:** `tests/test_portfolio_{state,engine,status}.py`, `tests/test_update_prices.py`
(23 new; 58 total passing). **Verified** end-to-end in the live app: created a fresh $10k S&P 500
portfolio, ran the approve flow vetoing MU, applied — equal-weight $1k/name, cash conserved to
exactly $10k, stale-price guard blocked the rebalance, holdings/txn log/curve rendered.

**Note:** ignores transaction costs/taxes (live forward data avoids survivorship, unlike backtests).

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
