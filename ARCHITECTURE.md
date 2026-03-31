# Architecture

## Directory Structure

```
trading-strategy/
│
├── data/                        # Data layer
│   ├── fetcher.py               # Entry point: download → normalise → store
│   ├── database.py              # SQLite schema + read/write helpers
│   └── indicators.py            # MA50, MA200 calculations
│
├── backtest/                    # Strategy & analysis layer
│   ├── simulator.py             # Multi-position portfolio simulator
│   ├── metrics.py               # Sharpe, drawdown, win rate, etc.
│   └── optimizer.py             # Parameter sweep across rule configs
│
├── dashboard/
│   └── app.py                   # Streamlit UI (631 lines)
│
├── examples/
│   ├── ma50_momentum.json       # MA50 Momentum preset rules
│   └── golden_cross.json        # Golden Cross preset rules
│
├── db/
│   └── trading.db               # SQLite database (gitignored)
│
├── main.py                      # CLI: fetch data, run backtest from JSON
└── requirements.txt
```

---

## Data Flow

```
  Yahoo Finance (yfinance)
          │
          │  raw OHLCV DataFrame
          ▼
  data/fetcher.py
  ├── normalise columns (lowercase, rename)
  ├── data/indicators.py → add ma50, ma200
  └── data/database.py  → upsert into SQLite
          │
          ▼
  db/trading.db
  ┌─────────────────────────────────────────────┐
  │  prices table                               │
  │  symbol | date       | open | high | low |  │
  │         | close | volume | ma50 | ma200     │
  │  PK: (symbol, date)                         │
  │                                             │
  │  SPY   5,030 bars  2006-03-31 → 2026-03-30  │
  │  SSO   4,974 bars  2006-06-21 → 2026-03-30  │
  │  SPXL  4,375 bars  2008-11-05 → 2026-03-30  │
  └─────────────────────────────────────────────┘
          │
          │  pd.DataFrame via load_prices()
          ▼
  backtest/simulator.py  ──or──  backtest/optimizer.py
```

---

## Backtesting Layer

```
  rules (list of dicts)         prices DataFrame
        │                              │
        └──────────┬───────────────────┘
                   ▼
          backtest/simulator.py
          ┌─────────────────────────────────────────┐
          │  For each daily bar:                     │
          │  1. Increment days_held on all positions │
          │  2. Evaluate exit rules per position     │
          │     fields: position_return_pct,         │
          │             days_held, close, ma50 ...   │
          │  3. Close matching positions → cash      │
          │  4. Evaluate entry rules (first match)   │
          │     → open new position (position_pct    │
          │       of current portfolio value)        │
          │  5. Record equity, signals, cash         │
          └─────────────────────────────────────────┘
                   │
          ┌────────┴─────────┐
          ▼                  ▼
   equity_curve           trades[]
   (daily portfolio     (entry/exit/pnl/
    value series)        days_held/
                         unallocated_capital)
          │
          ▼
   backtest/metrics.py
   → total_return, ann_return, sharpe,
     max_drawdown, win_rate, avg_win, avg_loss
```

### Rule Schema

```
Entry rule:                          Exit rule:
{                                    {
  "type": "entry",                     "type": "exit",
  "label": "Above MA50",               "label": "2% profit target",
  "combinator": "AND",                 "combinator": "AND",
  "position_pct": 0.10,                "conditions": [
  "conditions": [                        { "left":  "position_return_pct",
    { "left":  "close",                    "op":    ">=",
      "op":    ">",                         "right": "2" }
      "right": "ma50" }                ]
  ]                                  }
}

Available fields:
  Market:   close, open, high, low, volume, ma50, ma200
            close_prev, open_prev, ma50_prev, ma200_prev
  Position: position_return_pct, days_held  (exit rules only)

Operators: >  <  >=  <=  ==  crosses_above  crosses_below
```

---

## Optimizer

```
  backtest/optimizer.py

  Parameter grid:
  ├── pos_pct_above  (e.g. 8%, 10%, 12%, 15%)
  ├── pos_pct_below  (auto = pos_pct_above / 2)
  ├── profit_target  (e.g. 1.5%, 2%, 2.5%, 3%)
  └── time_stop_days (e.g. 2, 3, 4, 5)

  itertools.product → 64 combinations
  × 3 symbols (SPY, SSO, SPXL)
  = 192 backtests per run

  Output: DataFrame ranked by Sharpe ratio
```

---

## Dashboard (Streamlit)

```
  Sidebar                        Main area
  ──────────────────             ──────────────────────────────────
  Symbol selector                {Symbol} Performance
  (SPY / SSO / SPXL)               price chart + MA50/MA200
  + fetch new symbol               4 stat metrics
                                 ──────────────────────────────────
  Portfolio settings             Rule Builder  [3 tabs]
  - Starting capital ($)           Add Entry Rule
  - Position sizing (%)              conditions + position_pct
    live $ preview                 Add Exit Rule
                                     conditions (incl. pos fields)
  Date range                       Presets
  - From / To                        MA50 Momentum
                                     Golden Cross
                                 ──────────────────────────────────
                                 Active Rules  (per symbol)
                                   Entry rules: Edit % | Remove
                                   Exit rules:  Remove
                                 ──────────────────────────────────
                                 [Run Backtest — {symbol}]
                                   Performance metrics (vs B&H)
                                   Price chart + signals
                                   Equity curve (3 lines):
                                     Strategy
                                     Symbol buy & hold
                                     SPY benchmark
                                   Trade history table
                                     + unallocated capital col
                                 ──────────────────────────────────
                                 Strategy Optimizer
                                   Parameter range config
                                   [Run Optimization]
                                   Tabs: SPY | SSO | SPXL | H2H
                                     Top 10 table
                                     Sharpe vs Return scatter
                                     Head-to-Head comparison
```

---

## Planned: Intraday / Multi-Source Architecture

```
  CURRENT                          PLANNED
  ───────────────────              ───────────────────────────────
  data/fetcher.py                  data/fetcher.py  (orchestrator)
  (monolithic — yfinance           ├── data/sources/yfinance.py
   hardcoded)                      └── data/sources/polygon.py

  prices table                     prices table
  PK: (symbol, date)               PK: (symbol, date, bar_size)
                                   bar_size: "1d" | "5m"

  Dashboard: no bar size           Dashboard: bar size selector
  selector                         "Daily" | "5-min"
```

New files needed:
- `data/sources/yfinance.py` — extract current Yahoo logic
- `data/sources/polygon.py` — Polygon REST API (needs API key)

Files unchanged by this change:
- `backtest/simulator.py` — bar size agnostic
- `backtest/optimizer.py` — bar size agnostic
- `backtest/metrics.py`   — bar size agnostic

**Blocker:** Polygon API key required.
