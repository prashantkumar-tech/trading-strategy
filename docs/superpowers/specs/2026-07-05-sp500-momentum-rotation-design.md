# S&P 500 Momentum Rotation — Design

**Date:** 2026-07-05
**Status:** Approved (design), pending implementation plan

## Summary

A cross-sectional momentum strategy. Each month, rank the S&P 500 by 12-1
momentum, hold an equal-weighted basket of the top 10 names (that are also
above their own 200-day moving average), and only rotate a name out once it
falls below rank 20 — pulling in the next-best eligible name to refill the
slot.

This is architecturally distinct from the existing `backtest/simulator.py`,
which evaluates entry/exit rules for **one symbol at a time**. Momentum
ranking is a **portfolio-level, cross-sectional** problem, so it is built as
a new module rather than a change to the simulator.

## Strategy parameters (decided)

| Lever            | Decision                                                        |
|------------------|-----------------------------------------------------------------|
| Universe         | S&P 500 constituents (current list), via yfinance               |
| Momentum metric  | 12-1 momentum: return over ~12 months, skipping the last ~1 mo  |
| Rebalance        | Monthly (month-end)                                             |
| Basket size      | Top 10                                                          |
| Sell rule        | Buffer — sell only when a holding exits the top 20              |
| Sizing           | Equal weight, 10% each, rebalanced monthly back to equal        |
| Trend filter     | Stock-level: only buy names above their own 200-day MA          |
| Cash handling    | If fewer than 10 names qualify, remaining slots stay in cash    |
| Starting capital | $10,000 (project default)                                       |

## Section 1 — Data layer

Ranking needs the **whole universe side-by-side**, unlike the current
one-symbol-at-a-time flow.

- **New data fetch**: pull the current S&P 500 constituent list (~500
  tickers) and download daily OHLCV for each via yfinance into the existing
  `prices` table (same schema, `PK: (symbol, date)`). One-time bulk fetch,
  then incremental refresh like other data.
- **Warm-up requirement**: 12-1 momentum needs ~252 trading days of history
  and the 200-day MA needs 200 — the backtest can only start trading a name
  ~1 year after its data begins.

### Known caveats (accepted for v1)

1. **Survivorship bias** — uses *today's* S&P 500 list. Dropped/delisted
   names (e.g. Lehman) are absent, making historical results look better than
   reality. Backtest numbers should be read as optimistic. A point-in-time
   constituent list is a later upgrade.
2. **Universe membership over time** — a stock added to the index in 2020 has
   price history back further, so the backtest would "trade" it in earlier
   years when it was not actually in the index. Acceptable for v1.

## Section 2 — Rotation engine

New module: **`backtest/momentum_rotation.py`**. Reuses
`backtest/metrics.py` for output (equity curve + trades → Sharpe, drawdown,
win rate, etc.).

### Algorithm

1. **Build a price panel** — load all universe symbols into one wide
   DataFrame (dates × symbols of daily closes). Compute per-symbol:
   - 200-day moving average.
   - 12-1 momentum signal = `close[~21 days ago] / close[~252 days ago] − 1`
     (12-month return, skipping the most recent ~1 month).

2. **Walk month-end rebalance dates.** On each rebalance date:
   - **Eligibility filter**: a stock qualifies only if it has ≥252 days of
     history *and* its close is above its own 200-day MA.
   - **Rank** all eligible names by 12-1 momentum, descending.
   - **Sell** any current holding that has fallen out of the **top 20**, or
     is no longer eligible (below its 200-MA). Held names ranked 11–20 are
     **kept** — this is the buffer.
   - **Refill**: for each open slot (target = 10 holdings), buy the
     highest-ranked eligible name not already held. If fewer than 10 qualify,
     leave the slot in **cash**.
   - **Rebalance to equal weight**: set every holding's target to
     portfolio_value / 10, trimming winners and topping up laggards. Execute
     at that day's close.

3. **Between rebalances**, positions drift — no daily checks (monthly
   cadence).

4. **Record** equity curve and a trade log: entry date/price, exit
   date/price, symbol, P&L, rank-at-exit, and exit reason
   (`fell out of top 20` / `below 200MA` / `rebalance trim`). Feed the equity
   series + trades into `metrics.py`.

### Config surface

Tunable without code edits (and future optimizer knobs):

```
top_n            = 10
buffer_rank      = 20
lookback_days    = 252
skip_days        = 21
ma_filter        = 200
rebalance        = "monthly"
initial_capital  = 10_000
```

## Section 3 — Dashboard, testing, integration

### Dashboard

New Streamlit page `dashboard/pages/17_📈_SP500_Momentum_Rotation.py`,
following the existing per-strategy page pattern. Shows:

- **Controls** (sidebar): date range plus the config knobs (top_n,
  buffer_rank, lookback/skip, MA filter, starting capital).
- **Results**: equity curve vs SPY buy-&-hold benchmark; standard metrics row
  (total / annualized return, Sharpe, max drawdown, win rate); a
  **current-basket table** (10 holdings with momentum rank); and the
  **trade / rotation log** (what got sold, why, and what replaced it).

### Testing (test-first)

- **Momentum calc**: synthetic panel with known prices → assert 12-1 values
  and correct skip-window handling.
- **Buffer behavior**: a holding at rank 15 is kept; at rank 21 it is sold.
- **Refill + cash slots**: when only 7 names are eligible → assert 7 holdings
  and 3 cash slots.
- **Equal-weight rebalance**: after a month of drift → assert each target
  returns to portfolio / 10.
- **End-to-end smoke**: small multi-symbol fixture runs start→finish →
  produces a sane equity curve + trade log.

### Untouched

`simulator.py`, `optimizer.py`, and all existing strategy pages — purely
additive. `metrics.py` reused as-is.

## Build order

1. Data: fetch S&P 500 list + bulk-download into `prices`.
2. `backtest/momentum_rotation.py` (test-first).
3. Dashboard page.
