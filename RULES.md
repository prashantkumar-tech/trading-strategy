# Fixed-Window Strategy Rules

Source of truth: `backtest/fixed_window.py`

---

## Entry

Rules are evaluated in order. **Only the first matching rule fires per trading day.**  
Once an entry fires for a given date, no further entries are made that day (`last_entry_date` guard).

Entry executes at `exec_close` if that column is present, otherwise `close`.

### Rule 1 — Negative Premarket Buy

- **Condition A:** `exec_close < prev_symbol_close` (the traded symbol's pre-market price is below its own previous regular-session close)
- **Condition B:** `bar_minutes < 570` (bar is before 09:30 ET)
- **Position size:** `pos_pct_below` (the smaller allocation)

### Rule 2 — Positive Open, Above MA50

- **Condition A:** `signal_day_open > prev_day_close` (session open is above the reference symbol's previous close)
- **Condition B:** `signal_day_open > ma50` (session open is above the reference MA50)
- **Condition C:** `bar_minutes == 585` (the 09:45 ET bar)
- **Position size:** `pos_pct_above` (the larger allocation)

### Rule 3 — Positive Open, Below MA50

- **Condition A:** `signal_day_open > prev_day_close`
- **Condition B:** `signal_day_open <= ma50`
- **Condition C:** `bar_minutes == 585`
- **Position size:** `pos_pct_below`

---

## Exit

All exit logic runs every bar. Exits are evaluated **before** entries on each bar.  
Exits only execute when `SELL_START_MINUTE (585) <= bar_minutes <= SELL_END_MINUTE (955)`.

### Time Stop (scheduled partial exits)

- Activated on the **first bar that matches the configured sell minute** (`schedule_sell_minute`, default 940 = 15:40 ET) once `days_held >= time_stop_days[0]`.
- On activation, the position is divided into equal chunks: `chunk_size = total_shares / len(schedule_days)`.
- On each scheduled day at the configured sell time, one chunk is sold.
- The **final chunk** sells all remaining shares (absorbs any rounding remainder).
- Each chunk sale is recorded as a separate trade with label e.g. `"Time stop @ 3:40 PM ET chunk 1/2"`.
- Once the time-stop schedule is active, **ladder exits and profit-target exits are suppressed** for that position.

### Ladder Exits (when `use_ladder_exits=True`)

- Only active when the time-stop schedule has **not yet activated** for a position.
- At each bar in the sell window, checks each ladder step in order.
- A step fires when `position_return_pct >= step` and that step has not already been sold.
- Sells `original_shares × ladder_fraction` (default 10%) per step.
- Steps are evenly spaced: from `ladder_step_pct` up to `ladder_max_pct` in increments of `ladder_step_pct`.
- Each ladder fill is recorded as a separate trade with label e.g. `"Ladder 0.4%"`.

### Profit Target (standard mode only)

- Only active when `use_ladder_exits=False` **and** the time-stop schedule has **not yet activated**.
- Fires when any configured exit rule matches (evaluated via `_eval_rule`).
- Default rule: `position_return_pct >= profit_target_%`.
- Closes the **entire remaining position** in one fill.

---

## Position Sizing

```
portfolio_value = cash + sum(shares × exec_close for all open positions)
spend           = portfolio_value × position_pct
shares_bought   = spend / exec_close
```

An entry only executes if `cash >= spend > 0`.

---

## Days Held

- Incremented **once per new trading date** (not per bar).
- Entry day counts as Day 1 (incremented at the start of the next new date, so on entry day `days_held = 0`; on the following trading day `days_held = 1`).

---

## End-of-Window Handling

Any position still open when the data window ends is recorded as a trade at the final bar's `exec_close` (or `close`) with exit rule `"End of window (open)"`. These are unrealised marks, not actual closes.

---

## Constants

| Name | Value | Meaning |
|---|---|---|
| `SELL_START_MINUTE` | 585 | 09:45 ET — earliest bar that can trigger an exit |
| `SELL_END_MINUTE` | 955 | 15:55 ET — latest bar that can trigger an exit |
| `DEFAULT_TIME_STOP_SELL_MINUTE` | 940 | 15:40 ET — default scheduled sell time |
