# Rajat Pyramiding Trading Strategy Spec

## Purpose

Rajat's Pyramiding Strategy is a long-only intraday SPY strategy built for 5-minute OHLCV backtesting. The strategy buys fixed 2-share lots on a schedule, avoids adding below the first entry price of the day, exits one share at a profit target, manages the second share with a trailing stop-limit rule, and closes all remaining positions before the end of the regular session.

## Instrument and Data

- Symbol: `SPY`
- Bar size: `5m`
- Data source: `polygon`
- Backtest data table: `prices`
- Required fields: `date`, `open`, `high`, `low`, `close`, `volume`
- Current UI page: `dashboard/pages/16_📈_Rajat_Pyramiding.py`
- Backtest engine: `backtest/rajat_pyramiding.py`

## Capital and Position Sizing

- Starting capital default: `$10,000`
- Margin: disabled
- Trading costs: disabled
- Slippage: disabled, except for the explicit stop-limit offset behavior
- Position size: lot ladder — `4` shares on the 1st entry of the day, then `3`, `2`, `1`, `1`, ... for subsequent entries
- Capital can be reused intraday after exits
- If available cash is insufficient for the scheduled lot-size entry, that entry is skipped

## Trading Session

- Regular session begins at `09:30`
- First scheduled entry: `09:35`
- Subsequent scheduled entries: every `60` minutes
- Last scheduled entry: `15:25`
- Forced liquidation time: `15:55`
- No overnight positions

Scheduled entry times:

```text
09:35, 10:35, 11:35, 12:35, 13:35, 14:35, 15:25
```

## Entry Rules

The strategy is long-only.

At each scheduled entry time:

1. Use the configured entry price column.
   - Default: `close`
   - Alternative supported by UI: `open`
2. The first valid entry of the day establishes the day's reference price.
3. **No entry is allowed if the current entry price is below the previous trading day's low.** Skip with reason `Below previous day's low`. On the first trading day in the dataset (no prior day available), this filter is not applied.
4. Each subsequent scheduled entry is allowed only when the current entry price is greater than or equal to the *previous* entry price of that day. Skip with reason `Below previous entry price`.
5. If cash is insufficient for the lot-size entry, skip the scheduled entry (`Insufficient cash`).

Checks are applied in order: previous day's low filter first, then previous entry price filter.

## Per-Entry Position Structure

Each scheduled entry creates N individual 1-share child positions (N determined by the lot ladder). All child positions share the same entry price and entry timestamp, and each is managed independently with the same trailing stop rules.

## Trailing Stop Exit

Each share position carries a fixed stop set at entry. The stop **never moves** during the day.

Stop trigger at entry:

```text
stop_trigger = entry_price * (1 - trail_pct / 100)
```

The stop remains at this level for the entire session. There is no ratchet, no high-water mark, and no intraday adjustment.

Default parameter:

- `trail_pct = 0.25` — stop placed 0.25% below entry price

When a 5-minute bar's `low` is less than or equal to the stop trigger, the position exits at:

```text
exit_price = stop_trigger
```

Exit rule label: `Trailing stop`

Positions that survive to 15:55 are closed at that bar's close (exit rule: `End of day`).

## End-of-Day Exit

At `15:55`, all open child positions are closed using that bar's `close`.

If a day or selected backtest window does not contain a `15:55` bar, remaining open positions are closed at the last available bar for that day or window. These exits are labeled `End of day fallback`.

## Intrabar Assumptions

The backtest uses 5-minute OHLC bars, not tick data.

Known limitation:

- The exact order of high and low inside a 5-minute bar is unknown.

Current implementation behavior:

- Existing exits are checked before new scheduled entries on each bar.
- Target exits use the bar high.
- Stop exits use the bar low.
- Runner stops are updated after checking whether the current bar hit the prior stop.

This avoids assuming that a bar's high first raises the trailing stop and that the same bar's low then hits the newly raised stop.

## Skipped Entry Reasons

Skipped scheduled entries are recorded with a reason:

- `Below previous day's low` — entry price is below the prior trading day's session low
- `Below previous entry price` — entry price is below the most recent entry of the same day
- `Insufficient cash` — not enough cash to fill the lot-size order

The UI shows these skipped entries in the selected-day drilldown.

## UI Behavior

The Streamlit page supports:

- Starting capital control
- Entry price selector: `close` or `open`
- Initial trail stop percentage
- Trail stop percentage
- Custom study window
- Preset study windows
- Single backtest
- Yearly breakdown
- Exit reason summary
- Equity curve
- Daily summary
- Day drilldown
- SPY 5-minute candlestick chart for a selected day
- Buy markers
- Exit markers
- Skipped scheduled-buy markers
- Entry detail table
- Trade detail table
- Parameter sweep across trail stop values

Current UI date restriction:

- The custom date range starts at `2026-01-01`
- The maximum date is the latest available local SPY 5-minute Polygon bar

## Current Defaults

```text
symbol                 = SPY
bar_size               = 5m
source                 = polygon
initial_capital        = 10000
entry_price_col        = close
trail_pct              = 0.40
first_entry_time       = 09:35
entry_interval         = 60 minutes
last_entry_time        = 15:25
forced_exit_time       = 15:55
lot_ladder             = 4, 3, 2, 1, 1, ...
```

## Output Definitions

### Entry

One scheduled 2-share buy.

### Trade / Share Exit

One child-position exit. Since each entry creates two child positions, one entry can produce two trades.

### Daily Summary

The daily summary separates:

- `entries`: number of scheduled 2-share buys
- `share_exits`: number of individual child-position exits
- `skipped_entries`: number of scheduled buys skipped
- `pnl`: realized daily P&L from share exits

## Open Questions / Future Improvements

- Decide whether stop-limit orders should remain unfilled if price gaps below the limit price.
- Add optional commissions and spread/slippage modeling.
- Add a conservative/optimistic intrabar execution mode for bars where target and stop are both touched.
- Add option to require trend confirmation before scheduled adds.
- Add option to change the first-entry start time and entry interval from the UI.
- Add option to use full available history instead of limiting UI custom ranges to 2026 onward.
