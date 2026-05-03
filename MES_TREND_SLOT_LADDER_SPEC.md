# MES Trend Slot Ladder Spec

## Purpose

This is a new, separate MES strategy. It should not be implemented as a variant of the existing `MES_Scheduled_Ladder` logic. It needs its own backtest module and its own Streamlit study page so the rules, parameters, and metrics remain isolated.

## Working Name

`MES Trend Slot Ladder`

Core idea: scheduled long-only entries every 30 minutes on trend days, with a fixed 2-lot structure, no PDH/PDL dependency, an explicit no-averaging-down rule, a cap on simultaneous campaigns, and a daily kill switch after repeated stopped campaigns.

## Strategy Intent

The strategy is trying to capture intraday trend persistence rather than breakout confirmation. The assumption is that a small number of strong trend days per month can drive the edge.

The system should:

- keep taking scheduled entries while price is moving up
- refuse to add if price is lower than the last accepted entry
- manage each entry as a self-contained 2-lot campaign
- flatten everything before the regular session ends

## Execution Model

### Market / Data

- Symbol: `MES`
- Bar size: `5m`
- Session: regular session only
- Timezone: New York session time

### Entry Schedule

- Evaluate entry slots every 30 minutes
- Default slots: `9:30, 10:00, 10:30, ..., 15:30 ET`
- At each valid slot, open a new campaign on the next bar open

### Campaign Structure

For each scheduled entry:

- Buy `2` contracts
- Contract 1:
  - target = `entry + 20 points`
  - stop = `entry - 10 points`
- Contract 2:
  - initial stop = `entry - 10 points`
  - no fixed target
  - becomes a runner after Contract 1 exits at target

### End Of Day

- Force close all remaining open contracts at `15:55 ET`

## Trend / Skip Rules

### No PDH / PDL Logic

The strategy should not require:

- PDH breakout
- PDL check
- prior-day structure filters

### Anti-Averaging-Down Rule

Maintain `last_accepted_entry_price` for the current trading day.

A new scheduled entry is allowed only if its candidate entry price is greater than or equal to the most recent accepted campaign entry price.

If the candidate price is lower, skip that slot entirely.

Interpretation:

- "when market moves down skip" means no new campaign at a worse price than the latest filled campaign
- existing open campaigns continue to be managed normally
- only the new slot is rejected
- the strategy keeps checking later scheduled slots on the same day
- a lower-priced rejection does not shut down the rest of the session by itself

### Daily Reset

- Reset `last_accepted_entry_price` at the start of each new session

### Daily Kill Switch

- Track which campaigns are stopped out during the current session
- Once `2` campaigns have stopped out in the same day, do not open any further campaigns for the rest of that day
- Existing open campaigns continue to be managed normally
- This kill switch is the only rule that shuts down new entries for the rest of the day

## Runner Management

### Primary Behavior

Once Contract 1 reaches `+20 points`, move Contract 2's stop to breakeven immediately.

### Trailing Behavior

After breakeven activation, Contract 2 uses a trailing stop that can only ratchet upward.

Default trailing distance:

- `20 points`

Runner stop formula:

- `max(current_stop, entry_price, highest_high_since_activation - 20)`

This means:

- first protect at breakeven
- then keep lifting the runner stop only if price extends higher

## Multiple Campaigns

This strategy should allow multiple same-day campaigns, but with a hard cap on simultaneous overlap.

Rules:

- each 30-minute slot can create a new independent 2-lot campaign
- campaigns may overlap
- campaigns cannot exceed the configured `max_open_campaigns` limit
- each campaign tracks its own:
  - entry time
  - entry price
  - target leg
  - runner leg
  - stop state
  - trailing state

This is intentionally different from the previous scheduled implementation, which is closer to filtered one-campaign-at-a-time behavior.

## Backtest Assumptions

To stay consistent with the rest of the repo:

- entry occurs at next bar open after a valid slot
- if both stop and target are touched in the same bar, resolve conservatively with stop first
- trailing updates apply only after runner activation
- final exit occurs on the first bar at or after `15:55 ET`

## Default Parameters

- `entry_interval_minutes = 30`
- `first_entry_minute = 9:30`
- `last_entry_minute = 15:30`
- `final_exit_minute = 15:55`
- `contracts_per_campaign = 2`
- `target1_points = 20`
- `stop_loss_points = 10`
- `runner_trail_points = 20`
- `skip_lower_entries = True`
- `max_open_campaigns = 4`
- `daily_stop_limit = 2`
- `point_value = 5`

## Current Operating Profile

Current tested profile:

- account size assumption: `$30,000`
- `stop_loss_points = 10`
- `max_open_campaigns = 4`
- `daily_stop_limit = 2`
- `contracts_per_campaign = 2`

Smaller-account profile discussed earlier:

- account size assumption: `$10,000`
- same `10` point stop
- tighter cap of `1` to `2` simultaneous campaigns
- same `daily_stop_limit = 2`

## Outputs / Metrics

Standard outputs:

- trades
- campaigns
- signals / events
- equity curve
- summary metrics

Strategy-specific summary:

- campaign days
- campaigns opened
- skipped slots due to lower price
- skipped slots due to daily kill switch
- target-1 hit rate
- runner win rate
- max concurrent campaigns
- average hold time
- exit reason mix

## UI / Product Spec

This should be a separate page, for example:

- `MES Trend Slot Ladder`

Controls:

- date range
- first entry time
- last entry time
- final exit time
- entry interval
- contracts per campaign
- target points
- stop points
- runner trail points
- skip lower entries toggle
- max simultaneous campaigns
- daily stop limit

Visuals:

- price chart with buy/sell markers
- equity curve
- trade table
- campaign table
- exit mix chart
- yearly summary table

## Key Differences From The Previous MES Scheduled Implementation

This implementation must differ in both logic and code structure:

- no reuse of PDH/PDL or breakout acceptance logic
- no dependence on above-open / above-VWAP filters by default
- supports repeated scheduled entries throughout the session
- explicitly enforces no averaging down using `last_accepted_entry_price`
- runner uses breakeven-then-trailing logic
- caps overlap using `max_open_campaigns`
- stops opening new campaigns after `daily_stop_limit` stopped campaigns in the same day
- should live in a new module, not as flags added onto the previous one

## Locked Design Choices

These decisions are now fixed in the current spec:

- compare new entries against the `last_accepted_entry_price`, not the day's highest accepted entry
- if a candidate entry is below `last_accepted_entry_price`, skip that slot only and continue checking later slots that day
- use a `10` point initial stop, not `20`
- keep the runner targetless and trail it only after Target 1 has hit
- cap simultaneous overlap through `max_open_campaigns`
- stop opening new campaigns after `2` stopped campaigns in the same session
