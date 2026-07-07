# Automode: daily price refresh (macOS `launchd`)

The Live Portfolio page only produces trustworthy signals on fresh end-of-day
closes. `data/update_prices.py` does an **incremental** refresh (pulls only bars
since each symbol's last stored date, recomputes the moving averages, upserts).
This `launchd` job runs it once every evening, unattended.

The strategy is daily-bar only, so a single evening pull after the US close is
enough — no intraday/real-time feed needed.

## Install

1. Edit `deploy/com.tradingstrategy.updateprices.plist`:
   - Replace every `/Users/CHANGE_ME/projects/trading-strategy` with the repo's
     absolute path on this machine.
   - Confirm the `python3` path (`which python3`) and the `Hour`/`Minute`
     (local time — 17:30 assumes US Eastern; adjust if you're elsewhere).

2. Copy it into the LaunchAgents dir and load it:

   ```sh
   cp deploy/com.tradingstrategy.updateprices.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.tradingstrategy.updateprices.plist
   ```

3. Verify / run once immediately:

   ```sh
   launchctl list | grep updateprices
   launchctl start com.tradingstrategy.updateprices
   tail -f db/update_prices.log
   ```

To stop: `launchctl unload ~/Library/LaunchAgents/com.tradingstrategy.updateprices.plist`.

## What it refreshes

By default the S&P 500 universe (`data/update_prices.py` `__main__`). Weekends
and holidays are no-ops (Yahoo returns no new bar; the updater is idempotent).
The Live Portfolio page also has a **🔄 Refresh prices now** button for on-demand
pulls, and a freshness guard that **blocks a rebalance when prices are stale**.

## cron fallback

If you prefer cron over `launchd`:

```cron
30 17 * * 1-5  cd /Users/CHANGE_ME/projects/trading-strategy && /usr/bin/python3 -m data.update_prices >> db/update_prices.log 2>&1
```
