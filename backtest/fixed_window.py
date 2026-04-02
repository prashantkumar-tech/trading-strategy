"""Fixed-window backtest helpers with optional laddered partial exits."""

from itertools import product
from typing import Optional
import pandas as pd

from backtest.metrics import compute_metrics
from backtest.simulator import _eval_rule

SELL_START_MINUTE = 9 * 60 + 45
SELL_END_MINUTE = 15 * 60 + 55
DEFAULT_TIME_STOP_SELL_MINUTE = 15 * 60 + 40   # 3:40 PM ET


def _filter_rules_for_exit_mode(rules: list, use_ladder_exits: bool) -> tuple[list, list]:
    entry_rules = [r for r in rules if r.get("type") == "entry"]
    exit_rules = [r for r in rules if r.get("type") == "exit"]
    if use_ladder_exits:
        exit_rules = [r for r in exit_rules if r.get("label") != "Profit target"]
    return entry_rules, exit_rules


def run_fixed_window_backtest(
    df: pd.DataFrame,
    rules: list,
    initial_capital: float = 10_000.0,
    use_ladder_exits: bool = False,
    ladder_step_pct: float = 0.4,
    ladder_max_pct: float = 4.0,
    ladder_fraction: float = 0.10,
) -> dict:
    df = df.dropna(subset=["ma50"]).reset_index(drop=True)
    parsed = pd.to_datetime(df["date"])
    if "bar_minutes" not in df.columns:
        df["bar_minutes"] = parsed.dt.hour * 60 + parsed.dt.minute
    if "_date_only" not in df.columns:
        df["_date_only"] = parsed.dt.date
    signal_close_source = "signal_day_close" if "signal_day_close" in df.columns else "close"
    if "prev_day_close" not in df.columns:
        daily_close = df.groupby("_date_only", sort=True)[signal_close_source].first()
        prev_close_map = daily_close.shift(1)
        df["prev_day_close"] = df["_date_only"].map(prev_close_map)
    entry_rules, exit_rules = _filter_rules_for_exit_mode(rules, use_ladder_exits)

    # Separate time stop from other exits so we can drive its timing explicitly
    time_stop_rule = next((r for r in exit_rules if r.get("label") == "Time stop"), None)
    time_stop_days = []
    time_stop_label = None
    time_stop_sell_minute = DEFAULT_TIME_STOP_SELL_MINUTE
    time_stop_sell_time_label = "3:40 PM ET"
    if time_stop_rule:
        time_stop_days = [int(day) for day in time_stop_rule.get("schedule_days", [])]
        if not time_stop_days:
            start_day = int(float(time_stop_rule["conditions"][0]["right"]))
            time_stop_days = [start_day, start_day + 2]
        time_stop_label = time_stop_rule.get("schedule_label")
        time_stop_sell_minute = int(time_stop_rule.get("schedule_sell_minute", DEFAULT_TIME_STOP_SELL_MINUTE))
        time_stop_sell_time_label = time_stop_rule.get("schedule_sell_time_label", "3:40 PM ET")
    non_ts_exit_rules = [r for r in exit_rules if r.get("label") != "Time stop"]

    cash = initial_capital
    positions = []
    trades = []
    equity_curve = []
    signals = []
    last_entry_date = None

    ladder_steps = []
    if use_ladder_exits:
        steps = int(round(ladder_max_pct / ladder_step_pct))
        ladder_steps = [round((i + 1) * ladder_step_pct, 6) for i in range(steps)]

    for i, row in df.iterrows():
        prev = df.iloc[i - 1] if i > 0 else None
        current_close = row["exec_close"] if "exec_close" in row.index else row["close"]
        current_date = row["_date_only"] if "_date_only" in row.index else pd.to_datetime(row["date"]).date()
        prev_date = prev["_date_only"] if prev is not None and "_date_only" in prev.index else None
        is_new_trading_day = prev is None or current_date != prev_date
        ts = pd.to_datetime(row["date"])
        current_minute = ts.hour * 60 + ts.minute
        can_sell_now = SELL_START_MINUTE <= current_minute <= SELL_END_MINUTE

        if is_new_trading_day:
            for pos in positions:
                pos["days_held"] += 1

        sell_signal = False
        day_trades = []
        remaining_positions = []

        for pos in positions:
            pos_ctx = {
                "position_return_pct": (current_close - pos["entry_price"]) / pos["entry_price"] * 100,
                "days_held": pos["days_held"],
            }
            ts_active = pos["ts_chunk_shares"] is not None
            is_ts_sell_bar = can_sell_now and current_minute == time_stop_sell_minute

            # Activate scheduled time-stop exits on the first configured sell bar.
            if (time_stop_days
                    and pos["days_held"] >= time_stop_days[0]
                    and is_ts_sell_bar
                    and not ts_active
                    and pos["shares"] > 1e-9):
                n_chunks = len(time_stop_days)
                pos["ts_chunk_shares"] = pos["shares"] / n_chunks
                ts_active = True

            # Sell on the configured time-stop days and configured sell time.
            if ts_active and is_ts_sell_bar and pos["shares"] > 1e-9:
                scheduled_idx = next(
                    (idx for idx, day in enumerate(time_stop_days) if pos["days_held"] == day),
                    None,
                )
                if scheduled_idx is not None and scheduled_idx not in pos["ts_chunks_sold"]:
                    # Final chunk sells all remaining shares to absorb rounding.
                    shares_to_sell = (
                        pos["shares"] if scheduled_idx == len(time_stop_days) - 1
                        else min(pos["ts_chunk_shares"], pos["shares"])
                    )
                    if shares_to_sell > 1e-9:
                        proceeds = shares_to_sell * current_close
                        cost = shares_to_sell * pos["entry_price"]
                        pnl = proceeds - cost
                        cash += proceeds
                        pos["shares"] -= shares_to_sell
                        pos["ts_chunks_sold"].add(scheduled_idx)
                        sell_signal = True
                        day_trades.append({
                            "entry_date": pos["entry_date"],
                            "exit_date": str(row["date"]),
                            "entry_price": round(pos["entry_price"], 4),
                            "exit_price": round(current_close, 4),
                            "shares": round(shares_to_sell, 6),
                            "days_held": pos["days_held"],
                            "pnl": round(pnl, 2),
                            "return_pct": round(pnl / cost * 100, 2) if cost else 0.0,
                            "exit_rule": (
                                f"{time_stop_label or 'Time stop'} @ {time_stop_sell_time_label} "
                                f"chunk {scheduled_idx + 1}/{len(time_stop_days)}"
                            ),
                        })

            # ── Ladder exits: profit steps (disabled once time stop activates) ─
            if use_ladder_exits and can_sell_now and not ts_active:
                for step in ladder_steps:
                    if step in pos["sold_steps"] or pos["shares"] <= 0:
                        continue
                    if pos_ctx["position_return_pct"] >= step:
                        shares_to_sell = min(pos["original_shares"] * ladder_fraction, pos["shares"])
                        if shares_to_sell <= 0:
                            continue
                        proceeds = shares_to_sell * current_close
                        cost = shares_to_sell * pos["entry_price"]
                        pnl = proceeds - cost
                        cash += proceeds
                        pos["shares"] -= shares_to_sell
                        pos["sold_steps"].add(step)
                        sell_signal = True
                        day_trades.append({
                            "entry_date": pos["entry_date"],
                            "exit_date": str(row["date"]),
                            "entry_price": round(pos["entry_price"], 4),
                            "exit_price": round(current_close, 4),
                            "shares": round(shares_to_sell, 6),
                            "days_held": pos["days_held"],
                            "pnl": round(pnl, 2),
                            "return_pct": round(pnl / cost * 100, 2) if cost else 0.0,
                            "exit_rule": f"Ladder {step:.1f}%",
                        })

            # ── Profit target exit (disabled once time stop activates) ─────────
            triggered_rule = None
            if can_sell_now and not ts_active:
                triggered_rule = next(
                    (r for r in non_ts_exit_rules if _eval_rule(r, row, prev, pos_ctx)), None
                )
            if triggered_rule is not None and pos["shares"] > 0:
                shares_to_sell = pos["shares"]
                proceeds = shares_to_sell * current_close
                cost = shares_to_sell * pos["entry_price"]
                pnl = proceeds - cost
                cash += proceeds
                pos["shares"] = 0.0
                sell_signal = True
                day_trades.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": str(row["date"]),
                    "entry_price": round(pos["entry_price"], 4),
                    "exit_price": round(current_close, 4),
                    "shares": round(shares_to_sell, 6),
                    "days_held": pos["days_held"],
                    "pnl": round(pnl, 2),
                    "return_pct": round(pnl / cost * 100, 2) if cost else 0.0,
                    "exit_rule": triggered_rule.get("label", ""),
                })

            if pos["shares"] > 1e-9:
                remaining_positions.append(pos)

        positions = remaining_positions

        buy_signal = False
        positions_market_value = sum(p["shares"] * current_close for p in positions)
        if last_entry_date != current_date:
            for entry_rule in entry_rules:
                if _eval_rule(entry_rule, row, prev):
                    position_pct = float(entry_rule.get("position_pct", 0.10))
                    portfolio_value = cash + positions_market_value
                    spend = portfolio_value * position_pct
                    if cash >= spend > 0:
                        shares = spend / current_close
                        positions.append({
                            "entry_price": current_close,
                            "entry_date": str(row["date"]),
                            "shares": shares,
                            "original_shares": shares,
                            "days_held": 0,
                            "rule_label": entry_rule.get("label", ""),
                            "sold_steps": set(),
                            "ts_chunk_shares": None,   # set at the configured time-stop sell time
                            "ts_chunks_sold": set(),
                        })
                        cash -= spend
                        positions_market_value += spend
                        buy_signal = True
                        last_entry_date = current_date
                    break

        for t in day_trades:
            t["unallocated_capital"] = round(cash, 2)
        trades.extend(day_trades)

        portfolio_value = cash + sum(p["shares"] * current_close for p in positions)
        equity_curve.append({"date": row["date"], "portfolio_value": portfolio_value})
        signal = ("BUY" if buy_signal else None) if not sell_signal else ("BUY+SELL" if buy_signal else "SELL")
        signals.append({"date": row["date"], "signal": signal})

    # Record any positions still open when the window ends
    if positions and not df.empty:
        last_row = df.iloc[-1]
        last_close = last_row["exec_close"] if "exec_close" in last_row.index else last_row["close"]
        for pos in positions:
            if pos["shares"] > 0:
                cost  = pos["shares"] * pos["entry_price"]
                value = pos["shares"] * last_close
                pnl   = value - cost
                trades.append({
                    "entry_date":           pos["entry_date"],
                    "exit_date":            str(last_row["date"]),
                    "entry_price":          round(pos["entry_price"], 4),
                    "exit_price":           round(last_close, 4),
                    "shares":               round(pos["shares"], 6),
                    "days_held":            pos["days_held"],
                    "pnl":                  round(pnl, 2),
                    "return_pct":           round(pnl / cost * 100, 2) if cost else 0.0,
                    "exit_rule":            "End of window (open)",
                    "unallocated_capital":  round(cash, 2),
                })

    equity_series = pd.DataFrame(equity_curve).set_index("date")["portfolio_value"]
    signals_df = pd.DataFrame(signals)
    metrics = compute_metrics(equity_series, trades)

    return {
        "equity_curve": equity_series,
        "trades": trades,
        "metrics": metrics,
        "signals_df": signals_df,
        "final_value": round(float(equity_series.iloc[-1]), 2) if not equity_series.empty else initial_capital,
    }


def run_fixed_window_optimization(
    df: pd.DataFrame,
    pos_pct_above_values: list,
    profit_target_values: list,
    time_stop_values: list,
    time_stop_sell_times: Optional[list] = None,
    initial_capital: float = 10_000.0,
    gap_up_rule: bool = False,
    premarket_rule: bool = False,
    gap_up_pct: float = 0.10,
    premarket_pct: float = 0.05,
    use_ladder_exits: bool = False,
    ladder_step_pct: float = 0.4,
    ladder_max_pct: float = 4.0,
    ladder_fraction: float = 0.10,
    rule_builder=None,
) -> pd.DataFrame:
    rows = []
    effective_profit_targets = [ladder_max_pct] if use_ladder_exits else profit_target_values
    effective_time_stop_sell_times = time_stop_sell_times or [(time_stop, "3:40 PM ET") for time_stop in time_stop_values]

    for pos_above, profit_target, time_stop_option in product(
        pos_pct_above_values,
        effective_profit_targets,
        effective_time_stop_sell_times,
    ):
        time_stop, sell_time = time_stop_option
        pos_below = round(pos_above / 2, 4)
        rules = rule_builder(
            pos_above,
            pos_below,
            profit_target,
            time_stop,
            sell_time,
            gap_up_rule=gap_up_rule,
            premarket_rule=premarket_rule,
            gap_up_pct=gap_up_pct,
            premarket_pct=premarket_pct,
        )
        result = run_fixed_window_backtest(
            df,
            rules,
            initial_capital=initial_capital,
            use_ladder_exits=use_ladder_exits,
            ladder_step_pct=ladder_step_pct,
            ladder_max_pct=ladder_max_pct,
            ladder_fraction=ladder_fraction,
        )
        m = result["metrics"]
        rows.append({
            "above_MA50_%": int(round(pos_above * 100)),
            "below_MA50_%": int(round(pos_below * 100)),
            "profit_target_%": float(profit_target),
            "time_stop_days": time_stop,
            "time_stop_sell_time": sell_time,
            "total_return_%": m["total_return_pct"],
            "ann_return_%": m["annualized_return_pct"],
            "sharpe": m["sharpe_ratio"],
            "max_drawdown_%": m["max_drawdown_pct"],
            "win_rate_%": m["win_rate_pct"],
            "num_trades": m["num_trades"],
            "final_value": result["final_value"],
        })

    df_results = pd.DataFrame(rows).sort_values("sharpe", ascending=False).reset_index(drop=True)
    df_results.index += 1
    return df_results
