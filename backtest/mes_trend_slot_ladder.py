"""MES scheduled trend-slot ladder backtest on 5-minute data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backtest.metrics import compute_metrics

REGULAR_OPEN_MINUTE = 9 * 60 + 30
REGULAR_CLOSE_MINUTE = 16 * 60

DEFAULT_PARAMS = {
    "entry_interval_minutes": 30,
    "first_entry_minute": 9 * 60 + 30,
    "last_entry_minute": 15 * 60 + 30,
    "final_exit_minute": 15 * 60 + 55,
    "contracts_per_campaign": 2,
    "target1_points": 20.0,
    "stop_loss_points": 10.0,
    "runner_trail_points": 20.0,
    "skip_lower_entries": True,
    "max_open_campaigns": 4,
    "daily_stop_limit": 2,
    "point_value": 5.0,
}


@dataclass
class Leg:
    campaign_id: int
    name: str
    contracts: int
    entry_time: pd.Timestamp
    entry_price: float
    stop_price: float
    target_price: Optional[float] = None
    active: bool = True
    trail_active: bool = False
    trail_high: Optional[float] = None


def _merge_params(params: Optional[dict]) -> dict:
    merged = DEFAULT_PARAMS.copy()
    if params:
        merged.update(params)
    return merged


def _prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date").reset_index(drop=True)
    data["bar_minutes"] = data["date"].dt.hour * 60 + data["date"].dt.minute
    data["_date_only"] = data["date"].dt.date
    data["regular"] = data["bar_minutes"].between(REGULAR_OPEN_MINUTE, REGULAR_CLOSE_MINUTE)

    regular = data[data["regular"]].copy()
    if regular.empty:
        return regular

    daily = regular.groupby("_date_only", sort=True).agg(session_open=("open", "first")).reset_index()
    regular = regular.merge(daily, on="_date_only", how="left")
    return regular.reset_index(drop=True)


def _record_event(events: list, ts: pd.Timestamp, price: float, signal: str, detail: str) -> None:
    events.append({"date": ts, "price": round(float(price), 4), "signal": signal, "detail": detail})


def _record_exit(
    leg: Leg,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_rule: str,
    point_value: float,
    trades: list,
    events: list,
) -> float:
    pnl_points = float(exit_price) - leg.entry_price
    pnl = pnl_points * point_value * leg.contracts
    hold_bars = max(int((exit_time - leg.entry_time).total_seconds() // 300), 0)
    trades.append({
        "campaign_id": leg.campaign_id,
        "leg": leg.name,
        "entry_date": str(leg.entry_time),
        "exit_date": str(exit_time),
        "entry_price": round(leg.entry_price, 4),
        "exit_price": round(float(exit_price), 4),
        "contracts": leg.contracts,
        "pnl": round(pnl, 2),
        "return_pct": round((pnl_points / leg.entry_price) * 100, 2),
        "exit_rule": exit_rule,
        "hold_bars": hold_bars,
    })
    _record_event(events, exit_time, exit_price, "SELL", f"Campaign {leg.campaign_id} {leg.name} {exit_rule}")
    leg.active = False
    return pnl


def run_backtest(df: pd.DataFrame, params: Optional[dict] = None, initial_capital: float = 10_000.0) -> dict:
    params = _merge_params(params)
    data = _prepare_data(df)
    if data.empty:
        empty_ec = pd.Series([initial_capital], index=[pd.Timestamp("1970-01-01")], dtype=float)
        return {
            "params": params,
            "metrics": compute_metrics(empty_ec, []),
            "summary": {
                "campaign_days": 0,
                "campaigns_opened": 0,
                "skipped_lower_slots": 0,
                "skipped_daily_stop_slots": 0,
                "target1_hit_rate_pct": 0.0,
                "runner_win_rate_pct": 0.0,
                "max_concurrent_campaigns": 0,
                "avg_hold_bars": 0.0,
                "exit_reason_counts": {},
            },
            "trades": [],
            "campaigns": [],
            "equity_curve": empty_ec,
            "signals_df": pd.DataFrame(columns=["date", "price", "signal", "detail"]),
            "final_value": initial_capital,
            "price_df": data,
        }

    point_value = float(params["point_value"])
    cash = initial_capital
    trades = []
    campaigns = []
    events = []
    equity_points = []
    exit_reason_counts = {}
    active_legs: list[Leg] = []
    last_accepted_entry_price = None
    active_day = None
    campaign_id = 0
    skipped_lower_slots = 0
    skipped_daily_stop_slots = 0
    max_concurrent_campaigns = 0
    daily_stopped_campaigns: set[int] = set()

    for i, row in data.iterrows():
        ts = pd.to_datetime(row["date"])
        day = row["_date_only"]
        current_minute = int(row["bar_minutes"])
        close_price = float(row["close"])
        high_price = float(row["high"])
        low_price = float(row["low"])

        if active_day != day:
            active_day = day
            last_accepted_entry_price = None
            active_legs = [leg for leg in active_legs if leg.active]
            daily_stopped_campaigns = set()

        target_hits_this_bar = set()

        for leg in list(active_legs):
            if not leg.active:
                continue

            stop_hit = low_price <= leg.stop_price
            target_hit = leg.target_price is not None and high_price >= leg.target_price
            exit_rule = None
            exit_price = None

            if stop_hit:
                exit_rule = "Stop"
                exit_price = leg.stop_price
            elif target_hit:
                exit_rule = "Target"
                exit_price = leg.target_price
                if leg.name == "Target leg":
                    target_hits_this_bar.add(leg.campaign_id)
            elif current_minute >= int(params["final_exit_minute"]):
                exit_rule = "End of day"
                exit_price = close_price

            if exit_rule is not None:
                pnl = _record_exit(leg, ts, exit_price, exit_rule, point_value, trades, events)
                cash += pnl
                exit_reason_counts[exit_rule] = exit_reason_counts.get(exit_rule, 0) + 1
                if exit_rule == "Stop":
                    daily_stopped_campaigns.add(leg.campaign_id)

        active_legs = [leg for leg in active_legs if leg.active]

        for current_campaign_id in target_hits_this_bar:
            for leg in active_legs:
                if leg.campaign_id == current_campaign_id and leg.name == "Runner":
                    leg.stop_price = max(leg.stop_price, leg.entry_price)
                    leg.trail_active = True
                    leg.trail_high = max(leg.entry_price, high_price)
                    _record_event(events, ts, leg.stop_price, "TRAIL", f"Campaign {current_campaign_id} runner breakeven")
            for campaign in campaigns:
                if campaign["campaign_id"] == current_campaign_id:
                    campaign["target1_hit"] = True
                    campaign["breakeven_armed"] = True

        for leg in active_legs:
            if leg.name != "Runner" or not leg.trail_active:
                continue
            prior_stop = leg.stop_price
            leg.trail_high = max(float(leg.trail_high or leg.entry_price), high_price)
            trail_stop = leg.trail_high - float(params["runner_trail_points"])
            leg.stop_price = max(leg.stop_price, leg.entry_price, trail_stop)
            if leg.stop_price > prior_stop:
                _record_event(events, ts, leg.stop_price, "TRAIL", f"Campaign {leg.campaign_id} runner trail")

        for campaign in campaigns:
            if campaign.get("exit_time") is not None:
                continue
            campaign_legs = [leg for leg in active_legs if leg.campaign_id == campaign["campaign_id"]]
            if not campaign_legs:
                matching_trades = [trade for trade in trades if trade["campaign_id"] == campaign["campaign_id"]]
                if matching_trades:
                    campaign["exit_time"] = max(pd.to_datetime(trade["exit_date"]) for trade in matching_trades)
                    campaign["realized_pnl"] = round(sum(float(trade["pnl"]) for trade in matching_trades), 2)
                    campaign["runner_realized"] = round(
                        sum(float(trade["pnl"]) for trade in matching_trades if trade["leg"] == "Runner"),
                        2,
                    )
                    campaign["exit_reason"] = matching_trades[-1]["exit_rule"]

        is_entry_slot = (
            current_minute >= int(params["first_entry_minute"])
            and current_minute <= int(params["last_entry_minute"])
            and (current_minute - int(params["first_entry_minute"])) % int(params["entry_interval_minutes"]) == 0
        )

        if is_entry_slot and i + 1 < len(data):
            next_row = data.iloc[i + 1]
            next_day = next_row["_date_only"]
            next_minute = int(next_row["bar_minutes"])
            if next_day == day and next_minute <= int(params["final_exit_minute"]):
                entry_price = float(next_row["open"])
                daily_stop_active = len(daily_stopped_campaigns) >= int(params["daily_stop_limit"])
                lower_price_rejected = (
                    bool(params["skip_lower_entries"])
                    and last_accepted_entry_price is not None
                    and entry_price < float(last_accepted_entry_price)
                )
                if lower_price_rejected:
                    skipped_lower_slots += 1
                    _record_event(
                        events,
                        pd.to_datetime(next_row["date"]),
                        entry_price,
                        "SKIP",
                        f"Skipped slot below last accepted entry {last_accepted_entry_price:.2f}",
                    )
                elif daily_stop_active:
                    skipped_daily_stop_slots += 1
                    _record_event(
                        events,
                        pd.to_datetime(next_row["date"]),
                        entry_price,
                        "KILL",
                        f"Daily stop limit reached ({int(params['daily_stop_limit'])} stopped campaigns)",
                    )
                elif len({leg.campaign_id for leg in active_legs if leg.active}) >= int(params["max_open_campaigns"]):
                    _record_event(
                        events,
                        pd.to_datetime(next_row["date"]),
                        entry_price,
                        "CAP",
                        f"Skipped slot at max concurrent campaigns ({int(params['max_open_campaigns'])})",
                    )
                else:
                    campaign_id += 1
                    stop_price = entry_price - float(params["stop_loss_points"])
                    target_price = entry_price + float(params["target1_points"])
                    target_leg = Leg(campaign_id, "Target leg", 1, pd.to_datetime(next_row["date"]), entry_price, stop_price, target_price)
                    runner_contracts = max(int(params["contracts_per_campaign"]) - 1, 0)
                    runner_leg = (
                        [Leg(campaign_id, "Runner", runner_contracts, pd.to_datetime(next_row["date"]), entry_price, stop_price)]
                        if runner_contracts > 0
                        else []
                    )
                    active_legs.extend([target_leg] + runner_leg)
                    last_accepted_entry_price = entry_price
                    campaigns.append({
                        "campaign_id": campaign_id,
                        "day": str(day),
                        "slot_minute": current_minute,
                        "entry_time": pd.to_datetime(next_row["date"]),
                        "entry_price": round(entry_price, 4),
                        "target1_price": round(target_price, 4),
                        "initial_stop_price": round(stop_price, 4),
                        "target1_hit": False,
                        "breakeven_armed": False,
                        "exit_time": None,
                        "exit_reason": None,
                        "realized_pnl": 0.0,
                        "runner_realized": 0.0,
                    })
                    _record_event(events, pd.to_datetime(next_row["date"]), entry_price, "BUY", f"Campaign {campaign_id} scheduled entry")

        open_campaign_count = len({leg.campaign_id for leg in active_legs if leg.active})
        max_concurrent_campaigns = max(max_concurrent_campaigns, open_campaign_count)

        portfolio_value = cash
        for leg in active_legs:
            if leg.active:
                portfolio_value += (close_price - leg.entry_price) * point_value * leg.contracts
        equity_points.append({"date": ts, "portfolio_value": portfolio_value})

    if active_legs and not data.empty:
        last_row = data.iloc[-1]
        last_ts = pd.to_datetime(last_row["date"])
        last_close = float(last_row["close"])
        for leg in list(active_legs):
            pnl = _record_exit(leg, last_ts, last_close, "End of data", point_value, trades, events)
            cash += pnl
            exit_reason_counts["End of data"] = exit_reason_counts.get("End of data", 0) + 1
        equity_points.append({"date": last_ts, "portfolio_value": cash})

    for campaign in campaigns:
        if campaign.get("exit_time") is None:
            matching_trades = [trade for trade in trades if trade["campaign_id"] == campaign["campaign_id"]]
            if matching_trades:
                campaign["exit_time"] = max(pd.to_datetime(trade["exit_date"]) for trade in matching_trades)
                campaign["realized_pnl"] = round(sum(float(trade["pnl"]) for trade in matching_trades), 2)
                campaign["runner_realized"] = round(
                    sum(float(trade["pnl"]) for trade in matching_trades if trade["leg"] == "Runner"),
                    2,
                )
                campaign["exit_reason"] = matching_trades[-1]["exit_rule"]

    equity_df = pd.DataFrame(equity_points)
    equity_curve = pd.Series(equity_df["portfolio_value"].values, index=pd.to_datetime(equity_df["date"]))
    metrics = compute_metrics(equity_curve, trades)
    runner_trades = [trade for trade in trades if trade["leg"] == "Runner"]
    summary = {
        "campaign_days": len({campaign["day"] for campaign in campaigns}),
        "campaigns_opened": len(campaigns),
        "skipped_lower_slots": skipped_lower_slots,
        "skipped_daily_stop_slots": skipped_daily_stop_slots,
        "target1_hit_rate_pct": round(sum(1 for campaign in campaigns if campaign["target1_hit"]) / len(campaigns) * 100, 2) if campaigns else 0.0,
        "runner_win_rate_pct": round(sum(1 for trade in runner_trades if trade["pnl"] > 0) / len(runner_trades) * 100, 2) if runner_trades else 0.0,
        "max_concurrent_campaigns": max_concurrent_campaigns,
        "avg_hold_bars": round(pd.DataFrame(trades)["hold_bars"].mean(), 2) if trades else 0.0,
        "exit_reason_counts": exit_reason_counts,
    }
    return {
        "params": params,
        "metrics": metrics,
        "summary": summary,
        "trades": trades,
        "campaigns": campaigns,
        "equity_curve": equity_curve,
        "signals_df": pd.DataFrame(events),
        "final_value": float(equity_curve.iloc[-1]) if not equity_curve.empty else initial_capital,
        "price_df": data,
    }
