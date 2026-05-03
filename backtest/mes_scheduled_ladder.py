"""MES scheduled trend ladder backtest on 5-minute data."""

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
    "stop_loss_points": 20.0,
    "breakeven_after_target1": True,
    "runner_management": "breakeven_only",
    "require_above_open": True,
    "require_above_vwap": True,
    "max_open_campaigns": 1,
    "point_value": 5.0,
}


@dataclass
class Leg:
    name: str
    contracts: int
    entry_time: pd.Timestamp
    entry_price: float
    stop_price: float
    target_price: Optional[float] = None
    active: bool = True


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

    regular["typical_price"] = (regular["high"] + regular["low"] + regular["close"]) / 3
    regular["tpv"] = regular["typical_price"] * regular["volume"]
    regular["cum_tpv"] = regular.groupby("_date_only")["tpv"].cumsum()
    regular["cum_volume"] = regular.groupby("_date_only")["volume"].cumsum()
    regular["vwap"] = regular["cum_tpv"] / regular["cum_volume"].where(regular["cum_volume"] != 0)
    daily = regular.groupby("_date_only", sort=True).agg(session_open=("open", "first")).reset_index()
    regular = regular.merge(daily, on="_date_only", how="left")
    return regular.reset_index(drop=True)


def _record_event(events: list, ts: pd.Timestamp, price: float, signal: str, detail: str) -> None:
    events.append({"date": ts, "price": round(float(price), 4), "signal": signal, "detail": detail})


def _record_exit(leg: Leg, exit_time: pd.Timestamp, exit_price: float, exit_rule: str, point_value: float, trades: list, events: list) -> float:
    pnl_points = float(exit_price) - leg.entry_price
    pnl = pnl_points * point_value * leg.contracts
    hold_bars = max(int((exit_time - leg.entry_time).total_seconds() // 300), 0)
    trades.append({
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
    _record_event(events, exit_time, exit_price, "SELL", f"{leg.name} {exit_rule}")
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
                "target1_hit_rate_pct": 0.0,
                "runner_win_rate_pct": 0.0,
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
    active_campaign = None
    active_day = None

    for i, row in data.iterrows():
        ts = pd.to_datetime(row["date"])
        day = row["_date_only"]
        current_minute = int(row["bar_minutes"])
        close_price = float(row["close"])

        if active_day != day:
            active_day = day
            active_legs = [leg for leg in active_legs if leg.active]

        # Manage current open legs.
        target1_hit_this_bar = False
        for leg in list(active_legs):
            if not leg.active:
                continue
            stop_hit = float(row["low"]) <= leg.stop_price
            target_hit = leg.target_price is not None and float(row["high"]) >= leg.target_price
            exit_rule = None
            exit_price = None

            if stop_hit:
                exit_rule = "Stop"
                exit_price = leg.stop_price
            elif target_hit:
                exit_rule = "Target"
                exit_price = leg.target_price
                if leg.name == "Target leg":
                    target1_hit_this_bar = True
            elif current_minute >= int(params["final_exit_minute"]):
                exit_rule = "End of day"
                exit_price = close_price

            if exit_rule is not None:
                pnl = _record_exit(leg, ts, exit_price, exit_rule, point_value, trades, events)
                cash += pnl
                exit_reason_counts[exit_rule] = exit_reason_counts.get(exit_rule, 0) + 1

        active_legs = [leg for leg in active_legs if leg.active]

        # Tighten runner after target leg exits.
        if active_campaign is not None and active_legs and target1_hit_this_bar and bool(params["breakeven_after_target1"]):
            for leg in active_legs:
                if leg.name == "Runner":
                    leg.stop_price = max(leg.stop_price, active_campaign["entry_price"])
                    active_campaign["breakeven_active"] = True

        if (
            active_campaign is not None
            and active_campaign.get("breakeven_active")
            and params["runner_management"] == "trail_2bar_low"
            and active_legs
            and len(data.iloc[: i + 1][data.iloc[: i + 1]["_date_only"] == day]) >= 3
        ):
            day_slice = data.iloc[: i + 1]
            day_slice = day_slice[day_slice["_date_only"] == day]
            trail_price = float(day_slice["low"].iloc[-3:-1].min())
            for leg in active_legs:
                if leg.name == "Runner":
                    leg.stop_price = max(leg.stop_price, trail_price)

        if active_campaign is not None and not active_legs and active_campaign.get("exit_time") is None:
            active_campaign["exit_time"] = ts
            active_campaign["exit_reason"] = "Flat"
            active_campaign = None

        # Scheduled entries only if flat.
        is_entry_slot = (
            current_minute >= int(params["first_entry_minute"])
            and current_minute <= int(params["last_entry_minute"])
            and (current_minute - int(params["first_entry_minute"])) % int(params["entry_interval_minutes"]) == 0
        )
        filter_ok = True
        if bool(params["require_above_open"]):
            filter_ok = filter_ok and close_price > float(row["session_open"])
        if bool(params["require_above_vwap"]):
            filter_ok = filter_ok and close_price > float(row["vwap"])

        if (
            is_entry_slot
            and filter_ok
            and len(active_legs) < int(params["max_open_campaigns"])
            and not active_legs
            and i + 1 < len(data)
        ):
            next_row = data.iloc[i + 1]
            if next_row["_date_only"] == day and int(next_row["bar_minutes"]) <= int(params["final_exit_minute"]):
                entry_price = float(next_row["open"])
                stop_price = entry_price - float(params["stop_loss_points"])
                target_price = entry_price + float(params["target1_points"])
                target_leg = Leg("Target leg", 1, pd.to_datetime(next_row["date"]), entry_price, stop_price, target_price)
                runner_leg = Leg("Runner", max(int(params["contracts_per_campaign"]) - 1, 0), pd.to_datetime(next_row["date"]), entry_price, stop_price, None)
                active_legs = [target_leg] + ([runner_leg] if runner_leg.contracts > 0 else [])
                active_campaign = {
                    "day": str(day),
                    "entry_time": pd.to_datetime(next_row["date"]),
                    "entry_price": entry_price,
                    "scheduled_slot": current_minute,
                    "target1_hit": False,
                    "breakeven_active": False,
                    "exit_time": None,
                    "exit_reason": None,
                }
                campaigns.append(active_campaign)
                _record_event(events, pd.to_datetime(next_row["date"]), entry_price, "BUY", f"2-lot entry from {current_minute}")

        if active_campaign is not None:
            active_campaign["target1_hit"] = active_campaign["target1_hit"] or target1_hit_this_bar

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

    equity_df = pd.DataFrame(equity_points)
    equity_curve = pd.Series(equity_df["portfolio_value"].values, index=pd.to_datetime(equity_df["date"]))
    metrics = compute_metrics(equity_curve, trades)
    runner_trades = [t for t in trades if t["leg"] == "Runner"]
    summary = {
        "campaign_days": len({c["day"] for c in campaigns}),
        "campaigns_opened": len(campaigns),
        "target1_hit_rate_pct": round(sum(1 for c in campaigns if c["target1_hit"]) / len(campaigns) * 100, 2) if campaigns else 0.0,
        "runner_win_rate_pct": round(sum(1 for t in runner_trades if t["pnl"] > 0) / len(runner_trades) * 100, 2) if runner_trades else 0.0,
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
