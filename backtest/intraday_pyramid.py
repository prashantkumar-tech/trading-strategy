"""TQQQ intraday pyramid backtest using MES-style campaign rules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backtest.metrics import compute_metrics

REGULAR_OPEN_MINUTE = 9 * 60 + 30
FINAL_EXIT_MINUTE = 15 * 60 + 55
REGULAR_CLOSE_MINUTE = 15 * 60 + 55

DEFAULT_PARAMS = {
    "confirm_bars": 3,
    "first_entry_minute": 10 * 60 + 15,
    "last_entry_minute": 11 * 60 + 15,
    "last_add_minute": 13 * 60,
    "campaign_allocation_pct": 1.0,
    "base_allocation_pct": 0.40,
    "add_allocation_pcts": [0.35, 0.25],
    "breakout_buffer_pct": 0.20,
    "stop_loss_pct": 0.90,
    "take_profit_pct": 2.00,
    "daily_loss_limit_r": -3.0,
    "add_trigger_pcts": [0.70, 1.20],
    "add2_tighten_delay_bars": 1,
    "profit_lock_pct_after_add2": 0.30,
}


@dataclass
class Tier:
    name: str
    shares: float
    entry_price: float
    entry_time: pd.Timestamp
    cost_basis: float
    stop_price: float
    target_price: float
    active: bool = True


def _merge_params(params: Optional[dict]) -> dict:
    merged = DEFAULT_PARAMS.copy()
    if params:
        merged.update(params)
    merged["add_allocation_pcts"] = [float(x) for x in merged.get("add_allocation_pcts", [])]
    merged["add_trigger_pcts"] = [float(x) for x in merged.get("add_trigger_pcts", [])]
    return merged


def _prepare_session_context(df: pd.DataFrame) -> pd.DataFrame:
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

    daily = (
        regular.groupby("_date_only", sort=True)
        .agg(
            session_open=("open", "first"),
            session_high=("high", "max"),
        )
        .reset_index()
    )
    daily["pdh"] = daily["session_high"].shift(1)
    regular = regular.merge(daily[["_date_only", "session_open", "pdh"]], on="_date_only", how="left")
    regular["above_pdh"] = regular["close"] > regular["pdh"]
    regular["above_vwap"] = regular["close"] > regular["vwap"]
    regular["above_open"] = regular["close"] > regular["session_open"]
    regular["long_setup_ok"] = regular["above_pdh"] & regular["above_vwap"] & regular["above_open"]
    return regular.reset_index(drop=True)


def _record_event(events: list, ts: pd.Timestamp, price: float, signal: str, detail: str) -> None:
    events.append({"date": ts, "price": round(float(price), 4), "signal": signal, "detail": detail})


def _better_stop(current_stop: float, candidate_stop: float) -> float:
    return max(current_stop, candidate_stop)


def _sync_stops_after_add(tiers: list[Tier], campaign: dict, params: dict, current_index: int) -> None:
    active = {tier.name: tier for tier in tiers if tier.active}
    if "Add 1" in active:
        base_entry = campaign["base_entry_price"]
        if "Base" in active:
            active["Base"].stop_price = _better_stop(active["Base"].stop_price, base_entry)
        add1_floor = max(
            active["Add 1"].entry_price * (1 - float(params["stop_loss_pct"]) / 100),
            base_entry,
        )
        active["Add 1"].stop_price = _better_stop(active["Add 1"].stop_price, add1_floor)

    if "Add 2" in active:
        base_lock = campaign["base_entry_price"] * (1 + float(params["profit_lock_pct_after_add2"]) / 100)
        add1_entry = campaign.get("add1_entry_price", campaign["base_entry_price"])
        add2_entry_index = campaign.get("add2_entry_index")
        delay_bars = int(params.get("add2_tighten_delay_bars", 0))
        if "Base" in active:
            active["Base"].stop_price = _better_stop(active["Base"].stop_price, base_lock)
        if "Add 1" in active:
            active["Add 1"].stop_price = _better_stop(active["Add 1"].stop_price, add1_entry)
        if add2_entry_index is not None and current_index - int(add2_entry_index) >= delay_bars:
            add2_floor = max(
                active["Add 2"].entry_price * (1 - float(params["stop_loss_pct"]) / 100),
                add1_entry,
            )
            active["Add 2"].stop_price = _better_stop(active["Add 2"].stop_price, add2_floor)


def _record_exit(
    tier: Tier,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    cash: float,
    trades: list,
    events: list,
) -> float:
    proceeds = tier.shares * float(exit_price)
    pnl = proceeds - tier.cost_basis
    hold_bars = max(int((exit_time - tier.entry_time).total_seconds() // 300), 0)
    trades.append({
        "tier": tier.name,
        "entry_date": str(tier.entry_time),
        "exit_date": str(exit_time),
        "entry_price": round(tier.entry_price, 4),
        "exit_price": round(float(exit_price), 4),
        "shares": round(tier.shares, 6),
        "pnl": round(pnl, 2),
        "return_pct": round((float(exit_price) / tier.entry_price - 1) * 100, 2),
        "exit_rule": exit_reason,
        "hold_bars": hold_bars,
    })
    _record_event(events, exit_time, exit_price, "SELL", f"{tier.name} {exit_reason}")
    tier.active = False
    return cash + proceeds


def _tier_value(portfolio_value: float, params: dict, allocation_pct: float) -> float:
    return portfolio_value * float(params["campaign_allocation_pct"]) * allocation_pct


def run_backtest(
    df: pd.DataFrame,
    params: Optional[dict] = None,
    initial_capital: float = 10_000.0,
) -> dict:
    params = _merge_params(params)
    data = _prepare_session_context(df)
    if data.empty:
        empty_ec = pd.Series([initial_capital], index=[pd.Timestamp("1970-01-01")], dtype=float)
        return {
            "params": params,
            "metrics": compute_metrics(empty_ec, []),
            "summary": {
                "setup_days": 0,
                "campaign_days": 0,
                "avg_shares_per_campaign": 0.0,
                "add1_hit_rate_pct": 0.0,
                "add2_hit_rate_pct": 0.0,
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

    stop_loss_pct = float(params["stop_loss_pct"])
    take_profit_pct = float(params["take_profit_pct"])
    add_allocation_pcts = params["add_allocation_pcts"]
    add_trigger_pcts = params["add_trigger_pcts"]

    cash = initial_capital
    tiers: list[Tier] = []
    trades = []
    events = []
    campaigns = []
    equity_points = []
    exit_reason_counts = {}
    daily_realized_pnl: dict = {}
    setup_days = set()

    active_day = None
    campaign = None
    next_add_index = 0
    campaign_started = False

    for i, row in data.iterrows():
        ts = pd.to_datetime(row["date"])
        day = row["_date_only"]
        current_minute = int(row["bar_minutes"])
        close_price = float(row["close"])

        if active_day != day:
            active_day = day
            tiers = [tier for tier in tiers if tier.active]
            campaign = None
            next_add_index = 0
            campaign_started = False

        for tier in list(tiers):
            if not tier.active:
                continue
            stop_hit = float(row["low"]) <= tier.stop_price
            target_hit = float(row["high"]) >= tier.target_price
            exit_reason = None
            exit_price = None

            if stop_hit:
                exit_reason = "Stop"
                exit_price = tier.stop_price
            elif target_hit:
                exit_reason = "Target"
                exit_price = tier.target_price
            elif current_minute >= FINAL_EXIT_MINUTE:
                exit_reason = "End of day"
                exit_price = close_price

            if exit_reason is not None:
                cash = _record_exit(tier, ts, exit_price, exit_reason, cash, trades, events)
                pnl = trades[-1]["pnl"]
                daily_realized_pnl[day] = daily_realized_pnl.get(day, 0.0) + pnl
                exit_reason_counts[exit_reason] = exit_reason_counts.get(exit_reason, 0) + 1

        tiers = [tier for tier in tiers if tier.active]
        if campaign is not None and tiers:
            _sync_stops_after_add(tiers, campaign, params, i)

        if campaign is not None and not tiers and campaign.get("exit_time") is None:
            campaign["exit_time"] = ts
            campaign["exit_reason"] = "All tiers closed"

        if (
            not campaign_started
            and not tiers
            and current_minute >= int(params["first_entry_minute"])
            and current_minute < int(params["last_entry_minute"])
        ):
            lookback = data.iloc[max(0, i - int(params["confirm_bars"]) + 1): i + 1]
            daily_loss_limit = float(params["daily_loss_limit_r"]) * stop_loss_pct / 100 * initial_capital
            trigger_met = (
                pd.notna(row["pdh"])
                and close_price >= float(row["pdh"]) * (1 + float(params["breakout_buffer_pct"]) / 100)
            )
            if (
                len(lookback) == int(params["confirm_bars"])
                and lookback["_date_only"].nunique() == 1
                and bool(lookback["long_setup_ok"].all())
                and trigger_met
                and daily_realized_pnl.get(day, 0.0) > daily_loss_limit
                and i + 1 < len(data)
            ):
                next_row = data.iloc[i + 1]
                if next_row["_date_only"] == day and int(next_row["bar_minutes"]) <= int(params["last_entry_minute"]):
                    entry_price = float(next_row["open"])
                    portfolio_value = cash
                    tier_value = _tier_value(portfolio_value, params, float(params["base_allocation_pct"]))
                    if tier_value > 0 and cash >= tier_value:
                        shares = tier_value / entry_price
                        base = Tier(
                            name="Base",
                            shares=shares,
                            entry_price=entry_price,
                            entry_time=pd.to_datetime(next_row["date"]),
                            cost_basis=tier_value,
                            stop_price=entry_price * (1 - stop_loss_pct / 100),
                            target_price=entry_price * (1 + take_profit_pct / 100),
                        )
                        cash -= tier_value
                        tiers = [base]
                        campaign_started = True
                        next_add_index = 0
                        campaign = {
                            "day": str(day),
                            "entry_time": pd.to_datetime(next_row["date"]),
                            "base_entry_price": entry_price,
                            "add1_hit": False,
                            "add2_hit": False,
                            "total_shares": shares,
                            "exit_time": None,
                            "exit_reason": None,
                        }
                        campaigns.append(campaign)
                        setup_days.add(day)
                        _record_event(events, pd.to_datetime(next_row["date"]), entry_price, "BUY", "Base")

        if (
            campaign is not None
            and tiers
            and next_add_index < len(add_trigger_pcts)
            and next_add_index < len(add_allocation_pcts)
            and current_minute < int(params["last_add_minute"])
        ):
            while next_add_index < len(add_allocation_pcts) and float(add_allocation_pcts[next_add_index]) <= 0:
                next_add_index += 1
            if next_add_index < len(add_trigger_pcts) and next_add_index < len(add_allocation_pcts):
                trigger_price = campaign["base_entry_price"] * (1 + float(add_trigger_pcts[next_add_index]) / 100)
                trigger_hit = close_price >= trigger_price
                if trigger_hit and i + 1 < len(data):
                    next_row = data.iloc[i + 1]
                    if next_row["_date_only"] == day and int(next_row["bar_minutes"]) <= int(params["last_add_minute"]):
                        tier_name = f"Add {next_add_index + 1}"
                        existing_names = {tier.name for tier in tiers if tier.active}
                        if tier_name not in existing_names:
                            entry_price = float(next_row["open"])
                            portfolio_value = cash + sum(tier.shares * close_price for tier in tiers if tier.active)
                            tier_value = _tier_value(portfolio_value, params, float(add_allocation_pcts[next_add_index]))
                            if tier_value > 0 and cash >= tier_value:
                                shares = tier_value / entry_price
                                tier = Tier(
                                    name=tier_name,
                                    shares=shares,
                                    entry_price=entry_price,
                                    entry_time=pd.to_datetime(next_row["date"]),
                                    cost_basis=tier_value,
                                    stop_price=entry_price * (1 - stop_loss_pct / 100),
                                    target_price=entry_price * (1 + take_profit_pct / 100),
                                )
                                tiers.append(tier)
                                cash -= tier_value
                                campaign["total_shares"] += shares
                                if next_add_index == 0:
                                    campaign["add1_hit"] = True
                                    campaign["add1_entry_price"] = entry_price
                                elif next_add_index == 1:
                                    campaign["add2_hit"] = True
                                    campaign["add2_entry_price"] = entry_price
                                    campaign["add2_entry_index"] = i + 1
                                _sync_stops_after_add(tiers, campaign, params, i + 1)
                                _record_event(events, pd.to_datetime(next_row["date"]), entry_price, "ADD", tier_name)
                                next_add_index += 1

        portfolio_value = cash + sum(tier.shares * close_price for tier in tiers if tier.active)
        equity_points.append({"date": ts, "portfolio_value": portfolio_value})

    if tiers and not data.empty:
        last_row = data.iloc[-1]
        last_ts = pd.to_datetime(last_row["date"])
        last_close = float(last_row["close"])
        for tier in list(tiers):
            cash = _record_exit(tier, last_ts, last_close, "End of data", cash, trades, events)
            exit_reason_counts["End of data"] = exit_reason_counts.get("End of data", 0) + 1
        if campaign is not None and campaign.get("exit_time") is None:
            campaign["exit_time"] = last_ts
            campaign["exit_reason"] = "End of data"
        equity_points.append({"date": last_ts, "portfolio_value": cash})

    equity_df = pd.DataFrame(equity_points)
    equity_curve = pd.Series(equity_df["portfolio_value"].values, index=pd.to_datetime(equity_df["date"]))
    metrics = compute_metrics(equity_curve, trades)

    summary = {
        "setup_days": len(setup_days),
        "campaign_days": len(campaigns),
        "avg_shares_per_campaign": round(pd.DataFrame(campaigns)["total_shares"].mean(), 2) if campaigns else 0.0,
        "add1_hit_rate_pct": round(sum(1 for c in campaigns if c["add1_hit"]) / len(campaigns) * 100, 2) if campaigns else 0.0,
        "add2_hit_rate_pct": round(sum(1 for c in campaigns if c["add2_hit"]) / len(campaigns) * 100, 2) if campaigns else 0.0,
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
