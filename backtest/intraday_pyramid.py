"""Intraday pyramid breakout backtest for TQQQ."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backtest.metrics import compute_metrics

REGULAR_OPEN_MINUTE = 9 * 60 + 30
FIRST_ENTRY_MINUTE = 10 * 60
LAST_ENTRY_MINUTE = 13 * 60
FINAL_EXIT_MINUTE = 15 * 60 + 55
REGULAR_CLOSE_MINUTE = 15 * 60 + 55

DEFAULT_PARAMS = {
    "campaign_allocation_pct": 1.0,
    "confirm_bars": 3,
    "tier_weights": [0.50, 0.30, 0.20],
    "tier2_trigger_pct": 0.35,
    "tier3_trigger_pct": 0.70,
    "initial_stop_cap_pct": 0.60,
    "post_tier1_stop_cap_pct": 0.30,
    "reclaim_buffer_pct": 0.10,
    "tier1_profit_lock_pct": 0.20,
}


@dataclass
class Tier:
    name: str
    shares: float
    entry_price: float
    entry_time: pd.Timestamp
    stop_price: float
    initial_value: float
    active: bool = True


def _prepare_intraday(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["bar_minutes"] = df["date"].dt.hour * 60 + df["date"].dt.minute
    df["_date_only"] = df["date"].dt.date
    df["regular"] = df["bar_minutes"].between(REGULAR_OPEN_MINUTE, REGULAR_CLOSE_MINUTE)

    regular = df[df["regular"]].copy()
    regular["typical_price"] = (regular["high"] + regular["low"] + regular["close"]) / 3
    regular["tpv"] = regular["typical_price"] * regular["volume"]
    regular["cum_tpv"] = regular.groupby("_date_only")["tpv"].cumsum()
    regular["cum_volume"] = regular.groupby("_date_only")["volume"].cumsum()
    regular["vwap"] = regular["cum_tpv"] / regular["cum_volume"].where(regular["cum_volume"] != 0)

    daily_high = (
        regular.groupby("_date_only", sort=True)["high"]
        .max()
        .rename("session_high")
        .reset_index()
    )
    daily_high["yesterday_high"] = daily_high["session_high"].shift(1)

    regular = regular.merge(daily_high[["_date_only", "yesterday_high"]], on="_date_only", how="left")
    regular["above_yhigh"] = regular["close"] > regular["yesterday_high"]
    regular["above_vwap"] = regular["close"] > regular["vwap"]
    regular["setup_ok"] = regular["above_yhigh"] & regular["above_vwap"]
    return regular.reset_index(drop=True)


def _merge_params(params: Optional[dict]) -> dict:
    merged = DEFAULT_PARAMS.copy()
    if params:
        merged.update(params)
    return merged


def _trade_value(portfolio_value: float, allocation_pct: float, weight: float) -> float:
    return portfolio_value * allocation_pct * weight


def _record_event(events: list, ts: pd.Timestamp, price: float, signal: str, detail: str = "") -> None:
    events.append({"date": ts, "price": round(float(price), 4), "signal": signal, "detail": detail})


def _tighten_remaining_tiers_after_tier1_exit(tiers: list[Tier], params: dict) -> None:
    """Once Tier 1 is gone, tighten remaining tiers around their own entries."""
    active_names = {tier.name for tier in tiers if tier.active}
    if "Tier 1" in active_names:
        return
    for tier in tiers:
        if not tier.active or tier.name == "Tier 1":
            continue
        tightened_stop = tier.entry_price * (1 - params["post_tier1_stop_cap_pct"] / 100)
        tier.stop_price = max(tier.stop_price, tightened_stop)


def _exit_tier(
    tier: Tier,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    cash: float,
    trades: list,
    events: list,
) -> float:
    proceeds = tier.shares * exit_price
    pnl = proceeds - tier.initial_value
    cash += proceeds
    tier.active = False
    trades.append({
        "tier": tier.name,
        "entry_date": str(tier.entry_time),
        "exit_date": str(exit_time),
        "entry_price": round(tier.entry_price, 4),
        "exit_price": round(float(exit_price), 4),
        "shares": round(tier.shares, 6),
        "pnl": round(pnl, 2),
        "return_pct": round((exit_price / tier.entry_price - 1) * 100, 2),
        "exit_rule": exit_reason,
        "hold_bars": None,
    })
    _record_event(events, exit_time, exit_price, "SELL", f"{tier.name} {exit_reason}")
    return cash


def run_backtest(
    df: pd.DataFrame,
    params: Optional[dict] = None,
    initial_capital: float = 10_000.0,
) -> dict:
    params = _merge_params(params)
    data = _prepare_intraday(df)
    if data.empty:
        empty_ec = pd.Series([initial_capital], index=[pd.Timestamp("1970-01-01")], dtype=float)
        return {
            "params": params,
            "metrics": compute_metrics(empty_ec, []),
            "trades": [],
            "campaigns": [],
            "equity_curve": empty_ec,
            "signals_df": pd.DataFrame(columns=["date", "price", "signal", "detail"]),
            "final_value": initial_capital,
            "price_df": data,
        }

    cash = initial_capital
    tiers: list[Tier] = []
    trades = []
    events = []
    campaigns = []
    equity_points = []
    last_day = None
    day_started = False
    day_entry_done = False
    day_first_entry_price = None
    day_campaign = None

    for i, row in data.iterrows():
        ts = row["date"]
        current_day = row["_date_only"]
        price = float(row["close"])
        current_minute = int(row["bar_minutes"])

        if last_day != current_day:
            last_day = current_day
            day_started = False
            day_entry_done = False
            day_first_entry_price = None
            day_campaign = None
            tiers = [tier for tier in tiers if tier.active]

        # 1. Stop exits first.
        active_tiers = [tier for tier in tiers if tier.active]
        for tier in list(active_tiers):
            if float(row["low"]) <= tier.stop_price:
                cash = _exit_tier(tier, ts, tier.stop_price, "Stop", cash, trades, events)

        tiers = [tier for tier in tiers if tier.active]
        _tighten_remaining_tiers_after_tier1_exit(tiers, params)

        # 2. Forced end-of-day flat.
        if tiers and current_minute >= FINAL_EXIT_MINUTE:
            for tier in list(tiers):
                cash = _exit_tier(tier, ts, price, "End of day", cash, trades, events)
            tiers = []
            if day_campaign is not None and day_campaign.get("exit_time") is None:
                day_campaign["exit_time"] = ts
                day_campaign["exit_reason"] = "End of day"

        # 3. Initial entry.
        if (
            not day_started
            and not day_entry_done
            and current_minute >= FIRST_ENTRY_MINUTE
            and current_minute <= LAST_ENTRY_MINUTE
            and pd.notna(row["yesterday_high"])
        ):
            lookback = data.iloc[max(0, i - params["confirm_bars"] + 1): i + 1]
            if (
                len(lookback) == params["confirm_bars"]
                and lookback["_date_only"].nunique() == 1
                and bool(lookback["setup_ok"].all())
            ):
                portfolio_value = cash + sum(tier.shares * price for tier in tiers)
                tier_value = _trade_value(
                    portfolio_value,
                    params["campaign_allocation_pct"],
                    params["tier_weights"][0],
                )
                if cash >= tier_value > 0:
                    initial_stop = min(
                        float(row["yesterday_high"]) * (1 - params["reclaim_buffer_pct"] / 100),
                        price * (1 - params["initial_stop_cap_pct"] / 100),
                    )
                    shares = tier_value / price
                    tier = Tier("Tier 1", shares, price, ts, initial_stop, tier_value)
                    tiers.append(tier)
                    cash -= tier_value
                    day_started = True
                    day_entry_done = True
                    day_first_entry_price = price
                    day_campaign = {
                        "day": str(current_day),
                        "entry_time": ts,
                        "entry_price": round(price, 4),
                        "tier2_hit": False,
                        "tier3_hit": False,
                        "exit_time": None,
                        "exit_reason": None,
                    }
                    campaigns.append(day_campaign)
                    _record_event(events, ts, price, "BUY", "Tier 1")

        # 4. Adds after the base is active.
        if tiers and day_first_entry_price is not None and current_minute <= LAST_ENTRY_MINUTE:
            active_names = {tier.name for tier in tiers if tier.active}
            portfolio_value = cash + sum(tier.shares * price for tier in tiers)
            added_this_bar = False

            if "Tier 2" not in active_names and price >= day_first_entry_price * (1 + params["tier2_trigger_pct"] / 100):
                tier_value = _trade_value(
                    portfolio_value,
                    params["campaign_allocation_pct"],
                    params["tier_weights"][1],
                )
                if cash >= tier_value > 0:
                    shares = tier_value / price
                    tier = Tier("Tier 2", shares, price, ts, day_first_entry_price, tier_value)
                    tiers.append(tier)
                    cash -= tier_value
                    for existing in tiers:
                        if existing.name == "Tier 1" and existing.active:
                            existing.stop_price = max(existing.stop_price, existing.entry_price)
                    if day_campaign is not None:
                        day_campaign["tier2_hit"] = True
                    _record_event(events, ts, price, "ADD", "Tier 2")
                    active_names.add("Tier 2")
                    added_this_bar = True

            if (
                not added_this_bar
                and "Tier 2" in active_names
                and "Tier 3" not in active_names
                and price >= day_first_entry_price * (1 + params["tier3_trigger_pct"] / 100)
            ):
                tier2_entry = next((tier.entry_price for tier in tiers if tier.name == "Tier 2" and tier.active), None)
                tier_value = _trade_value(
                    portfolio_value,
                    params["campaign_allocation_pct"],
                    params["tier_weights"][2],
                )
                if cash >= tier_value > 0 and tier2_entry is not None:
                    shares = tier_value / price
                    tier = Tier("Tier 3", shares, price, ts, tier2_entry, tier_value)
                    tiers.append(tier)
                    cash -= tier_value
                    for existing in tiers:
                        if not existing.active:
                            continue
                        if existing.name == "Tier 1":
                            existing.stop_price = max(existing.stop_price, existing.entry_price * (1 + params["tier1_profit_lock_pct"] / 100))
                        elif existing.name == "Tier 2":
                            existing.stop_price = max(existing.stop_price, existing.entry_price)
                    if day_campaign is not None:
                        day_campaign["tier3_hit"] = True
                    _record_event(events, ts, price, "ADD", "Tier 3")

        # 5. Trailing stops once all tiers are active.
        active_tiers = [tier for tier in tiers if tier.active]
        active_names = {tier.name for tier in active_tiers}
        if {"Tier 1", "Tier 2", "Tier 3"}.issubset(active_names):
            day_slice = data.iloc[: i + 1]
            day_slice = day_slice[day_slice["_date_only"] == current_day]
            prior1 = day_slice["low"].iloc[-2] if len(day_slice) >= 2 else None
            prior2 = day_slice["low"].iloc[-3:-1].min() if len(day_slice) >= 3 else None
            prior3 = day_slice["low"].iloc[-4:-1].min() if len(day_slice) >= 4 else None
            for tier in active_tiers:
                if tier.name == "Tier 1" and prior1 is not None:
                    tier.stop_price = max(tier.stop_price, float(prior1))
                elif tier.name == "Tier 2" and prior2 is not None:
                    tier.stop_price = max(tier.stop_price, float(prior2))
                elif tier.name == "Tier 3" and prior3 is not None:
                    tier.stop_price = max(tier.stop_price, float(prior3))

        # 6. Equity mark.
        portfolio_value = cash + sum(tier.shares * price for tier in tiers if tier.active)
        equity_points.append({"date": ts, "portfolio_value": portfolio_value})

        if day_campaign is not None and not tiers and day_campaign.get("exit_time") is None:
            day_campaign["exit_time"] = ts
            day_campaign["exit_reason"] = "Stop"

    if tiers and not data.empty:
        last_row = data.iloc[-1]
        last_ts = pd.to_datetime(last_row["date"])
        last_price = float(last_row["close"])
        for tier in list(tiers):
            cash = _exit_tier(tier, last_ts, last_price, "End of data", cash, trades, events)
        tiers = []
        if day_campaign is not None and day_campaign.get("exit_time") is None:
            day_campaign["exit_time"] = last_ts
            day_campaign["exit_reason"] = "End of data"
        equity_points.append({"date": last_ts, "portfolio_value": cash})

    for trade in trades:
        entry_ts = pd.to_datetime(trade["entry_date"])
        exit_ts = pd.to_datetime(trade["exit_date"])
        trade["hold_bars"] = max(int((exit_ts - entry_ts).total_seconds() // 300), 0)

    equity_df = pd.DataFrame(equity_points)
    equity_curve = pd.Series(equity_df["portfolio_value"].values, index=pd.to_datetime(equity_df["date"]))
    metrics = compute_metrics(equity_curve, trades) if not equity_curve.empty else {}

    exit_reason_counts = {}
    if campaigns:
        for campaign in campaigns:
            reason = campaign.get("exit_reason") or "Unknown"
            exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1

    summary = {
        "campaign_days": len(campaigns),
        "tier2_hit_rate_pct": round(sum(1 for c in campaigns if c["tier2_hit"]) / len(campaigns) * 100, 2) if campaigns else 0.0,
        "tier3_hit_rate_pct": round(sum(1 for c in campaigns if c["tier3_hit"]) / len(campaigns) * 100, 2) if campaigns else 0.0,
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
