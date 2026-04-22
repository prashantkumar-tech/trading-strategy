"""MES intraday pyramid backtest using prior-day high acceptance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backtest.metrics import compute_metrics

REGULAR_OPEN_MINUTE = 9 * 60 + 30
FINAL_EXIT_MINUTE = 15 * 60 + 55
REGULAR_CLOSE_MINUTE = 16 * 60

DEFAULT_PARAMS = {
    "confirm_bars": 3,
    "first_entry_minute": 10 * 60 + 15,
    "last_entry_minute": 11 * 60 + 15,
    "last_add_minute": 13 * 60,
    "breakout_buffer_points": 2.0,
    "stop_loss_points": 11.0,
    "take_profit_points": 20.0,
    "daily_loss_limit_r": -3.0,
    "base_contracts": 1,
    "add_contracts": [2, 1],
    "add_trigger_points": [7.0, 9.0],
    "add2_tighten_delay_bars": 1,
    "profit_lock_points_after_add2": 2.0,
    "point_value": 5.0,
    "trade_direction": "long",
    "short_regime_filter": "always",
}


@dataclass
class Tier:
    name: str
    direction: str
    contracts: int
    entry_price: float
    entry_time: pd.Timestamp
    stop_price: float
    target_price: float
    active: bool = True


def _merge_params(params: Optional[dict]) -> dict:
    merged = DEFAULT_PARAMS.copy()
    if params:
        merged.update(params)
    merged["add_contracts"] = list(merged.get("add_contracts", []))
    merged["add_trigger_points"] = [float(x) for x in merged.get("add_trigger_points", [])]
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
            session_low=("low", "min"),
        )
        .reset_index()
    )
    daily["pdh"] = daily["session_high"].shift(1)
    daily["pdl"] = daily["session_low"].shift(1)
    regular = regular.merge(daily[["_date_only", "session_open", "pdh", "pdl"]], on="_date_only", how="left")
    regular["above_pdh"] = regular["close"] > regular["pdh"]
    regular["above_vwap"] = regular["close"] > regular["vwap"]
    regular["above_open"] = regular["close"] > regular["session_open"]
    regular["below_pdl"] = regular["close"] < regular["pdl"]
    regular["below_vwap"] = regular["close"] < regular["vwap"]
    regular["below_open"] = regular["close"] < regular["session_open"]
    regular["long_setup_ok"] = regular["above_pdh"] & regular["above_vwap"] & regular["above_open"]
    regular["short_setup_ok"] = regular["below_pdl"] & regular["below_vwap"] & regular["below_open"]
    return regular.reset_index(drop=True)


def _record_event(events: list, ts: pd.Timestamp, price: float, signal: str, detail: str) -> None:
    events.append({"date": ts, "price": round(float(price), 4), "signal": signal, "detail": detail})


def _record_exit(
    tier: Tier,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    point_value: float,
    trades: list,
    events: list,
) -> float:
    direction_sign = 1.0 if tier.direction == "long" else -1.0
    pnl_points = (float(exit_price) - tier.entry_price) * direction_sign
    pnl = pnl_points * point_value * tier.contracts
    hold_bars = max(int((exit_time - tier.entry_time).total_seconds() // 300), 0)
    trades.append({
        "tier": tier.name,
        "entry_date": str(tier.entry_time),
        "exit_date": str(exit_time),
        "entry_price": round(tier.entry_price, 4),
        "exit_price": round(float(exit_price), 4),
        "direction": tier.direction.capitalize(),
        "contracts": tier.contracts,
        "pnl": round(pnl, 2),
        "return_pct": round((pnl_points / tier.entry_price) * 100, 2),
        "exit_rule": exit_reason,
        "hold_bars": hold_bars,
    })
    exit_signal = "SELL" if tier.direction == "long" else "COVER"
    _record_event(events, exit_time, exit_price, exit_signal, f"{tier.direction.capitalize()} {tier.name} {exit_reason}")
    tier.active = False
    return pnl


def _better_stop(direction: str, current_stop: float, candidate_stop: float) -> float:
    return max(current_stop, candidate_stop) if direction == "long" else min(current_stop, candidate_stop)


def _sync_stops_after_add(tiers: list[Tier], campaign: dict, params: dict, current_index: int) -> None:
    active = {tier.name: tier for tier in tiers if tier.active}
    direction = campaign["direction"]
    if "Add 1" in active:
        base_entry = campaign["base_entry_price"]
        if "Base" in active:
            active["Base"].stop_price = _better_stop(direction, active["Base"].stop_price, base_entry)
        if direction == "long":
            add1_floor = max(active["Add 1"].entry_price - float(params["stop_loss_points"]), base_entry)
        else:
            add1_floor = min(active["Add 1"].entry_price + float(params["stop_loss_points"]), base_entry)
        active["Add 1"].stop_price = _better_stop(direction, active["Add 1"].stop_price, add1_floor)

    if "Add 2" in active:
        if direction == "long":
            base_lock = campaign["base_entry_price"] + float(params["profit_lock_points_after_add2"])
        else:
            base_lock = campaign["base_entry_price"] - float(params["profit_lock_points_after_add2"])
        add1_entry = campaign.get("add1_entry_price", campaign["base_entry_price"])
        add2_entry_index = campaign.get("add2_entry_index")
        delay_bars = int(params.get("add2_tighten_delay_bars", 0))
        if "Base" in active:
            active["Base"].stop_price = _better_stop(direction, active["Base"].stop_price, base_lock)
        if "Add 1" in active:
            active["Add 1"].stop_price = _better_stop(direction, active["Add 1"].stop_price, add1_entry)
        if add2_entry_index is not None and current_index - int(add2_entry_index) >= delay_bars:
            if direction == "long":
                add2_floor = max(active["Add 2"].entry_price - float(params["stop_loss_points"]), add1_entry)
            else:
                add2_floor = min(active["Add 2"].entry_price + float(params["stop_loss_points"]), add1_entry)
            active["Add 2"].stop_price = _better_stop(direction, active["Add 2"].stop_price, add2_floor)


def _campaign_config(direction: str, row: pd.Series, params: dict) -> dict:
    breakout_buffer = float(params["breakout_buffer_points"])
    stop_points = float(params["stop_loss_points"])
    target_points = float(params["take_profit_points"])
    if direction == "long":
        return {
            "setup_col": "long_setup_ok",
            "level_col": "pdh",
            "signal": "BUY",
            "level_ready": pd.notna(row["pdh"]),
            "trigger_met": float(row["close"]) >= float(row["pdh"]) + breakout_buffer,
            "make_stop": lambda price: price - stop_points,
            "make_target": lambda price: price + target_points,
            "add_trigger": lambda base_price, trigger_points: base_price + trigger_points,
        }
    return {
        "setup_col": "short_setup_ok",
        "level_col": "pdl",
        "signal": "SHORT",
        "level_ready": pd.notna(row["pdl"]),
        "trigger_met": float(row["close"]) <= float(row["pdl"]) - breakout_buffer,
        "make_stop": lambda price: price + stop_points,
        "make_target": lambda price: price - target_points,
        "add_trigger": lambda base_price, trigger_points: base_price - trigger_points,
    }


def _short_regime_ok(row: pd.Series, mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "red_open":
        return bool(float(row["session_open"]) < float(row["pdl"])) if pd.notna(row["pdl"]) else False
    if mode == "below_open_and_vwap":
        return bool(row["below_open"] and row["below_vwap"])
    if mode == "red_open_and_below_vwap":
        if pd.isna(row["pdl"]):
            return False
        return bool(float(row["session_open"]) < float(row["pdl"]) and row["below_vwap"])
    return True


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
                "campaign_days": 0,
                "avg_contracts_per_campaign": 0.0,
                "add1_hit_rate_pct": 0.0,
                "add2_hit_rate_pct": 0.0,
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
    stop_points = float(params["stop_loss_points"])
    target_points = float(params["take_profit_points"])
    add_contracts = params["add_contracts"]
    add_trigger_points = params["add_trigger_points"]
    trade_direction = params.get("trade_direction", "long")
    short_regime_filter = params.get("short_regime_filter", "always")
    allowed_directions = ["long", "short"] if trade_direction == "both" else [trade_direction]

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
            if tier.direction == "long":
                stop_hit = float(row["low"]) <= tier.stop_price
                target_hit = float(row["high"]) >= tier.target_price
            else:
                stop_hit = float(row["high"]) >= tier.stop_price
                target_hit = float(row["low"]) <= tier.target_price
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
                pnl = _record_exit(tier, ts, exit_price, exit_reason, point_value, trades, events)
                cash += pnl
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
            daily_loss_limit = float(params["daily_loss_limit_r"]) * stop_points * point_value
            for direction in allowed_directions:
                config = _campaign_config(direction, row, params)
                regime_ok = True if direction == "long" else _short_regime_ok(row, short_regime_filter)
                if (
                    config["level_ready"]
                    and len(lookback) == int(params["confirm_bars"])
                    and lookback["_date_only"].nunique() == 1
                    and bool(lookback[config["setup_col"]].all())
                    and config["trigger_met"]
                    and regime_ok
                    and daily_realized_pnl.get(day, 0.0) > daily_loss_limit
                    and i + 1 < len(data)
                ):
                    next_row = data.iloc[i + 1]
                    if next_row["_date_only"] == day and int(next_row["bar_minutes"]) <= int(params["last_entry_minute"]):
                        entry_price = float(next_row["open"])
                        base = Tier(
                            name="Base",
                            direction=direction,
                            contracts=int(params["base_contracts"]),
                            entry_price=entry_price,
                            entry_time=pd.to_datetime(next_row["date"]),
                            stop_price=config["make_stop"](entry_price),
                            target_price=config["make_target"](entry_price),
                        )
                        tiers = [base]
                        campaign_started = True
                        next_add_index = 0
                        campaign = {
                            "day": str(day),
                            "direction": direction,
                            "entry_time": pd.to_datetime(next_row["date"]),
                            "base_entry_price": entry_price,
                            "add1_hit": False,
                            "add2_hit": False,
                            "total_contracts": base.contracts,
                            "exit_time": None,
                            "exit_reason": None,
                        }
                        campaigns.append(campaign)
                        setup_days.add(day)
                        _record_event(events, pd.to_datetime(next_row["date"]), entry_price, config["signal"], f"{direction.capitalize()} Base")
                        break

        if (
            campaign is not None
            and tiers
            and next_add_index < len(add_trigger_points)
            and next_add_index < len(add_contracts)
            and current_minute < int(params["last_add_minute"])
        ):
            while next_add_index < len(add_contracts) and int(add_contracts[next_add_index]) <= 0:
                next_add_index += 1
            if next_add_index >= len(add_trigger_points) or next_add_index >= len(add_contracts):
                trigger_price = None
            else:
                trigger_price = _campaign_config(campaign["direction"], row, params)["add_trigger"](campaign["base_entry_price"], float(add_trigger_points[next_add_index]))
            trigger_hit = (
                trigger_price is not None
                and (
                    (campaign["direction"] == "long" and close_price >= trigger_price)
                    or (campaign["direction"] == "short" and close_price <= trigger_price)
                )
            )
            if trigger_hit and i + 1 < len(data):
                next_row = data.iloc[i + 1]
                if next_row["_date_only"] == day and int(next_row["bar_minutes"]) <= int(params["last_add_minute"]):
                    tier_name = f"Add {next_add_index + 1}"
                    existing_names = {tier.name for tier in tiers if tier.active}
                    if tier_name not in existing_names:
                        entry_price = float(next_row["open"])
                        config = _campaign_config(campaign["direction"], row, params)
                        tier = Tier(
                            name=tier_name,
                            direction=campaign["direction"],
                            contracts=int(add_contracts[next_add_index]),
                            entry_price=entry_price,
                            entry_time=pd.to_datetime(next_row["date"]),
                            stop_price=config["make_stop"](entry_price),
                            target_price=config["make_target"](entry_price),
                        )
                        tiers.append(tier)
                        campaign["total_contracts"] += tier.contracts
                        if next_add_index == 0:
                            campaign["add1_hit"] = True
                            campaign["add1_entry_price"] = entry_price
                        elif next_add_index == 1:
                            campaign["add2_hit"] = True
                            campaign["add2_entry_price"] = entry_price
                            campaign["add2_entry_index"] = i + 1
                        _sync_stops_after_add(tiers, campaign, params, i + 1)
                        add_signal = "ADD" if campaign["direction"] == "long" else "ADD SHORT"
                        _record_event(events, pd.to_datetime(next_row["date"]), entry_price, add_signal, f"{campaign['direction'].capitalize()} {tier_name}")
                        next_add_index += 1

        portfolio_value = cash
        for tier in tiers:
            if tier.active:
                direction_sign = 1.0 if tier.direction == "long" else -1.0
                portfolio_value += (close_price - tier.entry_price) * direction_sign * point_value * tier.contracts
        equity_points.append({"date": ts, "portfolio_value": portfolio_value})

    if tiers and not data.empty:
        last_row = data.iloc[-1]
        last_ts = pd.to_datetime(last_row["date"])
        last_close = float(last_row["close"])
        for tier in list(tiers):
            pnl = _record_exit(tier, last_ts, last_close, "End of data", point_value, trades, events)
            cash += pnl
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
        "long_campaign_days": sum(1 for c in campaigns if c["direction"] == "long"),
        "short_campaign_days": sum(1 for c in campaigns if c["direction"] == "short"),
        "avg_contracts_per_campaign": round(pd.DataFrame(campaigns)["total_contracts"].mean(), 2) if campaigns else 0.0,
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
