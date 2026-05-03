"""Rajat's SPY intraday pyramiding strategy backtest."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backtest.metrics import compute_metrics

REGULAR_OPEN_MINUTE = 9 * 60 + 30
FIRST_ENTRY_MINUTE = 9 * 60 + 35
ENTRY_INTERVAL_MINUTES = 60
LAST_ENTRY_MINUTE = 15 * 60 + 25
FINAL_EXIT_MINUTE = 15 * 60 + 55

DEFAULT_PARAMS = {
    "trail_pct": 0.25,          # initial stop: this % below entry price
    "hourly_ratchet_pct": 0.25, # stop moves up by this % of entry price each hour
    "entry_price_col": "close",
    "initial_capital": 10_000.0,
}

# Lot ladder: 4 shares on the first entry of each day, then 3, 2, and 1 for all subsequent.
_LOT_LADDER = [4, 3, 2]


def _shares_for_day_entry(day_entry_num: int) -> int:
    """Return lot size for the nth entry of the day (1-indexed)."""
    if day_entry_num <= len(_LOT_LADDER):
        return _LOT_LADDER[day_entry_num - 1]
    return 1


@dataclass
class ChildPosition:
    entry_id: int
    role: str
    shares: int
    entry_time: pd.Timestamp
    entry_price: float
    stop_trigger: Optional[float] = None

    @property
    def cost_basis(self) -> float:
        return self.shares * self.entry_price


def _merge_params(params: Optional[dict]) -> dict:
    merged = DEFAULT_PARAMS.copy()
    if params:
        merged.update(params)
    return merged


def _minute_of_day(ts: pd.Timestamp) -> int:
    return int(ts.hour) * 60 + int(ts.minute)


def _prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values("date").reset_index(drop=True)
    data["bar_minutes"] = data["date"].map(_minute_of_day)
    data["session_date"] = data["date"].dt.date
    data = data[
        data["bar_minutes"].between(REGULAR_OPEN_MINUTE, FINAL_EXIT_MINUTE)
    ].copy()
    return data.reset_index(drop=True)


def _scheduled_entry_minutes() -> set[int]:
    return set(range(FIRST_ENTRY_MINUTE, LAST_ENTRY_MINUTE + 1, ENTRY_INTERVAL_MINUTES))


def _record_exit(
    position: ChildPosition,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_rule: str,
    trades: list[dict],
) -> float:
    proceeds = position.shares * exit_price
    pnl = proceeds - position.cost_basis
    trades.append({
        "entry_id": position.entry_id,
        "role": position.role,
        "entry_date": str(position.entry_time),
        "exit_date": str(exit_time),
        "entry_price": round(position.entry_price, 4),
        "exit_price": round(exit_price, 4),
        "shares": position.shares,
        "pnl": round(pnl, 2),
        "return_pct": round((exit_price / position.entry_price - 1) * 100, 4),
        "exit_rule": exit_rule,
        "hold_bars": max(int((exit_time - position.entry_time).total_seconds() // 300), 0),
    })
    return proceeds


def run_backtest(
    df: pd.DataFrame,
    params: Optional[dict] = None,
    initial_capital: float = 10_000.0,
) -> dict:
    """Run Rajat's long-only SPY pyramiding strategy on 5-minute OHLCV data."""
    params = _merge_params(params)
    data = _prepare_data(df)
    if data.empty:
        equity_curve = pd.Series(
            [initial_capital],
            index=[pd.Timestamp("1970-01-01")],
            dtype=float,
        )
        return {
            "params": params,
            "metrics": compute_metrics(equity_curve, []),
            "summary": {},
            "trades": [],
            "entries": [],
            "skipped_entry_events": [],
            "equity_curve": equity_curve,
            "final_value": initial_capital,
            "price_df": data,
        }

    trail_pct = float(params["trail_pct"]) / 100
    hourly_ratchet_pct = float(params["hourly_ratchet_pct"]) / 100
    entry_price_col = str(params["entry_price_col"])
    entry_minutes = _scheduled_entry_minutes()

    if entry_price_col not in {"open", "close"}:
        raise ValueError("entry_price_col must be either 'open' or 'close'.")

    cash = float(initial_capital)
    active: list[ChildPosition] = []
    trades: list[dict] = []
    entries: list[dict] = []
    skipped_entry_events: list[dict] = []
    equity_points: list[dict] = []
    skipped_entries = 0
    entry_id = 0
    active_day = None
    first_entry_price = None
    previous_entry_price = None
    previous_ts = None
    previous_close = None
    traded_days = set()
    fallback_eod_exits = 0
    day_entry_count = 0

    for _, row in data.iterrows():
        ts = pd.to_datetime(row["date"])
        day = row["session_date"]
        minute = int(row["bar_minutes"])
        close_price = float(row["close"])

        if day != active_day:
            if active and previous_ts is not None and previous_close is not None:
                for position in list(active):
                    cash += _record_exit(
                        position,
                        previous_ts,
                        previous_close,
                        "End of day fallback",
                        trades,
                    )
                active = []
                fallback_eod_exits += 1
                equity_points.append({"date": previous_ts, "portfolio_value": cash})
            active_day = day
            first_entry_price = None
            previous_entry_price = None
            day_entry_count = 0

        if minute == FINAL_EXIT_MINUTE:
            for position in list(active):
                cash += _record_exit(position, ts, close_price, "End of day", trades)
            active = []
            equity_points.append({"date": ts, "portfolio_value": cash})
            previous_ts = ts
            previous_close = close_price
            continue

        remaining: list[ChildPosition] = []
        for position in active:
            if float(row["low"]) <= float(position.stop_trigger):
                cash += _record_exit(
                    position, ts, float(position.stop_trigger), "Trailing stop", trades
                )
            else:
                hours_elapsed = int((ts - position.entry_time).total_seconds() // 3600)
                ratcheted_stop = (
                    position.entry_price * (1 - trail_pct)
                    + hours_elapsed * position.entry_price * hourly_ratchet_pct
                )
                position.stop_trigger = max(float(position.stop_trigger), ratcheted_stop)
                remaining.append(position)
        active = remaining

        if minute in entry_minutes:
            entry_price = float(row[entry_price_col])
            is_first_entry = previous_entry_price is None
            can_add = is_first_entry or entry_price >= float(previous_entry_price)
            lot_size = _shares_for_day_entry(day_entry_count + 1)
            required_cash = lot_size * entry_price
            if can_add:
                day_entry_count += 1
                entry_id += 1
                if first_entry_price is None:
                    first_entry_price = entry_price
                    traded_days.add(day)
                previous_entry_price = entry_price
                cash -= required_cash
                for share_num in range(1, lot_size + 1):
                    active.append(ChildPosition(
                        entry_id=entry_id,
                        role=f"share_{share_num}",
                        shares=1,
                        entry_time=ts,
                        entry_price=entry_price,
                        stop_trigger=entry_price * (1 - trail_pct),
                    ))
                entries.append({
                    "entry_id": entry_id,
                    "date": str(ts),
                    "price": round(entry_price, 4),
                    "shares": lot_size,
                    "first_entry_price": round(float(first_entry_price), 4),
                    "previous_entry_price": round(float(previous_entry_price), 4),
                })
            else:
                skipped_entries += 1
                skipped_entry_events.append({
                    "date": str(ts),
                    "price": round(entry_price, 4),
                    "first_entry_price": round(float(first_entry_price), 4) if first_entry_price is not None else None,
                    "previous_entry_price": round(float(previous_entry_price), 4) if previous_entry_price is not None else None,
                    "required_cash": round(required_cash, 2),
                    "available_cash": round(cash, 2),
                    "reason": "Below previous entry price",
                })

        portfolio_value = cash + sum(position.shares * close_price for position in active)
        equity_points.append({"date": ts, "portfolio_value": portfolio_value})
        previous_ts = ts
        previous_close = close_price

    if active:
        last_row = data.iloc[-1]
        last_ts = pd.to_datetime(last_row["date"])
        last_close = float(last_row["close"])
        for position in list(active):
            cash += _record_exit(position, last_ts, last_close, "End of day fallback", trades)
        active = []
        fallback_eod_exits += 1
        equity_points.append({"date": last_ts, "portfolio_value": cash})

    equity_df = pd.DataFrame(equity_points)
    equity_curve = pd.Series(
        equity_df["portfolio_value"].values,
        index=pd.to_datetime(equity_df["date"]),
    )
    metrics = compute_metrics(equity_curve, trades)
    trades_df = pd.DataFrame(trades)
    exit_counts = trades_df["exit_rule"].value_counts().to_dict() if not trades_df.empty else {}

    summary = {
        "traded_days": len(traded_days),
        "entries": len(entries),
        "skipped_entries": skipped_entries,
        "fallback_eod_exits": fallback_eod_exits,
        "avg_entries_per_traded_day": round(len(entries) / len(traded_days), 2) if traded_days else 0.0,
        "exit_reason_counts": exit_counts,
    }

    return {
        "params": params,
        "metrics": metrics,
        "summary": summary,
        "trades": trades,
        "entries": entries,
        "skipped_entry_events": skipped_entry_events,
        "equity_curve": equity_curve,
        "final_value": float(equity_curve.iloc[-1]) if not equity_curve.empty else initial_capital,
        "price_df": data,
    }


def build_yearly_summary(
    df: pd.DataFrame,
    params: Optional[dict] = None,
    initial_capital: float = 10_000.0,
) -> pd.DataFrame:
    """Run the strategy independently for each calendar year in df."""
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])
    rows = []
    for year in sorted(data["date"].dt.year.unique()):
        year_df = data[data["date"].dt.year == year].copy()
        result = run_backtest(year_df, params=params, initial_capital=initial_capital)
        metrics = result["metrics"]
        summary = result["summary"]
        rows.append({
            "year": int(year),
            "start": str(year_df["date"].min().date()),
            "end": str(year_df["date"].max().date()),
            "final_value": round(float(result["final_value"]), 2),
            "return_pct": float(metrics["total_return_pct"]),
            "max_drawdown_pct": float(metrics["max_drawdown_pct"]),
            "sharpe": float(metrics["sharpe_ratio"]),
            "trades": int(metrics["num_trades"]),
            "win_rate_pct": float(metrics["win_rate_pct"]),
            "entries": int(summary.get("entries", 0)),
            "traded_days": int(summary.get("traded_days", 0)),
            "skipped_entries": int(summary.get("skipped_entries", 0)),
        })
    return pd.DataFrame(rows)
