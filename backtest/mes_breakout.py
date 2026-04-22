"""MES breakout backtest using prior-day high acceptance."""

from __future__ import annotations

from typing import Optional

import pandas as pd

from backtest.metrics import compute_metrics

REGULAR_OPEN_MINUTE = 9 * 60 + 30
FIRST_ENTRY_MINUTE = 10 * 60
LAST_ENTRY_MINUTE = 11 * 60 + 30
FINAL_EXIT_MINUTE = 15 * 60 + 55
REGULAR_CLOSE_MINUTE = 16 * 60

DEFAULT_PARAMS = {
    "confirm_bars": 3,
    "first_entry_minute": 10 * 60 + 15,
    "last_entry_minute": 11 * 60 + 15,
    "breakout_buffer_points": 2.0,
    "stop_loss_points": 13.0,
    "take_profit_points": 20.0,
    "daily_loss_limit_r": -2.0,
    "contracts": 1,
    "point_value": 5.0,
}


def _merge_params(params: Optional[dict]) -> dict:
    merged = DEFAULT_PARAMS.copy()
    if params:
        merged.update(params)
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
    regular["setup_ok"] = regular["above_pdh"] & regular["above_vwap"] & regular["above_open"]
    return regular.reset_index(drop=True)


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
            "summary": {"setup_days": 0, "trade_days": 0, "exit_reason_counts": {}},
            "trades": [],
            "equity_curve": empty_ec,
            "signals_df": pd.DataFrame(columns=["date", "price", "signal", "detail"]),
            "final_value": initial_capital,
            "price_df": data,
        }

    cash = initial_capital
    contracts = float(params["contracts"])
    point_value = float(params["point_value"])
    open_trade = None
    trades = []
    events = []
    equity_points = []
    setup_days = set()
    traded_days = set()
    exit_reason_counts = {}
    daily_realized_pnl = {}

    for i, row in data.iterrows():
        ts = row["date"]
        day = row["_date_only"]
        current_minute = int(row["bar_minutes"])
        close_price = float(row["close"])

        # 1. Manage open position first.
        if open_trade is not None:
            stop_hit = float(row["low"]) <= open_trade["stop_price"]
            target_hit = float(row["high"]) >= open_trade["target_price"]
            exit_reason = None
            exit_price = None

            if stop_hit:
                exit_reason = "Stop"
                exit_price = open_trade["stop_price"]
            elif target_hit:
                exit_reason = "Target"
                exit_price = open_trade["target_price"]
            elif current_minute >= FINAL_EXIT_MINUTE:
                exit_reason = "End of day"
                exit_price = close_price

            if exit_reason is not None:
                pnl_points = exit_price - open_trade["entry_price"]
                pnl = pnl_points * point_value * contracts
                cash += pnl
                hold_bars = max(int((ts - open_trade["entry_time"]).total_seconds() // 300), 0)
                trades.append({
                    "entry_date": str(open_trade["entry_time"]),
                    "exit_date": str(ts),
                    "entry_price": round(open_trade["entry_price"], 4),
                    "exit_price": round(exit_price, 4),
                    "contracts": int(contracts),
                    "pnl": round(pnl, 2),
                    "return_pct": round((pnl / initial_capital) * 100, 2),
                    "exit_rule": exit_reason,
                    "hold_bars": hold_bars,
                })
                events.append({"date": ts, "price": round(exit_price, 4), "signal": "SELL", "detail": exit_reason})
                exit_reason_counts[exit_reason] = exit_reason_counts.get(exit_reason, 0) + 1
                daily_realized_pnl[day] = daily_realized_pnl.get(day, 0.0) + pnl
                open_trade = None

        # 2. New entry only if flat and within window.
        if (
            open_trade is None
            and current_minute < int(params["last_entry_minute"])
            and current_minute >= int(params["first_entry_minute"])
            and day not in traded_days
            and pd.notna(row["pdh"])
        ):
            lookback = data.iloc[max(0, i - params["confirm_bars"] + 1): i + 1]
            if (
                len(lookback) == params["confirm_bars"]
                and lookback["_date_only"].nunique() == 1
                and bool(lookback["setup_ok"].all())
                and float(lookback["close"].iloc[-1]) >= float(row["pdh"]) + float(params["breakout_buffer_points"])
            ):
                setup_days.add(day)
                if daily_realized_pnl.get(day, 0.0) > params["daily_loss_limit_r"] * params["stop_loss_points"] * point_value:
                    if i + 1 < len(data):
                        next_row = data.iloc[i + 1]
                        if next_row["_date_only"] == day and int(next_row["bar_minutes"]) <= int(params["last_entry_minute"]):
                            entry_price = float(next_row["open"])
                            open_trade = {
                                "entry_time": pd.to_datetime(next_row["date"]),
                                "entry_price": entry_price,
                                "stop_price": entry_price - float(params["stop_loss_points"]),
                                "target_price": entry_price + float(params["take_profit_points"]),
                            }
                            traded_days.add(day)
                            events.append({
                                "date": pd.to_datetime(next_row["date"]),
                                "price": round(entry_price, 4),
                                "signal": "BUY",
                                "detail": "PDH breakout confirmation",
                            })

        portfolio_value = cash
        if open_trade is not None:
            open_pnl = (close_price - open_trade["entry_price"]) * point_value * contracts
            portfolio_value += open_pnl
        equity_points.append({"date": ts, "portfolio_value": portfolio_value})

    if open_trade is not None and not data.empty:
        last_row = data.iloc[-1]
        last_ts = pd.to_datetime(last_row["date"])
        last_close = float(last_row["close"])
        pnl_points = last_close - open_trade["entry_price"]
        pnl = pnl_points * point_value * contracts
        cash += pnl
        hold_bars = max(int((last_ts - open_trade["entry_time"]).total_seconds() // 300), 0)
        trades.append({
            "entry_date": str(open_trade["entry_time"]),
            "exit_date": str(last_ts),
            "entry_price": round(open_trade["entry_price"], 4),
            "exit_price": round(last_close, 4),
            "contracts": int(contracts),
            "pnl": round(pnl, 2),
            "return_pct": round((pnl / initial_capital) * 100, 2),
            "exit_rule": "End of data",
            "hold_bars": hold_bars,
        })
        events.append({"date": last_ts, "price": round(last_close, 4), "signal": "SELL", "detail": "End of data"})
        exit_reason_counts["End of data"] = exit_reason_counts.get("End of data", 0) + 1
        equity_points.append({"date": last_ts, "portfolio_value": cash})

    equity_df = pd.DataFrame(equity_points)
    equity_curve = pd.Series(equity_df["portfolio_value"].values, index=pd.to_datetime(equity_df["date"]))
    metrics = compute_metrics(equity_curve, trades)
    summary = {
        "setup_days": len(setup_days),
        "trade_days": len(traded_days),
        "avg_hold_bars": round(pd.DataFrame(trades)["hold_bars"].mean(), 2) if trades else 0.0,
        "exit_reason_counts": exit_reason_counts,
    }
    return {
        "params": params,
        "metrics": metrics,
        "summary": summary,
        "trades": trades,
        "equity_curve": equity_curve,
        "signals_df": pd.DataFrame(events),
        "final_value": float(equity_curve.iloc[-1]) if not equity_curve.empty else initial_capital,
        "price_df": data,
    }
