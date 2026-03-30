"""
Multi-position portfolio simulator.

Rule schema
-----------
Entry rule:
  {
    "type": "entry",
    "label": "Above MA50",
    "conditions": [{"left": "close", "op": ">", "right": "ma50"}],
    "combinator": "AND" | "OR",   # default AND
    "position_pct": 0.10,         # fraction of portfolio to deploy
  }

Exit rule:
  {
    "type": "exit",
    "label": "2% profit target",
    "conditions": [{"left": "position_return_pct", "op": ">=", "right": "2"}],
    "combinator": "AND" | "OR",
  }

Available fields for conditions
--------------------------------
Market fields (same every row):
  open, high, low, close, volume, ma50, ma200
  open_prev, close_prev, ma50_prev, ma200_prev

Position fields (only valid in exit rules, evaluated per position):
  position_return_pct   (close - entry_price) / entry_price * 100
  days_held             calendar trading days since entry (entry day = 1)

Operators: >  <  >=  <=  ==  crosses_above  crosses_below

Entry rule behaviour
--------------------
- Rules are evaluated in order; the FIRST matching entry rule fires each day.
- A new position is opened using that rule's position_pct.
- Multiple entry rules should be mutually exclusive (e.g. above/below MA50).

Exit rule behaviour
-------------------
- ALL exit rules are evaluated for EVERY open position each day.
- A position is closed if ANY exit rule matches (logical OR across exit rules).
- Exits are processed before the day's entry so freed cash can be redeployed.
"""

from typing import Optional
import pandas as pd
import numpy as np
from backtest.metrics import compute_metrics

_NUMERIC_OPS = {
    ">":  lambda a, b: a > b,
    "<":  lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
}

_MARKET_PREV_FIELDS = {"open", "high", "low", "close", "volume", "ma50", "ma200"}
_POSITION_FIELDS = {"position_return_pct", "days_held"}


def _resolve(operand: str, row: pd.Series, prev: Optional[pd.Series],
             pos_ctx: Optional[dict]) -> float:
    """Return the numeric value of an operand (field name or literal float)."""
    # Literal number
    try:
        return float(operand)
    except ValueError:
        pass

    # _prev suffix → previous row market field
    if operand.endswith("_prev"):
        field = operand[:-5]
        return float(prev[field]) if prev is not None else np.nan

    # Position context fields
    if operand in _POSITION_FIELDS:
        if pos_ctx is None:
            return np.nan
        return float(pos_ctx[operand])

    # Regular market field
    return float(row[operand])


def _eval_condition(cond: dict, row: pd.Series, prev: Optional[pd.Series],
                    pos_ctx: Optional[dict]) -> bool:
    left  = _resolve(cond["left"],  row, prev, pos_ctx)
    right = _resolve(cond["right"], row, prev, pos_ctx)
    op    = cond["op"]

    if pd.isna(left) or pd.isna(right):
        return False

    if op in _NUMERIC_OPS:
        return bool(_NUMERIC_OPS[op](left, right))

    if op == "crosses_above":
        if prev is None:
            return False
        pl = _resolve(cond["left"],  prev, None, None)
        pr = _resolve(cond["right"], prev, None, None)
        if any(pd.isna(v) for v in [pl, pr]):
            return False
        return bool(pl <= pr and left > right)

    if op == "crosses_below":
        if prev is None:
            return False
        pl = _resolve(cond["left"],  prev, None, None)
        pr = _resolve(cond["right"], prev, None, None)
        if any(pd.isna(v) for v in [pl, pr]):
            return False
        return bool(pl >= pr and left < right)

    raise ValueError(f"Unknown operator: {op}")


def _eval_rule(rule: dict, row: pd.Series, prev: Optional[pd.Series],
               pos_ctx: Optional[dict] = None) -> bool:
    results = [_eval_condition(c, row, prev, pos_ctx) for c in rule["conditions"]]
    return any(results) if rule.get("combinator", "AND") == "OR" else all(results)


def run_backtest(
    df: pd.DataFrame,
    rules: list,
    initial_capital: float = 10_000.0,
) -> dict:
    """
    Run a multi-position backtest.

    Parameters
    ----------
    df              : price DataFrame (date, open, high, low, close, volume, ma50, ma200)
    rules           : list of entry/exit rule dicts (see module docstring)
    initial_capital : starting cash

    Returns
    -------
    dict with keys: equity_curve, trades, metrics, signals_df, final_value
    """
    df = df.dropna(subset=["ma50", "ma200"]).reset_index(drop=True)

    entry_rules = [r for r in rules if r.get("type") == "entry"]
    exit_rules  = [r for r in rules if r.get("type") == "exit"]

    cash      = initial_capital
    positions = []   # list of position dicts
    trades    = []
    equity_curve = []
    signals   = []   # (date, "BUY" | "SELL" | None)

    for i, row in df.iterrows():
        prev = df.iloc[i - 1] if i > 0 else None

        # ── 1. Increment days_held for all open positions ──────────────────
        for pos in positions:
            pos["days_held"] += 1

        # ── 2. Evaluate exit rules for each position ───────────────────────
        to_close = []
        for pos in positions:
            pos_ctx = {
                "position_return_pct": (row["close"] - pos["entry_price"])
                                       / pos["entry_price"] * 100,
                "days_held": pos["days_held"],
            }
            if any(_eval_rule(r, row, prev, pos_ctx) for r in exit_rules):
                to_close.append(pos)

        # Collect closed trade records without cash yet — stamp after buys
        sell_signal = False
        day_trades = []
        for pos in to_close:
            proceeds = pos["shares"] * row["close"]
            cost     = pos["shares"] * pos["entry_price"]
            pnl      = proceeds - cost
            cash += proceeds
            positions.remove(pos)
            sell_signal = True
            day_trades.append({
                "entry_date":  pos["entry_date"],
                "exit_date":   str(row["date"]),
                "entry_price": round(pos["entry_price"], 4),
                "exit_price":  round(row["close"], 4),
                "shares":      round(pos["shares"], 6),
                "days_held":   pos["days_held"],
                "pnl":         round(pnl, 2),
                "return_pct":  round(pnl / cost * 100, 2),
                "exit_rule":   pos.get("exit_trigger", ""),
            })

        # ── 3. Evaluate entry rules (first match wins) ─────────────────────
        buy_signal = False
        for entry_rule in entry_rules:
            if _eval_rule(entry_rule, row, prev):
                position_pct    = float(entry_rule.get("position_pct", 0.10))
                portfolio_value = cash + sum(p["shares"] * row["close"] for p in positions)
                spend           = portfolio_value * position_pct

                if cash >= spend > 0:
                    shares = spend / row["close"]
                    positions.append({
                        "entry_price": row["close"],
                        "entry_date":  str(row["date"]),
                        "shares":      shares,
                        "days_held":   0,
                        "rule_label":  entry_rule.get("label", ""),
                    })
                    cash -= spend
                    buy_signal = True
                break   # only first matching entry rule fires

        # Stamp all of today's closed trades with true end-of-day cash
        for t in day_trades:
            t["unallocated_capital"] = round(cash, 2)
        trades.extend(day_trades)

        # ── 4. Record equity & signal ──────────────────────────────────────
        portfolio_value = cash + sum(p["shares"] * row["close"] for p in positions)
        equity_curve.append({"date": row["date"], "portfolio_value": portfolio_value})

        signal = ("BUY" if buy_signal else None) if not sell_signal else \
                 ("BUY+SELL" if buy_signal else "SELL")
        signals.append({"date": row["date"], "signal": signal})

    equity_series = pd.DataFrame(equity_curve).set_index("date")["portfolio_value"]
    signals_df    = pd.DataFrame(signals)
    metrics       = compute_metrics(equity_series, trades)

    return {
        "equity_curve": equity_series,
        "trades":       trades,
        "metrics":      metrics,
        "signals_df":   signals_df,
        "final_value":  round(float(equity_series.iloc[-1]), 2) if not equity_series.empty
                        else initial_capital,
    }
