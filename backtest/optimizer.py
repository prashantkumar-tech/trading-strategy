"""
Parameter sweep optimizer for the MA50 Momentum strategy.

Sweeps combinations of:
  - pos_pct_above  : position size when close > MA50
  - pos_pct_below  : position size when close <= MA50  (kept at half of above)
  - profit_target  : % gain at which to exit
  - time_stop_days : max days to hold a position before forced exit
"""

from itertools import product
import pandas as pd
from backtest.simulator import run_backtest


def _build_rules(
    pos_pct_above: float,
    pos_pct_below: float,
    profit_target: float,
    time_stop_days: int,
    gap_up_rule: bool = False,
    premarket_rule: bool = False,
    gap_up_pct: float = 0.10,
    premarket_pct: float = 0.05,
) -> list:
    rules = [
        {
            "type": "entry", "label": "Above MA50", "combinator": "AND",
            "position_pct": pos_pct_above,
            "conditions": [{"left": "close", "op": ">", "right": "ma50"}],
        },
        {
            "type": "entry", "label": "Below MA50", "combinator": "AND",
            "position_pct": pos_pct_below,
            "conditions": [{"left": "close", "op": "<=", "right": "ma50"}],
        },
    ]

    # Rule: if market gaps up from prev day close, buy 15 min after open (9:45 AM bar = 585 min)
    if gap_up_rule:
        rules.append({
            "type": "entry", "label": "Gap Up — Buy at 9:45", "combinator": "AND",
            "position_pct": gap_up_pct,
            "conditions": [
                {"left": "close",          "op": ">",  "right": "prev_day_close"},
                {"left": "bar_minutes",    "op": "==", "right": "585"},
            ],
        })

    # Rule: if premarket price is below prev day close, buy in premarket
    if premarket_rule:
        rules.append({
            "type": "entry", "label": "Negative Premarket Buy", "combinator": "AND",
            "position_pct": premarket_pct,
            "conditions": [
                {"left": "close",       "op": "<",  "right": "prev_day_close"},
                {"left": "bar_minutes", "op": "<",  "right": "570"},   # before 9:30 AM ET
            ],
        })

    rules += [
        {
            "type": "exit", "label": "Profit target", "combinator": "AND",
            "conditions": [{"left": "position_return_pct", "op": ">=",
                            "right": str(profit_target)}],
        },
        {
            "type": "exit", "label": "Time stop", "combinator": "AND",
            "conditions": [{"left": "days_held", "op": ">=",
                            "right": str(time_stop_days)}],
        },
    ]
    return rules


def run_optimization(
    df: pd.DataFrame,
    pos_pct_above_values: list,   # e.g. [0.08, 0.10, 0.12, 0.15]
    profit_target_values: list,   # e.g. [1.5, 2.0, 2.5, 3.0]
    time_stop_values: list,       # e.g. [2, 3, 4, 5]
    initial_capital: float = 10_000.0,
    gap_up_rule: bool = False,
    premarket_rule: bool = False,
    gap_up_pct: float = 0.10,
    premarket_pct: float = 0.05,
) -> pd.DataFrame:
    """
    Run all parameter combinations and return a results DataFrame
    sorted by Sharpe ratio descending.
    pos_pct_below is always set to half of pos_pct_above.
    """
    rows = []
    combos = list(product(pos_pct_above_values, profit_target_values, time_stop_values))

    for pos_above, profit_target, time_stop in combos:
        pos_below = round(pos_above / 2, 4)
        rules = _build_rules(pos_above, pos_below, profit_target, int(time_stop),
                             gap_up_rule=gap_up_rule, premarket_rule=premarket_rule,
                             gap_up_pct=gap_up_pct, premarket_pct=premarket_pct)
        result = run_backtest(df, rules, initial_capital)
        m = result["metrics"]
        rows.append({
            "above_MA50_%":    int(round(pos_above * 100)),
            "below_MA50_%":    int(round(pos_below * 100)),
            "profit_target_%": float(profit_target),
            "time_stop_days":  int(time_stop),
            "total_return_%":  m["total_return_pct"],
            "ann_return_%":    m["annualized_return_pct"],
            "sharpe":          m["sharpe_ratio"],
            "max_drawdown_%":  m["max_drawdown_pct"],
            "win_rate_%":      m["win_rate_pct"],
            "num_trades":      m["num_trades"],
            "final_value":     result["final_value"],
        })

    df_results = pd.DataFrame(rows).sort_values("sharpe", ascending=False).reset_index(drop=True)
    df_results.index += 1   # rank starts at 1
    return df_results
