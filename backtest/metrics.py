"""Performance metrics for a completed backtest."""

from typing import List
import numpy as np
import pandas as pd


def compute_metrics(equity_curve: pd.Series, trades: List[dict]) -> dict:
    """
    equity_curve: pd.Series indexed by date, values = portfolio value
    trades: list of dicts with keys entry_date, exit_date, pnl, return_pct
    """
    daily_returns = equity_curve.pct_change().dropna()

    total_return = (equity_curve.iloc[-1] / equity_curve.iloc[0] - 1) * 100
    n_years = len(equity_curve) / 252
    annualized_return = ((1 + total_return / 100) ** (1 / n_years) - 1) * 100 if n_years > 0 else 0.0

    sharpe = (
        (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
        if daily_returns.std() > 0 else 0.0
    )

    rolling_max = equity_curve.cummax()
    drawdown = (equity_curve - rolling_max) / rolling_max
    max_drawdown = drawdown.min() * 100

    winning_trades = [t for t in trades if t["pnl"] > 0]
    win_rate = len(winning_trades) / len(trades) * 100 if trades else 0.0
    avg_win = np.mean([t["pnl"] for t in winning_trades]) if winning_trades else 0.0
    losing_trades = [t for t in trades if t["pnl"] <= 0]
    avg_loss = np.mean([t["pnl"] for t in losing_trades]) if losing_trades else 0.0

    return {
        "total_return_pct": round(total_return, 2),
        "annualized_return_pct": round(annualized_return, 2),
        "sharpe_ratio": round(sharpe, 3),
        "max_drawdown_pct": round(max_drawdown, 2),
        "num_trades": len(trades),
        "win_rate_pct": round(win_rate, 2),
        "avg_win_usd": round(avg_win, 2),
        "avg_loss_usd": round(avg_loss, 2),
    }
