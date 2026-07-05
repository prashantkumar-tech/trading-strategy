import numpy as np
import pandas as pd
from backtest.momentum_rotation import compute_momentum, month_end_dates, select_basket, build_price_panel, run_momentum_rotation


def test_compute_momentum_uses_skip_and_lookback_windows():
    dates = pd.date_range("2020-01-01", periods=6, freq="D")
    close = pd.DataFrame({"A": [10, 11, 12, 13, 14, 15]}, index=dates)
    mom = compute_momentum(close, lookback_days=3, skip_days=1)
    # at row 3: close[2] / close[0] - 1 = 12/10 - 1 = 0.2
    assert abs(mom["A"].iloc[3] - 0.2) < 1e-9
    # first 3 rows have insufficient history -> NaN
    assert mom["A"].iloc[:3].isna().all()


def test_month_end_dates_returns_last_trading_day_per_month():
    dates = pd.to_datetime(["2020-01-30", "2020-01-31", "2020-02-03", "2020-02-28"])
    ends = month_end_dates(dates)
    assert ends == [pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-28")]


def _momentum_series(n):
    # S1..Sn with descending momentum so S1 is rank 1
    return pd.Series({f"S{i}": (n - i + 1) for i in range(1, n + 1)}, dtype=float)


def test_select_basket_keeps_holding_inside_buffer_sells_outside():
    mom = _momentum_series(25)
    eligible = set(mom.index)
    basket, rank = select_basket(mom, eligible, ["S15", "S21"], top_n=10, buffer_rank=20)
    assert rank["S15"] == 15 and rank["S21"] == 21
    assert "S15" in basket          # rank 15 <= buffer -> kept
    assert "S21" not in basket      # rank 21 > buffer -> sold
    assert len(basket) == 10


def test_select_basket_leaves_cash_slots_when_few_eligible():
    mom = _momentum_series(25)
    eligible = {"S1", "S2", "S3", "S4", "S5", "S6", "S7"}  # only 7 qualify
    basket, _ = select_basket(mom, eligible, [], top_n=10, buffer_rank=20)
    assert len(basket) == 7


def test_select_basket_refills_open_slots_from_top_ranks():
    mom = _momentum_series(25)
    eligible = set(mom.index)
    basket, _ = select_basket(mom, eligible, [], top_n=10, buffer_rank=20)
    assert basket == [f"S{i}" for i in range(1, 11)]  # top 10 by momentum


def test_build_price_panel_pivots_symbols_into_columns():
    dates = pd.date_range("2020-01-01", periods=3, freq="D")

    def fake_loader(symbol, start=None, end=None):
        base = {"A": 10.0, "B": 20.0}[symbol]
        return pd.DataFrame({
            "date": dates,
            "close": [base, base + 1, base + 2],
            "ma200": [base - 1, base - 1, base - 1],
        })

    close, ma200 = build_price_panel(["A", "B"], loader=fake_loader)
    assert list(close.columns) == ["A", "B"]
    assert close.loc[dates[2], "A"] == 12.0
    assert close.loc[dates[0], "B"] == 20.0
    assert ma200.loc[dates[0], "A"] == 9.0
    assert close.index.equals(ma200.index)


def _make_loader(price_map, dates):
    """price_map: {symbol: list_of_closes}. ma200 set well below close (always eligible)."""
    def loader(symbol, start=None, end=None):
        closes = price_map[symbol]
        return pd.DataFrame({
            "date": dates,
            "close": closes,
            "ma200": [c * 0.5 for c in closes],
        })
    return loader


def test_run_momentum_rotation_uptrend_grows_equity_and_holds_basket():
    dates = pd.date_range("2020-01-01", periods=8, freq="ME")  # 8 month-ends
    # 12 symbols, all rising; steeper slope = higher momentum
    price_map = {f"S{i}": list(100 + np.arange(8) * i) for i in range(1, 13)}
    loader = _make_loader(price_map, dates)

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=10, buffer_rank=20,
        lookback_days=2, skip_days=1, loader=loader,
    )

    assert set(result.keys()) >= {"equity_curve", "trades", "metrics", "final_value", "holdings"}
    assert result["final_value"] > 10_000.0            # uptrend grows equity
    assert 0 < len(result["holdings"]) <= 10           # holds at most top_n
    assert "sharpe_ratio" in result["metrics"]


def test_run_momentum_rotation_logs_full_exit_when_momentum_collapses():
    dates = pd.date_range("2020-01-01", periods=8, freq="ME")
    # 11 steady risers plus one name that spikes then crashes so it exits the basket
    price_map = {f"S{i}": list(100 + np.arange(8) * i) for i in range(1, 12)}
    price_map["CRASH"] = [100, 130, 170, 210, 250, 120, 90, 60]
    loader = _make_loader(price_map, dates)

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=10, buffer_rank=20,
        lookback_days=2, skip_days=1, loader=loader,
    )

    reasons = {t["exit_reason"] for t in result["trades"]}
    assert reasons.issubset({"fell out of top 20", "below 200MA"})
    # every trade carries the required schema
    for t in result["trades"]:
        assert {"symbol", "entry_date", "exit_date", "pnl", "return_pct",
                "exit_reason", "rank_at_exit"} <= set(t.keys())
