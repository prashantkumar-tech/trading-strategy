import pandas as pd
from backtest.momentum_rotation import compute_momentum, month_end_dates, select_basket


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
