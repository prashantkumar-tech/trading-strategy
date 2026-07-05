import pandas as pd
from backtest.metrics import yearly_performance


def test_yearly_performance_computes_calendar_year_pnl_and_returns():
    idx = pd.to_datetime(["2020-01-02", "2020-12-31", "2021-06-01", "2021-12-31", "2022-12-30"])
    eq = pd.Series([100.0, 110.0, 121.0, 132.0, 118.8], index=idx)

    yp = yearly_performance(eq).set_index("year")

    # 2020: 110/100 - 1 = +10%,  P&L +10
    # 2021: 132/110 - 1 = +20%,  P&L +22   (base is prior year-end 110)
    # 2022: 118.8/132 - 1 = -10%, P&L -13.2
    assert list(yp.index) == [2020, 2021, 2022]
    assert abs(yp.loc[2020, "return_pct"] - 10.0) < 1e-6
    assert abs(yp.loc[2021, "return_pct"] - 20.0) < 1e-6
    assert abs(yp.loc[2022, "return_pct"] + 10.0) < 1e-6
    assert abs(yp.loc[2020, "pnl"] - 10.0) < 1e-6
    assert abs(yp.loc[2021, "pnl"] - 22.0) < 1e-6
    assert abs(yp.loc[2022, "pnl"] + 13.2) < 1e-6


def test_yearly_performance_single_year_uses_first_value_as_base():
    idx = pd.to_datetime(["2023-03-01", "2023-06-01", "2023-12-29"])
    eq = pd.Series([1000.0, 1100.0, 1250.0], index=idx)

    yp = yearly_performance(eq)
    assert list(yp["year"]) == [2023]
    assert abs(yp.loc[0, "return_pct"] - 25.0) < 1e-6   # 1250/1000 - 1
    assert abs(yp.loc[0, "pnl"] - 250.0) < 1e-6
