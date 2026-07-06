import numpy as np
import pandas as pd
import pytest
from backtest.momentum_rotation import compute_momentum, month_end_dates, select_basket, build_price_panel, run_momentum_rotation, momentum_leaderboard


def test_momentum_leaderboard_ranks_by_momentum_with_ma_flag():
    dates = pd.date_range("2020-01-01", periods=6, freq="D")

    def loader(symbol, start=None, end=None):
        series = {
            "A": [100, 110, 121, 133, 146, 161],   # fastest riser
            "B": [100, 103, 106, 109, 112, 115],   # medium
            "C": [100, 101, 102, 103, 104, 105],   # slowest
        }[symbol]
        ma = [s * 0.5 for s in series]
        if symbol == "C":
            ma = [s * 2 for s in series]           # C sits below its 200MA
        return pd.DataFrame({"date": dates, "close": series, "ma200": ma})

    lb = momentum_leaderboard(["A", "B", "C"], top_n=3, lookback_days=2, skip_days=1, loader=loader)

    assert list(lb["symbol"]) == ["A", "B", "C"]       # ranked by momentum, desc
    assert list(lb["rank"]) == [1, 2, 3]
    assert lb.loc[lb.symbol == "A", "above_200ma"].iloc[0] == True
    assert lb.loc[lb.symbol == "C", "above_200ma"].iloc[0] == False
    # momentum_pct is a rounded percentage; A's latest 146/133-1 ~= 9.77%
    assert abs(lb.loc[lb.symbol == "A", "momentum_pct"].iloc[0] - 9.77) < 0.1


def test_momentum_leaderboard_includes_name_and_sector_when_meta_given():
    dates = pd.date_range("2020-01-01", periods=6, freq="D")

    def loader(symbol, start=None, end=None):
        series = {"A": [100, 110, 121, 133, 146, 161], "B": [100, 103, 106, 109, 112, 115]}[symbol]
        return pd.DataFrame({"date": dates, "close": series, "ma200": [s * 0.5 for s in series]})

    meta = {
        "A": {"name": "Apple-ish Inc.", "sector": "Tech"},
        "B": {"name": "Banana-ish Corp.", "sector": "Food"},
    }
    lb = momentum_leaderboard(["A", "B"], top_n=2, lookback_days=2, skip_days=1, loader=loader, meta=meta)

    # name and sector inserted right after symbol
    assert list(lb.columns) == ["rank", "symbol", "name", "sector", "momentum_pct", "above_200ma"]
    assert lb.loc[lb.symbol == "A", "name"].iloc[0] == "Apple-ish Inc."
    assert lb.loc[lb.symbol == "A", "sector"].iloc[0] == "Tech"


def test_momentum_leaderboard_truncates_to_top_n():
    dates = pd.date_range("2020-01-01", periods=4, freq="D")

    def loader(symbol, start=None, end=None):
        slope = int(symbol[1:])
        series = [100 + slope * i for i in range(4)]
        return pd.DataFrame({"date": dates, "close": series, "ma200": [s * 0.5 for s in series]})

    lb = momentum_leaderboard([f"S{i}" for i in range(1, 8)], top_n=3, lookback_days=2, skip_days=1, loader=loader)
    assert len(lb) == 3
    assert list(lb["symbol"]) == ["S7", "S6", "S5"]    # steepest slopes rank highest


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


def test_select_basket_enforces_sector_cap():
    mom = pd.Series({"S1": 6, "S2": 5, "S3": 4, "S4": 3, "S5": 2, "S6": 1}, dtype=float)
    eligible = set(mom.index)
    sectors = {"S1": "Tech", "S2": "Tech", "S3": "Tech", "S4": "Tech",
               "S5": "Health", "S6": "Health"}
    basket, _ = select_basket(mom, eligible, [], top_n=4, buffer_rank=10,
                              sectors=sectors, max_per_sector=2)
    # top momentum is all Tech, but only 2 Tech allowed -> fill rest from Health
    assert basket == ["S1", "S2", "S5", "S6"]


def test_select_basket_sector_cap_drops_over_cap_kept_holding():
    mom = pd.Series({"S1": 6, "S2": 5, "S3": 4, "S4": 3, "S5": 2, "S6": 1}, dtype=float)
    eligible = set(mom.index)
    sectors = {k: ("Tech" if k in {"S1", "S2", "S3"} else "Health") for k in mom.index}
    # holding 3 Tech names; cap of 2 must evict the lowest-momentum Tech (S3)
    basket, _ = select_basket(mom, eligible, ["S1", "S2", "S3"], top_n=4, buffer_rank=10,
                              sectors=sectors, max_per_sector=2)
    assert "S3" not in basket
    assert set(basket) == {"S1", "S2", "S4", "S5"}


def test_select_basket_no_cap_when_params_absent():
    mom = _momentum_series(25)
    eligible = set(mom.index)
    # sectors given but max_per_sector None -> cap inactive, unchanged behavior
    sectors = {s: "Tech" for s in mom.index}
    basket, _ = select_basket(mom, eligible, [], top_n=10, buffer_rank=20, sectors=sectors)
    assert basket == [f"S{i}" for i in range(1, 11)]


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


def test_run_momentum_rotation_market_regime_goes_to_cash():
    dates = pd.date_range("2020-01-01", periods=8, freq="ME")  # 8 month-end rebalances
    price_map = {f"S{i}": list(100 + np.arange(8) * i) for i in range(1, 13)}
    loader = _make_loader(price_map, dates)

    # Risk-on every rebalance except the last -> forced to cash at the end.
    regime = pd.Series(True, index=dates)
    regime.iloc[-1] = False

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=10, buffer_rank=20,
        lookback_days=2, skip_days=1, loader=loader, regime=regime,
    )

    assert result["holdings"] == []   # cashed out on the regime-off rebalance
    assert any(t["exit_reason"] == "market regime off" for t in result["trades"])


def test_run_momentum_rotation_reports_open_positions():
    dates = pd.date_range("2020-01-01", periods=8, freq="ME")
    price_map = {f"S{i}": list(100 + np.arange(8) * i) for i in range(1, 13)}
    loader = _make_loader(price_map, dates)

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=10, buffer_rank=20,
        lookback_days=2, skip_days=1, loader=loader,
    )
    ops = result["open_positions"]

    # one open position per current holding, each with full purchase detail
    assert {p["symbol"] for p in ops} == set(result["holdings"])
    required = {"symbol", "entry_date", "entry_price", "shares",
                "current_price", "market_value", "return_since_entry_pct"}
    for p in ops:
        assert required <= set(p.keys())
        assert p["shares"] > 0
        assert p["market_value"] > 0


def test_run_momentum_rotation_reports_invested_curve():
    dates = pd.date_range("2020-01-01", periods=8, freq="ME")
    price_map = {f"S{i}": list(100 + np.arange(8) * i) for i in range(1, 13)}
    loader = _make_loader(price_map, dates)

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=10, buffer_rank=20,
        lookback_days=2, skip_days=1, loader=loader,
    )
    inv = result["invested_curve"]
    eq = result["equity_curve"]

    assert list(inv.index) == list(eq.index)
    assert (inv <= eq + 1e-6).all()      # invested never exceeds total equity
    assert inv.iloc[-1] > 0              # fully-invested uptrend at the end


def test_run_momentum_rotation_invested_is_zero_when_regime_off():
    dates = pd.date_range("2020-01-01", periods=8, freq="ME")
    price_map = {f"S{i}": list(100 + np.arange(8) * i) for i in range(1, 13)}
    loader = _make_loader(price_map, dates)
    regime = pd.Series(True, index=dates)
    regime.iloc[-1] = False

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=10, buffer_rank=20,
        lookback_days=2, skip_days=1, loader=loader, regime=regime,
    )
    # cashed out on the final rebalance -> nothing invested, all in cash
    assert abs(result["invested_curve"].iloc[-1]) < 1e-6


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

    # top_n=5, buffer_rank=6 on this 12-symbol universe: CRASH spikes early
    # (bought into the small basket), then collapses to the worst rank
    # (> buffer_rank), forcing a real "fell out of top 20" exit.
    result = run_momentum_rotation(
        list(price_map.keys()), top_n=5, buffer_rank=6,
        lookback_days=2, skip_days=1, loader=loader,
    )

    trades = result["trades"]
    assert len(trades) >= 1
    crash_trades = [t for t in trades if t["symbol"] == "CRASH"]
    assert crash_trades, f"expected a CRASH exit, got trades: {trades}"
    assert crash_trades[0]["exit_reason"] == "fell out of top 20"

    reasons = {t["exit_reason"] for t in trades}
    assert reasons.issubset({"fell out of top 20", "below 200MA"})
    # every trade carries the required schema
    for t in trades:
        assert {"symbol", "entry_date", "exit_date", "pnl", "return_pct",
                "exit_reason", "rank_at_exit"} <= set(t.keys())


def test_run_momentum_rotation_carries_last_price_through_nan_gap():
    dates = pd.date_range("2020-01-01", periods=4, freq="ME")
    # Two symbols, both eligible and held; B has a NaN gap on the 3rd bar.
    price_map = {
        "A": [100.0, 101.0, 102.0, 103.0],
        "B": [100.0, 101.0, np.nan, 103.0],
    }
    loader = _make_loader(price_map, dates)

    result = run_momentum_rotation(
        list(price_map.keys()), top_n=2, buffer_rank=2,
        lookback_days=1, skip_days=0, loader=loader,
    )

    equity = result["equity_curve"]
    gap_date = dates[2]
    prior_date = dates[1]
    # B's NaN gap must not be valued at $0 -- equity should hold steady
    # (carried at B's last known price), not collapse on the gap bar.
    assert equity.loc[gap_date] >= equity.loc[prior_date] - 1e-6


def test_run_momentum_rotation_raises_value_error_on_empty_price_panel():
    def empty_loader(symbol, start=None, end=None):
        return pd.DataFrame(columns=["date", "close", "ma200"])

    with pytest.raises(ValueError):
        run_momentum_rotation(
            ["A", "B"], top_n=2, buffer_rank=2,
            lookback_days=1, skip_days=0, loader=empty_loader,
        )
