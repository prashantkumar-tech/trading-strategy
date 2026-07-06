import pandas as pd
from dashboard.backtest_cache import save, load


def test_save_and_load_roundtrip(tmp_path):
    obj = {"metrics": {"sharpe": 1.2}, "eq": pd.Series([1.0, 2.0, 3.0]), "holdings": ["A", "B"]}
    save("backtest", obj, cache_dir=tmp_path)
    loaded = load("backtest", cache_dir=tmp_path)
    assert loaded["metrics"] == {"sharpe": 1.2}
    assert list(loaded["eq"]) == [1.0, 2.0, 3.0]
    assert loaded["holdings"] == ["A", "B"]


def test_load_missing_returns_none(tmp_path):
    assert load("does_not_exist", cache_dir=tmp_path) is None
