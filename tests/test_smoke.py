def test_repo_is_importable():
    import backtest.metrics
    import data.database
    assert hasattr(backtest.metrics, "compute_metrics")
    assert hasattr(data.database, "load_prices")
