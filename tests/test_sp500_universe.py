import pandas as pd
from data.sp500_universe import normalize_ticker, parse_sp500_table


def test_normalize_ticker_converts_dot_to_dash():
    assert normalize_ticker("BRK.B") == "BRK-B"
    assert normalize_ticker(" aapl ") == "AAPL"


def test_parse_sp500_table_extracts_normalized_symbols():
    df = pd.DataFrame({"Symbol": ["AAPL", "BRK.B", "MSFT"], "Security": ["a", "b", "c"]})
    tickers = parse_sp500_table(df)
    assert tickers == ["AAPL", "BRK-B", "MSFT"]
