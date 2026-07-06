from data.broad_universe import normalize_symbol, parse_symbol_directory, parse_screener_market_caps


def test_parse_screener_market_caps_parses_dollar_strings():
    payload = {"data": {"table": {"rows": [
        {"symbol": "AAPL", "marketCap": "4,532,958,682,280"},
        {"symbol": "BRK.B", "marketCap": "1,000,000,000"},
        {"symbol": "ZZZZ", "marketCap": ""},          # no cap -> skipped
        {"symbol": "YYYY", "marketCap": "0"},          # zero -> skipped
    ]}}}
    caps = parse_screener_market_caps(payload)
    assert caps == {"AAPL": 4532958682280.0, "BRK-B": 1000000000.0}


def test_normalize_symbol_rules():
    assert normalize_symbol("aapl") == "AAPL"
    assert normalize_symbol("BRK.B") == "BRK-B"
    assert normalize_symbol("ABC$") is None       # preferred marker dropped
    assert normalize_symbol("XYZ.WS") == "XYZ-WS"  # dots -> dashes (kept)
    assert normalize_symbol("A B") is None         # space dropped
    assert normalize_symbol("") is None


def test_parse_nasdaqlisted_keeps_only_common_stock():
    text = "\n".join([
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
        "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N",
        "SPY|SPDR S&P 500 ETF Trust|Q|N|N|100|Y|N",           # ETF -> dropped
        "TEST|Test Issue - Common Stock|Q|Y|N|100|N|N",        # test issue -> dropped
        "PFDX|Some Co - Preferred Stock|Q|N|N|100|N|N",        # not common -> dropped
        "MSFT|Microsoft Corporation - Common Stock|Q|N|N|100|N|N",
        "File Creation Time: 0101202512:00",
    ])
    rows = parse_symbol_directory(text, "Symbol")
    syms = [s for s, _ in rows]
    assert syms == ["AAPL", "MSFT"]


def test_parse_otherlisted_uses_act_symbol_column():
    text = "\n".join([
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
        "A|Agilent Technologies, Inc. Common Stock|N|A|N|100|N|A",
        "IWV|iShares Russell 3000 ETF|P|IWV|Y|100|N|IWV",      # ETF -> dropped
        "BRK.B|Berkshire Hathaway Inc. Common Stock|N|BRK.B|N|100|N|BRK.B",
    ])
    rows = parse_symbol_directory(text, "ACT Symbol")
    syms = [s for s, _ in rows]
    assert syms == ["A", "BRK-B"]
