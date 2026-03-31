"""Live quote fetching via IB Gateway using ib_insync."""

from typing import Optional
import pandas as pd
from ib_insync import IB, Stock

GATEWAY_HOST = "127.0.0.1"
PAPER_PORT   = 4002   # IB Gateway paper trading
LIVE_PORT    = 4001   # IB Gateway live trading


def fetch_quotes(symbols: list, port: int = PAPER_PORT, timeout: int = 10) -> pd.DataFrame:
    """
    Connect to IB Gateway, request snapshot quotes for each symbol, disconnect.

    Returns a DataFrame with columns:
      symbol, last, bid, ask, bid_size, ask_size, volume, close, change, change_pct
    Returns an empty DataFrame with an 'error' key in attrs if connection fails.
    """
    ib = IB()
    try:
        ib.connect(GATEWAY_HOST, port, clientId=10, timeout=timeout, readonly=True)
    except Exception as e:
        df = pd.DataFrame()
        df.attrs["error"] = str(e)
        return df

    try:
        contracts = [Stock(sym, "SMART", "USD") for sym in symbols]
        tickers   = ib.reqTickers(*contracts)

        rows = []
        for sym, t in zip(symbols, tickers):
            last   = t.last   if t.last   and t.last   > 0 else t.close
            close  = t.close  if t.close  and t.close  > 0 else None
            change     = round(last - close, 2)        if last and close else None
            change_pct = round(change / close * 100, 2) if change and close else None
            rows.append({
                "symbol":     sym,
                "last":       _fmt(last),
                "bid":        _fmt(t.bid),
                "ask":        _fmt(t.ask),
                "bid_size":   int(t.bidSize)  if t.bidSize  else "—",
                "ask_size":   int(t.askSize)  if t.askSize  else "—",
                "volume":     f"{int(t.volume):,}" if t.volume else "—",
                "prev_close": _fmt(close),
                "change":     f"{change:+.2f}" if change is not None else "—",
                "change_%":   f"{change_pct:+.2f}%" if change_pct is not None else "—",
            })
        return pd.DataFrame(rows)

    finally:
        ib.disconnect()


def _fmt(val: Optional[float]) -> str:
    if val is None or val != val:   # nan check
        return "—"
    if val <= 0:
        return "—"
    return f"{val:.2f}"
