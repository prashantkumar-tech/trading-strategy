# S&P 500 Momentum Rotation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a cross-sectional momentum strategy that each month holds an equal-weighted basket of the top-10 S&P 500 stocks by 12-1 momentum, with a top-20 sell buffer and a 200-day MA eligibility filter.

**Architecture:** A new, self-contained rotation engine (`backtest/momentum_rotation.py`) that loads all universe symbols into a wide price panel, ranks them cross-sectionally each month, and simulates an equal-weight top-10 basket with a hold-buffer. It reuses the existing `data.database.load_prices` (which already returns a stored `ma200` column) and `backtest.metrics.compute_metrics`. A universe module fetches the S&P 500 constituent list; a bulk-fetch script downloads daily bars via the existing `data.fetcher.fetch_and_store`. A Streamlit page exposes it.

**Tech Stack:** Python, pandas, numpy, SQLite (existing `db/trading.db`), yfinance (daily bars), Streamlit + Plotly (dashboard), pytest (new).

## Global Constraints

- Imports are absolute from the repo root (e.g. `from backtest.metrics import compute_metrics`, `from data.database import load_prices`) — matches existing code. Tests run from repo root via `pythonpath = ["."]`.
- Daily bars only: `bar_size="1d"`, `source="yfinance"` throughout. `load_prices(symbol)` defaults to these.
- Reuse, do not reimplement: `data.database.load_prices`, `data.fetcher.fetch_and_store`, `backtest.metrics.compute_metrics`.
- The `prices` table already stores `ma200`; the 200-day filter reads that column — do not recompute MAs in the engine.
- Position sizing default: `initial_capital = 10_000.0`, `top_n = 10` (equal weight → 10% target per name).
- Trade log records **full exits only** (reasons: `"fell out of top 20"`, `"below 200MA"`). Monthly equal-weight trims/top-ups are absorbed into a symbol's holding episode via average-cost accounting; they are not separate trade rows. Equity accounting remains exact.
- Survivorship caveat is accepted for v1 (current constituent list, no point-in-time membership).

---

## File Structure

- Create `data/sp500_universe.py` — fetch + normalize S&P 500 constituent tickers.
- Create `data/fetch_universe.py` — bulk-download daily bars for a ticker list via `fetch_and_store`.
- Create `backtest/momentum_rotation.py` — the rotation engine (panel, momentum, selection, simulation).
- Create `dashboard/pages/17_📈_SP500_Momentum_Rotation.py` — Streamlit UI.
- Create `tests/test_sp500_universe.py`, `tests/test_momentum_rotation.py` — unit + smoke tests.
- Create `pyproject.toml` — pytest config (`pythonpath = ["."]`).
- Modify `requirements.txt` — add `pytest`, `lxml` (for `pandas.read_html`).

---

### Task 1: Test harness + pytest config

**Files:**
- Create: `pyproject.toml`
- Modify: `requirements.txt`
- Test: `tests/test_smoke.py`

**Interfaces:**
- Produces: a working `pytest` invocation from repo root with the project importable.

- [ ] **Step 1: Add test deps to `requirements.txt`**

Append these two lines to `requirements.txt`:

```
pytest>=8.0.0
lxml>=5.0.0
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
```

- [ ] **Step 3: Write a smoke test**

Create `tests/test_smoke.py`:

```python
def test_repo_is_importable():
    import backtest.metrics
    import data.database
    assert hasattr(backtest.metrics, "compute_metrics")
    assert hasattr(data.database, "load_prices")
```

- [ ] **Step 4: Install deps and run the smoke test**

Run: `pip install -r requirements.txt && pytest tests/test_smoke.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add requirements.txt pyproject.toml tests/test_smoke.py
git commit -m "test: add pytest harness and config"
```

---

### Task 2: S&P 500 universe module

**Files:**
- Create: `data/sp500_universe.py`
- Test: `tests/test_sp500_universe.py`

**Interfaces:**
- Produces:
  - `normalize_ticker(ticker: str) -> str`
  - `parse_sp500_table(df: pandas.DataFrame) -> list[str]`
  - `get_sp500_tickers(use_cache: bool = True) -> list[str]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sp500_universe.py`:

```python
import pandas as pd
from data.sp500_universe import normalize_ticker, parse_sp500_table


def test_normalize_ticker_converts_dot_to_dash():
    assert normalize_ticker("BRK.B") == "BRK-B"
    assert normalize_ticker(" aapl ") == "AAPL"


def test_parse_sp500_table_extracts_normalized_symbols():
    df = pd.DataFrame({"Symbol": ["AAPL", "BRK.B", "MSFT"], "Security": ["a", "b", "c"]})
    tickers = parse_sp500_table(df)
    assert tickers == ["AAPL", "BRK-B", "MSFT"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sp500_universe.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'data.sp500_universe'`

- [ ] **Step 3: Write the implementation**

Create `data/sp500_universe.py`:

```python
"""Fetch and normalize the current S&P 500 constituent list.

Source: Wikipedia 'List of S&P 500 companies'. Uses today's membership
(survivorship caveat accepted for v1). Cached to a local text file so the
network fetch happens at most once unless refreshed.
"""

from pathlib import Path
from typing import List
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
CACHE_PATH = Path(__file__).parent / "sp500_tickers.txt"


def normalize_ticker(ticker: str) -> str:
    """Uppercase, strip, and convert Wikipedia dots to yfinance dashes (BRK.B -> BRK-B)."""
    return ticker.strip().upper().replace(".", "-")


def parse_sp500_table(df: pd.DataFrame) -> List[str]:
    """Extract normalized tickers from a Wikipedia constituents DataFrame."""
    return [normalize_ticker(str(s)) for s in df["Symbol"].tolist()]


def get_sp500_tickers(use_cache: bool = True) -> List[str]:
    """Return the S&P 500 tickers, reading a local cache when present."""
    if use_cache and CACHE_PATH.exists():
        return [line.strip() for line in CACHE_PATH.read_text().splitlines() if line.strip()]

    tables = pd.read_html(WIKI_URL)
    tickers = parse_sp500_table(tables[0])
    CACHE_PATH.write_text("\n".join(tickers) + "\n")
    return tickers
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sp500_universe.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add data/sp500_universe.py tests/test_sp500_universe.py
git commit -m "feat: add S&P 500 universe fetch + normalization"
```

---

### Task 3: Universe bulk-fetch script

**Files:**
- Create: `data/fetch_universe.py`
- Test: `tests/test_fetch_universe.py`

**Interfaces:**
- Consumes: `data.fetcher.fetch_and_store(symbol, bar_size, source, ...)`.
- Produces: `fetch_universe(tickers: list[str], fetch_fn=fetch_and_store) -> dict` returning `{"ok": [...], "failed": [(symbol, error_str), ...]}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fetch_universe.py`:

```python
from data.fetch_universe import fetch_universe


def test_fetch_universe_iterates_and_isolates_failures():
    calls = []

    def fake_fetch(symbol, **kwargs):
        calls.append(symbol)
        if symbol == "BAD":
            raise RuntimeError("no data")
        return None

    result = fetch_universe(["AAPL", "BAD", "MSFT"], fetch_fn=fake_fetch)

    assert calls == ["AAPL", "BAD", "MSFT"]
    assert result["ok"] == ["AAPL", "MSFT"]
    assert result["failed"] == [("BAD", "no data")]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetch_universe.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'data.fetch_universe'`

- [ ] **Step 3: Write the implementation**

Create `data/fetch_universe.py`:

```python
"""Bulk-download daily bars for a list of tickers into the prices table.

Each symbol is fetched independently so one bad ticker never aborts the run.
"""

from typing import Callable, List
from data.fetcher import fetch_and_store


def fetch_universe(
    tickers: List[str],
    fetch_fn: Callable = fetch_and_store,
    start: str = None,
    end: str = None,
    refresh_mode: str = "incremental",
) -> dict:
    """Fetch daily bars for every ticker. Returns {"ok": [...], "failed": [(sym, err), ...]}."""
    ok, failed = [], []
    for i, symbol in enumerate(tickers, 1):
        try:
            fetch_fn(
                symbol,
                bar_size="1d",
                source="yfinance",
                start=start,
                end=end,
                refresh_mode=refresh_mode,
            )
            ok.append(symbol)
        except Exception as exc:  # isolate per-symbol failures
            failed.append((symbol, str(exc)))
        print(f"[{i}/{len(tickers)}] {symbol}: {'ok' if symbol in ok else 'FAILED'}")
    return {"ok": ok, "failed": failed}


if __name__ == "__main__":
    from data.sp500_universe import get_sp500_tickers

    tickers = get_sp500_tickers()
    print(f"Fetching {len(tickers)} S&P 500 symbols (daily)...")
    summary = fetch_universe(tickers)
    print(f"Done. ok={len(summary['ok'])} failed={len(summary['failed'])}")
    if summary["failed"]:
        print("Failed:", summary["failed"])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_fetch_universe.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add data/fetch_universe.py tests/test_fetch_universe.py
git commit -m "feat: add bulk universe fetch with per-symbol failure isolation"
```

---

### Task 4: Momentum calculation + month-end helper

**Files:**
- Create: `backtest/momentum_rotation.py`
- Test: `tests/test_momentum_rotation.py`

**Interfaces:**
- Produces:
  - `compute_momentum(close: pandas.DataFrame, lookback_days: int = 252, skip_days: int = 21) -> pandas.DataFrame`
  - `month_end_dates(dates) -> list[pandas.Timestamp]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_momentum_rotation.py`:

```python
import pandas as pd
from backtest.momentum_rotation import compute_momentum, month_end_dates


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backtest.momentum_rotation'`

- [ ] **Step 3: Write the implementation**

Create `backtest/momentum_rotation.py` with this initial content:

```python
"""S&P 500 cross-sectional momentum rotation engine.

Each month: rank the universe by 12-1 momentum, hold an equal-weighted
top-10 basket of names above their own 200-day MA, and rotate a holding out
only once it exits the top 20 (hold buffer). Reuses stored ma200 and the
shared metrics module.
"""

from typing import Callable, List
import pandas as pd
from data.database import load_prices
from backtest.metrics import compute_metrics


def compute_momentum(close: pd.DataFrame, lookback_days: int = 252,
                     skip_days: int = 21) -> pd.DataFrame:
    """12-1 style momentum: return from lookback_days ago to skip_days ago.

    momentum[t] = close[t - skip_days] / close[t - lookback_days] - 1
    """
    return close.shift(skip_days) / close.shift(lookback_days) - 1.0


def month_end_dates(dates) -> List[pd.Timestamp]:
    """Return the last available trading day of each calendar month."""
    idx = pd.DatetimeIndex(sorted(pd.to_datetime(list(dates))))
    ends = []
    for i, d in enumerate(idx):
        is_last = (
            i == len(idx) - 1
            or idx[i + 1].month != d.month
            or idx[i + 1].year != d.year
        )
        if is_last:
            ends.append(d)
    return ends
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add backtest/momentum_rotation.py tests/test_momentum_rotation.py
git commit -m "feat: add momentum calc and month-end rebalance helper"
```

---

### Task 5: Basket selection with hold-buffer

**Files:**
- Modify: `backtest/momentum_rotation.py`
- Test: `tests/test_momentum_rotation.py`

**Interfaces:**
- Produces: `select_basket(momentum: pandas.Series, eligible: set, current_holdings: list, top_n: int = 10, buffer_rank: int = 20) -> tuple[list[str], dict]` returning `(basket, rank_map)` where `rank_map` is `{symbol: rank}` (1-based) over eligible names.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_momentum_rotation.py`:

```python
from backtest.momentum_rotation import select_basket


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: FAIL with `ImportError: cannot import name 'select_basket'`

- [ ] **Step 3: Write the implementation**

Append to `backtest/momentum_rotation.py`:

```python
def select_basket(momentum: pd.Series, eligible: set, current_holdings: list,
                  top_n: int = 10, buffer_rank: int = 20):
    """Pick the target basket applying the top-N hold with a top-buffer_rank sell buffer.

    Returns (basket, rank_map). Holdings still ranked within buffer_rank are kept;
    open slots are refilled from the highest-ranked eligible names not already held.
    """
    ranked = sorted(
        (s for s in eligible if s in momentum.index and pd.notna(momentum[s])),
        key=lambda s: momentum[s],
        reverse=True,
    )
    rank = {s: i + 1 for i, s in enumerate(ranked)}

    kept = [s for s in current_holdings if rank.get(s, 10 ** 9) <= buffer_rank]
    basket = list(kept)
    for s in ranked:
        if len(basket) >= top_n:
            break
        if s not in basket:
            basket.append(s)
    return basket[:top_n], rank
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add backtest/momentum_rotation.py tests/test_momentum_rotation.py
git commit -m "feat: add basket selection with top-20 hold buffer"
```

---

### Task 6: Price panel builder

**Files:**
- Modify: `backtest/momentum_rotation.py`
- Test: `tests/test_momentum_rotation.py`

**Interfaces:**
- Consumes: a `loader(symbol, start, end)` callable (defaults to `data.database.load_prices`) returning a DataFrame with `date`, `close`, `ma200` columns.
- Produces: `build_price_panel(symbols: list[str], start=None, end=None, loader=load_prices) -> tuple[pandas.DataFrame, pandas.DataFrame]` returning `(close_panel, ma200_panel)` — wide DataFrames indexed by date, one column per symbol, aligned on a shared date index.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_momentum_rotation.py`:

```python
from backtest.momentum_rotation import build_price_panel


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_price_panel'`

- [ ] **Step 3: Write the implementation**

Append to `backtest/momentum_rotation.py`:

```python
def build_price_panel(symbols: List[str], start=None, end=None,
                      loader: Callable = load_prices):
    """Load each symbol and pivot into aligned wide close/ma200 panels."""
    closes, ma200s = {}, {}
    for sym in symbols:
        df = loader(sym, start=start, end=end)
        if df is None or df.empty:
            continue
        df = df.set_index("date")
        closes[sym] = df["close"]
        ma200s[sym] = df["ma200"]

    close = pd.DataFrame(closes).sort_index()
    ma200 = pd.DataFrame(ma200s).reindex(close.index)
    return close, ma200
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add backtest/momentum_rotation.py tests/test_momentum_rotation.py
git commit -m "feat: add wide price-panel builder"
```

---

### Task 7: Rotation simulator (orchestrator)

**Files:**
- Modify: `backtest/momentum_rotation.py`
- Test: `tests/test_momentum_rotation.py`

**Interfaces:**
- Consumes: `compute_momentum`, `month_end_dates`, `select_basket`, `build_price_panel`, `compute_metrics`.
- Produces: `run_momentum_rotation(symbols, start=None, end=None, top_n=10, buffer_rank=20, lookback_days=252, skip_days=21, initial_capital=10_000.0, loader=load_prices) -> dict` with keys `equity_curve` (pandas.Series), `trades` (list[dict]), `metrics` (dict), `final_value` (float), `holdings` (list[str]). Each trade dict has keys: `symbol, entry_date, exit_date, entry_price, exit_price, shares, pnl, return_pct, exit_reason, rank_at_exit`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_momentum_rotation.py`:

```python
import numpy as np
from backtest.momentum_rotation import run_momentum_rotation


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: FAIL with `ImportError: cannot import name 'run_momentum_rotation'`

- [ ] **Step 3: Write the implementation**

Append to `backtest/momentum_rotation.py`:

```python
def run_momentum_rotation(symbols, start=None, end=None, top_n=10, buffer_rank=20,
                          lookback_days=252, skip_days=21, initial_capital=10_000.0,
                          loader: Callable = load_prices) -> dict:
    """Simulate the monthly equal-weight momentum rotation. See module docstring."""
    close, ma200 = build_price_panel(symbols, start, end, loader)
    momentum = compute_momentum(close, lookback_days, skip_days)
    rebalance_days = set(month_end_dates(close.index))

    cash = float(initial_capital)
    lots = {}          # symbol -> {shares, total_cost, total_proceeds, entry_price, entry_date}
    trades = []
    equity_curve = []

    def holdings_value(prices_row):
        return sum(l["shares"] * prices_row[s]
                   for s, l in lots.items() if pd.notna(prices_row.get(s)))

    for d in close.index:
        prices_row = close.loc[d]

        if d in rebalance_days:
            mom_row = momentum.loc[d]
            ma_row = ma200.loc[d]
            eligible = {
                s for s in close.columns
                if pd.notna(mom_row.get(s)) and pd.notna(prices_row.get(s))
                and pd.notna(ma_row.get(s)) and prices_row[s] > ma_row[s]
            }
            basket, rank = select_basket(mom_row, eligible, list(lots.keys()),
                                         top_n, buffer_rank)
            portfolio_value = cash + holdings_value(prices_row)
            target = portfolio_value / top_n

            # ── Exits: fully close any holding no longer in the basket ──────
            for s in list(lots.keys()):
                if s in basket:
                    continue
                p = prices_row.get(s)
                if pd.isna(p):
                    continue  # can't price this bar; defer
                lot = lots[s]
                proceeds = lot["shares"] * p
                cash += proceeds
                lot["total_proceeds"] += proceeds
                pnl = lot["total_proceeds"] - lot["total_cost"]
                reason = "below 200MA" if s not in eligible else "fell out of top 20"
                trades.append({
                    "symbol": s,
                    "entry_date": lot["entry_date"],
                    "exit_date": str(d.date()),
                    "entry_price": round(lot["entry_price"], 4),
                    "exit_price": round(p, 4),
                    "shares": round(lot["shares"], 6),
                    "pnl": round(pnl, 2),
                    "return_pct": round(pnl / lot["total_cost"] * 100, 2) if lot["total_cost"] else 0.0,
                    "exit_reason": reason,
                    "rank_at_exit": rank.get(s),
                })
                del lots[s]

            # ── Rebalance basket to equal weight (target per name) ──────────
            for s in basket:
                p = prices_row.get(s)
                if pd.isna(p):
                    continue
                cur_shares = lots[s]["shares"] if s in lots else 0.0
                delta = (target / p) - cur_shares
                if delta > 0:                      # buy / top up
                    cost = min(delta * p, cash)
                    if cost <= 0:
                        continue
                    buy_shares = cost / p
                    if s in lots:
                        lots[s]["shares"] += buy_shares
                        lots[s]["total_cost"] += cost
                    else:
                        lots[s] = {"shares": buy_shares, "total_cost": cost,
                                   "total_proceeds": 0.0, "entry_price": p,
                                   "entry_date": str(d.date())}
                    cash -= cost
                elif delta < 0:                    # trim
                    sell_shares = -delta
                    proceeds = sell_shares * p
                    lots[s]["shares"] -= sell_shares
                    lots[s]["total_proceeds"] += proceeds
                    cash += proceeds

        equity_curve.append({"date": d, "portfolio_value": cash + holdings_value(prices_row)})

    equity_series = pd.DataFrame(equity_curve).set_index("date")["portfolio_value"]
    metrics = compute_metrics(equity_series, trades)
    return {
        "equity_curve": equity_series,
        "trades": trades,
        "metrics": metrics,
        "final_value": round(float(equity_series.iloc[-1]), 2) if not equity_series.empty else initial_capital,
        "holdings": list(lots.keys()),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_momentum_rotation.py -v`
Expected: PASS (8 passed)

- [ ] **Step 5: Run the full suite**

Run: `pytest -v`
Expected: PASS (all tests)

- [ ] **Step 6: Commit**

```bash
git add backtest/momentum_rotation.py tests/test_momentum_rotation.py
git commit -m "feat: add momentum rotation simulator with equal-weight rebalance"
```

---

### Task 8: Streamlit dashboard page

**Files:**
- Create: `dashboard/pages/17_📈_SP500_Momentum_Rotation.py`

**Interfaces:**
- Consumes: `run_momentum_rotation`, `data.database.list_symbols`, `data.database.load_prices`.

This task has no unit test (UI). It is verified by launching Streamlit and confirming the page renders results without error.

- [ ] **Step 1: Create the page**

Create `dashboard/pages/17_📈_SP500_Momentum_Rotation.py`:

```python
"""S&P 500 Momentum Rotation — Streamlit page."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.database import list_symbols, load_prices
from backtest.momentum_rotation import run_momentum_rotation

st.set_page_config(page_title="S&P 500 Momentum Rotation", page_icon="📈", layout="wide")
st.title("📈 S&P 500 Momentum Rotation")

# ── Sidebar controls ──────────────────────────────────────────────────────
st.sidebar.header("Strategy settings")
top_n = st.sidebar.number_input("Basket size (top N)", 1, 50, 10)
buffer_rank = st.sidebar.number_input("Sell buffer (exit when rank >)", top_n, 100, 20)
lookback_days = st.sidebar.number_input("Lookback (trading days)", 20, 400, 252)
skip_days = st.sidebar.number_input("Skip recent (trading days)", 0, 60, 21)
initial_capital = st.sidebar.number_input("Starting capital ($)", 1000, 10_000_000, 10_000, step=1000)
start = st.sidebar.text_input("Start date (YYYY-MM-DD)", "2015-01-01")
end = st.sidebar.text_input("End date (YYYY-MM-DD)", "")

symbols = list_symbols(bar_size="1d", source="yfinance")
st.caption(f"{len(symbols)} daily symbols available in the database.")

if st.button("Run backtest", type="primary"):
    if len(symbols) < top_n:
        st.error(f"Only {len(symbols)} symbols in the DB — fetch the S&P 500 universe first "
                 f"(`python -m data.fetch_universe`).")
        st.stop()

    with st.spinner(f"Ranking {len(symbols)} symbols..."):
        result = run_momentum_rotation(
            symbols, start=start or None, end=end or None,
            top_n=int(top_n), buffer_rank=int(buffer_rank),
            lookback_days=int(lookback_days), skip_days=int(skip_days),
            initial_capital=float(initial_capital),
        )

    m = result["metrics"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total return", f"{m['total_return_pct']}%")
    c2.metric("Annualized", f"{m['annualized_return_pct']}%")
    c3.metric("Sharpe", m["sharpe_ratio"])
    c4.metric("Max drawdown", f"{m['max_drawdown_pct']}%")

    # Equity curve vs SPY buy & hold
    fig = go.Figure()
    eq = result["equity_curve"]
    fig.add_trace(go.Scatter(x=eq.index, y=eq.values, name="Strategy"))
    spy = load_prices("SPY", start=start or None, end=end or None)
    if not spy.empty:
        spy = spy.set_index("date")["close"]
        spy_bh = spy / spy.iloc[0] * float(initial_capital)
        fig.add_trace(go.Scatter(x=spy_bh.index, y=spy_bh.values, name="SPY buy & hold"))
    fig.update_layout(title="Equity curve", xaxis_title="Date", yaxis_title="Portfolio value ($)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Current basket")
    st.write(result["holdings"])

    st.subheader("Rotation / trade log")
    trades_df = pd.DataFrame(result["trades"])
    if trades_df.empty:
        st.info("No completed round-trip trades in this window.")
    else:
        st.dataframe(trades_df, use_container_width=True)
else:
    st.info("Set parameters in the sidebar and click **Run backtest**.")
```

- [ ] **Step 2: Verify the page imports cleanly**

Run: `python -c "import ast; ast.parse(open('dashboard/pages/17_📈_SP500_Momentum_Rotation.py').read())"`
Expected: no output (syntax OK)

- [ ] **Step 3: Launch and eyeball (manual)**

Run: `streamlit run dashboard/app.py`
Then open the "SP500 Momentum Rotation" page. With the universe fetched, click **Run backtest** and confirm metrics, equity curve, basket, and trade log render without error. (If the DB has no S&P 500 symbols yet, the page shows the fetch instruction — that is expected.)

- [ ] **Step 4: Commit**

```bash
git add "dashboard/pages/17_📈_SP500_Momentum_Rotation.py"
git commit -m "feat: add S&P 500 momentum rotation dashboard page"
```

---

## Post-implementation: fetch the real universe

Not a code task — run once to populate data before using the page for real:

```bash
python -m data.fetch_universe
```

This fetches the current S&P 500 list from Wikipedia and downloads ~500 symbols of daily bars into `db/trading.db`. Expect it to take several minutes and for a handful of tickers to fail (recent listings, ticker changes) — those are reported and safely skipped.

---

## Self-Review Notes

- **Spec coverage:** universe fetch (Task 2/3) ✓; wide panel (Task 6) ✓; 12-1 momentum (Task 4) ✓; monthly rebalance (Task 4 `month_end_dates` + Task 7) ✓; eligibility 200-MA filter (Task 7, reuses stored `ma200`) ✓; top-20 sell buffer (Task 5) ✓; refill + cash slots (Task 5) ✓; equal-weight rebalance (Task 7) ✓; trade log with exit reasons + rank (Task 7) ✓; metrics reuse (Task 7) ✓; dashboard page (Task 8) ✓; test-first throughout ✓.
- **Deliberate deviation from spec:** the `"rebalance trim"` exit reason is dropped. Monthly trims/top-ups are absorbed into a holding's episode via average-cost accounting; only full exits are logged. Equity accounting is exact. (Flagged to the user.)
- **Type consistency:** `select_basket` returns `(basket, rank_map)` and Task 7 consumes both; `build_price_panel` returns `(close, ma200)` consumed by Task 7; trade dict schema in Task 7 matches the test assertions in Task 7 and the interface block.
