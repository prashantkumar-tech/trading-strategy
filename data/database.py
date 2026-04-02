"""SQLite database setup and queries for historical price data."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "db" / "trading.db"

DEFAULT_SOURCE_BY_BAR_SIZE = {
    "1d": "yfinance",
    "1m": "polygon",
    "5m": "polygon",
    "15m": "polygon",
    "30m": "polygon",
    "1h": "polygon",
}


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with get_connection() as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _ensure_prices_schema(conn)
        _ensure_datasets_schema(conn)
        dataset_count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
        price_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        if dataset_count == 0 and price_count > 0:
            _rebuild_dataset_metadata(conn)


def _ensure_prices_schema(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='prices'"
    ).fetchone()
    if not exists:
        _create_prices_table(conn)
        return

    cols = [r[1] for r in conn.execute("PRAGMA table_info(prices)").fetchall()]
    needs_bar_size = "bar_size" not in cols
    needs_source = "source" not in cols
    if not needs_bar_size and not needs_source:
        return

    conn.execute("ALTER TABLE prices RENAME TO prices_legacy")
    _create_prices_table(conn)

    legacy_cols = [r[1] for r in conn.execute("PRAGMA table_info(prices_legacy)").fetchall()]
    select_bar_size = "bar_size" if "bar_size" in legacy_cols else "'1d'"
    select_source = (
        "source"
        if "source" in legacy_cols
        else f"CASE WHEN {select_bar_size} = '1d' THEN 'yfinance' ELSE 'polygon' END"
    )
    conn.execute(
        f"""
        INSERT INTO prices (symbol, date, bar_size, source, open, high, low, close, volume, ma50, ma200)
        SELECT symbol, date, {select_bar_size}, {select_source}, open, high, low, close, volume, ma50, ma200
        FROM prices_legacy
        """
    )
    conn.execute("DROP TABLE prices_legacy")


def _ensure_datasets_schema(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='datasets'"
    ).fetchone()
    if not exists:
        _create_datasets_table(conn)
        return

    cols = [r[1] for r in conn.execute("PRAGMA table_info(datasets)").fetchall()]
    if "source" in cols:
        return

    conn.execute("ALTER TABLE datasets RENAME TO datasets_legacy")
    _create_datasets_table(conn)
    conn.execute(
        """
        INSERT INTO datasets (symbol, bar_size, source, bar_count, min_date, max_date, last_updated)
        SELECT
            symbol,
            bar_size,
            CASE WHEN bar_size = '1d' THEN 'yfinance' ELSE 'polygon' END,
            bar_count,
            min_date,
            max_date,
            last_updated
        FROM datasets_legacy
        """
    )
    conn.execute("DROP TABLE datasets_legacy")


def _create_prices_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE prices (
            symbol      TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            bar_size    TEXT    NOT NULL DEFAULT '1d',
            source      TEXT    NOT NULL,
            open        REAL    NOT NULL,
            high        REAL    NOT NULL,
            low         REAL    NOT NULL,
            close       REAL    NOT NULL,
            volume      INTEGER NOT NULL,
            ma50        REAL,
            ma200       REAL,
            PRIMARY KEY (symbol, date, bar_size, source)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_lookup ON prices(symbol, bar_size, source, date)")


def _create_datasets_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE datasets (
            symbol       TEXT    NOT NULL,
            bar_size     TEXT    NOT NULL,
            source       TEXT    NOT NULL,
            bar_count    INTEGER NOT NULL,
            min_date     TEXT    NOT NULL,
            max_date     TEXT    NOT NULL,
            last_updated TEXT    NOT NULL,
            PRIMARY KEY (symbol, bar_size, source)
        )
    """)


def upsert_prices(df: pd.DataFrame, symbol: str, bar_size: str = "1d", source: Optional[str] = None) -> None:
    """Delete existing rows for (symbol, bar_size, source) then insert fresh data."""
    source = source or DEFAULT_SOURCE_BY_BAR_SIZE.get(bar_size, "polygon")
    df = df.copy()
    df["source"] = source
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM prices WHERE symbol = ? AND bar_size = ? AND source = ?",
            (symbol, bar_size, source),
        )
        df.to_sql("prices", conn, if_exists="append", index=False,
                  method="multi", chunksize=500)
        _update_dataset_metadata(conn, symbol, bar_size, source)


def _update_dataset_metadata(conn: sqlite3.Connection, symbol: str, bar_size: str, source: str) -> None:
    row = conn.execute(
        """
        SELECT COUNT(*),
               MIN(substr(date, 1, 10)),
               MAX(substr(date, 1, 10))
        FROM prices
        WHERE symbol = ? AND bar_size = ? AND source = ?
        """,
        (symbol, bar_size, source),
    ).fetchone()

    bar_count, min_date, max_date = row if row else (0, None, None)
    if not bar_count or not min_date or not max_date:
        conn.execute(
            "DELETE FROM datasets WHERE symbol = ? AND bar_size = ? AND source = ?",
            (symbol, bar_size, source),
        )
        return

    conn.execute(
        """
        INSERT INTO datasets (symbol, bar_size, source, bar_count, min_date, max_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, bar_size, source) DO UPDATE SET
            bar_count = excluded.bar_count,
            min_date = excluded.min_date,
            max_date = excluded.max_date,
            last_updated = excluded.last_updated
        """,
        (symbol, bar_size, source, int(bar_count), min_date, max_date, datetime.utcnow().isoformat()),
    )


def _rebuild_dataset_metadata(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT symbol,
               bar_size,
               source,
               COUNT(*) AS bar_count,
               MIN(substr(date, 1, 10)) AS min_date,
               MAX(substr(date, 1, 10)) AS max_date
        FROM prices
        GROUP BY symbol, bar_size, source
        """
    ).fetchall()

    conn.execute("DELETE FROM datasets")
    if not rows:
        return

    now = datetime.utcnow().isoformat()
    conn.executemany(
        """
        INSERT INTO datasets (symbol, bar_size, source, bar_count, min_date, max_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(symbol, bar_size, source, int(bar_count), min_date, max_date, now)
         for symbol, bar_size, source, bar_count, min_date, max_date in rows],
    )


def load_dataset_inventory() -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT symbol,
                   bar_size,
                   source,
                   bar_count AS bars,
                   min_date  AS from_date,
                   max_date  AS to_date,
                   last_updated
            FROM datasets
            ORDER BY symbol, bar_size, source
            """,
            conn,
        )


def load_prices(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    bar_size: str = "1d",
    source: Optional[str] = None,
) -> pd.DataFrame:
    source = source or DEFAULT_SOURCE_BY_BAR_SIZE.get(bar_size, "polygon")
    if start and bar_size != "1d" and len(start) == 10:
        start = f"{start} 00:00:00"
    if end and bar_size != "1d" and len(end) == 10:
        end = f"{end} 23:59:59"

    query = "SELECT * FROM prices WHERE symbol = ? AND bar_size = ? AND source = ?"
    params: list = [symbol, bar_size, source]
    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date ASC"

    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=params, parse_dates=["date"])
    return df


def list_symbols(bar_size: Optional[str] = None, source: Optional[str] = None) -> list:
    with get_connection() as conn:
        if bar_size and source:
            rows = conn.execute(
                "SELECT symbol FROM datasets WHERE bar_size = ? AND source = ? ORDER BY symbol",
                (bar_size, source),
            ).fetchall()
        elif bar_size:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM datasets WHERE bar_size = ? ORDER BY symbol",
                (bar_size,),
            ).fetchall()
        elif source:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM datasets WHERE source = ? ORDER BY symbol",
                (source,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM datasets ORDER BY symbol"
            ).fetchall()
    return [r[0] for r in rows]


def get_date_range(symbol: str, bar_size: str, source: Optional[str] = None) -> tuple:
    """Return (min_date_str, max_date_str) for the given symbol + bar_size + source, or (None, None)."""
    source = source or DEFAULT_SOURCE_BY_BAR_SIZE.get(bar_size, "polygon")
    with get_connection() as conn:
        row = conn.execute(
            "SELECT min_date, max_date FROM datasets WHERE symbol = ? AND bar_size = ? AND source = ?",
            (symbol, bar_size, source),
        ).fetchone()
    return (row[0], row[1]) if row and row[0] else (None, None)


def list_bar_sizes(symbol: str, source: Optional[str] = None) -> list:
    """Return which bar sizes are available for a symbol."""
    with get_connection() as conn:
        if source:
            rows = conn.execute(
                "SELECT bar_size FROM datasets WHERE symbol = ? AND source = ? ORDER BY bar_size",
                (symbol, source),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT bar_size FROM datasets WHERE symbol = ? ORDER BY bar_size",
                (symbol,),
            ).fetchall()
    return [r[0] for r in rows]
