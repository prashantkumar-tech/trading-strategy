"""SQLite database setup and queries for historical price data."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "db" / "trading.db"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                symbol      TEXT    NOT NULL,
                date        TEXT    NOT NULL,
                bar_size    TEXT    NOT NULL DEFAULT '1d',
                open        REAL    NOT NULL,
                high        REAL    NOT NULL,
                low         REAL    NOT NULL,
                close       REAL    NOT NULL,
                volume      INTEGER NOT NULL,
                ma50        REAL,
                ma200       REAL,
                PRIMARY KEY (symbol, date, bar_size)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                symbol       TEXT    NOT NULL,
                bar_size     TEXT    NOT NULL,
                bar_count    INTEGER NOT NULL,
                min_date     TEXT    NOT NULL,
                max_date     TEXT    NOT NULL,
                last_updated TEXT    NOT NULL,
                PRIMARY KEY (symbol, bar_size)
            )
        """)
        # Migrate existing tables that predate bar_size column
        cols = [r[1] for r in conn.execute("PRAGMA table_info(prices)").fetchall()]
        if "bar_size" not in cols:
            conn.execute("ALTER TABLE prices ADD COLUMN bar_size TEXT NOT NULL DEFAULT '1d'")
            conn.execute("UPDATE prices SET bar_size = '1d' WHERE bar_size IS NULL OR bar_size = ''")
        dataset_count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
        price_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        if dataset_count == 0 and price_count > 0:
            _rebuild_dataset_metadata(conn)


def upsert_prices(df: pd.DataFrame, symbol: str, bar_size: str = "1d") -> None:
    """Delete existing rows for (symbol, bar_size) then insert fresh data."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM prices WHERE symbol = ? AND bar_size = ?",
            (symbol, bar_size),
        )
        df.to_sql("prices", conn, if_exists="append", index=False,
                  method="multi", chunksize=500)
        _update_dataset_metadata(conn, symbol, bar_size)


def _update_dataset_metadata(conn: sqlite3.Connection, symbol: str, bar_size: str) -> None:
    row = conn.execute(
        """
        SELECT COUNT(*),
               MIN(substr(date, 1, 10)),
               MAX(substr(date, 1, 10))
        FROM prices
        WHERE symbol = ? AND bar_size = ?
        """,
        (symbol, bar_size),
    ).fetchone()

    bar_count, min_date, max_date = row if row else (0, None, None)
    if not bar_count or not min_date or not max_date:
        conn.execute(
            "DELETE FROM datasets WHERE symbol = ? AND bar_size = ?",
            (symbol, bar_size),
        )
        return

    conn.execute(
        """
        INSERT INTO datasets (symbol, bar_size, bar_count, min_date, max_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, bar_size) DO UPDATE SET
            bar_count = excluded.bar_count,
            min_date = excluded.min_date,
            max_date = excluded.max_date,
            last_updated = excluded.last_updated
        """,
        (symbol, bar_size, int(bar_count), min_date, max_date, datetime.utcnow().isoformat()),
    )


def _rebuild_dataset_metadata(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT symbol,
               bar_size,
               COUNT(*) AS bar_count,
               MIN(substr(date, 1, 10)) AS min_date,
               MAX(substr(date, 1, 10)) AS max_date
        FROM prices
        GROUP BY symbol, bar_size
        """
    ).fetchall()

    conn.execute("DELETE FROM datasets")
    if not rows:
        return

    now = datetime.utcnow().isoformat()
    conn.executemany(
        """
        INSERT INTO datasets (symbol, bar_size, bar_count, min_date, max_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [(symbol, bar_size, int(bar_count), min_date, max_date, now)
         for symbol, bar_size, bar_count, min_date, max_date in rows],
    )


def load_dataset_inventory() -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT symbol,
                   bar_size,
                   bar_count AS bars,
                   min_date  AS from_date,
                   max_date  AS to_date,
                   last_updated
            FROM datasets
            ORDER BY symbol, bar_size
            """,
            conn,
        )


def load_prices(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    bar_size: str = "1d",
) -> pd.DataFrame:
    if start and bar_size != "1d" and len(start) == 10:
        start = f"{start} 00:00:00"
    if end and bar_size != "1d" and len(end) == 10:
        end = f"{end} 23:59:59"

    query = "SELECT * FROM prices WHERE symbol = ? AND bar_size = ?"
    params: list = [symbol, bar_size]
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


def list_symbols(bar_size: Optional[str] = None) -> list:
    with get_connection() as conn:
        if bar_size:
            rows = conn.execute(
                "SELECT symbol FROM datasets WHERE bar_size = ? ORDER BY symbol",
                (bar_size,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM datasets ORDER BY symbol"
            ).fetchall()
    return [r[0] for r in rows]


def get_date_range(symbol: str, bar_size: str) -> tuple:
    """Return (min_date_str, max_date_str) for the given symbol + bar_size, or (None, None)."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT min_date, max_date FROM datasets WHERE symbol = ? AND bar_size = ?",
            (symbol, bar_size),
        ).fetchone()
    return (row[0], row[1]) if row and row[0] else (None, None)


def list_bar_sizes(symbol: str) -> list:
    """Return which bar sizes are available for a symbol."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT bar_size FROM datasets WHERE symbol = ? ORDER BY bar_size",
            (symbol,),
        ).fetchall()
    return [r[0] for r in rows]
