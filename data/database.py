"""SQLite database setup and queries for historical price data."""

import sqlite3
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
        # Migrate existing tables that predate bar_size column
        cols = [r[1] for r in conn.execute("PRAGMA table_info(prices)").fetchall()]
        if "bar_size" not in cols:
            conn.execute("ALTER TABLE prices ADD COLUMN bar_size TEXT NOT NULL DEFAULT '1d'")
            conn.execute("UPDATE prices SET bar_size = '1d' WHERE bar_size IS NULL OR bar_size = ''")


def upsert_prices(df: pd.DataFrame, symbol: str, bar_size: str = "1d") -> None:
    """Delete existing rows for (symbol, bar_size) then insert fresh data."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM prices WHERE symbol = ? AND bar_size = ?",
            (symbol, bar_size),
        )
        df.to_sql("prices", conn, if_exists="append", index=False,
                  method="multi", chunksize=500)


def load_prices(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    bar_size: str = "1d",
) -> pd.DataFrame:
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
                "SELECT DISTINCT symbol FROM prices WHERE bar_size = ? ORDER BY symbol",
                (bar_size,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM prices ORDER BY symbol"
            ).fetchall()
    return [r[0] for r in rows]


def get_date_range(symbol: str, bar_size: str) -> tuple:
    """Return (min_date_str, max_date_str) for the given symbol + bar_size, or (None, None)."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT MIN(substr(date,1,10)), MAX(substr(date,1,10)) FROM prices WHERE symbol = ? AND bar_size = ?",
            (symbol, bar_size),
        ).fetchone()
    return (row[0], row[1]) if row and row[0] else (None, None)


def list_bar_sizes(symbol: str) -> list:
    """Return which bar sizes are available for a symbol."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT bar_size FROM prices WHERE symbol = ? ORDER BY bar_size",
            (symbol,),
        ).fetchall()
    return [r[0] for r in rows]
