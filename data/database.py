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
                open        REAL    NOT NULL,
                high        REAL    NOT NULL,
                low         REAL    NOT NULL,
                close       REAL    NOT NULL,
                volume      INTEGER NOT NULL,
                ma50        REAL,
                ma200       REAL,
                PRIMARY KEY (symbol, date)
            )
        """)


def upsert_prices(df: pd.DataFrame, symbol: str) -> None:
    """Delete existing rows for symbol then insert fresh data."""
    with get_connection() as conn:
        conn.execute("DELETE FROM prices WHERE symbol = ?", (symbol,))
        df.to_sql("prices", conn, if_exists="append", index=False,
                  method="multi", chunksize=500)


def load_prices(symbol: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    query = "SELECT * FROM prices WHERE symbol = ?"
    params: list = [symbol]
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


def list_symbols() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT symbol FROM prices ORDER BY symbol").fetchall()
    return [r[0] for r in rows]
