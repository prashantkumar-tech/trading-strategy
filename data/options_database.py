"""SQLite storage for option contract metadata and OHLCV bars."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd

from data.database import DB_PATH, get_connection


def init_options_db() -> None:
    """Create option tables if they do not already exist."""
    DB_PATH.parent.mkdir(exist_ok=True)
    with get_connection() as conn:
        _create_option_contracts_table(conn)
        _create_option_prices_table(conn)
        _create_option_marks_table(conn)
        _create_option_daily_targets_table(conn)
        _create_option_datasets_table(conn)


def _create_option_contracts_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_contracts (
            raw_symbol          TEXT    PRIMARY KEY,
            underlying          TEXT    NOT NULL,
            instrument_id       INTEGER,
            option_type         TEXT    NOT NULL,
            strike              REAL    NOT NULL,
            expiration          TEXT    NOT NULL,
            first_seen          TEXT,
            last_seen           TEXT,
            contract_multiplier REAL,
            source              TEXT    NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_lookup "
        "ON option_contracts(underlying, expiration, strike, option_type)"
    )


def _create_option_prices_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_prices (
            raw_symbol TEXT    NOT NULL,
            date       TEXT    NOT NULL,
            bar_size   TEXT    NOT NULL,
            source     TEXT    NOT NULL,
            open       REAL    NOT NULL,
            high       REAL    NOT NULL,
            low        REAL    NOT NULL,
            close      REAL    NOT NULL,
            volume     INTEGER NOT NULL,
            PRIMARY KEY (raw_symbol, date, bar_size, source),
            FOREIGN KEY (raw_symbol) REFERENCES option_contracts(raw_symbol)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_option_prices_lookup "
        "ON option_prices(raw_symbol, bar_size, source, date)"
    )


def _create_option_marks_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_marks (
            raw_symbol  TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            source      TEXT    NOT NULL,
            mark_time   TEXT    NOT NULL,
            bid         REAL    NOT NULL,
            ask         REAL    NOT NULL,
            mid         REAL    NOT NULL,
            spread      REAL    NOT NULL,
            spread_pct  REAL    NOT NULL,
            bid_size    INTEGER,
            ask_size    INTEGER,
            PRIMARY KEY (raw_symbol, date, source),
            FOREIGN KEY (raw_symbol) REFERENCES option_contracts(raw_symbol)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_option_marks_lookup "
        "ON option_marks(raw_symbol, source, date)"
    )


def _create_option_datasets_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_datasets (
            underlying   TEXT    NOT NULL,
            bar_size     TEXT    NOT NULL,
            source       TEXT    NOT NULL,
            contract_count INTEGER NOT NULL,
            bar_count    INTEGER NOT NULL,
            min_date     TEXT,
            max_date     TEXT,
            last_updated TEXT    NOT NULL,
            PRIMARY KEY (underlying, bar_size, source)
        )
    """)


def _create_option_daily_targets_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_daily_targets (
            strategy_id      TEXT    NOT NULL,
            date             TEXT    NOT NULL,
            underlying       TEXT    NOT NULL,
            raw_symbol       TEXT    NOT NULL,
            option_type      TEXT    NOT NULL,
            strike           REAL    NOT NULL,
            expiration       TEXT    NOT NULL,
            dte              INTEGER NOT NULL,
            underlying_close REAL    NOT NULL,
            target_moneyness REAL    NOT NULL,
            source           TEXT    NOT NULL,
            PRIMARY KEY (strategy_id, date, source),
            FOREIGN KEY (raw_symbol) REFERENCES option_contracts(raw_symbol)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_option_daily_targets_symbol "
        "ON option_daily_targets(raw_symbol, date)"
    )


def upsert_option_contracts(df: pd.DataFrame, source: str = "databento") -> None:
    """Insert or update option contract metadata."""
    if df.empty:
        return

    rows = []
    for row in df.itertuples(index=False):
        rows.append((
            row.raw_symbol,
            row.underlying,
            int(row.instrument_id) if pd.notna(row.instrument_id) else None,
            row.option_type,
            float(row.strike),
            str(row.expiration),
            str(row.first_seen) if pd.notna(row.first_seen) else None,
            str(row.last_seen) if pd.notna(row.last_seen) else None,
            float(row.contract_multiplier) if pd.notna(row.contract_multiplier) else None,
            source,
        ))

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO option_contracts (
                raw_symbol, underlying, instrument_id, option_type, strike,
                expiration, first_seen, last_seen, contract_multiplier, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(raw_symbol) DO UPDATE SET
                underlying = excluded.underlying,
                instrument_id = excluded.instrument_id,
                option_type = excluded.option_type,
                strike = excluded.strike,
                expiration = excluded.expiration,
                first_seen = MIN(
                    COALESCE(option_contracts.first_seen, excluded.first_seen),
                    COALESCE(excluded.first_seen, option_contracts.first_seen)
                ),
                last_seen = MAX(
                    COALESCE(option_contracts.last_seen, excluded.last_seen),
                    COALESCE(excluded.last_seen, option_contracts.last_seen)
                ),
                contract_multiplier = excluded.contract_multiplier,
                source = excluded.source
            """,
            rows,
        )


def upsert_option_prices(
    df: pd.DataFrame,
    underlying: str,
    bar_size: str = "1d",
    source: str = "databento",
) -> None:
    """Insert or replace option OHLCV rows."""
    if df.empty:
        return

    insert_df = df.copy()
    insert_df["bar_size"] = bar_size
    insert_df["source"] = source
    insert_df = insert_df[[
        "raw_symbol", "date", "bar_size", "source", "open", "high", "low", "close", "volume"
    ]]

    with get_connection() as conn:
        insert_df.to_sql(
            "option_prices",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        _update_option_dataset_metadata(conn, underlying, bar_size, source)


def replace_option_prices(
    df: pd.DataFrame,
    raw_symbols: list[str],
    underlying: str,
    bar_size: str = "1d",
    source: str = "databento",
) -> None:
    """Replace stored rows for the supplied symbols, then insert fresh rows."""
    with get_connection() as conn:
        conn.executemany(
            "DELETE FROM option_prices WHERE raw_symbol = ? AND bar_size = ? AND source = ?",
            [(symbol, bar_size, source) for symbol in raw_symbols],
        )
        if not df.empty:
            insert_df = df.copy()
            insert_df["bar_size"] = bar_size
            insert_df["source"] = source
            insert_df = insert_df[[
                "raw_symbol", "date", "bar_size", "source", "open", "high", "low", "close", "volume"
            ]]
            insert_df.to_sql(
                "option_prices",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )
        _update_option_dataset_metadata(conn, underlying, bar_size, source)


def replace_option_daily_targets(
    df: pd.DataFrame,
    strategy_id: str,
    source: str = "databento",
) -> None:
    """Replace daily target-contract rows for a strategy."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM option_daily_targets WHERE strategy_id = ? AND source = ?",
            (strategy_id, source),
        )
        if df.empty:
            return
        insert_df = df.copy()
        insert_df["strategy_id"] = strategy_id
        insert_df["source"] = source
        insert_df = insert_df[[
            "strategy_id",
            "date",
            "underlying",
            "raw_symbol",
            "option_type",
            "strike",
            "expiration",
            "dte",
            "underlying_close",
            "target_moneyness",
            "source",
        ]]
        insert_df.to_sql(
            "option_daily_targets",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )


def replace_option_marks(
    df: pd.DataFrame,
    raw_symbols: list[str],
    source: str = "databento",
) -> None:
    """Replace daily quote marks for the supplied symbols."""
    with get_connection() as conn:
        conn.executemany(
            "DELETE FROM option_marks WHERE raw_symbol = ? AND source = ?",
            [(symbol, source) for symbol in raw_symbols],
        )
        if df.empty:
            return
        insert_df = df.copy()
        insert_df["source"] = source
        insert_df = insert_df[[
            "raw_symbol",
            "date",
            "source",
            "mark_time",
            "bid",
            "ask",
            "mid",
            "spread",
            "spread_pct",
            "bid_size",
            "ask_size",
        ]]
        insert_df.to_sql(
            "option_marks",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )


def _update_option_dataset_metadata(
    conn: sqlite3.Connection,
    underlying: str,
    bar_size: str,
    source: str,
) -> None:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT p.raw_symbol),
               COUNT(*),
               MIN(p.date),
               MAX(p.date)
        FROM option_prices p
        JOIN option_contracts c ON c.raw_symbol = p.raw_symbol
        WHERE c.underlying = ? AND p.bar_size = ? AND p.source = ?
        """,
        (underlying, bar_size, source),
    ).fetchone()

    contract_count, bar_count, min_date, max_date = row if row else (0, 0, None, None)
    conn.execute(
        """
        INSERT INTO option_datasets (
            underlying, bar_size, source, contract_count, bar_count,
            min_date, max_date, last_updated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(underlying, bar_size, source) DO UPDATE SET
            contract_count = excluded.contract_count,
            bar_count = excluded.bar_count,
            min_date = excluded.min_date,
            max_date = excluded.max_date,
            last_updated = excluded.last_updated
        """,
        (
            underlying,
            bar_size,
            source,
            int(contract_count or 0),
            int(bar_count or 0),
            min_date,
            max_date,
            datetime.utcnow().isoformat(),
        ),
    )


def load_option_prices(
    underlying: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    option_type: Optional[str] = None,
    min_expiration: Optional[str] = None,
    max_expiration: Optional[str] = None,
    bar_size: str = "1d",
    source: str = "databento",
) -> pd.DataFrame:
    """Load option bars joined with contract metadata."""
    query = """
        SELECT p.*, c.underlying, c.option_type, c.strike, c.expiration, c.contract_multiplier
        FROM option_prices p
        JOIN option_contracts c ON c.raw_symbol = p.raw_symbol
        WHERE c.underlying = ? AND p.bar_size = ? AND p.source = ?
    """
    params: list = [underlying.upper(), bar_size, source]
    if start:
        query += " AND p.date >= ?"
        params.append(start)
    if end:
        query += " AND p.date <= ?"
        params.append(end)
    if option_type:
        query += " AND c.option_type = ?"
        params.append(option_type.upper())
    if min_expiration:
        query += " AND c.expiration >= ?"
        params.append(min_expiration)
    if max_expiration:
        query += " AND c.expiration <= ?"
        params.append(max_expiration)
    query += " ORDER BY p.date ASC, c.expiration ASC, c.strike ASC, c.option_type ASC"

    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params, parse_dates=["date", "expiration"])


def load_option_daily_targets(
    strategy_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    source: str = "databento",
) -> pd.DataFrame:
    """Load daily selected option contracts for a strategy."""
    query = """
        SELECT t.*,
               p.open, p.high, p.low, p.close, p.volume,
               m.mark_time, m.bid, m.ask, m.mid, m.spread, m.spread_pct
        FROM option_daily_targets t
        LEFT JOIN option_prices p
          ON p.raw_symbol = t.raw_symbol
         AND p.date = t.date
         AND p.source = t.source
         AND p.bar_size = '1d'
        LEFT JOIN option_marks m
          ON m.raw_symbol = t.raw_symbol
         AND m.date = t.date
         AND m.source = t.source
        WHERE t.strategy_id = ? AND t.source = ?
    """
    params: list = [strategy_id, source]
    if start:
        query += " AND t.date >= ?"
        params.append(start)
    if end:
        query += " AND t.date <= ?"
        params.append(end)
    query += " ORDER BY t.date ASC"

    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params, parse_dates=["date", "expiration"])
