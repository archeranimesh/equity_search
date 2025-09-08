"""
DuckDB helpers for persisting equity symbols.

API:
- connect(db_path): open/create a DuckDB file
- ensure_schema(con): create table/index if absent
- upsert_symbols(con, df): idempotent insert of ['symbol'] DataFrame
- fetch_all_symbols(con): return all symbols (sorted)
"""

from __future__ import annotations
from pathlib import Path
from typing import Iterable
import duckdb
import pandas as pd


_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS equities (
    symbol TEXT PRIMARY KEY
);
"""
_INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_equities_symbol ON equities(symbol);"


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open (and create if missing) a DuckDB database file."""
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(p))


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create required table(s)/index(es) if they don't exist."""
    con.execute(_TABLE_SQL)
    con.execute(_INDEX_SQL)


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase, trim, and dedupe the 'symbol' column."""
    if "symbol" not in df.columns:
        raise ValueError("symbols DataFrame must have a 'symbol' column")
    out = (
        df.assign(symbol=df["symbol"].astype(str).str.strip().str.upper())
        .dropna(subset=["symbol"])
        .drop_duplicates(subset=["symbol"])
        .loc[:, ["symbol"]]
        .reset_index(drop=True)
    )
    return out


def upsert_symbols(con: duckdb.DuckDBPyConnection, symbols: pd.DataFrame) -> int:
    """
    Insert symbols idempotently using EXCEPT (portable across older DuckDB versions).
    Strategy: insert rows in tmp_symbols that are not already in equities.
    Returns total row count after upsert.
    """
    df = _normalize(symbols)
    con.register("tmp_symbols", df)

    con.execute(
        """
        INSERT INTO equities (symbol)
        SELECT symbol FROM tmp_symbols
        EXCEPT
        SELECT symbol FROM equities;
        """
    )

    con.unregister("tmp_symbols")
    return int(con.execute("SELECT COUNT(*) FROM equities").fetchone()[0])


def fetch_all_symbols(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all symbols sorted ascending."""
    return con.execute("SELECT symbol FROM equities ORDER BY symbol").df()
