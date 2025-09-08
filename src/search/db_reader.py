from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Tuple
from dataclasses import dataclass

import duckdb
import pandas as pd

log = logging.getLogger(__name__)


# --- add this helper at top-level ---
def _resolve_table(con: duckdb.DuckDBPyConnection, preferred: str) -> str:
    """
    Return a valid table/view name.
    Priority: preferred → 'equities' → error.
    """
    rows = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    names = {r[0] for r in rows}
    if preferred in names:
        return preferred
    if "equities" in names:
        return "equities"
    raise duckdb.CatalogException(
        f"Neither '{preferred}' nor 'equities' exist. Available: {sorted(list(names))[:20]}..."
    )


@dataclass(frozen=True)
class EquityRow:
    symbol: str
    name: str
    index: str
    isin: Optional[str] = None
    sector: Optional[str] = None


# --- replace _conn() ---
def _conn(db_path: str) -> duckdb.DuckDBPyConnection:
    # Open read-write so new DB paths work too.
    # (DuckDB creates the file on first connect.)
    return duckdb.connect(database=db_path, read_only=False)


# --- update fetch_candidates() to use resolver ---
def fetch_candidates(
    db_path: str,
    query: str,
    table: str = "equities",  # ← default to your real table
    limit: int = 500,
) -> pd.DataFrame:
    """
    Pull candidates from DuckDB by symbol only (exact + prefix).
    Your DB has no 'name' column, so we don't reference it.
    """
    q = (query or "").strip()
    if not q:
        return pd.DataFrame(columns=["symbol"])

    sql = f"""
      SELECT symbol
      FROM {table}
      WHERE UPPER(symbol) = UPPER(?)
         OR UPPER(symbol) LIKE UPPER(?)
      LIMIT {limit}
    """
    params = [q, f"{q}%"]
    with _conn(db_path) as con:
        df = con.execute(sql, params).df()
    log.info(
        "db_candidates_fetched", extra={"query": q, "rows": len(df), "table": table}
    )
    return df


def fetch_indices_for_symbol(
    db_path: str,
    symbol: str,
    table: str = "equity_membership",
) -> List[str]:
    """
    Return distinct indices for a given symbol (exact match, case-insensitive).
    """
    sql = f"""
      SELECT DISTINCT index
      FROM {table}
      WHERE UPPER(symbol) = UPPER(?)
      ORDER BY index
    """
    with _conn(db_path) as con:
        rows = con.execute(sql, [symbol]).fetchall()
    out = [r[0] for r in rows]
    log.info("db_symbol_indices", extra={"symbol": symbol, "count": len(out)})
    return out
