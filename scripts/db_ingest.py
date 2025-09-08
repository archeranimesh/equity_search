#!/usr/bin/env python3
"""
Ingest symbols from a raw NSE CSV/HTML file into a DuckDB database,
and map all symbols to the NIFTY50 index in equity_membership.

Usage:
  python scripts/db_ingest.py --csv data/nifty50.csv --db data/universe.duckdb
  python scripts/db_ingest.py --csv data/nifty50.csv --db data/universe.duckdb --log DEBUG
"""

from __future__ import annotations
import argparse
import logging
from pathlib import Path
import sys

# Allow running without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from equity_search.extract import extract_symbols  # noqa: E402
from equity_search.db import (  # noqa: E402
    connect,
    ensure_schema,
    upsert_symbols,
    fetch_all_symbols,
)


# -------------------- logging & args -------------------- #
def _setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest tickers into DuckDB (+ NIFTY50 membership)"
    )
    p.add_argument(
        "--csv", required=True, help="Path to raw NSE CSV (or HTML masquerading as CSV)"
    )
    p.add_argument(
        "--db",
        default="data/universe.duckdb",
        help="DuckDB path (default: data/universe.duckdb)",
    )
    p.add_argument(
        "--log", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    return p.parse_args()


# -------------------- membership helpers -------------------- #
def _ensure_membership_table(con) -> None:
    """Create equity_membership if missing."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS equity_membership (
          symbol TEXT NOT NULL,
          index  TEXT NOT NULL,
          PRIMARY KEY(symbol, index)
        );
        """
    )


def _count_nifty50(con) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM equity_membership WHERE index='NIFTY50';"
    ).fetchone()[0]


def _sync_nifty50_membership(con) -> tuple[int, int]:
    """
    Insert (symbol,'NIFTY50') for all equities not already present.
    Returns (inserted_now, total_nifty50_rows).
    """
    before = _count_nifty50(con)

    # Use LEFT JOIN anti-pattern (portable) instead of ANTI JOIN / MERGE
    con.execute(
        """
        INSERT INTO equity_membership(symbol, index)
        SELECT e.symbol, 'NIFTY50'
        FROM equities e
        LEFT JOIN equity_membership m
          ON m.symbol = e.symbol AND m.index = 'NIFTY50'
        WHERE m.symbol IS NULL;
        """
    )

    after = _count_nifty50(con)
    inserted_now = after - before
    return inserted_now, after


# -------------------- main flow -------------------- #
def main() -> None:
    args = _parse_args()
    _setup_logging(args.log)
    log = logging.getLogger("equity_search.db_ingest")

    csv_path = Path(args.csv)
    db_path = Path(args.db)

    log.info("Extracting symbols from %s", csv_path)
    symbols = extract_symbols(csv_path)
    log.info("Extracted %d symbols", len(symbols))

    con = connect(db_path)
    ensure_schema(con)

    before = len(fetch_all_symbols(con))
    total = upsert_symbols(con, symbols)
    inserted = total - before

    # --- NEW: sync membership → NIFTY50
    _ensure_membership_table(con)
    inserted_now, total_nifty50 = _sync_nifty50_membership(con)

    log.info("Ingest complete")
    log.info("   source:        %s", csv_path)
    log.info("   db:            %s", db_path)
    log.info("   found:         %d symbols in source", len(symbols))
    log.info("   before:        %d rows in equities", before)
    log.info("   inserted:      %d new rows in equities", inserted)
    log.info("   total:         %d rows in equities now", total)
    log.info("   NIFTY50 add:   %d new membership rows", inserted_now)
    log.info("   NIFTY50 total: %d membership rows now", total_nifty50)


if __name__ == "__main__":
    main()
