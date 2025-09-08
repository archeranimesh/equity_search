#!/usr/bin/env python3
"""
Ingest symbols from a raw NSE CSV/HTML file into a DuckDB database.

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
from equity_search.db import (
    connect,
    ensure_schema,
    upsert_symbols,
    fetch_all_symbols,
)  # noqa: E402


def _setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest tickers into DuckDB")
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

    log.info("Ingest complete")
    log.info("   source:   %s", csv_path)
    log.info("   db:       %s", db_path)
    log.info("   found:    %d symbols in source", len(symbols))
    log.info("   before:   %d rows in DB", before)
    log.info("   inserted: %d new rows", inserted)
    log.info("   total:    %d rows in DB now", total)


if __name__ == "__main__":
    main()
