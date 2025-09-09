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
import pandas as pd


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
        "--index",
        help="Index label for membership (e.g., NIFTY50, NIFTYNEXT50). If omitted, inferred from CSV filename.",
    )

    p.add_argument(
        "--log", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    # add to your existing args
    p.add_argument(
        "--debug-missing",
        action="store_true",
        help="Log sample of symbols missing membership before/after insert",
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


def _count_index(con, index_label: str) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM equity_membership WHERE index = ?;",
        [index_label],
    ).fetchone()[0]


def _sync_membership(con, symbols, index_label: str) -> tuple[int, int]:
    """Insert (symbol, index_label) for provided symbols only (idempotent). Returns (inserted_now, total_for_index)."""
    before = _count_index(con, index_label)
    import pandas as _pd  # local to avoid top-level import issues if already present

    df = _pd.DataFrame({"symbol": [s.strip().upper() for s in symbols if s]})
    con.register("tmp_syms", df)
    con.execute(
        """
        INSERT INTO equity_membership(symbol, index)
        SELECT t.symbol, ?
        FROM tmp_syms t
        LEFT JOIN equity_membership m
          ON m.symbol = t.symbol AND m.index = ?
        WHERE m.symbol IS NULL;
        """,
        [index_label, index_label],
    )
    con.unregister("tmp_syms")
    after = _count_index(con, index_label)
    inserted_now = after - before
    return inserted_now, after


import pandas as pd  # make sure this import exists at top of your file


def _count_index(con, index_label: str) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM equity_membership WHERE index = ?;",
        [index_label],
    ).fetchone()[0]


def _debug_missing_membership(
    con, symbols, index_label: str, limit: int = 20
) -> tuple[int, list[str]]:
    """
    Return (missing_count, sample_list) for the given symbols not present in equity_membership for index_label.
    """
    df = pd.DataFrame({"symbol": [str(s).strip().upper() for s in symbols if s]})
    if df.empty:
        return 0, []
    con.register("tmp_syms_dbg", df)
    res = con.execute(
        """
        SELECT t.symbol
        FROM tmp_syms_dbg t
        LEFT JOIN equity_membership m
          ON m.symbol = t.symbol AND m.index = ?
        WHERE m.symbol IS NULL
        ORDER BY t.symbol
        LIMIT ?;
    """,
        [index_label, limit],
    ).fetchall()
    con.unregister("tmp_syms_dbg")

    # Count missing (do a COUNT(*) with the same anti-join)
    con.register("tmp_syms_dbg2", df)
    missing_count = con.execute(
        """
        SELECT COUNT(*)
        FROM tmp_syms_dbg2 t
        LEFT JOIN equity_membership m
          ON m.symbol = t.symbol AND m.index = ?
        WHERE m.symbol IS NULL;
    """,
        [index_label],
    ).fetchone()[0]
    con.unregister("tmp_syms_dbg2")

    sample = [r[0] for r in res]
    return missing_count, sample


def _sync_membership(
    con, symbols, index_label: str, log, debug_missing: bool
) -> tuple[int, int]:
    """
    Insert (symbol, index_label) for symbols from the current CSV that are not already present.
    Returns (inserted_now, total_rows_for_index).
    """
    # BEFORE: what’s missing?
    missing_before, sample_before = _debug_missing_membership(
        con, symbols, index_label, limit=10
    )
    log.debug(
        "membership(%s) missing_before=%d sample=%s",
        index_label,
        missing_before,
        sample_before,
    )

    before_count = _count_index(con, index_label)

    # Insert idempotently
    df = pd.DataFrame({"symbol": [str(s).strip().upper() for s in symbols if s]})
    con.register("tmp_syms", df)
    con.execute(
        """
        INSERT INTO equity_membership(symbol, index)
        SELECT t.symbol, ?
        FROM tmp_syms t
        LEFT JOIN equity_membership m
          ON m.symbol = t.symbol AND m.index = ?
        WHERE m.symbol IS NULL;
    """,
        [index_label, index_label],
    )
    con.unregister("tmp_syms")

    after_count = _count_index(con, index_label)
    inserted_now = after_count - before_count

    # AFTER: did anything remain missing?
    missing_after, sample_after = _debug_missing_membership(
        con, symbols, index_label, limit=10
    )
    log.debug(
        "membership(%s) missing_after=%d sample=%s",
        index_label,
        missing_after,
        sample_after,
    )

    if debug_missing and missing_after > 0:
        log.warning(
            "Some symbols still missing membership for %s (showing up to 10): %s",
            index_label,
            sample_after,
        )

    return inserted_now, after_count


def _sanitize_symbols(raw: list) -> list[str]:
    """Normalize CSV values into uppercase symbols and drop header noise like 'SYMBOL'."""
    out = []
    for x in raw:
        s = str(x).strip().upper()
        if not s:
            continue
        if s == "SYMBOL":  # header sneaking in
            continue
        out.append(s)
    # de-dupe preserving order
    seen = set()
    deduped = []
    for s in out:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


import pandas as pd  # ensure this is imported at top


def _sanitize_to_df(ob) -> pd.DataFrame:
    """
    Convert whatever extract_symbols() returned into a DataFrame with a single
    uppercase 'symbol' column, dropping header noise like 'SYMBOL', blanks, and dups.
    """
    if isinstance(ob, pd.DataFrame):
        df = ob.copy()
    else:
        # try to coerce sequences/dicts to DataFrame
        try:
            df = pd.DataFrame(ob)
        except Exception:
            df = pd.DataFrame({"symbol": list(ob)})

    # normalize header names
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "symbol" not in df.columns:
        for alt in ("ticker", "security_id", "securityid", "SYMBOL", "Symbol"):
            if alt.lower() in df.columns:
                df = df.rename(columns={alt.lower(): "symbol"})
                break

    if "symbol" not in df.columns:
        raise ValueError("No 'symbol' column found in extracted CSV data")

    # clean values
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df = df[df["symbol"].ne("")]  # drop blanks
    df = df[df["symbol"] != "SYMBOL"]  # drop header noise
    df = df.drop_duplicates(subset=["symbol"])
    return df[["symbol"]]


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
    index_label = (
        (getattr(args, "index", None) or csv_path.stem)
        .upper()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
    )
    symbols_df = _sanitize_to_df(symbols)
    log.debug(
        "sanitized_symbols=%d | sample=%s",
        len(symbols_df),
        symbols_df["symbol"].head(5).tolist(),
    )
    if symbols_df.empty:
        log.warning(
            "No valid symbols parsed from %s (check CSV header/format).", csv_path
        )
        return 1
    # log a quick sample of CSV symbols
    sample_syms = [str(s).strip().upper() for s in symbols[:5]]
    log.debug(
        "index_label=%s | csv_symbols=%d | sample=%s",
        index_label,
        len(symbols),
        sample_syms,
    )

    con = connect(db_path)

    # ensure membership table exists (you may already have this)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS equity_membership(
        symbol TEXT NOT NULL,
        index  TEXT NOT NULL,
        PRIMARY KEY(symbol, index)
        );
    """
    )

    ensure_schema(con)

    before = len(fetch_all_symbols(con))
    total = upsert_symbols(con, symbols_df)
    inserted = total - before

    # --- NEW: sync membership → NIFTY50
    _ensure_membership_table(con)
    # do the sync with rich logs
    inserted_now, total_for_index = _sync_membership(
        con,
        symbols_df["symbol"].tolist(),  # <<< list of symbols
        index_label,
        log,
        debug_missing=getattr(args, "debug_missing", False),
    )

    log.info("Ingest complete")
    log.info("   source:        %s", csv_path)
    log.info("   db:            %s", db_path)
    log.info("   found:         %d symbols in source", len(symbols))
    log.info("   before:        %d rows in equities", before)
    log.info("   inserted:      %d new rows in equities", inserted)
    log.info("   total:         %d rows in equities now", total)
    # proper logging (f-string or placeholders; don't print "{index_label}")
    log.info("   %s add:   %d new membership rows", index_label, inserted_now)
    log.info("   %s total: %d membership rows now", index_label, total_for_index)


if __name__ == "__main__":
    main()
