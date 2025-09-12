#!/usr/bin/env python3
"""
Ingest NSE equity company names from a Dhan CSV by filtering INSTRUMENT == 'EQUITY'.

- Reads a local CSV (compact or detailed).
- Keeps only rows where INSTRUMENT (case-insensitive) equals 'EQUITY'.
- Derives symbol from DISPLAY/TRADING/SYMBOL fields and strips '-EQ/-BE/-BZ'.
- Upserts into DuckDB table: equity_names(symbol PRIMARY KEY, name, source, as_of TIMESTAMP).

Usage:
  python scripts/ingest_equity_names_from_csv.py \
    --db data/universe.duckdb \
    --csv data/api-scrip-master.csv \
    --log INFO
"""

from __future__ import annotations
import argparse
import logging
from typing import Optional, List, Tuple

import duckdb
import pandas as pd


# ---------- small helpers (≤15 lines each) ---------- #


def setup_log(level: str) -> None:
    """Configure root logger."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    """CLI args."""
    p = argparse.ArgumentParser(
        description="Ingest equity names from Dhan CSV (INSTRUMENT=EQUITY)"
    )
    p.add_argument(
        "--db", required=True, help="DuckDB path, e.g., data/universe.duckdb"
    )
    p.add_argument(
        "--csv", required=True, help="Path to Dhan CSV (compact or detailed)"
    )
    p.add_argument(
        "--log", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)"
    )
    p.add_argument(
        "--source",
        default="dhan-csv:INSTRUMENT=EQUITY",
        help="Source tag stored in equity_names.source",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="If INSTRUMENT column is missing, proceed without filter (NOT recommended)",
    )
    return p.parse_args()


def read_csv(path: str) -> pd.DataFrame:
    """Load CSV with normalized headers: UPPER_SNAKE_CASE."""
    df = pd.read_csv(path, low_memory=False)
    df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]
    return df


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Pick first existing column name from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def filter_equity(df: pd.DataFrame, force: bool) -> pd.DataFrame:
    """
    Keep only rows where the instrument column indicates 'EQUITY'.
    Supports SEM_INSTRUMENT_NAME (new) and INSTRUMENT / INSTRUMENT_TYPE (old).
    """
    col = pick_col(df, ["SEM_INSTRUMENT_NAME", "INSTRUMENT", "INSTRUMENT_TYPE"])
    if not col:
        if force:
            logging.warning(
                "No instrument column found; proceeding without filter (--force)."
            )
            return df
        raise ValueError(
            "CSV lacks SEM_INSTRUMENT_NAME/INSTRUMENT/INSTRUMENT_TYPE; use --force to bypass."
        )

    logging.debug("Using instrument column: %s", col)
    mask = df[col].astype(str).str.upper().eq("EQUITY")
    out = df[mask].copy()
    if out.empty and not force:
        raise ValueError(f"No rows with {col}='EQUITY'. Check the CSV or use --force.")
    return out


def strip_series_suffix(symbol: str) -> str:
    """Remove trailing '-EQ'/'-BE'/'-BZ' if present; uppercase result."""
    s = str(symbol).strip().upper()
    for suf in ("-EQ", "-BE", "-BZ"):
        if s.endswith(suf):
            return s[: -len(suf)]
    return s


def extract_pairs(df: pd.DataFrame) -> pd.DataFrame:
    disp_col = pick_col(
        df,
        [
            "DISPLAY_NAME",
            "TRADING_SYMBOL",
            "TRADING_SYMBOL_NAME",
            "SEM_TRADING_SYMBOL",
            "SEM_CUSTOM_SYMBOL",
            "SYMBOL",
            "SCRIP_SYMBOL",
        ],
    )
    name_col = pick_col(
        df,
        ["SYMBOL_NAME", "SECURITY_NAME", "COMPANY_NAME", "NAME", "SM_SYMBOL_NAME"],
    )
    if not disp_col or not name_col:
        raise ValueError(
            f"Could not find display/name columns. Present: {list(df.columns)[:15]}..."
        )

    out = pd.DataFrame(
        {
            "symbol": [strip_series_suffix(x) for x in df[disp_col]],
            "name": df[name_col].astype(str).str.strip(),
        }
    )

    # Series filter if available (supports SEM_SERIES too)
    series_col = pick_col(df, ["SERIES", "SEM_SERIES"])
    if series_col:
        series_mask = df[series_col].astype(str).str.upper().isin(["EQ", "BE", "BZ"])
        if len(series_mask) == len(out):
            out = out[series_mask.values]

    out = out.dropna(subset=["symbol", "name"])
    out = out[out["symbol"].ne("")]
    out = out.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    return out


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create equity_names table if absent."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS equity_names(
          symbol TEXT PRIMARY KEY,
          name   TEXT NOT NULL,
          source TEXT,
          as_of  TIMESTAMP
        );
    """
    )


def upsert_names(
    con: duckdb.DuckDBPyConnection, pairs: pd.DataFrame, source: str
) -> Tuple[int, int]:
    """
    Upsert symbol→name pairs.
    - Uses INSERT OR REPLACE (idempotent).
    - as_of is set with NOW() on the DB side.
    Returns (n_rows_written, total_rows_after).
    """
    if pairs.empty:
        total = con.execute("SELECT COUNT(*) FROM equity_names;").fetchone()[0]
        return 0, total

    con.register("tmp_names", pairs)
    con.execute(
        """
        INSERT OR REPLACE INTO equity_names(symbol, name, source, as_of)
        SELECT symbol, name, ?, NOW()
        FROM tmp_names;
    """,
        [source],
    )
    con.unregister("tmp_names")

    total = con.execute("SELECT COUNT(*) FROM equity_names;").fetchone()[0]
    return len(pairs), total


# ---------- main ---------- #


def main() -> int:
    args = parse_args()
    setup_log(args.log)
    log = logging.getLogger("ingest_equity_names_from_csv")

    log.info("Loading CSV: %s", args.csv)
    df = read_csv(args.csv)
    logging.debug("Columns: %s", list(df.columns)[:25])

    log.info("Filtering INSTRUMENT == 'EQUITY'")
    df_eq = filter_equity(df, force=args.force)
    log.info("Equity rows: %d (of %d)", len(df_eq), len(df))

    pairs = extract_pairs(df_eq)
    log.info(
        "Prepared %d symbol→name pairs (e.g., %s ...)",
        len(pairs),
        pairs.head(3).to_dict(orient="records"),
    )

    with duckdb.connect(args.db, read_only=False) as con:
        ensure_table(con)
        written, total = upsert_names(con, pairs, args.source)

    log.info(
        "Upsert complete | written_now=%d | total_in_equity_names=%d", written, total
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
