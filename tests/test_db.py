from __future__ import annotations
from pathlib import Path
import pandas as pd

from equity_search.extract import extract_symbols
from equity_search.db import connect, ensure_schema, upsert_symbols, fetch_all_symbols


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_db_upsert_and_idempotency(tmp_path: Path) -> None:
    # Sample CSV matching Stage-1 cases (messy header + mixed casing)
    csv = """SYMBOL \n,OPEN \n
NIFTY 50,24802.60
tcs,000
RELIANCE,000
tata-motors ,000
"""
    csv_path = tmp_path / "nifty50.csv"
    _write(csv_path, csv)

    # Extract symbols via Stage-1 module
    symbols = extract_symbols(csv_path)  # -> DataFrame with ['symbol']

    # Create DB, ensure schema, upsert
    db_path = tmp_path / "equity.duckdb"
    con = connect(db_path)
    ensure_schema(con)

    total_after_first = upsert_symbols(con, symbols)
    assert total_after_first == 3  # TCS, RELIANCE, TATA-MOTORS

    # Idempotent re-upsert
    total_after_second = upsert_symbols(con, symbols)
    assert total_after_second == 3

    # Exact contents
    got = set(fetch_all_symbols(con)["symbol"])
    assert got == {"TCS", "RELIANCE", "TATA-MOTORS"}


def test_db_rejects_missing_symbol_column(tmp_path: Path) -> None:
    db_path = tmp_path / "equity.duckdb"
    con = connect(db_path)
    ensure_schema(con)

    bad_df = pd.DataFrame({"ticker": ["TCS", "INFY"]})
    try:
        upsert_symbols(con, bad_df)
        assert False, "Expected ValueError for missing 'symbol' column"
    except ValueError as e:
        assert "symbol" in str(e).lower()
