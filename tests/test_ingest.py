import pandas as pd
from pathlib import Path
from equity_search.ingest import load_and_clean_symbols


def _write_csv(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_and_clean_symbols_happy_path(tmp_path: Path) -> None:
    # messy headers + an index-like row
    csv = """SYMBOL \n,OPEN \n
NIFTY 50,24802.60
TCS,000
RELIANCE,000
tata-motors ,000
"""
    p = tmp_path / "nifty50.csv"
    _write_csv(p, csv)

    df = load_and_clean_symbols(p)

    assert list(df.columns) == ["symbol"]
    # NIFTY 50 should be dropped; others uppercased/trimmed
    assert set(df["symbol"]) == {"TCS", "RELIANCE", "TATA-MOTORS"}


def test_load_and_clean_symbols_no_symbol_column(tmp_path: Path) -> None:
    csv = """NAME,OPEN
Foo,1
Bar,2
"""
    p = tmp_path / "bad.csv"
    _write_csv(p, csv)

    try:
        load_and_clean_symbols(p)
        assert False, "Expected ValueError for missing SYMBOL column"
    except ValueError as e:
        assert "No SYMBOL column" in str(e)
