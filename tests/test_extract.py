from __future__ import annotations
from pathlib import Path
import pandas as pd
import textwrap

from equity_search.extract import extract_symbols


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_extract_symbols_happy_path(tmp_path: Path) -> None:
    # Messy headers with newlines; prices in 2nd col
    csv = """SYMBOL \n,OPEN \n
NIFTY 50,24802.60
TCS,000
RELIANCE,000
tata-motors ,000
"""
    p = tmp_path / "nifty50.csv"
    _write(p, csv)

    df = extract_symbols(p)

    assert list(df.columns) == ["symbol"]
    assert set(df["symbol"]) == {"TCS", "RELIANCE", "TATA-MOTORS"}


def test_extract_symbols_dedup_and_normalization(tmp_path: Path) -> None:
    csv = """SYMBOL,OPEN
tcs,1
TCS,2
 TCS ,3
RELIANCE,1
"""
    p = tmp_path / "dupes.csv"
    _write(p, csv)
    df = extract_symbols(p)
    assert set(df["symbol"]) == {"TCS", "RELIANCE"}  # dedup + uppercase + trim


def test_extract_symbols_rejects_missing_symbol_header(tmp_path: Path) -> None:
    csv = "NAME,OPEN\nFoo,1\nBar,2\n"
    p = tmp_path / "bad.csv"
    _write(p, csv)
    try:
        extract_symbols(p)
        assert False, "Expected ValueError when 'SYMBOL' column missing"
    except ValueError as e:
        assert "No column starting with 'SYMBOL'" in str(e)


def test_extract_symbols_handles_utf8_bom(tmp_path: Path) -> None:
    # Write file with BOM + normal header
    p = tmp_path / "bom.csv"
    p.write_text("\ufeffSYMBOL,OPEN\nTCS,1\n", encoding="utf-8")
    df = extract_symbols(p)
    assert list(df["symbol"]) == ["TCS"]


def test_extract_symbols_html_table_fallback(tmp_path: Path) -> None:
    # Some NSE “CSV” downloads are actually HTML tables
    html = textwrap.dedent(
        """
        <html><body>
        <table>
          <thead><tr><th>SYMBOL</th><th>OPEN</th></tr></thead>
          <tbody>
            <tr><td>INFY</td><td>1</td></tr>
            <tr><td>ITC</td><td>2</td></tr>
          </tbody>
        </table>
        </body></html>
        """
    )
    p = tmp_path / "table.html"
    _write(p, html)
    df = extract_symbols(p)
    assert set(df["symbol"]) == {"INFY", "ITC"}


def test_extract_symbols_filters_index_like_rows(tmp_path: Path) -> None:
    # Rows with spaces (e.g., 'NIFTY 50') must be excluded
    csv = """SYMBOL,OPEN
NIFTY 50,111
JSWSTEEL,1
HDFC-BANK,1
M&M,1
"""
    p = tmp_path / "index_like.csv"
    _write(p, csv)
    df = extract_symbols(p)
    assert set(df["symbol"]) == {"JSWSTEEL", "HDFC-BANK", "M&M"}
    assert "NIFTY 50" not in set(df["symbol"])


def test_extract_symbols_ignores_numeric_garbage_in_other_columns(
    tmp_path: Path,
) -> None:
    # Ensure numeric-looking values from other columns don't leak in
    csv = """SYMBOL,OPEN
TCS,000
RELIANCE,24802.60
"""
    p = tmp_path / "nums.csv"
    _write(p, csv)
    df = extract_symbols(p)
    assert set(df["symbol"]) == {"TCS", "RELIANCE"}
