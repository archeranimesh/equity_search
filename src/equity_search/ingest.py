"""
CSV → clean ticker list (strict; header-robust minimal).
- Merge a split first header line like 'SYMBOL \\n,OPEN \\n'
- Require a column that starts with 'SYMBOL'
"""

from __future__ import annotations
from pathlib import Path
import io
import re
import pandas as pd

_TICKER_RE = re.compile(r"^[A-Z0-9&.\-]+$")


def _merge_split_header_lines(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text
    i = next((k for k, ln in enumerate(lines) if ln.strip() != ""), None)
    if i is None or i + 1 >= len(lines):
        return text
    first, second = lines[i], lines[i + 1]
    if second.lstrip().startswith(","):
        merged = first + second
        return "\n".join(lines[:i] + [merged] + lines[i + 2 :])
    return text


def _read_csv(text: str) -> pd.DataFrame:
    fixed = _merge_split_header_lines(text)
    return pd.read_csv(
        io.StringIO(fixed), dtype=str, engine="python", on_bad_lines="skip"
    )


def _find_symbol_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).strip().upper().startswith("SYMBOL"):
            return c
    # exact message your ingest tests look for
    raise ValueError("No SYMBOL column found.")


def _filter_symbols(series: pd.Series) -> pd.DataFrame:
    s = series.astype(str).str.strip().str.upper()
    mask = s.str.match(_TICKER_RE, na=False)
    return s[mask].drop_duplicates().to_frame(name="symbol").reset_index(drop=True)


def load_and_clean_symbols(csv_path: str | Path) -> pd.DataFrame:
    text = Path(csv_path).read_text(encoding="utf-8", errors="replace")
    df = _read_csv(text).rename(columns=lambda c: str(c).strip())
    sym_col = _find_symbol_column(df)
    return _filter_symbols(df[sym_col])
