"""
Robust symbol extractor (CSV/HTML; handles split headers like 'SYMBOL \\n,OPEN \\n').
Returns a DataFrame with one column: ['symbol'] (uppercase, deduped, no spaces).
"""

from __future__ import annotations
from pathlib import Path
import io
import re
import pandas as pd
import csv  # add this near the top with other imports


_TICKER_RE = re.compile(r"^[A-Z0-9&.\-]+$")


# ------------------ helpers ------------------


def _is_html(text: str) -> bool:
    t = text.lower()
    return "<table" in t and "</table>" in t


def _parse_html_table_naive(text: str) -> pd.DataFrame:
    """Very small HTML table parser (no lxml dependency)."""
    m = re.search(r"<table.*?>(.*?)</table>", text, flags=re.I | re.S)
    if not m:
        raise ValueError("No table found")
    table = m.group(1)
    rows = re.findall(r"<tr.*?>(.*?)</tr>", table, flags=re.I | re.S)
    if not rows:
        raise ValueError("No rows in table")

    def _cells(x: str, tag: str) -> list[str]:
        cs = re.findall(rf"<{tag}.*?>(.*?)</{tag}>", x, flags=re.I | re.S)
        out = []
        for c in cs:
            c = re.sub(r"<.*?>", "", c)
            c = re.sub(r"\s+", " ", c).strip()
            out.append(c)
        return out

    header = _cells(rows[0], "th") or _cells(rows[0], "td")
    if not header:
        raise ValueError("HTML table has no header")
    header = [h.strip() for h in header]

    data = []
    for r in rows[1:]:
        tds = _cells(r, "td")
        if not tds:
            continue
        if len(tds) < len(header):
            tds += [""] * (len(header) - len(tds))
        elif len(tds) > len(header):
            tds = tds[: len(header)]
        data.append(tds)

    return pd.DataFrame(data, columns=header).astype(str)


def _merge_split_header_lines(text: str) -> str:
    """
    Merge a header that's split across lines, e.g.:
      'SYMBOL \\n,OPEN \\n'  ->  'SYMBOL ,OPEN '
    We only touch the very first two non-empty lines if the 2nd starts with ','.
    """
    lines = text.splitlines()
    if not lines:
        return text
    # find first non-empty line
    i = next((k for k, ln in enumerate(lines) if ln.strip() != ""), None)
    if i is None or i + 1 >= len(lines):
        return text
    first, second = lines[i], lines[i + 1]
    if second.lstrip().startswith(","):
        merged = first + second  # keep comma; remove the newline split
        return "\n".join(lines[:i] + [merged] + lines[i + 2 :])
    return text


def _read_csv_with_fixed_header(text: str) -> pd.DataFrame:
    """
    Read CSV from text after fixing split header lines.
    Tries several parser configurations to survive BOM/quoting edge cases.
    """
    fixed = _merge_split_header_lines(text)
    fixed = fixed.replace("\x00", "").replace("\r", "\n").lstrip("\ufeff")

    attempts = [
        dict(engine="python", dtype=str, on_bad_lines="skip"),
        dict(engine="python", dtype=str, sep=",", quotechar='"', on_bad_lines="skip"),
        dict(
            engine="python",
            dtype=str,
            sep=",",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            on_bad_lines="skip",
        ),
    ]

    last_err = None
    for opts in attempts:
        try:
            return pd.read_csv(io.StringIO(fixed), **opts)
        except Exception as e:
            last_err = e

    # Final fallback: relax quote handling further by stripping stray quotes
    fallback = fixed.replace('"\n', "\n").replace('",\n', ",\n")
    return pd.read_csv(
        io.StringIO(fallback), engine="python", dtype=str, on_bad_lines="skip"
    )


def _pick_symbol_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).strip().upper().startswith("SYMBOL"):
            return c
    # exact message expected by your tests
    raise ValueError("No column starting with 'SYMBOL'")


def _clean_symbols(series: pd.Series) -> pd.DataFrame:
    s = series.astype(str).str.strip().str.upper()
    mask = s.str.match(_TICKER_RE, na=False)
    return s[mask].drop_duplicates().to_frame(name="symbol").reset_index(drop=True)


# ------------------ public API ------------------


def extract_symbols(csv_path: str | Path) -> pd.DataFrame:
    """
    Flow:
      - Read text; if HTML table -> parse naively (no lxml).
      - Else, merge split header lines if present, then pandas-read the CSV.
      - Pick SYMBOL* column; return ticker-like values only.
    """
    path = Path(csv_path)
    text = path.read_text(encoding="utf-8", errors="replace")

    if _is_html(text):
        df = _parse_html_table_naive(text)
        df = df.rename(columns=lambda c: str(c).strip())
    else:
        df = _read_csv_with_fixed_header(text)
        df = df.rename(columns=lambda c: str(c).strip())

    sym_col = _pick_symbol_col(df)
    return _clean_symbols(df[sym_col])
