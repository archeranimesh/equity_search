#!/usr/bin/env python3
"""
Robust CSV peek: tries multiple parsers/encodings and prints first rows/columns.
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import io
import csv


def _try_read_csv(path: Path) -> pd.DataFrame:
    """Attempt several robust read_csv configs; raise last error if all fail."""
    attempts = [
        dict(engine="c", dtype=str, on_bad_lines="skip"),
        dict(engine="python", dtype=str, on_bad_lines="skip"),
        dict(engine="python", dtype=str, encoding="utf-8-sig", on_bad_lines="skip"),
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
            return pd.read_csv(path, **opts)
        except Exception as e:
            last_err = e
    # Final fallback: sanitize raw text then parse
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        text = text.replace("\r", "\n").replace("\x00", "")
        return pd.read_csv(
            io.StringIO(text), engine="python", dtype=str, on_bad_lines="skip"
        )
    except Exception as e:
        raise last_err or e


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--nrows", type=int, default=5, help="Rows to show")
    p.add_argument("--ncols", type=int, default=10, help="Columns to show")
    args = p.parse_args()

    path = Path(args.csv)
    df = _try_read_csv(path)

    # Slice rows and columns for preview
    preview = df.iloc[: args.nrows, : args.ncols]

    print(
        "\n=== Columns (first {} of {} total) ===".format(
            min(args.ncols, len(df.columns)), len(df.columns)
        )
    )
    for i, c in enumerate(df.columns[: args.ncols], 1):
        print(f"{i}. {repr(c)}")

    print(
        "\n=== Preview ({} rows x {} cols) ===".format(len(preview), preview.shape[1])
    )
    # Ensure string display without truncation of small preview
    with pd.option_context("display.max_colwidth", 200, "display.width", 200):
        print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
