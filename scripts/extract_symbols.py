#!/usr/bin/env python3
"""
CLI wrapper for extract_symbols() with optional --log DEBUG.
"""
from __future__ import annotations
import argparse, logging
from pathlib import Path
import pandas as pd

from equity_search.extract import extract_symbols  # uses module logging


def _setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv", required=True, help="Path to raw NSE CSV (or HTML masquerading as CSV)"
    )
    ap.add_argument(
        "--out", default=None, help="Optional path to write cleaned symbols CSV"
    )
    ap.add_argument(
        "--log", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    args = ap.parse_args()

    _setup_logging(args.log)

    out = extract_symbols(Path(args.csv))
    print(f"✅ symbols extracted: {len(out)}")
    print(out.head(10).to_string(index=False))

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(args.out, index=False)
        print(f"💾 saved -> {args.out}")


if __name__ == "__main__":
    main()
