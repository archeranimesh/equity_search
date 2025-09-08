#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import List, Optional

from src.search.loader import load_equities
from src.search.fuzzy_search import search_equities, SearchResult


def setup_log(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fuzzy search across NIFTY indices.")
    p.add_argument(
        "--sources",
        nargs="+",
        required=True,
        help="Files or directories (CSV/Parquet). Example: data/indices/ NIFTY50.csv",
    )
    p.add_argument(
        "--q",
        "--query",
        dest="query",
        required=True,
        help="Query text (symbol or name)",
    )
    p.add_argument(
        "--indices", nargs="*", help="Restrict to indices: NIFTY50 NIFTY100 ..."
    )
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--min-score", type=float, default=0.65)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def as_dict(r: SearchResult) -> dict:
    return {
        "symbol": r.symbol,
        "name": r.name,
        "index": r.index,
        "score": round(r.score, 3),
        "reason": r.reason,
    }


def main() -> int:
    args = parse_args()
    setup_log(args.log_level)

    sources = [Path(s) for s in args.sources]
    df = load_equities(sources)

    results = search_equities(
        df=df,
        query=args.query,
        indices=args.indices,
        top_k=args.top_k,
        min_score=args.min_score,
    )

    print(json.dumps([as_dict(r) for r in results], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
