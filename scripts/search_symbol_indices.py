#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging

from src.search.db_reader import (
    fetch_candidates,
    fetch_indices_for_symbol,
    fetch_names_for_symbols,
)
from src.search.search_db import rank_symbols, SymbolHit


def _setup_log(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Search symbol and list indices from DuckDB."
    )
    p.add_argument(
        "--db", required=True, help="Path to DuckDB (e.g., data/universe.duckdb)"
    )
    p.add_argument("--q", "--query", dest="query", required=True, help="Symbol query")
    p.add_argument(
        "--table", default="equities", help="Table with symbols (default: equities)"
    )
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--min-score", type=float, default=0.65)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def _as_dict(h: SymbolHit) -> dict:
    return {
        "symbol": h.symbol,
        "name": h.name,  # <-- include in output
        "indices": h.indices,
        "score": round(h.score, 3),
        "reason": h.reason,
    }


def main() -> int:
    args = _args()
    _setup_log(args.log_level)
    log = logging.getLogger("search_symbol_indices")

    # 1) candidates
    df = fetch_candidates(db_path=args.db, query=args.query, table=args.table)
    symbols = df["symbol"].dropna().astype(str).tolist()
    log.info("candidates | count=%d", len(symbols))
    log.debug("candidates_sample=%s", symbols[:5])

    # 2) rank
    hits = rank_symbols(
        symbols, query=args.query, min_score=args.min_score, top_k=args.top_k
    )
    log.info(
        "ranked | hits=%d top_k=%d min_score=%s", len(hits), args.top_k, args.min_score
    )
    log.debug("ranked_sample=%s", [h.symbol for h in hits[:5]])

    # 2b) names (batch)
    hit_symbols = [h.symbol for h in hits]
    names = fetch_names_for_symbols(args.db, hit_symbols)
    have_names = sum(1 for s in hit_symbols if names.get(s))
    log.info("names | found=%d/%d", have_names, len(hit_symbols))
    log.debug("names_missing=%s", [s for s in hit_symbols if not names.get(s)][:10])

    # 3) indices + build output
    enriched = []
    for h in hits:
        indices = fetch_indices_for_symbol(args.db, h.symbol)
        enriched.append(
            SymbolHit(
                symbol=h.symbol,
                indices=indices,
                score=h.score,
                reason=h.reason,
                name=names.get(
                    h.symbol, ""
                ),  # <- ensure SymbolHit has `name: str = ""`
            )
        )

    # optional peek at first enriched row
    if enriched:
        log.debug("enriched_sample=%s", _as_dict(enriched[0]))

    print(json.dumps([_as_dict(x) for x in enriched], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
