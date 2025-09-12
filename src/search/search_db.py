from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import List, Tuple

# Optional fast fuzzy
try:
    from rapidfuzz import fuzz  # type: ignore

    def _ratio(a: str, b: str) -> float:
        return fuzz.token_set_ratio(a, b) / 100.0

except Exception:  # pragma: no cover
    from difflib import SequenceMatcher

    def _ratio(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymbolHit:
    symbol: str
    indices: List[str]
    score: float
    reason: str  # exact | prefix | fuzzy
    name: str = ""  # <-- new, default empty


def _prep(s: str) -> str:
    return (s or "").strip().lower()


def _classify(query: str, symbol: str) -> Tuple[str, float]:
    q, sym = _prep(query), _prep(symbol)
    if q == sym:
        return "exact", 1.0
    if sym.startswith(q):
        return "prefix", 0.92
    return "fuzzy", _ratio(q, sym)


def rank_symbols(
    symbols: List[str],
    query: str,
    min_score: float = 0.65,
    top_k: int = 10,
) -> List[SymbolHit]:
    hits: List[SymbolHit] = []
    for sym in symbols:
        reason, score = _classify(query, sym)
        if score >= min_score:
            hits.append(SymbolHit(symbol=sym, indices=[], score=score, reason=reason))
    hits.sort(
        key=lambda r: (r.score, r.reason == "exact", r.reason == "prefix"), reverse=True
    )
    return hits[:top_k]
