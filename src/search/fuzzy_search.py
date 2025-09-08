from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

# Optional: try rapidfuzz for speed/quality; fallback to difflib
try:
    from rapidfuzz import fuzz  # type: ignore

    def _ratio(a: str, b: str) -> float:
        return fuzz.token_set_ratio(a, b) / 100.0

except Exception:  # pragma: no cover
    from difflib import SequenceMatcher

    def _ratio(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()


@dataclass(frozen=True)
class SearchResult:
    symbol: str
    name: str
    index: str
    score: float  # 0.0–1.0
    reason: str  # "exact", "prefix", "fuzzy"


def _prep(text: str) -> str:
    return (text or "").strip().lower()


def _exact_boost(q: str, symbol: str, name: str) -> Optional[Tuple[float, str]]:
    qn = _prep(q)
    if qn == _prep(symbol) or qn == _prep(name):
        return 1.0, "exact"
    return None


def _prefix_boost(q: str, symbol: str, name: str) -> Optional[Tuple[float, str]]:
    qn = _prep(q)
    if _prep(symbol).startswith(qn) or _prep(name).startswith(qn):
        # Strong but below perfect to let exact win ties
        return 0.92, "prefix"
    return None


def _fuzzy_score(q: str, symbol: str, name: str) -> Tuple[float, str]:
    s1 = _ratio(_prep(q), _prep(symbol))
    s2 = _ratio(_prep(q), _prep(name))
    return max(s1, s2), "fuzzy"


def _filter_indices(df: pd.DataFrame, indices: Optional[Iterable[str]]) -> pd.DataFrame:
    if not indices:
        return df
    wanted = {i.strip().upper() for i in indices}
    return df[df["index"].str.upper().isin(wanted)]


def search_equities(
    df: pd.DataFrame,
    query: str,
    indices: Optional[Iterable[str]] = None,
    top_k: int = 10,
    min_score: float = 0.65,
) -> List[SearchResult]:
    """
    Hybrid search: exact > prefix > fuzzy. Returns top_k results above min_score.
    """
    if not query or not query.strip():
        return []

    pool = _filter_indices(df, indices)

    rows: List[SearchResult] = []
    for row in pool.itertuples(index=False):
        symbol, name, index = row.symbol, row.name, row.index

        boost = _exact_boost(query, symbol, name) or _prefix_boost(query, symbol, name)
        if boost:
            score, reason = boost
        else:
            score, reason = _fuzzy_score(query, symbol, name)

        if score >= min_score:
            rows.append(
                SearchResult(
                    symbol=symbol, name=name, index=index, score=score, reason=reason
                )
            )

    rows.sort(
        key=lambda r: (r.score, r.reason == "exact", r.reason == "prefix"), reverse=True
    )
    out = rows[:top_k]
    log.info(
        "search_done",
        extra={
            "query": query,
            "indices": list(indices) if indices else None,
            "returned": len(out),
        },
    )
    return out
