from src.search.search_db import rank_symbols


def test_rank_symbols_simple():
    symbols = ["RELIANCE", "INFY", "HDFCBANK"]
    hits = rank_symbols(symbols, query="INFY")
    assert hits and hits[0].symbol == "INFY"
    assert hits[0].reason in {"exact", "prefix"}
