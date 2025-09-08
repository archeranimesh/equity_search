import duckdb
import pandas as pd
from src.search.db_reader import fetch_candidates
from src.search.search_db import rank_symbols
from src.search.db_reader import fetch_indices_for_symbol


def test_symbol_to_indices_roundtrip(tmp_path):
    db = tmp_path / "u.duckdb"
    con = duckdb.connect(str(db), read_only=False)
    con.execute("CREATE TABLE equities(symbol TEXT PRIMARY KEY);")
    con.execute(
        "CREATE TABLE equity_membership(symbol TEXT, index TEXT, PRIMARY KEY(symbol, index));"
    )
    con.execute("INSERT INTO equities VALUES ('RELIANCE'), ('INFY');")
    con.execute("INSERT INTO equity_membership VALUES ('RELIANCE','NIFTY50');")

    # candidates
    df = fetch_candidates(str(db), query="RELI")
    assert list(df["symbol"]) == ["RELIANCE"]

    # ranking
    hits = rank_symbols(df["symbol"].tolist(), query="RELI")
    assert hits and hits[0].symbol == "RELIANCE"

    # membership
    idx = fetch_indices_for_symbol(str(db), "RELIANCE")
    assert idx == ["NIFTY50"]
