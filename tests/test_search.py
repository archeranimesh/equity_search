from pathlib import Path
import pandas as pd

from src.search.fuzzy_search import search_equities
from src.search.loader import load_from_file


def _sample_df():
    return pd.DataFrame(
        [
            {
                "symbol": "RELIANCE",
                "name": "Reliance Industries Ltd",
                "index": "NIFTY50",
            },
            {"symbol": "INFY", "name": "Infosys Ltd", "index": "NIFTY50"},
            {"symbol": "HDFCBANK", "name": "HDFC Bank Ltd", "index": "NIFTY100"},
        ]
    )


def test_exact_symbol():
    df = _sample_df()
    out = search_equities(df, query="INFY")
    assert out and out[0].symbol == "INFY" and out[0].reason == "exact"


def test_prefix_name():
    df = _sample_df()
    out = search_equities(df, query="reli")
    assert out and out[0].symbol == "RELIANCE" and out[0].reason in {"prefix", "exact"}


def test_index_filter():
    df = _sample_df()
    out = search_equities(df, query="HDFC", indices=["NIFTY50"])
    assert out == []  # filtered out
