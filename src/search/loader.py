from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EquityRow:
    symbol: str
    name: str
    index: str  # e.g., "NIFTY50", "NIFTY100"
    isin: Optional[str] = None
    sector: Optional[str] = None


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize incoming DataFrame columns to a common schema."""
    rename_map = {
        "SYMBOL": "symbol",
        "Symbol": "symbol",
        "Name": "name",
        "Company Name": "name",
        "Index": "index",
        "INDEX": "index",
        "ISIN": "isin",
        "Sector": "sector",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    return df.rename(columns=cols)


def _clean_df(df: pd.DataFrame, forced_index: Optional[str]) -> pd.DataFrame:
    """Trim, lowercase where useful, and ensure required columns exist."""
    if "symbol" not in df or "name" not in df:
        missing = [c for c in ("symbol", "name") if c not in df]
        raise ValueError(f"Missing required columns: {missing}")

    if forced_index:
        df["index"] = forced_index

    if "index" not in df:
        df["index"] = "UNKNOWN"

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["index"] = df["index"].astype(str).str.strip().str.upper()

    # Optional fields
    for opt in ("isin", "sector"):
        if opt in df:
            df[opt] = df[opt].astype(str).str.strip()

    return df[["symbol", "name", "index", "isin", "sector"]]


def load_from_file(path: Path, forced_index: Optional[str] = None) -> pd.DataFrame:
    """
    Load one CSV/Parquet file and normalize it.
    If file has no 'index' column, forced_index is applied.
    """
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    elif path.suffix.lower() in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")

    df = _normalize_cols(df)
    return _clean_df(df, forced_index)


def load_from_dir(dirpath: Path) -> pd.DataFrame:
    """
    Load multiple index files from a directory.
    Convention: filename (without extension) is used as index if not present.
      e.g., data/indices/NIFTY50.csv → forced_index="NIFTY50"
    """
    frames: List[pd.DataFrame] = []
    for p in sorted(dirpath.glob("*")):
        if p.suffix.lower() not in {".csv", ".parquet", ".pq"}:
            continue
        forced_index = p.stem.upper()
        try:
            part = load_from_file(p, forced_index=forced_index)
            frames.append(part)
            log.info(
                "index_loaded",
                extra={"file": str(p), "rows": len(part), "index": forced_index},
            )
        except Exception as e:
            log.exception("index_load_failed", extra={"file": str(p)})
            raise e
    if not frames:
        raise FileNotFoundError(f"No index files found in {dirpath}")
    return pd.concat(frames, ignore_index=True)


def load_equities(sources: Iterable[Path]) -> pd.DataFrame:
    """
    Load equities from one or many files/directories.
    - If a path is a file → load that one.
    - If a path is a directory → load all supported files inside.
    """
    frames: List[pd.DataFrame] = []
    for src in sources:
        if src.is_dir():
            frames.append(load_from_dir(src))
        elif src.is_file():
            frames.append(load_from_file(src, forced_index=None))
        else:
            raise FileNotFoundError(f"Path not found: {src}")
    out = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["symbol", "index"]
    )
    log.info(
        "equities_loaded",
        extra={"rows": len(out), "sources": [str(s) for s in sources]},
    )
    return out
