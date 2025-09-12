#!/usr/bin/env python3
"""
Ingest company names from Dhan's instrument master into DuckDB.

- Filters to NSE / Equity / SERIES in ['EQ','BE','BZ'] (prefers 'EQ').
- Derives `symbol` from DISPLAY_NAME (strips a trailing '-EQ' if present).
- Uses `SYMBOL_NAME` as the company name (falls back to similar fields if needed).
- Upserts into `equity_names(symbol PRIMARY KEY, name, source, asof)`.

Usage:
  # Download from Dhan (detailed)
  python scripts/ingest_company_names.py --db data/universe.duckdb --log INFO

  # Or ingest from a local CSV you already downloaded
  python scripts/ingest_company_names.py --db data/universe.duckdb --csv path/to/api-scrip-master-detailed.csv --log INFO
"""
from __future__ import annotations
import argparse, logging, io, datetime as dt
from typing import Optional, Tuple, List
import duckdb, pandas as pd

DHAN_DETAILED_URL = (
    "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"  # per docs
)
SOURCE_TAG = "dhan-v2-detailed"


def setup_log(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest company names from Dhan instrument master"
    )
    p.add_argument("--db", required=True, help="DuckDB path, e.g. data/universe.duckdb")
    p.add_argument("--csv", help="Local CSV path (if omitted, fetches from Dhan)")
    p.add_argument("--log", default="INFO")
    return p.parse_args()


def _read_csv_local(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    return _normalize_columns(df)


def _fetch_csv_remote(url: str) -> pd.DataFrame:
    # Defer to pandas (handles http/https); if requests available, you can swap in.
    df = pd.read_csv(url, low_memory=False)
    return _normalize_columns(df)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    return df


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def extract_symbol_name_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return DataFrame with columns: symbol (UPPER), name (str).
    Works with Dhan compact/detailed CSVs. Keeps NSE cash/equity rows using
    flexible OR-masks (EXCHANGE_SEGMENT='NSE_EQ' OR EXCH_ID/EXCHANGE contains 'NSE'),
    excludes obvious derivatives, and strips series suffixes like '-EQ'.
    Falls back to a heuristic if filtering yields too few rows.
    """
    log = logging.getLogger("ingest_company_names")

    # Normalize headers once
    df = df.copy()
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    def pick(*cands: str) -> str | None:
        for c in cands:
            if c in df.columns:
                return c
        return None

    def col_contains_any(colnames: list[str], needle: str) -> pd.Series:
        """True if any provided column (if present) contains needle (case-insensitive)."""
        if df.empty:
            return pd.Series([], dtype=bool)
        mask = pd.Series(False, index=df.index)
        for c in colnames:
            if c in df.columns:
                mask = mask | df[c].astype(str).str.upper().str.contains(
                    needle.upper(), na=False
                )
        # If none of those columns exist, don't filter on them
        return mask if mask.any() else pd.Series(True, index=df.index)

    def mask_eq(colnames: list[str], val: str) -> pd.Series:
        for c in colnames:
            if c in df.columns:
                return df[c].astype(str).str.upper().eq(val.upper())
        return pd.Series(True, index=df.index)

    def mask_in(colnames: list[str], vals: list[str]) -> pd.Series:
        for c in colnames:
            if c in df.columns:
                return df[c].astype(str).str.upper().isin([v.upper() for v in vals])
        return pd.Series(True, index=df.index)

    # ---------- Flexible NSE / Equity masks ----------
    # Accept if:
    #   - EXCHANGE_SEGMENT == 'NSE_EQ'  OR
    #   - EXCH_ID/EXCHANGE/EXCHANGE_ID contains 'NSE'
    m_nse_seg = mask_eq(["EXCHANGE_SEGMENT"], "NSE_EQ")
    m_nse_id = col_contains_any(["EXCH_ID", "EXCHANGE", "EXCHANGE_ID", "EXCH"], "NSE")
    m_nse = m_nse_seg | m_nse_id

    # Equity-like segments/series (if present)
    m_equity_seg = mask_in(
        ["SEGMENT", "SEGMENT_NAME", "EXCHANGE_SEGMENT"],
        ["E", "EQ", "EQUITY", "CASH", "CM", "NSE_EQ"],
    )
    m_series_main = mask_in(["SERIES"], ["EQ", "BE", "BZ"])

    # Exclude obvious derivatives/currency (when column exists)
    m_is_deriv = mask_in(
        ["INSTRUMENT_TYPE", "INSTRUMENT"],
        [
            "FUT",
            "FUTIDX",
            "FUTSTK",
            "FUTCUR",
            "OPT",
            "OPTIDX",
            "OPTSTK",
            "OPTCUR",
            "CUR",
            "CURRENCY",
        ],
    )
    m_keep = m_nse & m_equity_seg & m_series_main & (~m_is_deriv)

    # Apply mask; if it killed everything, soften it stepwise
    sdf = df[m_keep].copy()
    if sdf.empty:
        log.warning("NSE/Equity mask produced 0 rows; relaxing filters…")
        sdf = df[m_nse & (~m_is_deriv)].copy()
    if sdf.empty:
        log.warning(
            "Still 0 after relaxing derivative filter; using full dataframe as last resort."
        )
        sdf = df.copy()

    # Identify best columns
    name_col = pick(
        "SYMBOL_NAME", "SM_SYMBOL_NAME", "SECURITY_NAME", "COMPANY_NAME", "NAME"
    )
    disp_col = pick(
        "DISPLAY_NAME",
        "TRADING_SYMBOL",
        "TRADING_SYMBOL_NAME",
        "SEM_CUSTOM_SYMBOL",
        "SYMBOL",
        "SCRIP_SYMBOL",
    )
    if not (name_col and disp_col):
        log.error("Missing name/display columns. Columns present: %s", list(df.columns))
        raise ValueError(
            "Cannot find symbol/name columns (need DISPLAY_NAME/TRADING_SYMBOL & SYMBOL_NAME/COMPANY_NAME)."
        )

    # Clean symbol: strip '-EQ'/'-BE'/'-BZ' suffixes
    series = (
        sdf["SERIES"].astype(str).str.upper()
        if "SERIES" in sdf.columns
        else pd.Series([None] * len(sdf), index=sdf.index)
    )

    def clean_symbol(d: str, ser: str | None) -> str:
        s = str(d).strip().upper()
        for suf in ("-EQ", "-BE", "-BZ"):
            if s.endswith(suf):
                return s[: -len(suf)]
        return s

    pairs = pd.DataFrame(
        {
            "symbol": [clean_symbol(d, s) for d, s in zip(sdf[disp_col], series)],
            "name": sdf[name_col].astype(str).str.strip(),
        }
    )
    pairs = pairs.dropna(subset=["symbol", "name"])
    pairs = pairs[pairs["symbol"].ne("")]
    pairs = pairs.drop_duplicates(subset=["symbol"], keep="first").reset_index(
        drop=True
    )

    # If we still have suspiciously few rows, add a heuristic fallback:
    if len(pairs) < 50:
        log.warning(
            "Prepared only %d pairs; applying heuristic fallback on original CSV.",
            len(pairs),
        )
        # heuristic: symbol-like strings w/o spaces, not FUT/OPT/etc, length <= 20
        disp_any = pick(
            "DISPLAY_NAME",
            "TRADING_SYMBOL",
            "TRADING_SYMBOL_NAME",
            "SEM_CUSTOM_SYMBOL",
            "SYMBOL",
            "SCRIP_SYMBOL",
        )
        name_any = pick(
            "SYMBOL_NAME", "SM_SYMBOL_NAME", "SECURITY_NAME", "COMPANY_NAME", "NAME"
        )
        if disp_any and name_any:
            tmp = df[[disp_any, name_any]].copy()
            tmp.columns = ["disp", "name"]
            tmp["disp"] = tmp["disp"].astype(str).str.strip().str.upper()
            tmp["name"] = tmp["name"].astype(str).str.strip()
            # strip series suffixes
            for suf in ("-EQ", "-BE", "-BZ"):
                tmp.loc[tmp["disp"].str.endswith(suf), "disp"] = tmp["disp"].str[
                    : -len(suf)
                ]
            # filter out derivatives/currency by tokens
            bad_tokens = (
                " FUT",
                " OPT",
                " CE",
                " PE",
                " INR ",
                " USD",
                " GBP",
                " EUR",
                " JPY",
                "BANKNIFTY",
                "FINNIFTY",
                "NIFTY ",
            )
            mask_sym = (
                tmp["disp"].str.len().le(20)
                & ~tmp["disp"].str.contains("|".join(bad_tokens), regex=True)
                & ~tmp["disp"].str.contains(r"\s")
            )
            tmp = tmp[mask_sym]
            tmp = tmp.rename(columns={"disp": "symbol"})[
                ["symbol", "name"]
            ].drop_duplicates(subset=["symbol"])
            if len(tmp) > len(pairs):
                pairs = tmp.reset_index(drop=True)

    log.info(
        "Prepared %d symbol→name pairs (e.g., %s ...)",
        len(pairs),
        pairs.head(3).to_dict(orient="records"),
    )
    return pairs


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS equity_names(
          symbol TEXT PRIMARY KEY,
          name   TEXT NOT NULL,
          source TEXT,
          as_of   TIMESTAMP
        );
    """
    )


def upsert_names(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> Tuple[int, int]:
    """
    Insert or update names. Strategy:
      - Insert new rows (LEFT JOIN anti-join).
      - Update changed names (if any) by deleting & inserting (simplest portable path).
    Returns: (inserted_or_updated_now, total_rows)
    """
    if df.empty:
        total = con.execute("SELECT COUNT(*) FROM equity_names;").fetchone()[0]
        return (0, total)

    now = dt.datetime.utcnow()
    df = df.assign(source=SOURCE_TAG, as_of=now)

    # Register and insert new
    con.register("tmp_names", df)
    con.execute(
        """
        INSERT INTO equity_names(symbol, name, source, as_of)
        SELECT t.symbol, t.name, t.source, t.as_of
        FROM tmp_names t
        LEFT JOIN equity_names e ON e.symbol = t.symbol
        WHERE e.symbol IS NULL;
        """
    )

    # For existing symbols, update name if changed
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _to_update AS
        SELECT t.symbol, t.name, t.source, t.as_of
        FROM tmp_names t
        JOIN equity_names e USING(symbol)
        WHERE COALESCE(t.name,'') <> COALESCE(e.name,'');
        """
    )
    changed = con.execute("SELECT COUNT(*) FROM _to_update;").fetchone()[0]
    if changed:
        con.execute(
            "DELETE FROM equity_names WHERE symbol IN (SELECT symbol FROM _to_update);"
        )
        con.execute("INSERT INTO equity_names SELECT * FROM _to_update;")

    con.unregister("tmp_names")
    total = con.execute("SELECT COUNT(*) FROM equity_names;").fetchone()[0]
    return (
        changed + 0,
        total,
    )  # (new inserts are not counted precisely here to keep it simple)


def main() -> int:
    args = parse_args()
    setup_log(args.log)
    log = logging.getLogger("ingest_company_names")

    # Load CSV
    if args.csv:
        df = _read_csv_local(args.csv)
        log.info("Loaded local CSV: %s", args.csv)
    else:
        df = _fetch_csv_remote(DHAN_DETAILED_URL)
        log.info("Fetched detailed CSV from Dhan: %s", DHAN_DETAILED_URL)

    # Extract NSE Equity symbol ↔ name
    pairs = extract_symbol_name_pairs(df)
    log.info(
        "Prepared %d symbol→name pairs (e.g., %s ...)",
        len(pairs),
        pairs.head(3).to_dict(orient="records"),
    )

    # Upsert to DB
    with duckdb.connect(args.db, read_only=False) as con:
        ensure_table(con)
        changed, total = upsert_names(con, pairs)
        log.info("Upserted names: changed_now=%d | total_rows=%d", changed, total)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
