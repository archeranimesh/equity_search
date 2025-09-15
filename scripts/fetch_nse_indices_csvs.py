#!/usr/bin/env python3
"""
fetch_nse_indices_csvs.py

Fetch indices from:
  https://www.nseindia.com/api/allIndices
Then download each index's constituents CSV from:
  https://www.nseindia.com/api/equity-stockIndices?csv=true&index=<INDEX>&selectValFormat=crores

Hardened against 401/403 by:
- Bootstrapping cookies via homepage + market-data page
- Browser-like headers (Referer/Origin/Sec-Fetch-*)
- Gentle delays + retries

Usage:
  python scripts/fetch_nse_indices_csvs.py --outdir data/indices/nse --log INFO
  python scripts/fetch_nse_indices_csvs.py --include "^NIFTY" --exclude "TRI|BANK" --limit 10 --log DEBUG
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"
EQUITY_STOCK_INDICES_URL = "https://www.nseindia.com/api/equity-stockIndices"

HOMEPAGE_URL = "https://www.nseindia.com/"
REFERER_URL = "https://www.nseindia.com/market-data/live-market-indices"

UA_POOL = [
    # rotate a few recent Chrome/Safari UAs
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]


def setup_log(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download NSE index CSVs")
    p.add_argument(
        "--outdir", default="data/indices/nse", help="Directory to save CSVs"
    )
    p.add_argument(
        "--indices",
        help="Comma-separated subset of index names to download (exact match)",
    )
    p.add_argument(
        "--include", help="Regex to include indices (applied to raw 'index')"
    )
    p.add_argument(
        "--exclude", help="Regex to exclude indices (applied to raw 'index')"
    )
    p.add_argument(
        "--limit", type=int, help="Max number of indices to download (after filters)"
    )
    p.add_argument(
        "--delay", type=float, default=1.2, help="Base delay between requests (seconds)"
    )
    p.add_argument(
        "--jitter", type=float, default=0.6, help="Random +/- jitter added to delay"
    )
    p.add_argument(
        "--retries", type=int, default=5, help="Retries per request on 4xx/5xx"
    )
    p.add_argument(
        "--log", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be downloaded without writing files",
    )
    # add to argparse section
    p.add_argument(
        "--list-keys",
        action="store_true",
        help="Only fetch and print JSON keys instead of saving CSVs",
    )
    p.add_argument(
        "--key-value",
        "--key",
        dest="key_value",
        help="Only download indices whose 'key' equals this value (case-insensitive). "
        "e.g. --key-value 'INDICES ELIGIBLE IN DERIVATIVES'",
    )

    return p.parse_args()


# -------------------- session + bootstrap -------------------- #
from typing import List, Dict, Any


def indices_for_key(payload: Dict[str, Any], key_value: str) -> List[str]:
    """
    Return sorted unique index names where item['key'] == key_value (case-insensitive).
    """
    if not key_value:
        return []
    target = key_value.strip().lower()
    data = payload.get("data", []) or []
    selected = {
        str(item.get("index")).strip()
        for item in data
        if isinstance(item, dict)
        and str(item.get("key", "")).strip().lower() == target
        and item.get("index")
    }
    return sorted(selected)


def _browser_headers() -> Dict[str, str]:
    ua = random.choice(UA_POOL)
    return {
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": REFERER_URL,
        "Origin": "https://www.nseindia.com",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Connection": "keep-alive",
    }


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_browser_headers())
    return s


def bootstrap_cookies(
    s: requests.Session, log: logging.Logger, pause: float = 0.8
) -> None:
    """Warm up cookies by visiting homepage and the market data page."""
    try:
        s.get(HOMEPAGE_URL, timeout=(7, 15))
    except Exception as e:
        log.debug("bootstrap_homepage: %s", e)
    time.sleep(pause)
    try:
        s.get(REFERER_URL, timeout=(7, 15))
    except Exception as e:
        log.debug("bootstrap_referer: %s", e)
    time.sleep(pause)


def _nap(base: float, jitter: float) -> None:
    time.sleep(max(0.0, base + random.uniform(-jitter, jitter)))


# -------------------- HTTP helpers -------------------- #


def get_json(
    s: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]],
    retries: int,
    delay: float,
    jitter: float,
    log: logging.Logger,
) -> Any:
    """GET JSON with retry; on 401/403 re-bootstrap cookies and retry."""
    for attempt in range(1, retries + 1):
        resp = s.get(url, params=params, timeout=(7, 20))
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                log.debug("json_decode_error: %s", e)
        elif resp.status_code in (401, 403):
            log.debug(
                "auth_block (%s) -> rebootstrap attempt=%d", resp.status_code, attempt
            )
            bootstrap_cookies(s, log, pause=0.8 + random.uniform(0, 0.5))
        else:
            log.debug("http_status=%s url=%s", resp.status_code, resp.url)
        if attempt < retries:
            _nap(delay, jitter)
    resp.raise_for_status()
    return None


def get_text(
    s: requests.Session,
    url: str,
    params: Dict[str, Any],
    retries: int,
    delay: float,
    jitter: float,
    log: logging.Logger,
) -> str:
    """GET text with retry; on 401/403 re-bootstrap cookies and retry."""
    for attempt in range(1, retries + 1):
        resp = s.get(url, params=params, timeout=(7, 25))
        if resp.status_code == 200 and resp.text:
            return resp.text
        elif resp.status_code in (401, 403):
            log.debug(
                "auth_block_csv (%s) -> rebootstrap attempt=%d for index=%s",
                resp.status_code,
                attempt,
                params.get("index"),
            )
            bootstrap_cookies(s, log, pause=0.8 + random.uniform(0, 0.5))
        else:
            log.debug("csv_http_status=%s url=%s", resp.status_code, resp.url)
        if attempt < retries:
            _nap(delay, jitter)
    resp.raise_for_status()
    return ""


# -------------------- transform helpers -------------------- #


def extract_indices(payload: Any) -> List[str]:
    """Accepts {'data':[...]} or [...] and returns unique list of 'index' values."""
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        rows = payload["data"]
    elif isinstance(payload, list):
        rows = payload
    else:
        raise ValueError("Unexpected allIndices payload structure")

    out: List[str] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        idx = (row.get("index") or row.get("indexSymbol") or "").strip()
        if idx and idx not in seen:
            out.append(idx)
            seen.add(idx)
    return out


def apply_filters(indices: List[str], args: argparse.Namespace) -> List[str]:
    result = indices
    if args.indices:
        picks = [x.strip() for x in args.indices.split(",") if x.strip()]
        result = [i for i in result if i in picks]
    if args.include:
        inc = re.compile(args.include, re.IGNORECASE)
        result = [i for i in result if inc.search(i)]
    if args.exclude:
        exc = re.compile(args.exclude, re.IGNORECASE)
        result = [i for i in result if not exc.search(i)]
    if args.limit is not None:
        result = result[: max(0, args.limit)]
    return result


def slugify(name: str) -> str:
    s = re.sub(r"\s+", "-", name.strip())
    s = re.sub(r"[^A-Za-z0-9\-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s.upper() or "INDEX"


# def save_csv(text: str, outdir: Path, index_name: str) -> Path:
#     outdir.mkdir(parents=True, exist_ok=True)
#     slug = slugify(index_name)
#     path = outdir / f"{slug}.csv"
#     path.write_text(text, encoding="utf-8")
#     return path


def normalize_index_name(name: str) -> str:
    """Convert index names to safe filenames (no dashes)."""
    return name.replace("-", "")


from typing import Dict, Any, Iterable


def _print_keys(payload: Dict[str, Any], log: logging.Logger) -> None:
    """Print top-level keys and (if present) keys from first item in 'data'."""
    top = list(payload.keys())
    print(top)  # <-- as requested: plain list to stdout

    # If you ALSO want index fields (first item in 'data'), uncomment below:
    # if isinstance(payload.get("data"), Iterable):
    #     data = payload["data"]
    #     if data:
    #         print(list(dict(data[0]).keys()))


def _print_key_values(payload: dict) -> None:
    """Print unique values of 'key' inside payload['data'] as a Python list."""
    data = payload.get("data", [])
    keys = sorted({item.get("key") for item in data if "key" in item})
    print(keys)


import re
import unicodedata


def normalize_index_filename(name: str) -> str:
    """
    Return a safe filename stem with hyphens removed.
    Examples:
      'NIFTY-100' -> 'NIFTY100'
      'INDIA-VIX' -> 'INDIAVIX'
      'NIFTY500-MULTICAP-50-25-25' -> 'NIFTY500MULTICAP502525'
    """
    s = unicodedata.normalize("NFKC", str(name))
    return s.replace("-", "")


import re
from pathlib import Path


def _normalize_filename_stem(name: str) -> str:
    """
    Remove hyphens and collapse to a safe stem.
    Examples:
      'NIFTY-100' -> 'NIFTY100'
      'INDIA-VIX' -> 'INDIAVIX'
      'NIFTY500-MULTICAP-50-25-25' -> 'NIFTY500MULTICAP502525'
    """
    stem = str(name)
    stem = stem.replace("-", "")  # <-- required
    # (optional) keep only letters, numbers, and underscores:
    stem = re.sub(r"[^A-Za-z0-9_]", "", stem)
    return stem


def save_csv(text: str, outdir: Path, stem: str) -> Path:
    """Force-normalize the filename so hyphens can’t sneak back in."""
    outdir.mkdir(parents=True, exist_ok=True)
    safe_stem = _normalize_filename_stem(stem)
    path = outdir / f"{safe_stem}.csv"
    Path.write_text(path, text, encoding="utf-8")
    return path


# -------------------- main -------------------- #


def main() -> int:
    args = parse_args()
    setup_log(args.log)
    log = logging.getLogger("fetch_nse_indices_csvs")

    s = build_session()
    bootstrap_cookies(s, log, pause=1.0 + random.uniform(0, 0.5))  # initial warm-up

    payload = get_json(
        s,
        ALL_INDICES_URL,
        params=None,
        retries=args.retries,
        delay=args.delay,
        jitter=args.jitter,
        log=log,
    )

    # --- NEW: fast path for listing keys ---
    if getattr(args, "list_keys", False):
        _print_key_values(payload)
        SystemExit(0)
        return 0

    if args.key_value:
        indices = indices_for_key(payload, args.key_value)
        if not indices:
            available_keys = sorted(
                {
                    str(item.get("key")).strip()
                    for item in (payload.get("data") or [])
                    if isinstance(item, dict) and item.get("key")
                }
            )
            log.error("No indices found for key_value=%r", args.key_value)
            log.info("Available 'key' groups: %s", available_keys)
            return 2
    else:
        indices = extract_indices(payload)
        indices = apply_filters(indices, args)

    log.info("indices_total=%d", len(indices))
    log.debug("indices_sample=%s", indices[:8])

    if args.dry_run:
        for i in indices:
            log.info("[DRY] would fetch CSV for index=%s", i)
        return 0

    outdir = Path(args.outdir)
    saved: List[str] = []
    for idx_name in indices:
        params = {"csv": "true", "index": idx_name, "selectValFormat": "crores"}
        text = get_text(
            s,
            EQUITY_STOCK_INDICES_URL,
            params=params,
            retries=args.retries,
            delay=args.delay,
            jitter=args.jitter,
            log=log,
        )
        # NEW: remove hyphens from the filename
        log.debug(
            "index_raw=%s | index_file_stem=%s",
            idx_name,
            _normalize_filename_stem(idx_name),
        )
        path = save_csv(text, outdir, idx_name)  # save_csv now enforces no hyphens
        saved.append(str(path))
        log.info("saved: %s", path)
        _nap(args.delay, args.jitter)

    log.info("done | files=%d | outdir=%s", len(saved), outdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
