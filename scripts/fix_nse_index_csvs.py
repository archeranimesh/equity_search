#!/usr/bin/env python3
"""
Repair NSE index CSVs with 'vertical' (multi-line) headers into tidy UTF-8 CSVs.

Usage:
  python scripts/fix_nse_index_csvs.py --csv data/indices/nse/NIFTY50.csv --inplace --log INFO
  python scripts/fix_nse_index_csvs.py --dir data/indices/nse --log INFO
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
from pathlib import Path
from typing import List, Optional

# -------------------- logging & args -------------------- #


def setup_log(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fix NSE index CSVs (vertical headers → tidy CSVs)."
    )
    p.add_argument("--csv", help="Single CSV file to fix.")
    p.add_argument("--dir", help="Directory containing CSV files to fix.")
    p.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite input CSV(s) instead of writing *.tidy.csv.",
    )
    p.add_argument(
        "--drop-summary",
        action="store_true",
        default=True,
        help="Drop the top index summary row (e.g., 'NIFTY 50'). Default: on.",
    )
    p.add_argument("--log", default="INFO")
    return p.parse_args()


# -------------------- byte/text helpers (<=15 lines) -------------------- #


def looks_like_html(b: bytes) -> bool:
    head = b[:4096].lower().lstrip()
    return head.startswith(b"<!doctype") or b"<html" in head


def decode_bytes(b: bytes) -> str:
    try:
        return b.decode("utf-8-sig")
    except UnicodeDecodeError:
        return b.decode("latin-1")


def normalize_text(s: str) -> str:
    junk = dict.fromkeys(map(ord, "\ufeff\u200b\u200c\u200d"), None)  # BOM + zero-width
    s = s.translate(junk).replace("\xa0", " ")
    return s.replace("\r\n", "\n").replace("\r", "\n")


# -------------------- header parsing (<=15 lines) -------------------- #


def _strip_date_tail(s: str) -> str:
    return re.sub(r"\b\d{1,2}-[A-Za-z]{3}-\d{4}\b", "", s).strip()


def _normalize_synonyms(name: str) -> str:
    u = name.upper()
    if u in {
        "SYMBOL",
        "TRADING SYMBOL",
        "TRADED SYMBOL",
        "TRADINGSYMBOL",
        "SECURITY ID",
        "SECURITYID",
        "SECURITY_ID",
    }:
        return "SYMBOL"
    return name


def _tidy_header(cols: List[str]) -> List[str]:
    out: List[str] = []
    for c in cols:
        n = c.replace('"', "").replace("â\x82¹", "₹")
        n = re.sub(r"\s+", " ", n).strip()
        n = n.replace("PREV. CLOSE", "PREV CLOSE").replace(
            "VALUE (₹ Crores)", "VALUE (₹ Cr)"
        )
        out.append(_normalize_synonyms(n))
    if out and "SYMBOL" not in {c.upper() for c in out}:
        out[0] = "SYMBOL"
    return out


def detect_header_lines(lines: List[str]) -> int:
    """
    Return the number of top lines that belong to the 'vertical' header block.
    We include lines until we hit the first real data row:
      - data row starts with a quote followed by a letter/number (e.g. "NIFTY 50")
      - header lines typically start with '",' or are a trailing date like ' 12-Sep-2024"'
    """
    for i, line in enumerate(lines):
        s = line.lstrip()
        if s.startswith('"') and (len(s) > 1) and s[1] not in {",", " ", '"'}:
            return i  # first data row line index
    return 0


def build_header_from_block(lines: List[str], header_lines: int) -> Optional[List[str]]:
    blob = "".join(lines[:header_lines]).replace("\ufeff", "")
    try:
        fields = next(csv.reader([blob], delimiter=",", quotechar='"'))
    except Exception:
        return None
    fields = [h.replace("\n", " ").replace("\r", " ").strip() for h in fields]
    if not fields:
        return None
    fields[-1] = _strip_date_tail(fields[-1])
    return _tidy_header(fields)


# -------------------- row parsing (<=15 lines) -------------------- #


def iter_rows(text: str):
    rdr = csv.reader(text.splitlines(), delimiter=",", quotechar='"')
    for r in rdr:
        if any(cell.strip() for cell in r):
            yield r


def filter_rows_by_len(rows: List[List[str]], ncols: int) -> List[List[str]]:
    out: List[List[str]] = []
    for r in rows:
        if len(r) == ncols:
            out.append([c.strip() for c in r])
    return out


def drop_summary(rows: List[List[str]]) -> List[List[str]]:
    if not rows:
        return rows
    if (rows[0][0] or "").strip().upper().startswith("NIFTY "):
        return rows[1:]
    return rows


def write_csv(
    path: Path, header: List[str], rows: List[List[str]], inplace: bool
) -> Path:
    out = path if inplace else path.with_suffix(".tidy.csv")
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    return out


# -------------------- core fix (<=15 lines) -------------------- #


def fix_file(
    path: Path, inplace: bool, drop_top_summary: bool, log: logging.Logger
) -> Optional[Path]:
    b = path.read_bytes()
    if looks_like_html(b):
        log.warning("HTML detected, skipped: %s", path.name)
        return None
    s = normalize_text(decode_bytes(b))
    lines = s.split("\n")

    header_lines = detect_header_lines(lines)
    if header_lines <= 0:
        log.error("Could not detect vertical header block in %s", path.name)
        return None

    header = build_header_from_block(lines, header_lines)
    if not header:
        log.error("Failed to reconstruct header for %s", path.name)
        return None

    data_text = "\n".join(lines[header_lines:])
    rows = filter_rows_by_len(list(iter_rows(data_text)), len(header))
    if drop_top_summary:
        rows = drop_summary(rows)

    if not rows:
        log.error("No data rows after repair for %s", path.name)
        return None

    out = write_csv(path, header, rows, inplace)
    log.info("fixed: %s -> rows=%d cols=%d", out.name, len(rows), len(header))
    return out


def fix_dir(d: Path, inplace: bool, drop_top_summary: bool, log: logging.Logger) -> int:
    n = 0
    for p in sorted(d.glob("*.csv")):
        try:
            if fix_file(p, inplace, drop_top_summary, log):
                n += 1
        except Exception as e:
            log.error("failed: %s | %s", p.name, e)
    return n


# -------------------- entry -------------------- #


def main() -> int:
    args = parse_args()
    setup_log(args.log)
    log = logging.getLogger("fix_nse_csvs")

    if not args.csv and not args.dir:
        log.error("Provide --csv or --dir")
        return 2

    if args.csv:
        p = Path(args.csv)
        if not p.exists():
            log.error("File not found: %s", p)
            return 2
        fix_file(p, args.inplace, args.drop_summary, log)
        return 0

    d = Path(args.dir)
    if not d.exists():
        log.error("Directory not found: %s", d)
        return 2
    fixed = fix_dir(d, args.inplace, args.drop_summary, log)
    log.info("done | fixed=%d | dir=%s", fixed, d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
