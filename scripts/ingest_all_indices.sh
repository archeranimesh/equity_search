#!/usr/bin/env bash
set -Eeuo pipefail

CSV_DIR="${1:-data/indices/nse}"
DB_PATH="${2:-data/universe.duckdb}"
LOG_LEVEL="${3:-INFO}"
PYTHON_BIN="${PYTHON:-python}"

echo "Ingesting CSVs from: ${CSV_DIR}"
echo "DuckDB: ${DB_PATH}"
echo "Log level: ${LOG_LEVEL}"
echo

total=0; ok=0; fail=0

# Process files in a stable order; avoid parallelism to prevent DB locks.
while IFS= read -r -d '' f; do
  ((total++)) || true
  echo "[$total] -> $f"
  if "${PYTHON_BIN}" scripts/db_ingest.py --csv "$f" --db "$DB_PATH" --log "$LOG_LEVEL"; then
    ((ok++)) || true
  else
    ((fail++)) || true
    echo "FAILED: $f" >&2
  fi
done < <(find "$CSV_DIR" -maxdepth 1 -type f -name '*.csv' -print0 | sort -z)

echo
echo "Summary: total=$total ok=$ok fail=$fail"
exit $(( fail > 0 ? 1 : 0 ))
