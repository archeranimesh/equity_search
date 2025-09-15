#!/usr/bin/env bash
set -Eeuo pipefail

# Run the CSV fixer over all files in a directory.
# Defaults:
#   CSV_DIR=data/indices/nse
#   INPLACE=true   (overwrite originals)
#   LOG=INFO
#   PYTHON=python  (override with env var if needed)

CSV_DIR="${1:-data/indices/nse}"
INPLACE="${2:-true}"         # true|false
LOG_LEVEL="${3:-INFO}"
PYTHON_BIN="${PYTHON:-python}"

echo "Fixing NSE index CSVs"
echo "  dir      : $CSV_DIR"
echo "  inplace  : $INPLACE"
echo "  log level: $LOG_LEVEL"
echo "  python   : $PYTHON_BIN"
echo

fix_one() {
  local file="$1"
  if [[ "$INPLACE" == "true" ]]; then
    "$PYTHON_BIN" scripts/fix_nse_index_csvs.py --csv "$file" --inplace --log "$LOG_LEVEL"
  else
    "$PYTHON_BIN" scripts/fix_nse_index_csvs.py --csv "$file" --log "$LOG_LEVEL"
  fi
}

total=0; ok=0; fail=0

# Use find+sort for stable order; avoid parallel to prevent IO thrash
while IFS= read -r -d '' f; do
  ((total++)) || true
  echo "[$total] $f"
  if fix_one "$f"; then
    ((ok++)) || true
  else
    ((fail++)) || true
    echo "  ↳ FAILED: $f" >&2
  fi
done < <(find "$CSV_DIR" -maxdepth 1 -type f -name '*.csv' -print0 | sort -z)

echo
echo "Summary: total=$total ok=$ok fail=$fail"
exit $(( fail > 0 ? 1 : 0 ))
