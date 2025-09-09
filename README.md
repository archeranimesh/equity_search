# Stage-3 — DB Search (Symbol → Indices)

A **fast, DuckDB-backed** lookup that:
- Finds symbols by **exact / prefix / fuzzy** match
- Returns **all index memberships** for each symbol (e.g., `NIFTY50`, `NIFTY100`)
- Is **idempotent**, tiny, and easy to maintain

---

## ✅ Prerequisites

```bash
python -m venv .venv
source .venv/bin/activate
pip install duckdb pandas rapidfuzz pytest
```

> `rapidfuzz` is optional (better fuzzy). Fallback to Python’s `difflib` is automatic.

---

## 🧾 CSV Placement & Format

### 1) NIFTY50 equity list (for `db_ingest.py`)
- Path: `data/nifty50.csv` (or pass another path via `--csv`)
- Must contain a `symbol` column (case-insensitive)

**Example (`data/nifty50.csv`):**
```csv
symbol
RELIANCE
INFY
HDFCBANK
```

### 2) (Optional) Other index lists
- Put CSVs under `data/indices/`
- **Filename stem = index name**, e.g. `NIFTY100.csv`, `NIFTY200.csv`
- Each CSV must have a `symbol` column

**Example (`data/indices/NIFTY100.csv`):**
```csv
symbol
RELIANCE
TCS
HDFCBANK
```

---

## 🗄️ DuckDB Schema (auto-created)

- `equities(symbol TEXT PRIMARY KEY)`
- `equity_membership(symbol TEXT, index TEXT, PRIMARY KEY(symbol, index))`

`db_ingest.py` ensures `equities` exists and **syncs** `(symbol, 'NIFTY50')` into `equity_membership` idempotently.

---

## 🚀 Quickstart (End-to-End)

### 1) Ingest NIFTY50 → `equities` (+ sync to `equity_membership`)
```bash
python scripts/db_ingest.py   --csv data/nifty50.csv   --db data/universe.duckdb   --log INFO
```

**What it does**
- Extracts symbols from `data/nifty50.csv`
- Upserts into `equities(symbol)`
- Inserts missing `(symbol, 'NIFTY50')` into `equity_membership` (safe to re-run)

---

### 2) (Optional) Load more indices from `data/indices/*`

If you have `scripts/ingest_index_lists.py`:
```bash
python scripts/ingest_index_lists.py   --db data/universe.duckdb   --dir data/indices   --log INFO
```

**No `ingest_index_lists.py` yet?** Use one of these DuckDB CLI snippets:

**A) Simple insert**
```bash
duckdb data/universe.duckdb "
  CREATE TABLE IF NOT EXISTS equity_membership(
    symbol TEXT, index TEXT, PRIMARY KEY(symbol, index)
  );
  INSERT INTO equity_membership(symbol, index)
  SELECT UPPER(symbol), 'NIFTY100'
  FROM read_csv('data/indices/NIFTY100.csv', AUTO_DETECT=TRUE);
"
```

**B) Idempotent insert (portable LEFT JOIN anti-pattern)**
```bash
duckdb data/universe.duckdb "
  CREATE TABLE IF NOT EXISTS equity_membership(
    symbol TEXT, index TEXT, PRIMARY KEY(symbol, index)
  );
  INSERT INTO equity_membership(symbol, index)
  SELECT t.symbol, 'NIFTY100'
  FROM read_csv('data/indices/NIFTY100.csv', AUTO_DETECT=TRUE) t
  LEFT JOIN equity_membership m
    ON UPPER(m.symbol) = UPPER(t.symbol) AND m.index = 'NIFTY100'
  WHERE m.symbol IS NULL;
"
```

---

### 3) Verify DB contents

```bash
duckdb data/universe.duckdb "
  PRAGMA show_tables;
  DESCRIBE equities;
  SELECT COUNT(*) AS n_equities FROM equities;
  SELECT * FROM equity_membership LIMIT 10;
"
```

**Useful checks**
```bash
duckdb data/universe.duckdb "
  -- All indices for a symbol
  SELECT DISTINCT index FROM equity_membership WHERE symbol='RELIANCE' ORDER BY 1;

  -- All symbols in a given index
  SELECT symbol FROM equity_membership WHERE index='NIFTY50' ORDER BY 1 LIMIT 20;
"
```

---

### 4) Search (symbol → indices)

**Exact**
```bash
python scripts/search_symbol_indices.py   --db data/universe.duckdb   --q RELIANCE
```

**Prefix / fuzzy**
```bash
python scripts/search_symbol_indices.py   --db data/universe.duckdb   --q RELI
```

**Example output**
```json
[
  {
    "symbol": "RELIANCE",
    "indices": ["NIFTY50"],
    "score": 1.0,
    "reason": "exact"
  }
]
```

---

## 🧪 Tests

Make sure `src` is importable during tests:

- Add `src/__init__.py` and `src/search/__init__.py`
- Create `pytest.ini` at repo root:

```ini
[pytest]
pythonpath = .
```

Run:
```bash
pytest -q
```

Or:
```bash
PYTHONPATH=. pytest -q
```

---

## 🧰 DuckDB One-Liners (Reference)

```bash
duckdb data/universe.duckdb
```

Inside the shell:
```sql
.tables;
DESCRIBE equities;
SELECT * FROM equities LIMIT 10;

-- Indices for a symbol
SELECT DISTINCT index FROM equity_membership
WHERE symbol='RELIANCE' ORDER BY 1;

-- Symbols in an index
SELECT symbol FROM equity_membership
WHERE index='NIFTY50' ORDER BY 1 LIMIT 20;
```

---

## 🪵 Logging

All scripts accept `--log` / `--log-level`:

```bash
python scripts/db_ingest.py --csv data/nifty50.csv --db data/universe.duckdb --log DEBUG
python scripts/search_symbol_indices.py --db data/universe.duckdb --q HDFC --log-level DEBUG
```

Typical lines:
```
INFO | equity_search.db_ingest | Extracted 50 symbols
INFO | src.search.db_reader     | db_candidates_fetched
INFO | src.search.db_reader     | db_symbol_indices
```

---

## 📝 Troubleshooting

- **IO Error: open database in read-only mode**  
  Ensure scripts open DB read-write (current scripts do) or pre-create the file by running any ingest.

- **Catalog Error: table not found (`equity_universe`)**  
  Use current tables: `equities` (symbols), `equity_membership` (membership). Update CLI `--table` only if you truly have a different table.

- **Referenced column `name` not found**  
  Current schema is symbol-only. Search uses `symbol`; don’t reference `name` unless you add that column/table.

- **`KeyError: 'index'` in ranking**  
  The ranker now takes a `List[str]` of symbols; no DataFrame columns are required. Ensure you’re on the updated `search_db.py`.

- **DuckDB version differences (e.g., `MERGE`, `ANTI`)**  
  We use **LEFT JOIN … WHERE IS NULL** for idempotent inserts to maximize compatibility across versions.

---

## 💡 Design Notes

- **Idempotent membership sync**: insert only missing `(symbol, index)` via `LEFT JOIN … WHERE IS NULL`.
- **Case-insensitive** comparisons with `UPPER(…)`.
- **Ranking**: exact (1.00) > prefix (0.92) > fuzzy (`rapidfuzz`/`difflib`).
- **Performance**: SQL narrows candidates; Python fuzzy is applied to a small candidate set.
- **Extensible**: if you add a `company_name` table later, we can join it and extend fuzzy to names without breaking the symbol-only path.

---
