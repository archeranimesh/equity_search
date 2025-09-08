# equity_search/tests/conftest.py
import sys
from pathlib import Path

# This file's dir: .../equity_search/tests
TESTS_DIR = Path(__file__).resolve().parent
# Project src dir: .../equity_search/src
SRC_DIR = TESTS_DIR.parent / "src"

# Prepend src path if not already present
p = str(SRC_DIR)
if p not in sys.path:
    sys.path.insert(0, p)
