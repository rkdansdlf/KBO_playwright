"""Pytest configuration shared across test modules.
Ensures the repository root is importable so `import src` works consistently.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Use a separate test database to avoid corrupting the production DB
TEST_DB_PATH = ROOT / "data" / "test_runtime.db"
os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH}"

import logging


class _CurrentStdoutHandler(logging.StreamHandler):
    """StreamHandler that always writes to sys.stdout (even when capsys patches it)."""

    def __init__(self) -> None:
        super().__init__(None)

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, value) -> None:
        pass


logging.basicConfig(level=logging.DEBUG, format="%(message)s", force=True)
root = logging.getLogger()
if root.handlers:
    root.handlers = [_CurrentStdoutHandler()]

LOCK_DIR = ROOT / "data" / "locks"


@pytest.fixture(autouse=True, scope="session")
def _clean_test_db(request):
    """Remove test database before each test to ensure clean state.

    Integration tests manage their own DB lifecycle, so skip cleanup for them.
    """
    if request.node.get_closest_marker("integration"):
        yield
        return
    test_db = ROOT / "data" / "test_runtime.db"
    if test_db.exists():
        test_db.unlink()
    # Also clean up WAL/SHM files
    for suffix in ("-wal", "-shm"):
        wal_file = ROOT / "data" / f"test_runtime.db{suffix}"
        if wal_file.exists():
            wal_file.unlink()
    yield
    # Cleanup after test
    if test_db.exists():
        test_db.unlink()
    for suffix in ("-wal", "-shm"):
        wal_file = ROOT / "data" / f"test_runtime.db{suffix}"
        if wal_file.exists():
            wal_file.unlink()


@pytest.fixture(autouse=True)
def _clean_locks():
    """Remove stale ProcessLock files between scheduler tests to prevent flaky lock contention."""
    import fnmatch
    import os

    test_path = os.environ.get("PYTEST_CURRENT_TEST", "")
    if not fnmatch.fnmatch(test_path, "*scheduler*"):
        yield
        return
    if LOCK_DIR.exists():
        for f in LOCK_DIR.glob("*.lock"):
            f.unlink(missing_ok=True)
    yield
    if LOCK_DIR.exists():
        for f in LOCK_DIR.glob("*.lock"):
            f.unlink(missing_ok=True)
