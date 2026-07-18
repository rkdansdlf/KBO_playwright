"""Pytest configuration shared across test modules.
Ensures the repository root is importable so `import src` works consistently.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

sqlite3.register_adapter(date, lambda value: value.isoformat())
sqlite3.register_adapter(datetime, lambda value: value.isoformat())

# Use a separate SQLite test database by default, while preserving an explicit
# non-SQLite URL for PostgreSQL integration jobs.
configured_database_url = os.environ.get("DATABASE_URL", "")
if configured_database_url and not configured_database_url.startswith("sqlite:"):
    TEST_DB_PATH: Path | None = None
else:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
    if worker_id:
        # Include the controller PID so concurrent xdist invocations cannot share a DB.
        TEST_DB_PATH = ROOT / "data" / f"test_runtime_{os.getppid()}_{worker_id}.db"
    else:
        TEST_DB_PATH = ROOT / "data" / "test_runtime.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH}"
if "OCI_DB_URL" not in os.environ:
    os.environ["OCI_DB_URL"] = ""
if "TARGET_DATABASE_URL" not in os.environ:
    os.environ["TARGET_DATABASE_URL"] = ""

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
    if request.node.get_closest_marker("integration") or TEST_DB_PATH is None:
        yield
        return
    test_db = TEST_DB_PATH
    if test_db.exists():
        test_db.unlink()
    # Also clean up WAL/SHM files
    for suffix in ("-wal", "-shm"):
        wal_file = test_db.with_name(f"{test_db.name}{suffix}")
        if wal_file.exists():
            wal_file.unlink()
    yield
    # Cleanup after test
    if test_db.exists():
        test_db.unlink()
    for suffix in ("-wal", "-shm"):
        wal_file = test_db.with_name(f"{test_db.name}{suffix}")
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
