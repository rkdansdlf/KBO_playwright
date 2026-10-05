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

# Block automatic `.env` loading before anything under `src/` is imported.
# Several src modules call `load_project_env()` at module scope; without this,
# importing `src.db.engine` alone injects a developer's real provider keys and
# Telegram chat ids into the worker process, which makes test outcomes depend on
# import order rather than on the code under test.
#
# The literal is duplicated rather than imported on purpose: importing
# `src.config.env_loader` here would make the flag depend on `src` import order.
# `tests/test_env_loading_contract.py` asserts the two stay in sync.
os.environ["KBO_ENV_FILE_LOADING"] = "0"

sqlite3.register_adapter(date, lambda value: value.isoformat())
sqlite3.register_adapter(datetime, lambda value: value.isoformat())

# Use a separate SQLite test database by default, while preserving an explicit
# non-SQLite URL for PostgreSQL integration jobs.
configured_database_url = os.environ.get("DATABASE_URL", "")


def _test_db_path(worker_id: str, pid: int, parent_pid: int) -> Path:
    """Return a per-invocation path for the SQLite test database.

    Every branch carries a process discriminator. The xdist branch always did;
    the plain branch did not, so two concurrent ``pytest`` runs handed out the
    same ``data/test_runtime.db``. That is safe by accident rather than by
    design: the session fixture unlinks the file at startup, so the second run
    orphans the first run's database rather than contending with it, and the
    isolation you rely on is a side effect of the cleanup.

    Args:
        worker_id: ``PYTEST_XDIST_WORKER`` for an xdist worker, else empty.
        pid: This process id, which distinguishes plain invocations.
        parent_pid: The parent process id, which is the xdist controller.

    Returns:
        A path unique to this invocation and worker.

    """
    if worker_id:
        return ROOT / "data" / f"test_runtime_{parent_pid}_{worker_id}.db"
    return ROOT / "data" / f"test_runtime_{pid}.db"


if configured_database_url and not configured_database_url.startswith("sqlite:"):
    TEST_DB_PATH: Path | None = None
else:
    TEST_DB_PATH = _test_db_path(os.environ.get("PYTEST_XDIST_WORKER", ""), os.getpid(), os.getppid())
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
    engine_module = sys.modules.get("src.db.engine")
    if engine_module is not None:
        test_engine = getattr(engine_module, "Engine", None)
        if test_engine is not None:
            test_engine.dispose()
    # Cleanup after test
    if test_db.exists():
        test_db.unlink()
    for suffix in ("-wal", "-shm"):
        wal_file = test_db.with_name(f"{test_db.name}{suffix}")
        if wal_file.exists():
            wal_file.unlink()


@pytest.fixture(autouse=True)
def _reset_db_fail_fast_gate():
    """Forget the DB gate's memoised probe between tests.

    ``_DB_GATE`` caches a failed probe for its cooldown window, which is exactly
    what makes an outage cost one query per window instead of one per tick. That
    same memo is process-global state, so a test that made the database look
    unreachable answers for the next test that calls a gated job -- and a gated job
    returns ``None`` silently by design. The assertion after it then passes without
    the job ever running.

    Applied to every test rather than only the scheduler ones: a gated job is
    reachable from anywhere, ``tests/cli/test_run_daily_update.py`` among them.
    """
    from src.scheduler.locks import _reset_db_gate

    _reset_db_gate()
    yield
    _reset_db_gate()


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
