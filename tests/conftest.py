"""Pytest configuration shared across test modules.
Ensures the repository root is importable so `import src` works consistently.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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
