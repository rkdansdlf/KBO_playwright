"""Tests for scripts/diagnose_scheduler_locks.py."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest.mock import patch

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "diagnose_scheduler_locks",
    Path(__file__).resolve().parents[2] / "scripts" / "diagnose_scheduler_locks.py",
)
diag = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(diag)


def _run_with_locks(tmp_path: Path, lock_names: list[str], scheduler_pid: str | None, schedulers: list[int]):
    """Point the module at ``tmp_path`` and run diagnose()."""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    for name in lock_names:
        (lock_dir / f"{name}.lock").write_text("999999\n")
    if scheduler_pid is not None:
        (lock_dir / "scheduler.pid").write_text(f"{scheduler_pid}\n")
    with (
        patch.object(diag, "LOCK_DIR", lock_dir),
        patch.object(diag, "SCHEDULER_PID_FILE", lock_dir / "scheduler.pid"),
        patch.object(diag, "_find_scheduler_processes", return_value=schedulers),
    ):
        return diag.diagnose()


def test_clean_state(tmp_path):
    code = _run_with_locks(tmp_path, [], None, [])
    assert code == 0


def test_stale_tier_lock_detected(tmp_path):
    # PID 999999 is dead (no such process) -> stale.
    code = _run_with_locks(tmp_path, ["daily_update"], None, [])
    assert code == 1


def test_duplicate_scheduler_detected(tmp_path):
    code = _run_with_locks(tmp_path, [], None, [111, 222])
    assert code == 1


def test_alive_scheduler_pid_ok(tmp_path, monkeypatch):
    # Use a real live PID (this test process) so _pid_alive returns True.
    pid = os.getpid()
    code = _run_with_locks(tmp_path, [], str(pid), [pid])
    assert code == 0


def test_stale_scheduler_pid_flagged(tmp_path):
    # scheduler.pid points to a dead PID.
    code = _run_with_locks(tmp_path, [], "999999", [])
    assert code == 1


def test_missing_lock_dir_returns_clean(tmp_path):
    missing = tmp_path / "nope"
    with patch.object(diag, "LOCK_DIR", missing):
        assert diag.diagnose() == 0
