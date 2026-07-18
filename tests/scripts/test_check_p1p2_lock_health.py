"""Tests for the scheduler lock-health post-check script (marker-based)."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_p1p2_lock_health.py"
KST = ZoneInfo("Asia/Seoul")


def _load_module():
    spec = importlib.util.spec_from_file_location("check_p1p2_lock_health", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_marker(tmp_path: Path, status: str, when: datetime) -> Path:
    marker = tmp_path / "p1p2_data.json"
    marker.write_text(json.dumps({"ts": when.isoformat(), "status": status}, ensure_ascii=False), encoding="utf-8")
    return marker


def test_forbidden_signatures_detected():
    mod = _load_module()
    problems = mod._collect_problems(
        "LockAcquisitionError happened here",
        require_run=False,
        run_diagnose=False,
    )
    assert any("LockAcquisitionError" in p for p in problems)


def test_empty_logs_reports_problem():
    mod = _load_module()
    problems = mod._collect_problems(
        "   \n  ",
        require_run=False,
        run_diagnose=False,
    )
    assert any("No scheduler log output" in p for p in problems)


def test_p1p2_marker_today_ok(tmp_path):
    mod = _load_module()
    marker = _write_marker(tmp_path, "ok", datetime.now(KST))
    mod.LAST_RUN_MARKER = marker
    ran_ok, detail = mod._p1p2_run_status()
    assert ran_ok is True
    assert "OK" in detail


def test_p1p2_marker_missing(tmp_path):
    mod = _load_module()
    mod.LAST_RUN_MARKER = tmp_path / "absent.json"
    ran_ok, detail = mod._p1p2_run_status()
    assert ran_ok is False
    assert "missing" in detail.lower()


def test_p1p2_marker_stale_date(tmp_path):
    mod = _load_module()
    yesterday = datetime.now(KST).replace(day=datetime.now(KST).day - 1)
    marker = _write_marker(tmp_path, "ok", yesterday)
    mod.LAST_RUN_MARKER = marker
    ran_ok, detail = mod._p1p2_run_status()
    assert ran_ok is False
    assert "expected" in detail.lower()


def test_p1p2_marker_error_status(tmp_path):
    mod = _load_module()
    marker = _write_marker(tmp_path, "error", datetime.now(KST))
    mod.LAST_RUN_MARKER = marker
    ran_ok, detail = mod._p1p2_run_status()
    assert ran_ok is False
    assert "error" in detail.lower()


def test_collect_problems_require_run_uses_marker(tmp_path):
    mod = _load_module()
    # Marker today + clean logs => no problems.
    marker = _write_marker(tmp_path, "ok", datetime.now(KST))
    mod.LAST_RUN_MARKER = marker
    problems = mod._collect_problems("clean logs", require_run=True, run_diagnose=False)
    assert problems == []

    # Missing marker => problem even with clean logs.
    mod.LAST_RUN_MARKER = tmp_path / "absent.json"
    problems = mod._collect_problems("clean logs", require_run=True, run_diagnose=False)
    assert any("did not run cleanly" in p for p in problems)
