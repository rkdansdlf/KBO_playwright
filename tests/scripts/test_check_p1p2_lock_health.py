"""Tests for the scheduler lock-health post-check script."""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_p1p2_lock_health.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_p1p2_lock_health", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_forbidden_signatures_detected():
    mod = _load_module()
    problems = mod._collect_problems(
        "LockAcquisitionError happened here",
        require_run=False,
        run_diagnose=False,
    )
    assert any("LockAcquisitionError" in p for p in problems)


def test_require_run_missing_signature_reports_problem():
    mod = _load_module()
    problems = mod._collect_problems(
        "no p1p2 here",
        require_run=True,
        run_diagnose=False,
    )
    assert any("crawl_p1p2_data_job" in p for p in problems)


def test_clean_logs_no_problems():
    mod = _load_module()
    clean = "crawl_p1p2_data_job completed ok\ncrawl_daily_games ok\n"
    problems = mod._collect_problems(
        clean,
        require_run=True,
        run_diagnose=False,
    )
    assert problems == []


def test_empty_logs_reports_problem():
    mod = _load_module()
    problems = mod._collect_problems(
        "   \n  ",
        require_run=False,
        run_diagnose=False,
    )
    assert any("No scheduler log output" in p for p in problems)
