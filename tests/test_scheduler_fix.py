"""Tests to verify that crawl_daily_games() correctly passes --fix to run_daily_update_main
based on the DAILY_AUTO_REMEDIATION environment variable.

Strategy: patch DAILY_LOCK, run_daily_update_main, _previous_day_kst, and alert_success/alert_failure
so that the function body executes fully in test.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch


def _noop_lock():
    lock = MagicMock()
    lock.acquire.return_value = True
    return lock


def _make_patches(extra_patches=None):
    """Return a list of patches needed to isolate crawl_daily_games."""
    patches = [
        patch("scripts.scheduler.DAILY_LOCK", _noop_lock()),
        patch("scripts.scheduler._previous_day_kst", return_value="20250401"),
        patch("scripts.scheduler.alert_success"),
        patch("scripts.scheduler.alert_failure"),
        patch("scripts.scheduler.format_stability_alert_summary", return_value="ok"),
    ]
    if extra_patches:
        patches.extend(extra_patches)
    return patches


def _apply_all(patches):
    """Enter all patches and return mocks."""
    return [p.__enter__() if hasattr(p, "__enter__") else p.start() for p in patches]


def _stop_all(patches):
    for p in patches:
        try:
            p.__exit__(None, None, None)
        except AttributeError:
            p.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_includes_fix_when_enabled(monkeypatch):
    """DAILY_AUTO_REMEDIATION=1 → --fix must be passed."""
    monkeypatch.setenv("DAILY_AUTO_REMEDIATION", "1")
    monkeypatch.delenv("OCI_DB_URL", raising=False)

    captured: list[list] = []

    def fake_main(args, *args_, **kwargs):
        captured.append(list(args))
        return {}

    from scripts import scheduler

    patches = _make_patches()
    for p in patches:
        p.start()
    try:
        with patch.object(scheduler, "run_daily_update_main", side_effect=fake_main):
            scheduler.crawl_daily_games()
    finally:
        for p in patches:
            p.stop()

    assert captured, "run_daily_update_main was never called"
    assert "--fix" in captured[0], f"Expected --fix in {captured[0]}"


def test_excludes_fix_when_disabled(monkeypatch):
    """DAILY_AUTO_REMEDIATION=0 → --fix must NOT be passed."""
    monkeypatch.setenv("DAILY_AUTO_REMEDIATION", "0")
    monkeypatch.delenv("OCI_DB_URL", raising=False)

    captured: list[list] = []

    def fake_main(args, *args_, **kwargs):
        captured.append(list(args))
        return {}

    from scripts import scheduler

    patches = _make_patches()
    for p in patches:
        p.start()
    try:
        with patch.object(scheduler, "run_daily_update_main", side_effect=fake_main):
            scheduler.crawl_daily_games()
    finally:
        for p in patches:
            p.stop()

    assert captured, "run_daily_update_main was never called"
    assert "--fix" not in captured[0], f"--fix should be absent from {captured[0]}"


def test_includes_fix_by_default(monkeypatch):
    """DAILY_AUTO_REMEDIATION unset → default=1 → --fix must be passed."""
    monkeypatch.delenv("DAILY_AUTO_REMEDIATION", raising=False)
    monkeypatch.delenv("OCI_DB_URL", raising=False)

    captured: list[list] = []

    def fake_main(args, *args_, **kwargs):
        captured.append(list(args))
        return {}

    from scripts import scheduler

    patches = _make_patches()
    for p in patches:
        p.start()
    try:
        with patch.object(scheduler, "run_daily_update_main", side_effect=fake_main):
            scheduler.crawl_daily_games()
    finally:
        for p in patches:
            p.stop()

    assert captured, "run_daily_update_main was never called"
    assert "--fix" in captured[0], f"Expected --fix in {captured[0]} (default on)"


def test_includes_sync_and_fix_together(monkeypatch):
    """OCI_DB_URL set + DAILY_AUTO_REMEDIATION=1 → both --sync and --fix passed."""
    monkeypatch.setenv("DAILY_AUTO_REMEDIATION", "1")
    monkeypatch.setenv("OCI_DB_URL", "postgresql://oci-host/kbo")

    captured: list[list] = []

    def fake_main(args, *args_, **kwargs):
        captured.append(list(args))
        return {}

    from scripts import scheduler

    patches = _make_patches()
    for p in patches:
        p.start()
    try:
        with patch.object(scheduler, "run_daily_update_main", side_effect=fake_main):
            scheduler.crawl_daily_games()
    finally:
        for p in patches:
            p.stop()

    assert captured, "run_daily_update_main was never called"
    assert "--fix" in captured[0], f"Expected --fix in {captured[0]}"
    assert "--sync" in captured[0], f"Expected --sync in {captured[0]}"


def test_includes_scoped_p0_flags_when_enabled(monkeypatch):
    """Scoped P0 env flags add skip flags while preserving the daily update path."""
    monkeypatch.setenv("DAILY_AUTO_REMEDIATION", "0")
    monkeypatch.setenv("DAILY_SKIP_SEASON_STATS", "1")
    monkeypatch.setenv("DAILY_SKIP_OCI_SUPPORTING_SYNC", "1")
    monkeypatch.setenv("OCI_DB_URL", "postgresql://oci-host/kbo")

    captured: list[list] = []

    def fake_main(args, *args_, **kwargs):
        captured.append(list(args))
        return {}

    from scripts import scheduler

    patches = _make_patches()
    for p in patches:
        p.start()
    try:
        with patch.object(scheduler, "run_daily_update_main", side_effect=fake_main):
            scheduler.crawl_daily_games()
    finally:
        for p in patches:
            p.stop()

    assert captured, "run_daily_update_main was never called"
    assert "--sync" in captured[0], f"Expected --sync in {captured[0]}"
    assert "--skip-season-stats" in captured[0], f"Expected --skip-season-stats in {captured[0]}"
    assert "--skip-oci-supporting-sync" in captured[0], f"Expected --skip-oci-supporting-sync in {captured[0]}"
    assert "--fix" not in captured[0], f"--fix should be absent from {captured[0]}"
