"""Phase 106F: Scheduler Recovery & Multi-Tier Locks Certification Test Suite.

Certifies the multi-tier scheduler locking architecture:
1. Tier Isolation: LIVE_LOCK, DAILY_LOCK, MAINTENANCE_LOCK, and SQLITE_WRITE_LOCK are independent.
2. Thread Safety & Thread-Local State: _LockState per-thread isolation across worker threads.
3. Single-Instance Guard: scheduler.pid lifecycle and process exclusivity.
4. Stale Lock Recovery: ForceProcessLock clears dead-PID locks safely.
5. Bounded Timeout & Lock Skip: _scheduler_job_lock timeouts and _LockSkipped signaling.
6. Lock Skip Monitoring: Prometheus delta detection and Slack alerting thresholds.
7. Fault Injection: Exception safety during lock held and SQLite writer fallback.
8. Nested Lock Prevention: Self re-acquisition prevention without deadlocks.
9. Diagnostic Tool: diagnose_scheduler_locks detection accuracy.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from scripts.diagnose_scheduler_locks import diagnose
from src.scheduler.locks import (
    DAILY_LOCK,
    LIVE_LOCK,
    MAINTENANCE_LOCK,
    SQLITE_WRITE_LOCK,
    _ensure_single_scheduler_instance,
    _LockSkipped,
    _release_scheduler_pid_file,
    _scheduler_job_lock,
    _sqlite_writer_lock,
    _with_lock_skip_guard,
    lock_skip_monitor_job,
)
from src.utils.lock import ForceProcessLock, ProcessLock, _LockState

if TYPE_CHECKING:
    from pathlib import Path


# ==============================================================================
# Category 1: Tier Isolation (5 tests)
# ==============================================================================


class TestPhase106FTierIsolation:
    """Certify that scheduler lock tiers are strictly isolated and independent."""

    def test_tier_locks_are_force_process_lock_instances(self) -> None:
        """All four primary scheduler locks must be ForceProcessLock instances."""
        assert isinstance(LIVE_LOCK, ForceProcessLock)
        assert isinstance(DAILY_LOCK, ForceProcessLock)
        assert isinstance(MAINTENANCE_LOCK, ForceProcessLock)
        assert isinstance(SQLITE_WRITE_LOCK, ForceProcessLock)

    def test_tier_lock_names_match_architecture_specification(self) -> None:
        """Lock names must match architectural identifiers."""
        assert LIVE_LOCK.name == "live_refresh"
        assert DAILY_LOCK.name == "daily_update"
        assert MAINTENANCE_LOCK.name == "maintenance"
        assert SQLITE_WRITE_LOCK.name == "sqlite_writer"

    def test_tier_locks_can_be_acquired_simultaneously_across_tiers(self, tmp_path: Path) -> None:
        """Holding one tier lock must not block acquiring a different tier lock."""
        lock_live = ForceProcessLock("tier_test_live", lock_dir=tmp_path)
        lock_daily = ForceProcessLock("tier_test_daily", lock_dir=tmp_path)
        lock_maint = ForceProcessLock("tier_test_maint", lock_dir=tmp_path)

        assert lock_live.acquire(blocking=False) is True
        try:
            assert lock_daily.acquire(blocking=False) is True
            try:
                assert lock_maint.acquire(blocking=False) is True
                lock_maint.release()
            finally:
                lock_daily.release()
        finally:
            lock_live.release()

    def test_tier_lock_files_are_distinct(self, tmp_path: Path) -> None:
        """Each tier lock writes to its own distinct lock file."""
        locks = [ForceProcessLock(f"distinct_lock_{i}", lock_dir=tmp_path) for i in range(3)]
        for lock in locks:
            assert lock.acquire() is True
            assert lock.lock_file_path.exists()
            assert lock.lock_file_path.name.endswith(".lock")
        for lock in locks:
            lock.release()

    def test_sqlite_writer_lock_coexists_with_tier_lock(self, tmp_path: Path) -> None:
        """A tier lock and SQLITE_WRITE_LOCK can be held by the same worker thread."""
        tier = ForceProcessLock("tier_batch", lock_dir=tmp_path)
        writer = ForceProcessLock("tier_writer", lock_dir=tmp_path)

        assert tier.acquire() is True
        try:
            assert writer.acquire() is True
            writer.release()
        finally:
            tier.release()


# ==============================================================================
# Category 2: Thread Safety & Thread-Local State (5 tests)
# ==============================================================================


class TestPhase106FThreadSafety:
    """Certify thread-local acquisition state and multi-threaded ProcessLock safety."""

    def test_lock_state_initializes_with_per_thread_defaults(self) -> None:
        """_LockState must provide clean thread-local defaults."""
        state = _LockState()
        assert state.file_fd is None
        assert state.thread_lock_acquired is False
        assert state.acquire_count == 0
        assert state.db_connection is None

    def test_lock_state_is_isolated_between_threads(self) -> None:
        """State mutated in thread A must not be visible in thread B."""
        state = _LockState()
        state.thread_lock_acquired = True
        state.acquire_count = 5

        observed_in_b: dict[str, object] = {}

        def worker() -> None:
            observed_in_b["acquired"] = state.thread_lock_acquired
            observed_in_b["count"] = state.acquire_count

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert observed_in_b["acquired"] is False
        assert observed_in_b["count"] == 0

    def test_shared_singleton_serialization_across_worker_threads(self, tmp_path: Path) -> None:
        """Two worker threads sharing a singleton ProcessLock serialize execution correctly."""
        lock = ProcessLock("test_shared_worker_sync", lock_dir=tmp_path)
        execution_order: list[str] = []
        barrier = threading.Barrier(2)

        def worker(worker_id: str) -> None:
            barrier.wait()
            with lock:
                execution_order.append(f"{worker_id}_start")
                time.sleep(0.05)
                execution_order.append(f"{worker_id}_end")

        t1 = threading.Thread(target=worker, args=("worker_1",))
        t2 = threading.Thread(target=worker, args=("worker_2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert execution_order in (
            ["worker_1_start", "worker_1_end", "worker_2_start", "worker_2_end"],
            ["worker_2_start", "worker_2_end", "worker_1_start", "worker_1_end"],
        )

    def test_concurrent_acquire_release_cycles_no_state_leak(self, tmp_path: Path) -> None:
        """Rapid multi-threaded acquire/release cycles must not leave dangling state."""
        lock = ProcessLock("test_rapid_cycle", lock_dir=tmp_path)
        errors: list[Exception] = []

        def worker() -> None:
            try:
                for _ in range(10):
                    with lock:
                        assert lock.file_fd is not None
                    assert lock.file_fd is None
            except (LockAcquisitionError, OSError, RuntimeError, AssertionError) as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors

    def test_thread_local_acquire_count_tracks_properly(self, tmp_path: Path) -> None:
        """Acquire count increments on acquire and decrements on release per-thread."""
        lock = ProcessLock("test_acquire_count_track", lock_dir=tmp_path)
        assert lock._state.acquire_count == 0
        with lock:
            assert lock._state.acquire_count == 1
        assert lock._state.acquire_count == 0


# ==============================================================================
# Category 3: Single-Instance Guard (4 tests)
# ==============================================================================


class TestPhase106FSingleInstanceGuard:
    """Certify single scheduler instance enforcement via PID file."""

    def test_ensure_single_instance_creates_pid_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """_ensure_single_scheduler_instance writes current PID to file."""
        pid_file = tmp_path / "scheduler.pid"
        monkeypatch.setattr("src.scheduler.locks._SCHEDULER_PID_FILE", pid_file)

        _ensure_single_scheduler_instance()
        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())

    def test_ensure_single_instance_clears_stale_pid_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A PID file with a dead PID is treated as stale and overwritten."""
        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text("9999999\n")
        monkeypatch.setattr("src.scheduler.locks._SCHEDULER_PID_FILE", pid_file)
        monkeypatch.setattr("src.scheduler.locks._scheduler_pid_alive", lambda pid: False)

        _ensure_single_scheduler_instance()
        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())

    def test_ensure_single_instance_exits_on_live_duplicate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If a live PID exists, sys.exit(1) is invoked to prevent duplicate schedulers."""
        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text("12345\n")
        monkeypatch.setattr("src.scheduler.locks._SCHEDULER_PID_FILE", pid_file)
        monkeypatch.setattr("src.scheduler.locks._scheduler_pid_alive", lambda pid: True)

        with pytest.raises(SystemExit) as exc_info:
            _ensure_single_scheduler_instance()
        assert exc_info.value.code == 1

    def test_release_scheduler_pid_file_only_removes_own_pid(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_release_scheduler_pid_file does not delete another process's PID file."""
        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text("12345\n")
        monkeypatch.setattr("src.scheduler.locks._SCHEDULER_PID_FILE", pid_file)

        _release_scheduler_pid_file()
        assert pid_file.exists()

        pid_file.write_text(f"{os.getpid()}\n")
        _release_scheduler_pid_file()
        assert not pid_file.exists()


# ==============================================================================
# Category 4: Stale Lock Recovery (3 tests)
# ==============================================================================


class TestPhase106FStaleLockRecovery:
    """Certify ForceProcessLock automatic clearing of stale lock files."""

    def test_force_process_lock_clears_dead_pid_lock_file(self, tmp_path: Path) -> None:
        """ForceProcessLock clears a lock file belonging to an extinct PID."""
        lock = ForceProcessLock("stale_test_dead_pid", lock_dir=tmp_path)
        dead_pid = 9999999
        lock.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        lock.lock_file_path.write_text(f"{dead_pid}\n")

        with patch("src.utils.lock.os.kill", side_effect=ProcessLookupError):
            assert lock.acquire() is True
            lock.release()

    def test_force_process_lock_clears_malformed_lock_file(self, tmp_path: Path) -> None:
        """ForceProcessLock clears a lock file with non-numeric / corrupted content."""
        lock = ForceProcessLock("stale_test_corrupt", lock_dir=tmp_path)
        lock.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        lock.lock_file_path.write_text("CORRUPTED_GARBAGE_DATA\n")

        assert lock.acquire() is True
        lock.release()

    def test_force_process_lock_does_not_clear_live_pid_lock_file(self, tmp_path: Path) -> None:
        """ForceProcessLock preserves a lock file when the owning PID is alive."""
        lock = ForceProcessLock("stale_test_live_pid", lock_dir=tmp_path)
        lock.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        lock.lock_file_path.write_text(f"{os.getpid()}\n")

        with patch("src.utils.lock.os.kill", return_value=None):
            lock._clear_stale_lock()
            assert lock.lock_file_path.exists()


# ==============================================================================
# Category 5: Bounded Timeout & Lock Skip (4 tests)
# ==============================================================================


class TestPhase106FBoundedTimeoutAndSkip:
    """Certify bounded timeouts and _LockSkipped signal handling."""

    def test_scheduler_job_lock_raises_lock_skipped_on_tier_timeout(self) -> None:
        """_scheduler_job_lock raises _LockSkipped when tier lock cannot be acquired."""
        mock_tier = MagicMock(spec=ForceProcessLock)
        mock_tier.acquire.return_value = False
        mock_tier.name = "mock_tier"

        with pytest.raises(_LockSkipped):
            with _scheduler_job_lock(mock_tier, lock_timeout=0.01):
                pass

    def test_scheduler_job_lock_raises_lock_skipped_on_sqlite_writer_timeout(self) -> None:
        """_scheduler_job_lock raises _LockSkipped when sqlite writer lock cannot be acquired."""
        mock_tier = MagicMock(spec=ForceProcessLock)
        mock_tier.acquire.return_value = True
        mock_tier.name = "mock_tier"

        with (
            patch("src.scheduler.locks._scheduler_uses_sqlite_database", return_value=True),
            patch("src.scheduler.locks._sqlite_writer_lock") as mock_sq_lock,
            pytest.raises(_LockSkipped),
        ):
            mock_sq_lock.return_value.__enter__.return_value = False
            with _scheduler_job_lock(mock_tier, sqlite_timeout=0.01):
                pass

        mock_tier.release.assert_called_once()

    def test_with_lock_skip_guard_suppresses_lock_skipped_and_returns_none(self) -> None:
        """_with_lock_skip_guard catches _LockSkipped and returns None without raising."""

        @_with_lock_skip_guard
        def job_that_skips() -> str:
            raise _LockSkipped

        result = job_that_skips()
        assert result is None

    def test_with_lock_skip_guard_passes_through_normal_return(self) -> None:
        """_with_lock_skip_guard passes through ordinary return values."""

        @_with_lock_skip_guard
        def normal_job() -> str:
            return "SUCCESS_DATA"

        assert normal_job() == "SUCCESS_DATA"


# ==============================================================================
# Category 6: Lock Skip Monitoring (3 tests)
# ==============================================================================


class TestPhase106FLockSkipMonitoring:
    """Certify lock skip rate monitoring and alert emission."""

    def test_lock_skip_monitor_detects_threshold_exceeded_and_sends_alert(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """lock_skip_monitor_job sends a Slack alert when skip count delta >= threshold."""
        mock_sample = MagicMock()
        mock_sample.name = "kbo_scheduler_lock_skip_total"
        mock_sample.labels = {"job_id": "crawl_p1p2_data", "lock": "sqlite_writer"}
        mock_sample.value = 10.0

        mock_metric = MagicMock()
        mock_metric.samples = [mock_sample]

        mock_counter = MagicMock()
        mock_counter.collect.return_value = [mock_metric]

        mock_slack = MagicMock()

        for mod_name in ("scripts.scheduler", "src.scheduler", "src.scheduler.locks"):
            if mod_name in sys.modules:
                monkeypatch.setattr(f"{mod_name}.KBO_SCHEDULER_LOCK_SKIP_TOTAL", mock_counter, raising=False)
                monkeypatch.setattr(f"{mod_name}.SlackWebhookClient", mock_slack, raising=False)
                monkeypatch.setattr(f"{mod_name}.LOCK_SKIP_ALERT_THRESHOLD", 5, raising=False)
                monkeypatch.setattr(
                    f"{mod_name}._LAST_LOCK_SKIP", {("crawl_p1p2_data", "sqlite_writer"): 0.0}, raising=False
                )

        lock_skip_monitor_job()

        mock_slack.send_alert.assert_called_once()
        sent_message = mock_slack.send_alert.call_args[0][0]
        assert "crawl_p1p2_data" in sent_message
        assert "skipped 10 times" in sent_message

    def test_lock_skip_monitor_no_alert_when_below_threshold(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """lock_skip_monitor_job does not alert when skip count delta < threshold."""
        mock_sample = MagicMock()
        mock_sample.name = "kbo_scheduler_lock_skip_total"
        mock_sample.labels = {"job_id": "crawl_daily_games", "lock": "sqlite_writer"}
        mock_sample.value = 2.0

        mock_metric = MagicMock()
        mock_metric.samples = [mock_sample]

        mock_counter = MagicMock()
        mock_counter.collect.return_value = [mock_metric]

        mock_slack = MagicMock()

        for mod_name in ("scripts.scheduler", "src.scheduler", "src.scheduler.locks"):
            if mod_name in sys.modules:
                monkeypatch.setattr(f"{mod_name}.KBO_SCHEDULER_LOCK_SKIP_TOTAL", mock_counter, raising=False)
                monkeypatch.setattr(f"{mod_name}.SlackWebhookClient", mock_slack, raising=False)
                monkeypatch.setattr(f"{mod_name}.LOCK_SKIP_ALERT_THRESHOLD", 5, raising=False)
                monkeypatch.setattr(
                    f"{mod_name}._LAST_LOCK_SKIP", {("crawl_daily_games", "sqlite_writer"): 0.0}, raising=False
                )

        lock_skip_monitor_job()

        mock_slack.send_alert.assert_not_called()

    def test_lock_skip_monitor_handles_unregistered_metrics_gracefully(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """lock_skip_monitor_job exits cleanly when counter raises during collect."""
        mock_counter = MagicMock()
        mock_counter.collect.side_effect = RuntimeError("Metric unregistered")

        for mod_name in ("scripts.scheduler", "src.scheduler", "src.scheduler.locks"):
            if mod_name in sys.modules:
                monkeypatch.setattr(f"{mod_name}.KBO_SCHEDULER_LOCK_SKIP_TOTAL", mock_counter, raising=False)

        lock_skip_monitor_job()


# ==============================================================================
# Category 7: Fault Injection & Exception Safety (3 tests)
# ==============================================================================


class TestPhase106FFaultInjection:
    """Certify exception safety and lock release under fault conditions."""

    def test_exception_in_lock_held_block_releases_lock(self, tmp_path: Path) -> None:
        """When an unhandled exception occurs inside a with lock block, the lock is released."""
        lock = ProcessLock("fault_test_exception", lock_dir=tmp_path)

        with pytest.raises(RuntimeError, match="Synthetic crash"):
            with lock:
                raise RuntimeError("Synthetic crash")

        assert lock.acquire(blocking=False) is True
        lock.release()

    def test_sqlite_writer_lock_yields_false_and_increments_metric_on_contention(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When SQLite writer lock cannot be acquired, yields False and increments Prometheus metric."""
        fake_lock = MagicMock()
        fake_lock.acquire.return_value = False
        fake_lock.name = "fake_writer"

        mock_counter = MagicMock()

        if "scripts.scheduler" in sys.modules:
            monkeypatch.setattr("scripts.scheduler.SQLITE_WRITE_LOCK", fake_lock, raising=False)
            monkeypatch.setattr("scripts.scheduler.KBO_SCHEDULER_LOCK_SKIP_TOTAL", mock_counter, raising=False)
        if "src.scheduler" in sys.modules:
            monkeypatch.setattr("src.scheduler.SQLITE_WRITE_LOCK", fake_lock, raising=False)
            monkeypatch.setattr("src.scheduler.KBO_SCHEDULER_LOCK_SKIP_TOTAL", mock_counter, raising=False)
        monkeypatch.setattr("src.scheduler.locks.SQLITE_WRITE_LOCK", fake_lock)
        monkeypatch.setattr("src.scheduler.locks.KBO_SCHEDULER_LOCK_SKIP_TOTAL", mock_counter)
        monkeypatch.setattr("src.scheduler.locks._scheduler_uses_sqlite_database", lambda: True)

        with _sqlite_writer_lock(job_id="test_fault_job") as acquired:
            assert acquired is False

        mock_counter.labels.assert_called_once_with(job_id="test_fault_job", lock="sqlite_writer")
        mock_counter.labels.return_value.inc.assert_called_once()

    def test_context_manager_suppresses_no_exceptions(self, tmp_path: Path) -> None:
        """ProcessLock context manager does not swallow unexpected exceptions."""
        lock = ProcessLock("fault_test_propagate", lock_dir=tmp_path)
        with pytest.raises(ValueError, match="propagate me"):
            with lock:
                raise ValueError("propagate me")


# ==============================================================================
# Category 8: Nested Lock Prevention (2 tests)
# ==============================================================================


class TestPhase106FNestedLockPrevention:
    """Certify that nested re-acquisition by the same thread fails gracefully without deadlocks."""

    def test_process_lock_same_thread_reacquire_returns_false(self, tmp_path: Path) -> None:
        """Re-acquiring an already held ProcessLock on the same thread returns False."""
        lock = ProcessLock("nested_test_process", lock_dir=tmp_path)
        assert lock.acquire() is True
        try:
            assert lock.acquire(blocking=False) is False
        finally:
            lock.release()

    def test_force_process_lock_same_thread_reacquire_does_not_force_clear(self, tmp_path: Path) -> None:
        """ForceProcessLock does not force-clear its own lock file when re-acquired on same thread."""
        lock = ForceProcessLock("nested_test_force", lock_dir=tmp_path)
        assert lock.acquire() is True
        try:
            assert lock.acquire() is False
            assert lock.lock_file_path.exists()
        finally:
            lock.release()


# ==============================================================================
# Category 9: Diagnostic Tool (2 tests)
# ==============================================================================


class TestPhase106FDiagnosticTool:
    """Certify diagnose_scheduler_locks operational utility."""

    def test_diagnose_returns_clean_when_no_stale_locks(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """diagnose() returns 0 when lock directory has no stale locks and no duplicates."""
        monkeypatch.setattr("scripts.diagnose_scheduler_locks.LOCK_DIR", tmp_path)
        monkeypatch.setattr("scripts.diagnose_scheduler_locks.SCHEDULER_PID_FILE", tmp_path / "scheduler.pid")
        monkeypatch.setattr("scripts.diagnose_scheduler_locks._find_scheduler_processes", list)

        exit_code = diagnose(verbose=True)
        assert exit_code == 0

    def test_diagnose_detects_stale_lock_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """diagnose() returns 1 when a lock file owned by a dead PID is detected."""
        stale_lock = tmp_path / "daily_update.lock"
        stale_lock.write_text("9999999\n")

        monkeypatch.setattr("scripts.diagnose_scheduler_locks.LOCK_DIR", tmp_path)
        monkeypatch.setattr("scripts.diagnose_scheduler_locks.SCHEDULER_PID_FILE", tmp_path / "scheduler.pid")
        monkeypatch.setattr("scripts.diagnose_scheduler_locks._find_scheduler_processes", list)
        monkeypatch.setattr("scripts.diagnose_scheduler_locks._pid_alive", lambda pid: False)

        exit_code = diagnose(verbose=False)
        assert exit_code == 1
