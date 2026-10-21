from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.services.recovery_manager import RecoveryManager


class TestRecoveryManager:
    def test_default_path(self):
        mgr = RecoveryManager("data/recovery/test_checkpoint.json")
        assert str(mgr.path).endswith("test_checkpoint.json")

    def test_initialize_run_sets_state(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("run1", ["g1", "g2", "g3"])
        assert mgr.state["run_id"] == "run1"
        assert mgr.state["total_count"] == 3
        assert mgr.state["pending"] == ["g1", "g2", "g3"]
        assert mgr.state["completed"] == []

    def test_initialize_run_resets_on_new_run(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("run1", ["g1"])
        mgr.mark_completed("g1")
        mgr.initialize_run("run2", ["g2"])
        assert mgr.state["run_id"] == "run2"
        assert mgr.state["completed"] == []
        assert mgr.state["pending"] == ["g2"]

    def test_initialize_run_preserves_on_same_run(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("run1", ["g1", "g2"])
        mgr.mark_completed("g1")
        mgr.initialize_run("run1", ["g1", "g2"])
        assert mgr.state["completed"] == ["g1"]

    def test_mark_completed(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("test", ["g1", "g2"])
        mgr.mark_completed("g1")
        assert "g1" in mgr.state["completed"]
        assert "g1" not in mgr.state["pending"]
        assert mgr.state["pending"] == ["g2"]

    def test_mark_completed_idempotent(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("test", ["g1"])
        mgr.mark_completed("g1")
        mgr.mark_completed("g1")
        assert mgr.state["completed"] == ["g1"]

    def test_mark_failed(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("test", ["g1"])
        mgr.mark_failed("g1", "error reason")
        assert mgr.state["failed"] == {"g1": "error reason"}
        assert "g1" not in mgr.state["pending"]

    def test_get_pending_targets(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("test", ["a", "b"])
        assert mgr.get_pending_targets() == ["a", "b"]

    def test_clear_removes_file(self, tmp_path: Path):
        path = tmp_path / "checkpoint.json"
        mgr = RecoveryManager(str(path))
        mgr.initialize_run("test", ["g1"])
        assert path.exists()
        mgr.clear()
        assert not path.exists()
        assert mgr.state["run_id"] is None

    def test_load_persisted_state(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("persist", ["g1", "g2"])
        mgr.mark_completed("g1")
        mgr2 = RecoveryManager(str(path))
        assert mgr2.state["run_id"] == "persist"
        assert mgr2.state["completed"] == ["g1"]
        assert mgr2.state["pending"] == ["g2"]

    def test_load_corrupted_file(self, tmp_path: Path):
        path = tmp_path / "bad.json"
        path.write_text("{corrupt")
        mgr = RecoveryManager(str(path))
        assert mgr.state["run_id"] is None

    def test_clear_resets_state(self, tmp_path: Path):
        path = str(tmp_path / "checkpoint.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("test", ["g1"])
        mgr.clear()
        assert mgr.get_pending_targets() == []
        assert mgr.state["completed"] == []
        assert mgr.state["failed"] == {}


class TestDetailRecoveryQueue:
    def test_detail_recovery_queue_retries_after_cooldown(self, tmp_path: Path):
        path = str(tmp_path / "detail_queue.json")
        mgr = RecoveryManager(path)
        base = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)

        mgr._utc_now = lambda: base  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260607", "20260607ABC")
        assert mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=30) == []

        mgr._utc_now = lambda: base + timedelta(minutes=31)  # type: ignore[method-assign]
        assert mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=30) == ["20260607ABC"]

        mgr.mark_detail_recovery_success("20260607", "20260607ABC")
        assert mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=1) == []

    def test_detail_recovery_queue_prunes_stale_entries(self, tmp_path: Path):
        path = str(tmp_path / "detail_queue.json")
        mgr = RecoveryManager(path)
        old = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
        fresh = datetime(2026, 6, 7, 0, 0, tzinfo=UTC)

        mgr._utc_now = lambda: old  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260601", "20260601ABC")
        mgr._utc_now = lambda: fresh  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260607", "20260607ABC")

        mgr.purge_detail_recovery_queue(max_age_days=2)
        assert mgr.get_due_detail_recovery_targets("20260601", cooldown_minutes=0) == []


class TestInitializeRunMergesNewTargets:
    """Issue #55: a checkpoint that never clears must not hide new anomalies."""

    def test_same_run_adds_new_targets_to_pending(self, tmp_path: Path):
        mgr = RecoveryManager(str(tmp_path / "checkpoint.json"))
        mgr.initialize_run("run1", ["g1", "g2"])
        mgr.mark_completed("g1")
        mgr.mark_failed("g2", "missing")

        mgr.initialize_run("run1", ["g2", "g3"])

        assert mgr.get_pending_targets() == ["g3"]
        assert mgr.state["completed"] == ["g1"]
        assert mgr.state["failed"] == {"g2": "missing"}

    def test_retry_ids_reopen_failed_targets(self, tmp_path: Path):
        path = tmp_path / "checkpoint.json"
        mgr = RecoveryManager(str(path))
        mgr.initialize_run("run1", ["g1"])
        mgr.mark_failed("g1", "fetch_404")

        reloaded = RecoveryManager(str(path))
        reloaded.initialize_run("run1", ["g1"], retry_ids=["g1"])

        assert reloaded.get_pending_targets() == ["g1"]
        assert reloaded.state["failed"] == {}

    def test_retry_ids_outside_targets_are_ignored(self, tmp_path: Path):
        mgr = RecoveryManager(str(tmp_path / "checkpoint.json"))
        mgr.initialize_run("run1", ["g1"])
        mgr.mark_failed("g1", "fetch_404")

        mgr.initialize_run("run1", ["g2"], retry_ids=["g1"])

        assert mgr.get_pending_targets() == ["g2"]
        assert mgr.state["failed"] == {"g1": "fetch_404"}


class TestInitializeRunPrunesMissingTargets:
    """PR #56 review: stale checkpoint entries are dropped on automatic runs."""

    def test_prune_missing_drops_ids_outside_targets(self, tmp_path: Path):
        mgr = RecoveryManager(str(tmp_path / "checkpoint.json"))
        mgr.initialize_run("run1", ["g1", "g2", "g3", "g4"])
        mgr.mark_completed("g1")
        mgr.mark_failed("g2", "missing")
        mgr.mark_failed("g4", "still stuck")

        mgr.initialize_run("run1", ["g4", "g5"], prune_missing=True)

        assert mgr.get_pending_targets() == ["g5"]
        assert mgr.state["completed"] == []
        assert mgr.state["failed"] == {"g4": "still stuck"}
        assert mgr.state["total_count"] == 2

    def test_prune_missing_keeps_retry_ids_retryable(self, tmp_path: Path):
        mgr = RecoveryManager(str(tmp_path / "checkpoint.json"))
        mgr.initialize_run("run1", ["old", "recent"])
        mgr.mark_failed("old", "x")
        mgr.mark_failed("recent", "fetch_404")

        mgr.initialize_run("run1", ["old", "recent"], retry_ids=["recent"], prune_missing=True)

        assert mgr.get_pending_targets() == ["recent"]
        assert mgr.state["failed"] == {"old": "x"}

    def test_without_prune_missing_entries_are_kept(self, tmp_path: Path):
        mgr = RecoveryManager(str(tmp_path / "checkpoint.json"))
        mgr.initialize_run("run1", ["g1", "g2"])
        mgr.mark_failed("g1", "missing")

        mgr.initialize_run("run1", ["g3"])

        assert mgr.get_pending_targets() == ["g2", "g3"]
        assert mgr.state["failed"] == {"g1": "missing"}


class TestMarkCompletedClearsFailure:
    def test_mark_completed_removes_failed_entry(self, tmp_path: Path):
        path = tmp_path / "checkpoint.json"
        mgr = RecoveryManager(str(path))
        mgr.initialize_run("run1", ["g1"])
        mgr.mark_failed("g1", "fetch_404")
        mgr.state["pending"].append("g1")

        mgr.mark_completed("g1")

        reloaded = RecoveryManager(str(path))
        assert reloaded.state["completed"] == ["g1"]
        assert reloaded.state["failed"] == {}
        assert reloaded.get_pending_targets() == []
