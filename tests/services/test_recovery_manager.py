from __future__ import annotations

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
