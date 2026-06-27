from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.services.recovery_manager import RecoveryManager


class TestParseIsoDatetime:
    def test_none_input(self):
        assert RecoveryManager._parse_iso_datetime(None) is None

    def test_non_string_input(self):
        assert RecoveryManager._parse_iso_datetime(12345) is None

    def test_invalid_string(self):
        assert RecoveryManager._parse_iso_datetime("not-a-date") is None

    def test_naive_datetime_gets_utc(self):
        result = RecoveryManager._parse_iso_datetime("2026-06-07T10:00:00")
        assert result is not None
        assert result.tzinfo == UTC

    def test_aware_datetime_preserved(self):
        original = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)
        result = RecoveryManager._parse_iso_datetime(original.isoformat())
        assert result == original


class TestDetailQueueKey:
    def test_key_format(self):
        key = RecoveryManager._detail_queue_key("20260607", "20260607DBLT")
        assert key == "20260607:20260607DBLT"

    def test_split_key(self):
        date_part, game_id = RecoveryManager._split_detail_queue_key("20260607:20260607DBLT")
        assert date_part == "20260607"
        assert game_id == "20260607DBLT"

    def test_split_key_no_colon(self):
        date_part, game_id = RecoveryManager._split_detail_queue_key("no-colon-here")
        assert date_part == ""
        assert game_id == "no-colon-here"


class TestGetDetailRecoveryQueue:
    def test_returns_dict(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        queue = mgr._get_detail_recovery_queue()
        assert isinstance(queue, dict)

    def test_replaces_non_dict(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.state["detail_recovery_queue"] = "not-a-dict"
        queue = mgr._get_detail_recovery_queue()
        assert isinstance(queue, dict)
        assert queue == {}


class TestGetDueDetailRecoveryTargets:
    def test_empty_queue(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        assert mgr.get_due_detail_recovery_targets("20260607") == []

    def test_filters_by_date(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        base = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)
        mgr._utc_now = lambda: base  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        mgr.mark_detail_recovery_failure("20260608", "20260608DBLT")
        targets = mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=0)
        assert targets == ["20260607DBLT"]

    def test_skips_non_dict_entries(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        queue = mgr._get_detail_recovery_queue()
        queue["20260607:20260607DBLT"] = "not-a-dict"
        targets = mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=0)
        assert targets == []

    def test_skips_non_string_keys(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        queue = mgr._get_detail_recovery_queue()
        queue[12345] = {"last_failed_at": datetime.now(UTC).isoformat()}  # type: ignore[dict-item]
        targets = mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=0)
        assert targets == []

    def test_cooldown_zero_allows_immediate(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        base = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)
        mgr._utc_now = lambda: base  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        targets = mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=0)
        assert targets == ["20260607DBLT"]

    def test_cooldown_blocks_recent(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        base = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)
        mgr._utc_now = lambda: base  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        targets = mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=30, now=base + timedelta(minutes=10))
        assert targets == []

    def test_returns_sorted_unique(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        base = datetime(2026, 6, 7, 10, 0, tzinfo=UTC)
        mgr._utc_now = lambda: base  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260607", "20260607ZZZZ")
        mgr.mark_detail_recovery_failure("20260607", "20260607AAAA")
        targets = mgr.get_due_detail_recovery_targets("20260607", cooldown_minutes=0)
        assert targets == ["20260607AAAA", "20260607ZZZZ"]


class TestMarkDetailRecoverySuccess:
    def test_removes_from_queue(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        mgr.mark_detail_recovery_success("20260607", "20260607DBLT")
        queue = mgr._get_detail_recovery_queue()
        assert "20260607:20260607DBLT" not in queue

    def test_missing_key_no_error(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_success("20260607", "nonexistent")


class TestMarkDetailRecoveryFailure:
    def test_increments_attempts(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        queue = mgr._get_detail_recovery_queue()
        assert queue["20260607:20260607DBLT"]["attempts"] == 2

    def test_stores_reason(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT", failure_reason="timeout")
        queue = mgr._get_detail_recovery_queue()
        assert queue["20260607:20260607DBLT"]["reason"] == "timeout"

    def test_ignores_none_reason(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT", failure_reason=None)
        queue = mgr._get_detail_recovery_queue()
        assert "reason" not in queue["20260607:20260607DBLT"]

    def test_normalizes_game_id(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        queue = mgr._get_detail_recovery_queue()
        assert queue["20260607:20260607DBLT"]["game_id"] == "20260607DBLT"


class TestPurgeDetailRecoveryQueue:
    def test_empty_queue(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.purge_detail_recovery_queue()

    def test_removes_stale_entries(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        old = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
        mgr._utc_now = lambda: old  # type: ignore[method-assign]
        mgr.mark_detail_recovery_failure("20260601", "20260601DBLT")
        mgr._utc_now = lambda: datetime(2026, 6, 7, 0, 0, tzinfo=UTC)  # type: ignore[method-assign]
        mgr.purge_detail_recovery_queue(max_age_days=2)
        queue = mgr._get_detail_recovery_queue()
        assert "20260601:20260601DBLT" not in queue

    def test_removes_entries_without_timestamp(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        queue = mgr._get_detail_recovery_queue()
        queue["20260607:20260607DBLT"] = {"attempts": 1}
        mgr.purge_detail_recovery_queue(max_age_days=7)
        assert "20260607:20260607DBLT" not in queue

    def test_removes_non_dict_entries(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        queue = mgr._get_detail_recovery_queue()
        queue["20260607:20260607DBLT"] = "invalid"
        mgr.purge_detail_recovery_queue(max_age_days=7)
        assert "20260607:20260607DBLT" not in queue

    def test_keeps_recent_entries(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        mgr.purge_detail_recovery_queue(max_age_days=7)
        queue = mgr._get_detail_recovery_queue()
        assert "20260607:20260607DBLT" in queue

    def test_max_age_floor_at_one(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        mgr.purge_detail_recovery_queue(max_age_days=0)
        queue = mgr._get_detail_recovery_queue()
        assert "20260607:20260607DBLT" in queue


class TestInitializeRunPreservesDetailQueue:
    def test_new_run_keeps_queue(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.mark_detail_recovery_failure("20260607", "20260607DBLT")
        mgr.initialize_run("run1", ["g1"])
        queue = mgr._get_detail_recovery_queue()
        assert "20260607:20260607DBLT" in queue

    def test_same_run_keeps_queue(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.initialize_run("run1", ["g1"])
        mgr.state["detail_recovery_queue"]["test:key"] = {"attempts": 1}
        mgr.initialize_run("run1", ["g1"])
        assert "test:key" in mgr.state["detail_recovery_queue"]

    def test_replaces_invalid_queue(self, tmp_path: Path):
        path = str(tmp_path / "queue.json")
        mgr = RecoveryManager(path)
        mgr.state["detail_recovery_queue"] = "invalid"
        mgr.initialize_run("run1", ["g1"])
        assert isinstance(mgr.state["detail_recovery_queue"], dict)
