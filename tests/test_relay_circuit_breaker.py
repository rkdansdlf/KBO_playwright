from __future__ import annotations

import asyncio
import csv
import time
from pathlib import Path

from src.sources.relay.base import NormalizedRelayResult
from src.sources.relay.circuit_breaker import SourceCircuitBreaker
from src.sources.relay.orchestrator import RelayRecoveryOrchestrator

# ===================================================================
# SourceCircuitBreaker unit tests
# ===================================================================


class TestSourceCircuitBreaker:
    def test_starts_available(self):
        cb = SourceCircuitBreaker()
        assert cb.is_available("naver", "2025_regular_kbo") is True
        assert cb.consecutive_failures("naver", "2025_regular_kbo") == 0

    def test_available_below_threshold(self):
        cb = SourceCircuitBreaker(threshold=3)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        assert cb.is_available("naver", "bucket1") is True
        assert cb.consecutive_failures("naver", "bucket1") == 2

    def test_opens_after_threshold(self):
        cb = SourceCircuitBreaker(threshold=3, cooldown_seconds=60.0)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        assert cb.is_available("naver", "bucket1") is False
        assert cb.is_open("naver", "bucket1") is True
        assert cb.consecutive_failures("naver", "bucket1") == 3

    def test_resets_after_cooldown(self):
        cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=0.05)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        assert cb.is_available("naver", "bucket1") is False
        time.sleep(0.06)
        assert cb.is_available("naver", "bucket1") is True
        assert cb.consecutive_failures("naver", "bucket1") == 0

    def test_success_resets_counter(self):
        cb = SourceCircuitBreaker(threshold=3)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        cb.record_success("naver", "bucket1")
        assert cb.consecutive_failures("naver", "bucket1") == 0
        assert cb.is_available("naver", "bucket1") is True

    def test_per_source_bucket_isolation(self):
        cb = SourceCircuitBreaker(threshold=2)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        assert cb.is_available("naver", "bucket1") is False
        assert cb.is_available("naver", "bucket2") is True
        assert cb.is_available("kbo", "bucket1") is True

    def test_reset_all(self):
        cb = SourceCircuitBreaker(threshold=1)
        cb.record_failure("n", "b1")
        cb.record_failure("n", "b2")
        assert cb.is_open("n", "b1")
        assert cb.is_open("n", "b2")
        cb.reset_all()
        assert cb.is_available("n", "b1") is True
        assert cb.is_available("n", "b2") is True

    def test_auto_reset_after_cooldown_clears_failures(self):
        cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=0.05)
        cb.record_failure("n", "b")
        cb.record_failure("n", "b")
        assert cb.consecutive_failures("n", "b") == 2
        assert cb.is_available("n", "b") is False
        time.sleep(0.06)
        assert cb.is_available("n", "b") is True
        assert cb.consecutive_failures("n", "b") == 0


# ===================================================================
# Orchestrator integration tests (no real HTTP)
# ===================================================================


class _OkAdapter:
    source_name = "ok"
    supports_bucket_probe = True
    cache_negative_probe = True

    async def fetch_game(self, game_id):
        return NormalizedRelayResult(
            game_id=game_id,
            source_name="ok",
            events=[{"dummy": True}],
            raw_pbp_rows=[],
            has_event_state=True,
            has_raw_pbp=False,
        )


class _EmptyAdapter:
    source_name = "empty"
    supports_bucket_probe = True
    cache_negative_probe = True

    async def fetch_game(self, game_id):
        return NormalizedRelayResult(
            game_id=game_id,
            source_name="empty",
            notes="always empty",
        )


class _TimeoutAdapter:
    source_name = "slow"
    supports_bucket_probe = True
    cache_negative_probe = True

    async def fetch_game(self, game_id):
        await asyncio.sleep(10)
        return NormalizedRelayResult(game_id=game_id, source_name="slow")


class _FailingAdapter:
    source_name = "fail"
    supports_bucket_probe = True
    cache_negative_probe = True

    async def fetch_game(self, game_id):
        msg = f"simulated failure for {game_id}"
        raise RuntimeError(msg)


class TestOrchestratorCircuitBreakerIntegration:
    """Verifies that the circuit breaker is checked during fetch_game and that
    sources on cooldown are skipped with status='cb_open'."""

    def _run_fetch(self, orch, game_id, bucket_id, source_order, validator=None):
        return asyncio.run(orch.fetch_game(game_id, bucket_id, source_order, validator=validator))

    def test_successful_source_records_success_and_returns(self):
        cb = SourceCircuitBreaker(threshold=2)
        orch = RelayRecoveryOrchestrator(
            adapters={"ok": _OkAdapter()},
            capability_path="/dev/null",
            circuit_breaker=cb,
        )
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["ok"])
        assert not result.is_empty
        assert cb.consecutive_failures("ok", "bucket1") == 0

    def test_empty_source_records_failure(self):
        cb = SourceCircuitBreaker(threshold=2)
        orch = RelayRecoveryOrchestrator(
            adapters={"empty": _EmptyAdapter()},
            capability_path="/dev/null",
            circuit_breaker=cb,
        )
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["empty"])
        assert result.is_empty
        assert cb.consecutive_failures("empty", "bucket1") == 1

    def test_timeout_source_records_failure(self):
        cb = SourceCircuitBreaker(threshold=1)
        orch = RelayRecoveryOrchestrator(
            adapters={"slow": _TimeoutAdapter()},
            capability_path="/dev/null",
            timeout_seconds=0.05,
            circuit_breaker=cb,
        )
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["slow"])
        assert result.is_empty
        assert cb.consecutive_failures("slow", "bucket1") == 1

    def test_source_skipped_when_circuit_open(self):
        """When a source exceeds threshold, the orchestrator should emit
        cb_open attempt records instead of fetching from it."""
        cb = SourceCircuitBreaker(threshold=1)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        assert cb.is_open("naver", "bucket1")

        orch = RelayRecoveryOrchestrator(
            adapters={"naver": _OkAdapter(), "ok": _OkAdapter()},
            capability_path="/dev/null",
            circuit_breaker=cb,
        )
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["naver", "ok"])
        assert not result.is_empty
        assert attempts[0]["status"] == "cb_open"
        assert attempts[1]["status"] == "success"
        assert result.source_name == "ok"

    def test_circuit_breaker_none_skips_cb_check(self):
        """Passing circuit_breaker=None should not cause cb checks."""
        orch = RelayRecoveryOrchestrator(
            adapters={"empty": _EmptyAdapter()},
            capability_path="/dev/null",
            circuit_breaker=None,
        )
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["empty"])
        assert result.is_empty
        assert len(attempts) == 1

    def test_circuit_breaker_not_passed_uses_default(self):
        """Omitting circuit_breaker should create a default one (source still works)."""
        orch = RelayRecoveryOrchestrator(
            adapters={"ok": _OkAdapter()},
            capability_path="/dev/null",
        )
        assert orch.circuit_breaker is not None
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["ok"])
        assert not result.is_empty

    def test_validator_failure_records_cb_failure(self):
        """If validator rejects the result, the source should be recorded as failure."""
        cb = SourceCircuitBreaker(threshold=2)
        orch = RelayRecoveryOrchestrator(
            adapters={"ok": _OkAdapter(), "empty": _EmptyAdapter()},
            capability_path="/dev/null",
            circuit_breaker=cb,
        )

        def reject_all(res):
            return "rejected"

        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["ok", "empty"], validator=reject_all)
        assert result.is_empty
        assert cb.consecutive_failures("ok", "bucket1") == 1

    def test_exception_source_records_failure(self):
        """If an adapter raises an exception (not timeout), the cb records a failure."""
        cb = SourceCircuitBreaker(threshold=1)
        orch = RelayRecoveryOrchestrator(
            adapters={"fail": _FailingAdapter()},
            capability_path="/dev/null",
            circuit_breaker=cb,
        )
        result, attempts = self._run_fetch(orch, "g1", "bucket1", ["fail"])
        assert result.is_empty
        assert cb.consecutive_failures("fail", "bucket1") == 1


# ===================================================================
# CSV persistence tests
# ===================================================================


class TestCircuitBreakerPersistence:
    """Verify that SourceCircuitBreaker can save/load state from CSV."""

    def test_no_persist_path_skips_io(self):
        cb = SourceCircuitBreaker(threshold=3)
        assert cb._persist_path is None

    def test_persist_not_exists_loads_clean(self, tmp_path: Path):
        p = tmp_path / "nonexistent.csv"
        cb = SourceCircuitBreaker(persist_path=p)
        assert not p.exists()
        assert cb.consecutive_failures("n", "b") == 0

    def test_save_and_reload_failures(self, tmp_path: Path):
        p = tmp_path / "cb_state.csv"
        cb = SourceCircuitBreaker(threshold=3, persist_path=p)
        cb.record_failure("naver", "bucket1")
        cb.record_failure("naver", "bucket1")
        cb.record_failure("kbo", "bucket2")
        assert p.exists()

        # Read raw CSV to confirm format
        rows = list(csv.DictReader(p.open("r", newline="")))
        assert len(rows) == 2
        row_by_source = {r["source_name"]: r for r in rows}
        assert row_by_source["naver"]["bucket_id"] == "bucket1"
        assert row_by_source["naver"]["failures"] == "2"
        assert row_by_source["kbo"]["failures"] == "1"
        # Not in cooldown yet
        assert row_by_source["naver"]["cooldown_until_epoch"] == ""

        # Reload from same CSV
        cb2 = SourceCircuitBreaker(threshold=3, persist_path=p)
        assert cb2.consecutive_failures("naver", "bucket1") == 2
        assert cb2.consecutive_failures("kbo", "bucket2") == 1
        assert cb2.consecutive_failures("naver", "bucket2") == 0

    def test_save_and_reload_cooldown(self, tmp_path: Path):
        p = tmp_path / "cb_state.csv"
        cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=300, persist_path=p)
        cb.record_failure("naver", "b1")
        cb.record_failure("naver", "b1")
        assert cb.is_open("naver", "b1")

        rows = list(csv.DictReader(p.open("r", newline="")))
        assert len(rows) == 1
        epoch = rows[0]["cooldown_until_epoch"]
        assert epoch != ""
        assert float(epoch) > time.time() + 250  # roughly 300s from now

        cb2 = SourceCircuitBreaker(threshold=2, cooldown_seconds=300, persist_path=p)
        assert cb2.is_open("naver", "b1")
        assert cb2.consecutive_failures("naver", "b1") == 2

    def test_expired_cooldown_not_restored(self, tmp_path: Path):
        p = tmp_path / "cb_state.csv"
        cb = SourceCircuitBreaker(threshold=1, cooldown_seconds=0.05, persist_path=p)
        cb.record_failure("n", "b")
        assert cb.is_open("n", "b")

        time.sleep(0.06)

        # Reload — cooldown should be expired (not restored)
        cb2 = SourceCircuitBreaker(threshold=1, persist_path=p)
        assert cb2.is_available("n", "b")
        assert cb2.consecutive_failures("n", "b") == 0

    def test_success_clears_persisted_state(self, tmp_path: Path):
        p = tmp_path / "cb_state.csv"
        cb = SourceCircuitBreaker(threshold=2, persist_path=p)
        cb.record_failure("n", "b")
        cb.record_failure("n", "b")
        assert len(list(csv.DictReader(p.open("r", newline="")))) == 1

        cb.record_success("n", "b")
        rows = list(csv.DictReader(p.open("r", newline="")))
        assert len(rows) == 0  # cleared from CSV

    def test_reset_all_clears_persisted_state(self, tmp_path: Path):
        p = tmp_path / "cb_state.csv"
        cb = SourceCircuitBreaker(threshold=1, persist_path=p)
        cb.record_failure("n", "b1")
        cb.record_failure("n", "b2")
        assert len(list(csv.DictReader(p.open("r", newline="")))) == 2

        cb.reset_all()
        rows = list(csv.DictReader(p.open("r", newline="")))
        assert len(rows) == 0

    def test_corrupted_csv_falls_back_clean(self, tmp_path: Path):
        p = tmp_path / "cb_state.csv"
        p.write_text("garbage,no,header\n")
        cb = SourceCircuitBreaker(persist_path=p)
        # Should start fresh without crashing
        assert cb.consecutive_failures("n", "b") == 0

    def test_cooldown_carryover_after_restart(self, tmp_path: Path):
        """Simulate a crash during cooldown: the remaining cooldown time
        should carry over to the new process."""
        p = tmp_path / "cb_state.csv"
        cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=10, persist_path=p)
        cb.record_failure("n", "b")
        cb.record_failure("n", "b")
        assert cb.is_open("n", "b")

        # Simulate reading what was written
        rows = list(csv.DictReader(p.open("r", newline="")))
        epoch = float(rows[0]["cooldown_until_epoch"])
        assert epoch > time.time() + 8  # ~10s from now

        cb2 = SourceCircuitBreaker(threshold=2, cooldown_seconds=10, persist_path=p)
        # Should still be open (cooldown hasn't expired yet since we only waited ~0s)
        assert cb2.is_open("n", "b")

    def test_can_disable_persistence(self):
        """persist_path=None (default) should not create any file."""
        cb = SourceCircuitBreaker()
        cb.record_failure("n", "b")
        assert cb._persist_path is None
