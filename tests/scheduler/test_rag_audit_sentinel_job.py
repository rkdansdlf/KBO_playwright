"""Tests for the RAG index audit sentinel job.

The job turns an audit exit code into either an incident or silence, so these
tests are mostly about *which* story it tells. Two ways it used to tell the
wrong one: it reported an unreachable vector store as an inconsistent index, and
it never cleared the warning it had opened.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from src.scheduler import locks
from src.scheduler.jobs.sentinel import (
    AUDIT_EXIT_STORE_UNREACHABLE,
    rag_audit_sentinel_job,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _reachable_gate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep the DB fail-fast gate out of these tests.

    The job is gated now, and the gate probes for real. Letting it do so here
    would make every assertion depend on the test database answering, and a
    memoised failure from another test could turn the whole file into vacuous
    passes. The gate's own behaviour is tested in ``test_db_fail_fast_jobs``.
    """
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()
    yield
    locks._reset_db_gate()


def _run(exit_code: int | None, exc: Exception | None = None) -> dict[str, Any]:
    captured: dict[str, Any] = {"warnings": [], "successes": [], "argv": None}

    def fake_main(argv: list[str]) -> int:
        captured["argv"] = list(argv)
        if exc is not None:
            raise exc
        return exit_code or 0

    def fake_warning(func_name: str, details: str | None = None) -> None:
        captured["warnings"].append((func_name, details))

    def fake_success(func_name: str, details: str | None = None) -> None:
        captured["successes"].append((func_name, details))

    with (
        patch("src.cli.rag.audit_rag_index.main", side_effect=fake_main),
        patch("src.scheduler.jobs.sentinel.alert_warning", side_effect=fake_warning),
        patch("src.scheduler.jobs.sentinel.alert_success", side_effect=fake_success),
    ):
        rag_audit_sentinel_job()
    return captured


def test_sentinel_passes_gate_flags_to_audit_cli() -> None:
    captured = _run(0)

    assert captured["argv"] == ["--require-nonempty", "--require-postings", "--json"]
    assert captured["warnings"] == []


class TestTheWarningIsRecovered:
    """A warning that never clears is a false signal the runbook cannot explain.

    ``alert_warning`` opens ``scheduler:rag_audit_sentinel:warning``, and nothing
    else owns that key -- the APScheduler lifecycle listener only resolves
    ``scheduler:<job_id>:failed``. So the only thing that can close it is this
    job noticing it passed.
    """

    def test_a_passing_audit_recovers_the_warning(self) -> None:
        captured = _run(0)

        assert captured["successes"] == [("rag_audit_sentinel", "RAG index audit passed")]
        assert captured["warnings"] == []

    def test_a_failure_does_not_report_success(self) -> None:
        """Otherwise a single bad run would be resolved by the next good one
        before anyone acted on it.
        """
        captured = _run(1)

        assert captured["successes"] == []
        assert len(captured["warnings"]) == 1

    def test_a_crash_does_not_report_success(self) -> None:
        captured = _run(None, exc=RuntimeError("Oracle unavailable"))

        assert captured["successes"] == []
        assert len(captured["warnings"]) == 1


class TestTheFailureMessageNamesTheRightSubsystem:
    """An unreachable store is not an inconsistent index.

    Telling an operator their chunks lack embeddings -- and to run a catch-up
    that writes to the store -- during an outage where the store is the problem
    sends them to the wrong half of the system.
    """

    def test_an_unreachable_store_is_not_reported_as_missing_embeddings(self) -> None:
        captured = _run(AUDIT_EXIT_STORE_UNREACHABLE)
        _name, details = captured["warnings"][0]

        assert "could not reach a vector backend" in details
        assert "was NOT examined" in details
        assert "reachability" in details

    def test_an_unreachable_store_does_not_recommend_a_write_it_cannot_do(self) -> None:
        """``--catch-up`` writes to the store that is down.

        The message may mention it to say it would fail; it must not offer it as
        the fix.
        """
        captured = _run(AUDIT_EXIT_STORE_UNREACHABLE)
        _name, details = captured["warnings"][0]

        assert "would fail too" in details

    @pytest.mark.parametrize("exit_code", [1, 3, 9])
    def test_an_inconsistent_index_keeps_the_index_message(self, exit_code: int) -> None:
        captured = _run(exit_code)
        _name, details = captured["warnings"][0]

        assert str(exit_code) in details
        assert "missing" in details
        assert "catch-up" in details
        assert "could not reach" not in details

    def test_the_store_code_matches_the_audit_contract(self) -> None:
        """The job restates the constant instead of importing it.

        Importing it would pull ``src.db.engine`` into every scheduler start,
        because the audit module builds the operational engine at import time.
        Restating is only safe while something checks the two still agree.
        """
        from src.cli.rag.audit_rag_index import EXIT_STORE_UNREACHABLE

        assert AUDIT_EXIT_STORE_UNREACHABLE == EXIT_STORE_UNREACHABLE


class TestTheGateRunsFirst:
    def test_a_dead_database_skips_before_the_audit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A full outage belongs to ``kbo_db_available``, not to this job.

        Without the gate the audit cannot reach the store, exits 2, and the
        operator is handed a RAG problem to investigate during a database
        outage.
        """
        monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)
        locks._reset_db_gate()
        called: list[int] = []

        with (
            patch("src.cli.rag.audit_rag_index.main", side_effect=lambda _argv: called.append(1)),
            patch("src.scheduler.jobs.sentinel.alert_warning") as warn,
        ):
            rag_audit_sentinel_job()

        assert called == [], "the audit ran while the database was unreachable"
        warn.assert_not_called()
