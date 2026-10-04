"""Coverage for the ``urls=`` spelling of the DB fail-fast gate.

``urls=`` exists for exactly one job today (``sync_rag_incremental_job``, which
reaches the RAG vector/sparse store alongside the operational database), and that
spelling had no test. Nothing asserted that the *named* URLs reach the probe
rather than the operational database, so a regression quietly collapsing every
gated job back to a single-URL probe would have passed the suite.

The memo semantics matter for the same reason: a failure is cached for
``DB_GATE_FAILURE_COOLDOWN_SECONDS`` so one outage costs one query per window,
while a success must not be cached, or recovery would wait out the cooldown before
the next tick noticed.
"""

from __future__ import annotations

import pytest

from src.scheduler import locks
from src.scheduler.locks import _DbGate, _reset_db_gate, _with_db_fail_fast_guard

OPERATIONAL = "postgresql://db.internal/kbo"
VECTOR = "postgresql://vectors.internal/kbo"


@pytest.fixture(autouse=True)
def _clean_gate() -> None:
    _reset_db_gate()


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Record every ``urls`` tuple the gate asks about, answering True."""
    seen: list[tuple[str, ...]] = []

    def _probe(*, urls: tuple[str, ...] | None = None) -> bool:
        seen.append(urls if urls is not None else ())
        return True

    monkeypatch.setattr(locks, "database_reachable", _probe)
    return seen


def test_urls_form_probes_the_named_targets(recorded: list[tuple[str, ...]]) -> None:
    """The named tuple must reach the probe, not the operational database."""

    @_with_db_fail_fast_guard(urls=lambda: (OPERATIONAL, VECTOR))
    def job() -> str:
        return "ran"

    assert job() == "ran"
    assert recorded == [(OPERATIONAL, VECTOR)]


def test_bare_form_probes_only_the_operational_database(
    monkeypatch: pytest.MonkeyPatch,
    recorded: list[tuple[str, ...]],
) -> None:
    """The bare spelling keeps the default of one operational URL."""

    @_with_db_fail_fast_guard
    def job() -> str:
        return "ran"

    assert job() == "ran"
    assert len(recorded[0]) == 1


def test_a_dead_named_target_skips_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """A split-store outage must skip even though the operational database is up.

    This is the case ``urls=`` was added for: the job holds ``MAINTENANCE_LOCK``
    and its census opens ``RAG_INDEX_DB_URL`` through a subprocess with a 1800s
    timeout, so discovering the dead store after taking the lock blocks every other
    maintenance job for the window.
    """
    ran: list[str] = []

    def _probe(*, urls: tuple[str, ...] | None = None) -> bool:
        # Operational answers; the vector store does not.
        return bool(urls) and VECTOR not in urls

    monkeypatch.setattr(locks, "database_reachable", _probe)

    @_with_db_fail_fast_guard(urls=lambda: (OPERATIONAL, VECTOR))
    def job() -> str:
        ran.append("ran")
        return "ran"

    assert job() is None
    assert ran == []


def test_a_dead_operational_database_also_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    """Naming a second store must not weaken the operational check."""
    ran: list[str] = []
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)

    @_with_db_fail_fast_guard(urls=lambda: (OPERATIONAL, VECTOR))
    def job() -> str:
        ran.append("ran")
        return "ran"

    assert job() is None
    assert ran == []


def test_success_is_not_memoised_so_recovery_is_immediate(recorded: list[tuple[str, ...]]) -> None:
    """Recovery has to show up on the next tick, not after a cooldown."""

    @_with_db_fail_fast_guard(urls=lambda: (OPERATIONAL,))
    def job() -> str:
        return "ran"

    job()
    job()
    assert len(recorded) == 2


def test_failure_is_memoised_for_the_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    """One outage must cost one probe per window, not one per job tick."""
    probes: list[int] = []
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: probes.append(1) or False)
    gate = _DbGate(cooldown_seconds=30)

    assert gate.check((OPERATIONAL,)) is False
    assert gate.check((OPERATIONAL,)) is False
    assert len(probes) == 1
    assert gate.probe_count == 1


def test_an_empty_target_set_still_probes_the_operational_database(recorded: list[tuple[str, ...]]) -> None:
    """No split store configured must degrade to the operational probe, not to none.

    ``planned_rag_target_urls`` returns an empty set when there is no dense target
    at all, and ``database_reachable`` treats a falsy ``urls`` as "the operational
    database". Both halves matter: an empty set that probed nothing would let the
    one ``urls=`` job run blind, and a non-empty set that skipped would silence it.
    """

    @_with_db_fail_fast_guard(urls=lambda: ())
    def job() -> str:
        return "ran"

    assert job() == "ran"
    # The gate forwarded the empty tuple; database_reachable substituted the
    # operational URL, so the operational database is still the thing checked.
    assert recorded == [()]
