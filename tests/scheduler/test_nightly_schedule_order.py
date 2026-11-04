"""The nightly window's order is a contract, not a preference.

Almost every job from 00:00 to 02:40 takes ``MAINTENANCE_LOCK``
(``crawl_daily_games`` takes ``DAILY_LOCK``), and a job that cannot get its tier
lock within 60s is *skipped*, not queued (``src/scheduler/locks.py``). A job that
starts before its input exists therefore does not run late -- it returns having
done nothing, and the only trace is one warning line. Moving a single entry in
this window is a data-freshness change with no failing gate behind it, which is
what these tests exist to prevent.

The schedule is read the way the scheduler reads it: ``tier2_jobs`` is a local
list inside ``_start_scheduler``, so registering against a fake scheduler is the
only reading that sees both tiers plus the inline entries, and the only one that
cannot drift from what actually runs.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from apscheduler.triggers.cron import CronTrigger

import scripts.scheduler as scripts_scheduler
from src.scheduler import registry

#: The nightly pipeline, in the order its inputs are produced: the previous KST
#: day is finalized first, the derived aggregates read that finalize, and the RAG
#: publish reads the aggregates.
NIGHTLY_CHAIN: tuple[str, ...] = (
    "crawl_daily_games",
    "compute_standings",
    "aggregate_team_defense",
    "compute_rankings",
    "auto_heal_games",
    "heal_pbp",
    "data_integrity_check",
    "sync_rag_incremental",
    "source_coverage",
    "rag_vector_backfill",
    "sparse_terms_catchup",
)

#: Weekly/monthly jobs that share ``MAINTENANCE_LOCK`` with the chain. They ran
#: ahead of it (02:00-03:00) until the chain moved to midnight; run there now,
#: they would start while the chain holds the lock and be skipped after 60s.
AFTER_THE_CHAIN: tuple[str, ...] = (
    "backup_db_weekly",
    "relay_state_cleanup_weekly",
    "cleanup_stale_data_weekly",
    "notification_retention_weekly",
    "trim_scheduler_logs_weekly",
    "crawl_retired_players",
    "crawl_monthly_unified_audit",
)

#: Gates that judge the RAG publish. They may run later than it; never earlier.
RAG_GATES: tuple[str, ...] = (
    "rag_audit_sentinel",
    "rag_coverage_sentinel",
)

#: The chain has to be clear of the weekly block, which starts at 03:00 Sunday.
WEEKLY_BLOCK_START: tuple[int, int] = (3, 0)


class _FakeScheduler:
    """Collects what the registry registers, without starting anything."""

    def __init__(self, added: dict[str, dict[str, object]]) -> None:
        """Keep the caller's accumulator so the fixture can read it afterwards."""
        self._added = added

    def add_job(self, fn: object, **kwargs: object) -> None:
        """Record one registration by its job id."""
        job_id = kwargs.get("id")
        if job_id:
            self._added[str(job_id)] = kwargs

    def add_listener(self, *_args: object, **_kwargs: object) -> None:
        """Accept APScheduler's event listener registration."""
        return

    def start(self) -> None:
        """Do nothing: the real call blocks forever."""
        return


@pytest.fixture(scope="module")
def registered_jobs() -> dict[str, dict[str, object]]:
    """Register the real schedule against a fake scheduler, once for this module.

    ``scripts.scheduler`` is patched rather than only the registry global, because
    ``_start_scheduler`` resolves the class through ``sys.modules``: patching one
    side lets the real blocking scheduler start and hang the run instead of
    failing it.
    """
    added: dict[str, dict[str, object]] = {}
    previous_ref = registry._SCHEDULER_REF
    try:
        with (
            patch.object(scripts_scheduler, "BlockingScheduler", lambda **_kw: _FakeScheduler(added)),
            patch.object(registry, "BlockingScheduler", lambda **_kw: _FakeScheduler(added)),
            patch.dict(os.environ, {"STARTUP_RUN": "0"}),
            patch("signal.signal"),
        ):
            registry._SCHEDULER_REF = None
            registry._start_scheduler(registry.build_arg_parser().parse_args(["--no-startup-run"]))
    finally:
        registry._SCHEDULER_REF = previous_ref

    return added


def _literal(kwargs: dict[str, object], job_id: str, name: str) -> int:
    """Return one cron field's literal value, refusing wildcards, ranges and steps.

    APScheduler compiles a single value ``X`` into ``RangeExpression(X)``, a range
    ``a-b`` into ``RangeExpression(a, b)`` and a step into ``RangeExpression(None,
    None, step)``, so "one specific value" is ``first == last``. Anything else
    makes every claim in this module vacuous by comparison rather than by
    assertion, so it fails here instead.
    """
    trigger = kwargs["trigger"]
    assert isinstance(trigger, CronTrigger), f"{job_id} is not on a cron trigger: {trigger!r}"
    fields = [field for field in trigger.fields if field.name == name]
    assert len(fields) == 1, f"{job_id} has {len(fields)} {name} fields"
    expression = fields[0].expressions[0]
    first, last = getattr(expression, "first", None), getattr(expression, "last", None)

    assert isinstance(first, int) and first == last, (
        f"{job_id} does not start at a single literal {name} ({expression!r}); "
        "its position in the nightly window cannot be read"
    )
    return first


def _slot(registered: dict[str, dict[str, object]], job_id: str) -> tuple[int, int]:
    """Return ``(hour, minute)`` for a registered job, insisting on literals."""
    kwargs = registered[job_id]
    return _literal(kwargs, job_id, "hour"), _literal(kwargs, job_id, "minute")


def _runs_on_sunday(registered: dict[str, dict[str, object]], job_id: str) -> bool:
    """Return whether the registered trigger names Sunday as its day of week."""
    return "day_of_week='sun'" in str(registered[job_id]["trigger"])


class TestTheNightlyChain:
    def test_it_starts_at_midnight(self, registered_jobs: dict[str, dict[str, object]]) -> None:
        """The operator's window: the nightly pipeline begins when the KST day turns."""
        assert _slot(registered_jobs, "crawl_daily_games") == (0, 0)

    def test_every_job_starts_after_the_one_it_reads(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        slots = [_slot(registered_jobs, job_id) for job_id in NIGHTLY_CHAIN]

        assert slots == sorted(slots), (
            f"the chain out of order: {dict(zip(NIGHTLY_CHAIN, slots, strict=True))}. "
            "A job that starts before its input exists is skipped by the tier lock, not queued."
        )

    def test_no_two_chain_jobs_share_a_slot(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        """Two jobs on one slot means one of them loses the lock race and skips."""
        slots = [_slot(registered_jobs, job_id) for job_id in NIGHTLY_CHAIN]

        duplicated = sorted({slot for slot in slots if slots.count(slot) > 1})

        assert not duplicated, f"these slots start two chain jobs: {duplicated}"

    def test_the_window_is_clear_before_the_weekly_block(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        last = max(_slot(registered_jobs, job_id) for job_id in NIGHTLY_CHAIN)

        assert last < WEEKLY_BLOCK_START, (
            f"the chain runs until {last}, into the weekly block that starts at {WEEKLY_BLOCK_START}"
        )


class TestTheRagPart:
    def test_the_publish_precedes_its_catch_up(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        """Catch-up is an insert-only resume after the last published chunk."""
        publish = _slot(registered_jobs, "sync_rag_incremental")
        catch_up = _slot(registered_jobs, "sparse_terms_catchup")

        assert publish < catch_up, (
            f"catch-up ({catch_up}) runs before the publish it resumes ({publish}), so it resumes nothing"
        )

    def test_the_gates_read_a_finished_window(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        """A gate may run later than the pipeline; never earlier, or it judges a partial index."""
        catch_up = _slot(registered_jobs, "sparse_terms_catchup")

        for job_id in RAG_GATES:
            assert _slot(registered_jobs, job_id) > catch_up, (
                f"{job_id} starts before the RAG window it judges has finished"
            )

    def test_the_source_coverage_check_follows_the_integrity_check(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        """`source_coverage_job` explains integrity findings, so it reads them first."""
        integrity = _slot(registered_jobs, "data_integrity_check")
        coverage = _slot(registered_jobs, "source_coverage")

        assert integrity < coverage, f"source coverage ({coverage}) runs before integrity ({integrity})"


class TestTheJobsThatShareTheMaintenanceLock:
    def test_every_shared_lock_job_starts_after_the_chain(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        chain_last = max(_slot(registered_jobs, job_id) for job_id in NIGHTLY_CHAIN)

        early = {
            job_id: _slot(registered_jobs, job_id)
            for job_id in AFTER_THE_CHAIN
            if _slot(registered_jobs, job_id) <= chain_last
        }

        assert not early, (
            f"these jobs start inside the chain's window ({early}); they take MAINTENANCE_LOCK too, "
            "so they would be skipped after the 60s timeout"
        )

    def test_the_sunday_block_never_stacks_two_jobs_on_one_slot(
        self,
        registered_jobs: dict[str, dict[str, object]],
    ) -> None:
        sunday_slots = [
            _slot(registered_jobs, job_id) for job_id in AFTER_THE_CHAIN if _runs_on_sunday(registered_jobs, job_id)
        ]

        duplicated = sorted({slot for slot in sunday_slots if sunday_slots.count(slot) > 1})

        assert sunday_slots, "no Sunday job is registered, so this contract cannot be tested"
        assert not duplicated, f"these Sunday slots start two maintenance jobs: {duplicated}"
