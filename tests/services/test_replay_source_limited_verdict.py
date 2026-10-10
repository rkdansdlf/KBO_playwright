"""A retry that never asked the source must not close the incident.

`_outcome_from_persisted_run` is where every replay handler ends, and it decided
success from the stored run's status alone. A policy skip is stored as `success`
-- deliberately, because nothing failed -- so a retried letter resolved as
recovered while nothing had been collected (BUG-016). The distinction is the
same one BUG-014 drew for freshness: "the source had nothing" and "we never
asked" are not the same answer.

The non-retryable classification is as load-bearing as the verdict.
`finalize_retry` picks the next status from `success` and the remaining budget,
so a policy skip reported as an ordinary failure would be retried until the
budget ran out -- measured as 91 letters reaching `exhausted` while no retry
could have succeeded.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.source_limited import SOURCE_LIMITED_OUTCOME
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.crawl_execution import (
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_SUCCESS,
    CrawlExecutionRun,
)
from src.services import crawl_replay_dispatcher as dispatcher
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_retry_policy import NON_RETRYABLE_CODES

if TYPE_CHECKING:
    from collections.abc import Iterator

_RUN_ID = "replay-run-1"


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> Iterator[sessionmaker]:
    """Point the verdict at an isolated ledger holding only the run table."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(dispatcher, "SessionLocal", factory)
    yield factory
    engine.dispose()


def _store_run(factory: sessionmaker, *, status: str, checkpoint: object = None, **overrides: object) -> None:
    fields: dict[str, object] = {
        "run_id": _RUN_ID,
        "crawler": "preview",
        "target_type": "preview_date",
        "status": status,
        "started_at": datetime(2026, 10, 10, 3, 0, 0),
        "records_read": 0,
        "records_written": 0,
        "records_failed": 0,
        "checkpoint": checkpoint,
    }
    fields.update(overrides)
    with factory() as session:
        session.add(CrawlExecutionRun(**fields))  # type: ignore[arg-type]
        session.commit()


class TestAPolicySkipIsNotARecovery:
    """The verdict the incident depends on."""

    def test_a_skipped_replay_is_not_success(self, ledger: sessionmaker) -> None:
        _store_run(
            ledger,
            status=RUN_STATUS_SUCCESS,
            checkpoint={"outcome": SOURCE_LIMITED_OUTCOME, "reason": "kbo_robots_blocked"},
        )

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.success is False

    def test_it_says_the_source_was_not_consulted(self, ledger: sessionmaker) -> None:
        """The status names the reason, so a reader is not left guessing.

        `success` would have been a lie; `failed` would send someone looking for a
        fault. The status says what actually happened.
        """
        _store_run(
            ledger,
            status=RUN_STATUS_SUCCESS,
            checkpoint={"outcome": SOURCE_LIMITED_OUTCOME, "reason": "compliance_blocked"},
        )

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.status == SOURCE_LIMITED_OUTCOME
        assert "compliance_blocked" in (outcome.error_message or "")
        assert "not recovered" in (outcome.error_message or "")

    def test_the_error_code_does_not_spend_the_retry_budget(self, ledger: sessionmaker) -> None:
        """A retryable code here is what produced the exhausted pile.

        `finalize_retry` reads `success` and the budget, not the code, for the
        next status -- but `_schedule_next_attempt` consults the policy on the way
        back to pending. Without a non-retryable code the letter loops until the
        budget is gone.
        """
        _store_run(
            ledger,
            status=RUN_STATUS_SUCCESS,
            checkpoint={"outcome": SOURCE_LIMITED_OUTCOME, "reason": "kbo_robots_blocked"},
        )

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.error_code == FailureCode.FETCH_BLOCKED.value
        assert outcome.error_code in NON_RETRYABLE_CODES

    def test_a_skip_recorded_for_a_failed_run_is_left_alone(self, ledger: sessionmaker) -> None:
        """The skip special-case only applies to a run that claims success.

        A failed run already has a code and a message from its own failure, and
        overwriting them with the policy explanation would hide the real fault.
        """
        _store_run(
            ledger,
            status=RUN_STATUS_FAILED,
            error_code="PERSIST_CONSTRAINT",
            error_message="db said no",
            checkpoint={"outcome": SOURCE_LIMITED_OUTCOME, "reason": "kbo_robots_blocked"},
        )

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.status == RUN_STATUS_FAILED
        assert outcome.error_code == "PERSIST_CONSTRAINT"


class TestOrdinaryOutcomesAreUnchanged:
    """The fix must not turn healthy replays into failures."""

    def test_an_ordinary_success_still_resolves(self, ledger: sessionmaker) -> None:
        _store_run(ledger, status=RUN_STATUS_SUCCESS, checkpoint={"game_date": "20261010", "previews": 3})

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.success is True
        assert outcome.error_code is None

    def test_a_confirmed_empty_success_still_resolves(self, ledger: sessionmaker) -> None:
        """The case the original rule was written for.

        A replay that confirms the source has nothing has answered the question,
        and it carries no `source_limited` checkpoint because the source *was*
        asked.
        """
        _store_run(ledger, status=RUN_STATUS_SUCCESS, checkpoint={"game_date": "20261010", "previews": 0})

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.success is True

    def test_a_run_with_no_checkpoint_still_resolves(self, ledger: sessionmaker) -> None:
        """The column is nullable and older rows predate it."""
        _store_run(ledger, status=RUN_STATUS_SUCCESS, checkpoint=None)

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.success is True

    def test_a_partial_still_does_not_resolve(self, ledger: sessionmaker) -> None:
        """Unchanged: a partial stored some rows and left the gap in place."""
        _store_run(ledger, status=RUN_STATUS_PARTIAL, error_code="PERSIST_TIMEOUT")

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.success is False

    def test_a_missing_run_is_still_missing(self, ledger: sessionmaker) -> None:
        outcome = dispatcher._outcome_from_persisted_run("no-such-run")

        assert outcome.success is False
        assert outcome.status == "missing"

    @pytest.mark.parametrize(
        "checkpoint",
        ["not a dict", ["a", "list"], 7, {"outcome": None}, {}],
        ids=["string", "list", "int", "null-outcome", "empty"],
    )
    def test_a_malformed_checkpoint_reads_as_consulted(self, ledger: sessionmaker, checkpoint: object) -> None:
        """Everything else is a verdict the projection cannot make.

        Treating an unreadable checkpoint as a skip would fail healthy replays;
        raising would lose the run entirely. Resolving is the conservative
        choice, and the crawlers that write this shape are covered by tests.
        """
        _store_run(ledger, status=RUN_STATUS_SUCCESS, checkpoint=checkpoint)

        outcome = dispatcher._outcome_from_persisted_run(_RUN_ID)

        assert outcome.success is True


class TestTheLetterActuallyTerminates:
    """End to end through the lifecycle, because the verdict alone is not enough.

    The unit assertions above show what the dispatcher reports. This shows what
    the queue does with it, which is the part that produced 91 exhausted letters
    when the verdict was wrong in the other direction.
    """

    def _letter(self, factory: sessionmaker) -> str:
        with factory() as session:
            letter = CrawlDeadLetter(
                dlq_id="dlq-1",
                original_run_id="original-run",
                crawler="preview",
                target_type="preview_date",
                target_id="20261010",
                failure_stage="fetch",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
                error_message="no preview data obtained",
                status=DlqStatus.PENDING.value,
                retry_count=0,
                max_retries=5,
                next_retry_at=datetime(2026, 10, 10, 3, 0, 0),
                created_at=datetime(2026, 10, 10, 3, 0, 0),
            )
            session.add(letter)
            session.commit()
        return "dlq-1"

    def _policy_outcome(self) -> dispatcher.ReplayOutcome:
        return dispatcher.ReplayOutcome(
            success=False,
            replay_run_id=_RUN_ID,
            status=SOURCE_LIMITED_OUTCOME,
            error_message="the source was not consulted",
            error_code=FailureCode.FETCH_BLOCKED.value,
        )

    def test_a_policy_skip_terminates_instead_of_looping(self, ledger: sessionmaker) -> None:
        dlq_id = self._letter(ledger)
        with ledger() as session:
            service = CrawlDeadLetterService(session)
            service.prepare_retry(dlq_id)
            result = service.finalize_retry(dlq_id, self._policy_outcome())
            session.commit()

        assert result.status is DlqStatus.EXHAUSTED
        assert result.status is not DlqStatus.RESOLVED

    def test_it_does_not_spend_a_retry_attempt_to_get_there(self, ledger: sessionmaker) -> None:
        """The point of the non-retryable code.

        Terminating by budget would take five attempts per letter and, with the
        crawler producing three letters per run, never catch up.
        """
        dlq_id = self._letter(ledger)
        with ledger() as session:
            service = CrawlDeadLetterService(session)
            service.prepare_retry(dlq_id)
            service.finalize_retry(dlq_id, self._policy_outcome())
            session.commit()

        with ledger() as session:
            letter = session.query(CrawlDeadLetter).filter_by(dlq_id=dlq_id).one()
            # One attempt was recorded (the replay did run); the budget was not
            # walked down to exhaustion.
            assert letter.retry_count < letter.max_retries
            assert letter.status == DlqStatus.EXHAUSTED.value

    def test_success_still_resolves(self, ledger: sessionmaker) -> None:
        """The contrast: an ordinary success is unchanged by this fix."""
        dlq_id = self._letter(ledger)
        with ledger() as session:
            service = CrawlDeadLetterService(session)
            service.prepare_retry(dlq_id)
            result = service.finalize_retry(
                dlq_id,
                dispatcher.ReplayOutcome(success=True, replay_run_id=_RUN_ID, status="success"),
            )
            session.commit()

        assert result.status is DlqStatus.RESOLVED
