"""Tests for the crawler dead letter queue repository."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session, sessionmaker

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.repositories.crawl_dead_letter_repository import (
    CrawlDeadLetterRepository,
    DeadLetterSpec,
)


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    CrawlDeadLetter.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    active = factory()
    try:
        yield active
    finally:
        active.close()


def _spec(**overrides: object) -> DeadLetterSpec:
    data: dict[str, object] = {
        "original_run_id": "run-a",
        "crawler": "awards",
        "target_type": "award_history",
        "target_id": "kbo_awards_yagoonara",
        "failure_stage": "fetch",
        "error_code": "FETCH_TIMEOUT",
    }
    data.update(overrides)
    return DeadLetterSpec(**data)  # type: ignore[arg-type]


class TestCreateAndLookup:
    def test_create_pending_letter(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        assert letter.id is not None
        assert letter.dlq_id
        assert letter.status == DlqStatus.PENDING.value
        assert letter.retry_count == 0
        assert letter.max_retries == 5
        assert letter.replay_run_id is None
        assert letter.next_retry_at is not None

    def test_create_with_explicit_dlq_id(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec(dlq_id="dlq-fixed"))
        assert letter.dlq_id == "dlq-fixed"

    def test_get_by_dlq_id(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec(dlq_id="dlq-lookup"))
        assert repo.get_by_dlq_id("dlq-lookup") is letter
        assert repo.get_by_dlq_id("missing") is None

    def test_find_incident_matches_same_source_run(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        found = repo.find_incident(
            crawler="awards",
            target_type="award_history",
            target_id="kbo_awards_yagoonara",
            original_run_id="run-a",
        )
        assert found is letter
        assert (
            repo.find_incident(
                crawler="awards",
                target_type="award_history",
                target_id="kbo_awards_yagoonara",
                original_run_id="run-b",
            )
            is None
        )

    def test_find_incident_handles_null_target(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec(target_id=None))
        found = repo.find_incident(
            crawler="awards",
            target_type="award_history",
            target_id=None,
            original_run_id="run-a",
        )
        assert found is letter


class TestQueries:
    def test_get_pending_orders_by_next_retry(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        first = repo.create_dead_letter(_spec(dlq_id="a"))
        second = repo.create_dead_letter(_spec(dlq_id="b", target_id="kbo_awards_wikipedia"))
        base = datetime(2026, 9, 26, tzinfo=UTC).replace(tzinfo=None)
        repo.set_next_retry_at(first, base + timedelta(minutes=10))
        repo.set_next_retry_at(second, base)
        assert [letter.dlq_id for letter in repo.get_pending()] == ["b", "a"]

    def test_get_retryable_excludes_future(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        now = datetime(2026, 9, 26, 12, 0, 0)
        ready = repo.create_dead_letter(_spec(dlq_id="ready"))
        later = repo.create_dead_letter(_spec(dlq_id="later", target_id="kbo_awards_wikipedia"))
        repo.set_next_retry_at(ready, now - timedelta(seconds=1))
        repo.set_next_retry_at(later, now + timedelta(minutes=5))
        assert [letter.dlq_id for letter in repo.get_retryable(now=now)] == ["ready"]

    def test_get_stale_retrying_only_returns_old_retrying(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        cutoff = datetime(2026, 9, 26, 12, 0, 0)

        stale = repo.create_dead_letter(_spec(dlq_id="stale"))
        repo.mark_retrying(stale)
        fresh = repo.create_dead_letter(_spec(dlq_id="fresh", target_id="kbo_awards_wikipedia"))
        repo.mark_retrying(fresh)
        repo.create_dead_letter(_spec(dlq_id="pending", target_id="other"))

        session.execute(
            update(CrawlDeadLetter)
            .where(CrawlDeadLetter.dlq_id == "stale")
            .values(updated_at=cutoff - timedelta(minutes=5)),
        )
        session.execute(
            update(CrawlDeadLetter)
            .where(CrawlDeadLetter.dlq_id.in_(["fresh", "pending"]))
            .values(updated_at=cutoff + timedelta(minutes=5)),
        )
        session.flush()

        assert [letter.dlq_id for letter in repo.get_stale_retrying(stale_before=cutoff)] == ["stale"]

    def test_list_recent_filters(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        pending = repo.create_dead_letter(_spec(dlq_id="p"))
        ignored = repo.create_dead_letter(_spec(dlq_id="i", target_id="kbo_awards_wikipedia"))
        repo.mark_ignored(ignored)
        assert {letter.dlq_id for letter in repo.list_recent()} == {"p", "i"}
        assert [letter.dlq_id for letter in repo.list_recent(status=DlqStatus.IGNORED.value)] == ["i"]
        assert pending.dlq_id == "p"


class TestLifecycleMutations:
    def test_mark_retrying_and_increment(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        repo.mark_retrying(letter)
        repo.increment_retry(letter)
        assert letter.status == DlqStatus.RETRYING.value
        assert letter.retry_count == 1

    def test_mark_resolved_links_replay_run(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        repo.mark_resolved(letter, replay_run_id="run-b")
        assert letter.status == DlqStatus.RESOLVED.value
        assert letter.replay_run_id == "run-b"
        assert letter.resolved_at is not None

    def test_mark_exhausted_records_message(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        repo.mark_exhausted(letter, error_message="still failing")
        assert letter.status == DlqStatus.EXHAUSTED.value
        assert letter.error_message == "still failing"

    def test_mark_ignored_and_link(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        letter = repo.create_dead_letter(_spec())
        repo.link_replay_run(letter, "run-b")
        repo.mark_ignored(letter, reason="non-retryable")
        assert letter.replay_run_id == "run-b"
        assert letter.status == DlqStatus.IGNORED.value


class TestTransactionContract:
    def test_repository_does_not_commit(self, session: Session) -> None:
        repo = CrawlDeadLetterRepository(session)
        repo.create_dead_letter(_spec())
        assert session.query(CrawlDeadLetter).count() == 1
        session.rollback()
        assert session.query(CrawlDeadLetter).count() == 0
