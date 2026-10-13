"""A run must be able to say which build wrote it.

Three questions today were answerable only by inferring from behaviour, and each
inference was wrong. `source_limited` was searched for in `status` and found
nothing while 90 rows carried it in `checkpoint.outcome`; the refusal code was
hardcoded as the retired spelling; and `parking_fee_kinds` sat empty while its
crawler reported success daily, with no way to ask whether the running build was
the one that writes it.

Inferring from behaviour is the failure. A missing value is not evidence of
absence, and a build cannot be identified by what it fails to do.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.db import build_contract as bc
from src.models.crawl_execution import CrawlExecutionRun

_REFERENCE = datetime.now(UTC).replace(tzinfo=None, microsecond=0)


@pytest.fixture
def ledger() -> sessionmaker:
    """A ledger holding only the checkpoint column this feature reads."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE crawl_execution_runs (id INTEGER PRIMARY KEY, crawler VARCHAR(64), "
                "checkpoint TEXT, started_at TIMESTAMP)"
            )
        )
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def _row(ledger: sessionmaker, checkpoint: object, *, hours_ago: int = 0) -> None:
    with ledger() as session:
        session.execute(
            text("INSERT INTO crawl_execution_runs (crawler, checkpoint, started_at) VALUES ('seat', :c, :t)"),
            {"c": None if checkpoint is None else json.dumps(checkpoint), "t": _REFERENCE - timedelta(hours=hours_ago)},
        )
        session.commit()


class TestStamp:
    def test_a_marker_is_added(self) -> None:
        stamped = bc.stamp(None)

        assert bc.parse_contract(stamped) == bc.build_contract()

    def test_a_crawlers_own_keys_survive(self) -> None:
        """Merged, not replaced.

        `outcome` and `reason` are the keys that identified a source-limited run in
        the first place. A marker that overwrote them would fix the identification
        problem by destroying the evidence for it.
        """
        stamped = bc.stamp({"outcome": "source_limited", "reason": "kbo_robots_blocked"})

        assert stamped["outcome"] == "source_limited"
        assert stamped["reason"] == "kbo_robots_blocked"
        assert bc.migration_of(stamped) == bc.build_contract()["migrations"]

    def test_the_callers_mapping_is_not_mutated(self) -> None:
        original: dict = {"outcome": "source_limited"}

        bc.stamp(original)

        assert original == {"outcome": "source_limited"}

    def test_a_crawler_supplied_contract_does_not_override_this_build(self) -> None:
        """A crawler has no business claiming which build is running."""
        stamped = bc.stamp({"contract": {"migrations": "001"}})

        assert bc.migration_of(stamped) == bc.build_contract()["migrations"]


class TestReadingOlderRows:
    """A row written before this existed is an older build, not a broken one.

    That distinction is the whole reason this is a marker rather than a join on a
    non-null column: the absence is the common case for a while, and reporting it
    as missing would make every old row look like a fault.
    """

    def test_a_row_without_a_marker_yields_nothing(self) -> None:
        assert bc.parse_contract({"outcome": "source_limited"}) == {}
        assert bc.migration_of({"outcome": "source_limited"}) is None
        assert bc.migration_of(None) is None

    def test_a_malformed_checkpoint_is_not_an_error(self) -> None:
        for value in ("not json", "[1, 2, 3]", b"\xff\xfe", 42):
            assert bc.migration_of(value) is None, value

    def test_the_reading_says_so_rather_than_calling_it_broken(self) -> None:
        assert "before builds were identified" in bc.describe_observation(None)


class TestBuildIdentity:
    def test_the_contract_names_the_migration_chain_this_build_carries(self) -> None:
        assert bc.highest_migration() is not None
        assert bc.build_contract()["migrations"] == bc.highest_migration()

    def test_the_contract_carries_the_vocabulary_version_separately(self) -> None:
        """Different questions: the migration number says which schema changes a
        build carries, the vocabulary version says which column a value is expected
        in. The `source_limited` misdiagnosis was the second kind.
        """
        from src.crawlers.run_vocabulary import CONTRACT_VERSION

        assert bc.build_contract()["vocabulary"] == CONTRACT_VERSION

    def test_an_unreadable_chain_does_not_invent_an_identity(self) -> None:
        bc.highest_migration.cache_clear()
        try:
            assert bc.highest_migration("no-such-dialect") is None
        finally:
            bc.highest_migration.cache_clear()


class TestTheLedgerIsStamped:
    def test_a_run_started_through_the_service_carries_the_marker(self, ledger: sessionmaker, monkeypatch) -> None:
        """The one place every run passes through, so no crawler can forget it."""
        import src.services.crawl_run_service as svc

        CrawlExecutionRun.__table__.drop(ledger.kw["bind"], checkfirst=True)
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        CrawlExecutionRun.__table__.create(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        monkeypatch.setattr(svc, "SessionLocal", factory)

        from src.repositories.crawl_execution_repository import CrawlRunSpec

        with svc.track_crawl_run(CrawlRunSpec(crawler="seat", target_type="seat_section")) as run:
            assert bc.migration_of(run.checkpoint) == bc.build_contract()["migrations"]

        engine.dispose()

    def test_a_run_keeps_its_own_checkpoint_through_the_service(self, ledger: sessionmaker, monkeypatch) -> None:
        import src.services.crawl_run_service as svc

        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        CrawlExecutionRun.__table__.create(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        monkeypatch.setattr(svc, "SessionLocal", factory)

        from src.repositories.crawl_execution_repository import CrawlRunSpec

        with svc.track_crawl_run(CrawlRunSpec(crawler="seat", target_type="seat_section")) as run:
            run.checkpoint = {"outcome": "source_limited", "reason": "kbo_robots_blocked"}
            svc_record = run
        assert bc.migration_of(svc_record.checkpoint) is not None

        engine.dispose()


class TestObservingWhichBuildRan:
    def test_rows_group_by_the_build_that_wrote_them(self, ledger: sessionmaker) -> None:
        from src.crawlers.run_vocabulary import builds_observed

        _row(ledger, bc.stamp(None))
        _row(ledger, bc.stamp(None), hours_ago=1)
        _row(ledger, {"outcome": "source_limited"}, hours_ago=2)

        observed = builds_observed(session_factory=ledger)["observed"]

        assert observed[bc.build_contract()["migrations"]] == 2
        assert observed["(before markers)"] == 1

    def test_the_report_names_the_build_it_is_reading_from(self, ledger: sessionmaker) -> None:
        """So a mismatch is visible without a second command."""
        from src.crawlers.run_vocabulary import builds_observed

        _row(ledger, bc.stamp(None))

        assert builds_observed(session_factory=ledger)["this_build"] == bc.build_contract()


class TestTheMarkerReachesTheDatabase:
    """The row that reaches the database, not the one in memory.

    The first version stamped at `start()` and a test asserting inside the block
    passed -- because the finalize paths set `checkpoint` from their own arguments
    and overwrote the marker. Every persisted row was unmarked while the test was
    green, which is the exact shape of the failure this feature exists to end.

    These build the real table rather than reusing the minimal `ledger` fixture,
    whose hand-written columns are a different schema from the model's.
    """

    @pytest.fixture
    def full(self) -> sessionmaker:
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        CrawlExecutionRun.__table__.create(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        yield factory
        engine.dispose()

    def _run_and_read(self, ledger: sessionmaker, monkeypatch, mutate) -> object:
        import src.services.crawl_run_service as svc

        monkeypatch.setattr(svc, "SessionLocal", ledger)
        from src.repositories.crawl_execution_repository import CrawlRunSpec

        with svc.track_crawl_run(CrawlRunSpec(crawler="seat", target_type="seat_section")) as run:
            mutate(run)

        with ledger() as session:
            stored = session.execute(
                text("SELECT checkpoint FROM crawl_execution_runs ORDER BY id DESC LIMIT 1")
            ).scalar()
        return json.loads(stored) if isinstance(stored, str) else stored

    def test_a_plain_run_persists_the_marker(self, full: sessionmaker, monkeypatch: pytest.MonkeyPatch) -> None:
        stored = self._run_and_read(full, monkeypatch, lambda run: None)

        assert bc.migration_of(stored) == bc.build_contract()["migrations"]

    def test_a_run_that_sets_its_own_checkpoint_keeps_both(
        self,
        full: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stored = self._run_and_read(
            full,
            monkeypatch,
            lambda run: run.__setattr__(
                "checkpoint",
                {"outcome": "source_limited", "reason": "kbo_robots_blocked"},
            ),
        )

        assert stored["outcome"] == "source_limited"
        assert stored["reason"] == "kbo_robots_blocked"
        assert bc.migration_of(stored) == bc.build_contract()["migrations"]
