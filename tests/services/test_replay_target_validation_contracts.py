"""Replay target identifiers must be validated before work can be resolved.

The dead-letter schema allows ``target_id`` to be null, but each crawler's
producer assigns a unit: a source key, date, month, page, team, or year range.
The dispatcher is therefore responsible for refusing a malformed letter rather
than letting a crawler's ordinary empty/default behavior claim it succeeded.

These tests use a local in-memory run ledger and replace every network-facing
method. A malformed target is refused as ``unaddressable`` before anything
runs; the schedule tests pin the distinct, safe ``missing`` outcome.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.result import CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.crawl_execution import CrawlExecutionRun
from src.services import crawl_replay_dispatcher as dispatcher
from src.services.crawl_dead_letter_service import retry_dead_letter
from src.services.crawl_run_service import track_crawl_run

RUN_ID = "REPLAY-TARGET-1"


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[sessionmaker, Callable[[str], None]]]:
    """Route replay verdicts and crawler run records to isolated SQLite."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(dispatcher, "SessionLocal", factory)

    def bind_tracker(module_path: str) -> None:
        """Make one crawler module use the same test ledger."""
        module = __import__(module_path, fromlist=["track_crawl_run"])
        monkeypatch.setattr(module, "track_crawl_run", lambda spec: track_crawl_run(spec, session_factory=factory))

    yield factory, bind_tracker
    engine.dispose()


def _letter(
    crawler: str,
    target_id: str | None,
    target_type: str,
    *,
    game_id: str | None = None,
) -> CrawlDeadLetter:
    """Build a detached letter with a valid crawler/type and a chosen target."""
    return CrawlDeadLetter(
        dlq_id=f"DLQ-{crawler}",
        original_run_id="ORIGINAL-RUN",
        crawler=crawler,
        target_type=target_type,
        target_id=target_id,
        game_id=game_id,
        failure_stage="fetch",
        error_code="FETCH_TIMEOUT",
    )


def _stored_run(factory: sessionmaker, run_id: str) -> CrawlExecutionRun | None:
    with factory() as session:
        return session.scalar(select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run_id))


@pytest.mark.parametrize(
    ("crawler", "module_path", "target_type"),
    [
        ("food", "src.crawlers.food_crawler", "food"),
        ("parking", "src.crawlers.parking_crawler", "parking"),
    ],
)
def test_unknown_team_target_is_refused(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    crawler: str,
    module_path: str,
    target_type: str,
) -> None:
    factory, bind_tracker = ledger
    bind_tracker(module_path)

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter(crawler, "NOT_A_TEAM", target_type),
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "unaddressable"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


def test_roster_missing_target_never_defaults_to_today(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, bind_tracker = ledger
    bind_tracker("src.crawlers.roster_transaction_crawler")
    import src.crawlers.roster_transaction_crawler as roster_module
    from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler

    requested_dates: list[str] = []

    async def empty_roster(_crawler: RosterTransactionCrawler, _crawl_date: date) -> CrawlResult[list[dict]]:
        requested_dates.append(_crawl_date.isoformat())
        return CrawlResult.success([])

    monkeypatch.setattr(roster_module.compliance, "is_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(RosterTransactionCrawler, "_resolve_crawl", empty_roster)
    monkeypatch.setattr(RosterTransactionCrawler, "_save_to_db", lambda *_args, **_kwargs: (0, 0))

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter("roster_transactions", None, "roster_date", game_id="2025-05-01"),
        replay_run_id=RUN_ID,
    )

    assert requested_dates in ([], ["2025-05-01"])
    if not requested_dates:
        assert outcome.success is False
        assert outcome.status == "unaddressable"
        assert outcome.error_code == "VALIDATION_SCHEMA"
        assert _stored_run(factory, RUN_ID) is None


def test_unknown_award_source_is_refused(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, bind_tracker = ledger
    bind_tracker("src.crawlers.award_crawler")
    from src.crawlers.award_crawler import AwardCrawler

    crawler = AwardCrawler()
    monkeypatch.setattr(crawler, "save", AsyncMock(return_value=(0, 0)))
    monkeypatch.setattr(dispatcher, "AwardCrawler", lambda: crawler)

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter("awards", "UNKNOWN_SOURCE", "award_history"),
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "unaddressable"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


def test_malformed_preview_date_is_refused(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, bind_tracker = ledger
    bind_tracker("src.crawlers.preview_crawler")
    from src.crawlers.preview_crawler import PreviewCrawler

    async def no_previews(_crawler: PreviewCrawler, _game_date: str) -> list[dict]:
        return []

    monkeypatch.setattr(PreviewCrawler, "crawl_preview_for_date", no_previews)
    monkeypatch.setattr(PreviewCrawler, "_date_is_confirmed_empty", AsyncMock(return_value=True))

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter("preview", "not-a-date", "preview_date"),
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "unaddressable"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


def test_reversed_player_movement_range_is_refused(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, bind_tracker = ledger
    bind_tracker("src.crawlers.player_movement_crawler")
    import src.crawlers.player_movement_crawler as movement_module
    from src.crawlers.player_movement_crawler import PlayerMovementCrawler

    async def empty_reversed_range(
        crawler: PlayerMovementCrawler,
        start_year: int,
        end_year: int,
        *,
        save_snapshots: bool = False,
    ) -> list[dict]:
        crawler._last_failure_reason = None
        crawler._year_failures = []
        crawler._year_reads = {}
        assert start_year > end_year
        return []

    monkeypatch.setattr(movement_module.compliance, "is_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(PlayerMovementCrawler, "crawl_years", empty_reversed_range)
    monkeypatch.setattr(PlayerMovementCrawler, "_save_snapshots", lambda _crawler: None)

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter("player_movement", "2026-2023", "player_movement"),
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "unaddressable"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


@pytest.mark.parametrize("target_id", [None, "not-a-month", "2026-13"])
def test_bad_schedule_target_stays_a_failed_missing_run(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
    target_id: str | None,
) -> None:
    """Schedule's early return is a failure, not the false-success seen elsewhere."""
    factory, _bind_tracker = ledger
    crawler = AsyncMock()
    monkeypatch.setattr(dispatcher, "ScheduleCrawler", lambda: crawler)

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter("schedule", target_id, "schedule_month"),
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "missing"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


@pytest.mark.parametrize(
    ("crawler", "target_type"),
    [
        ("awards", "food"),
        ("roster_transactions", "schedule_month"),
        ("schedule", "roster_date"),
        ("kbo_event", "game"),
    ],
)
def test_target_type_must_match_the_crawler_contract(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    crawler: str,
    target_type: str,
) -> None:
    """Dispatch runs on ``crawler``; a false ``target_type`` only poisons lineage.

    The letter still reached the right handler before this guard existed, so the
    harm was a RUN-B row recorded under a type the crawler never produces --
    invisible in the replay result and wrong in every metric grouped by type.
    """
    factory, _bind_tracker = ledger

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter(crawler, None, target_type),
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "unaddressable"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


def test_a_blank_target_type_is_not_treated_as_a_mismatch(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent type falls back to the canonical one; only a false claim is refused.

    ``target_type`` is non-nullable, so an empty value is a legacy or hand-built
    row rather than a wrong one, and every handler already normalizes it with
    ``target_type or ...``. Refusing here would strand those letters with no
    path to recovery.
    """
    factory, _bind_tracker = ledger
    crawler = AsyncMock()
    monkeypatch.setattr(dispatcher, "ScheduleCrawler", lambda: crawler)

    outcome = dispatcher.build_default_dispatcher().replay(
        _letter("schedule", "2026-05", ""),
        replay_run_id=RUN_ID,
    )

    assert outcome.status != "unaddressable"


def test_kbo_event_target_must_match_source_url(
    ledger: tuple[sessionmaker, Callable[[str], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page letter must not rename the page it intends to replay."""
    factory, _bind_tracker = ledger
    letter = _letter("kbo_event", "Different.aspx", "kbo_event")
    letter.source_url = "https://www.koreabaseball.com/Kbo/Event/Promotion1.aspx"

    outcome = dispatcher.build_default_dispatcher().replay(
        letter,
        replay_run_id=RUN_ID,
    )

    assert outcome.success is False
    assert outcome.status == "unaddressable"
    assert outcome.error_code == "VALIDATION_SCHEMA"
    assert _stored_run(factory, RUN_ID) is None


class _NoSuchGameDetailCrawler:
    """A well-formed game the source has never heard of returns no payload.

    This is the shape a real nonexistent game produces, and it is deliberately
    distinct from a timeout: the crawl completed and the source had nothing.
    """

    def __init__(self) -> None:
        self.requested: list[str] = []

    async def crawl_game_attempts(self, games: list[dict[str, object]], **_: object) -> list[object]:
        from src.crawlers.game_detail_outcome import attempt_from_result

        self.requested = [str(game["game_id"]) for game in games]
        return [attempt_from_result(str(game["game_id"]), None, lightweight=False) for game in games]

    async def close(self) -> None:
        return None


class TestAGameTheSourceDoesNotHave:
    """A well-formed ID that names no game: absence, not a shape failure.

    Both handlers accept the ID -- `season_of` and `game_date_of` both parse it --
    so the verdict comes from what the crawl reports back, not from the target
    check. That is why these tests exist rather than one more target guard.
    """

    NONEXISTENT = "20991231ZZZZ0"

    @staticmethod
    def _wire_run_ledgers(
        factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Point the run ledgers at the test database.

        Both ledgers bind `SessionLocal` when they are constructed, and they are
        constructed per call, so rebinding the module attribute is enough. Without
        this the run cannot be opened, the handler returns before fetching, and the
        test would observe `missing` -- an artifact of the wiring rather than the
        verdict under test.
        """
        for module in (
            "src.services.game_collection_service",
            "src.services.game_detail_runs",
            "src.services.relay_runs",
        ):
            monkeypatch.setattr(f"{module}.SessionLocal", factory)

    def test_game_detail_records_a_failure_rather_than_closing_the_letter(
        self,
        ledger: tuple[sessionmaker, Callable[[str], None]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        factory, _bind_tracker = ledger
        self._wire_run_ledgers(factory, monkeypatch)
        crawler = _NoSuchGameDetailCrawler()
        monkeypatch.setattr(dispatcher, "GameDetailCrawler", lambda: crawler)
        monkeypatch.setattr(
            "src.services.game_collection_service._detail_payload_failure_reason",
            lambda target, payload, *_args: (
                ("crawl_failed", "no payload", "no_detail_payload") if not payload else None
            ),
        )

        outcome = dispatcher.build_default_dispatcher().replay(
            _letter("game_detail", self.NONEXISTENT, "game"),
            replay_run_id=RUN_ID,
        )

        assert crawler.requested == [self.NONEXISTENT]
        assert outcome.success is False
        stored = _stored_run(factory, RUN_ID)
        assert stored is not None
        assert stored.status == "failed"

    def test_relay_records_success_because_the_source_has_nothing(
        self,
        ledger: tuple[sessionmaker, Callable[[str], None]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`EMPTY` is a completed answer, so this one is supposed to resolve.

        `_collect_one_relay` records success for `EMPTY` on purpose: a game the
        source never carried is finished work, and queuing it would be how a
        permanent absence turns into a retry loop. The test pins the asymmetry
        with `game_detail` rather than leaving it to be re-derived.
        """
        factory, _bind_tracker = ledger
        self._wire_run_ledgers(factory, monkeypatch)
        from src.crawlers.relay_outcome import RelayAttempt, RelayStatus

        class _Absent:
            async def crawl_relay_attempt(self, game_id: str) -> RelayAttempt:
                return RelayAttempt(
                    game_id=game_id,
                    status=RelayStatus.EMPTY,
                    result={},
                    resolution_attempted=True,
                )

            async def close(self) -> None:
                return None

        monkeypatch.setattr("src.crawlers.relay_crawler.RelayCrawler", _Absent)

        outcome = dispatcher.build_default_dispatcher().replay(
            _letter("relay", self.NONEXISTENT, "game"),
            replay_run_id=RUN_ID,
        )

        assert outcome.success is True
        stored = _stored_run(factory, RUN_ID)
        assert stored is not None
        assert stored.status == "success"
        assert stored.records_written == 0


def test_invalid_target_does_not_reschedule(
    ledger: tuple[sessionmaker, Callable[[str], None]],
) -> None:
    """A refused target returns a non-retryable outcome instead of spending a retry."""
    factory, _bind_tracker = ledger
    CrawlDeadLetter.__table__.create(factory().bind)
    with factory() as session:
        session.add(_letter("awards", "UNKNOWN_SOURCE", "award_history"))
        session.commit()

    result = retry_dead_letter(
        "DLQ-awards",
        dispatcher.build_default_dispatcher(),
        session_factory=factory,
    )

    assert result.success is False
    assert result.status is DlqStatus.EXHAUSTED
    with factory() as session:
        stored = session.scalar(select(CrawlDeadLetter).where(CrawlDeadLetter.crawler == "awards"))
        assert stored is not None
        assert stored.status == DlqStatus.EXHAUSTED.value
        assert stored.error_code == "FETCH_TIMEOUT"
        assert stored.retry_count == 1
