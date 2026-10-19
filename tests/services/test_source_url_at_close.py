"""When the ledger learns a run's source, and what a missing value must not erase.

A detail run and a relay run both discover their sources while they work: detail
asks Naver first and reaches the KBO page only when Naver has no record and the
policy allows the fallback, and relay asks the schedule lookup only when the id
derived from the KBO game id came back empty. So the run row cannot carry the
source when it opens, and the close is the first honest moment to write it.

That leaves one rule the ledger has to keep: a close that supplies no source must
leave whatever the row already holds. A replay opens under the original run's
spec -- including its URL -- and a retry that learns nothing new would otherwise
blank a fact it inherited.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services import game_collection_service as gcs
from src.services.crawl_run_ledger import RunCounts, TerminalOutcome
from src.services.game_collection_service import (
    GameCollectionItemResult,
    GameCollectionResult,
    GameCollectionTarget,
    RelayOutcome,
    _process_detail_target,
    _requested_source_url,
)
from src.services.game_detail_runs import (
    GAME_DETAIL_CRAWLER_NAME,
    GAME_DETAIL_TARGET_TYPE,
    GameDetailRunLedger,
    season_of,
)
from src.services.relay_runs import RELAY_CRAWLER_NAME, RELAY_TARGET_TYPE, RelayRunLedger

GAME = "20250501LGOB0"
REQUESTED = "https://api-gw.sports.naver.com/schedule/games/20250501LGOB02025/record"
SECONDARY = "https://www.koreabaseball.com/GameCenter/Game/Review.aspx?gameId=20250501LGOB0"
ORIGINAL = "https://original.test/page"

FULL_DETAIL: dict = {
    "hitters": {"away": [{}], "home": [{}]},
    "pitchers": {"away": [{}], "home": [{}]},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 5}},
    "metadata": {"stadium": "잠실"},
}


@pytest.fixture
def session_factory() -> Iterator[sessionmaker]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


@pytest.fixture(autouse=True)
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    for module in (
        "src.services.crawl_run_service",
        "src.services.game_detail_runs",
        "src.services.relay_runs",
        "src.services.game_collection_service",
    ):
        monkeypatch.setattr(f"{module}.SessionLocal", session_factory)


def _run(session_factory: sessionmaker, run_id: str) -> CrawlExecutionRun:
    with session_factory() as session:
        return session.execute(
            select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run_id),
        ).scalar_one()


def _inherited_detail_spec() -> CrawlRunSpec:
    return CrawlRunSpec(
        crawler=GAME_DETAIL_CRAWLER_NAME,
        target_type=GAME_DETAIL_TARGET_TYPE,
        target_id=GAME,
        game_id=GAME,
        season=season_of(GAME),
        source_url=ORIGINAL,
    )


def _inherited_relay_spec() -> CrawlRunSpec:
    return CrawlRunSpec(
        crawler=RELAY_CRAWLER_NAME,
        target_type=RELAY_TARGET_TYPE,
        target_id=GAME,
        game_id=GAME,
        season=season_of(GAME),
        source_url=ORIGINAL,
    )


class TestTheDetailLedgerTakesTheSourcesAtClose:
    def test_a_success_records_what_the_attempt_requested(self, session_factory: sessionmaker) -> None:
        ledger = GameDetailRunLedger()
        run_id = ledger.open_runs([GAME]).run_id_for(GAME)
        assert run_id is not None

        assert ledger.record_success(run_id, counts=RunCounts(read=1, written=1), source_url=REQUESTED)

        assert _run(session_factory, run_id).source_url == REQUESTED

    def test_a_failure_records_the_source_that_refused_it(self, session_factory: sessionmaker) -> None:
        """A refusal is only actionable when the refused URL is named."""
        ledger = GameDetailRunLedger()
        run_id = ledger.open_runs([GAME]).run_id_for(GAME)
        assert run_id is not None

        assert ledger.record_failed(
            run_id,
            error_code="FETCH_HTTP_ERROR",
            error_message="503 from the record endpoint",
            counts=RunCounts(),
            source_url=REQUESTED,
        )

        assert _run(session_factory, run_id).source_url == REQUESTED

    def test_a_close_without_a_source_keeps_the_one_the_run_inherited(
        self,
        session_factory: sessionmaker,
    ) -> None:
        """A replay opens with the original URL; learning nothing must not erase it."""
        ledger = GameDetailRunLedger()
        run_id = ledger.open_run(_inherited_detail_spec()).run_id_for(GAME)
        assert run_id is not None

        assert ledger.record_partial(
            run_id,
            error_code="VALIDATION_INCOMPLETE",
            error_message="stored but not complete",
            counts=RunCounts(read=1, written=1),
            source_url=None,
        )

        assert _run(session_factory, run_id).source_url == ORIGINAL


class TestTheRelayLedgerTakesTheSourcesAtClose:
    def test_a_success_records_what_the_attempt_requested(self, session_factory: sessionmaker) -> None:
        ledger = RelayRunLedger()
        run_id = ledger.open_runs([GAME]).run_id_for(GAME)
        assert run_id is not None

        assert ledger.record_success(run_id, counts=RunCounts(read=1, written=5), source_url=REQUESTED)

        assert _run(session_factory, run_id).source_url == REQUESTED

    def test_a_failure_records_the_source_that_refused_it(self, session_factory: sessionmaker) -> None:
        ledger = RelayRunLedger()
        run_id = ledger.open_runs([GAME]).run_id_for(GAME)
        assert run_id is not None

        assert ledger.record_failed(
            run_id,
            error_code="FETCH_TIMEOUT",
            error_message="relay request timed out",
            counts=RunCounts(),
            source_url=REQUESTED,
        )

        assert _run(session_factory, run_id).source_url == REQUESTED

    def test_a_close_without_a_source_keeps_the_one_the_run_inherited(
        self,
        session_factory: sessionmaker,
    ) -> None:
        ledger = RelayRunLedger()
        run_id = ledger.open_run(_inherited_relay_spec()).run_id_for(GAME)
        assert run_id is not None

        assert ledger.record_partial(
            run_id,
            error_code="FETCH_HTTP_ERROR",
            error_message="stopped mid-game",
            counts=RunCounts(read=1, written=3),
            source_url=None,
        )

        assert _run(session_factory, run_id).source_url == ORIGINAL


class TestWhatTheCrawlerReportedReachesTheClose:
    """The join happens once, at the boundary between the crawler's record and the ledger."""

    def test_the_record_is_joined_in_request_order(self) -> None:
        crawler = GameDetailCrawler()
        crawler._note_requested_url(GAME, REQUESTED)
        crawler._note_requested_url(GAME, SECONDARY)

        assert _requested_source_url(crawler, GAME) == f"{REQUESTED}|{SECONDARY}"

    def test_a_crawler_that_keeps_no_record_reports_nothing(self) -> None:
        """An older caller or a test double must not produce an invented URL."""
        assert _requested_source_url(object(), GAME) is None

    def test_a_crawler_that_asked_for_nothing_reports_nothing(self) -> None:
        assert _requested_source_url(GameDetailCrawler(), GAME) is None

    def test_a_getter_that_raises_does_not_lose_the_run(self) -> None:
        class _Broken:
            def get_requested_urls(self, _game_id: str) -> list[str]:
                raise RuntimeError("boom")

        assert _requested_source_url(_Broken(), GAME) is None


class TestTheServiceHandsTheSourcesToTheTransition:
    """The close helpers, not the caller, decide which transition records the source."""

    @pytest.fixture
    def recorded(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
        seen: dict[str, object] = {}

        class _Ledger:
            def record_success(self, run_id: str, *, counts: RunCounts, source_url: str | None = None) -> bool:
                seen.update(transition="success", run_id=run_id, source_url=source_url)
                return True

            def record_partial(
                self,
                run_id: str,
                *,
                error_code: str,
                error_message: str,
                counts: RunCounts,
                source_url: str | None = None,
            ) -> bool:
                seen.update(transition="partial", run_id=run_id, source_url=source_url)
                return True

            def record_failed(
                self,
                run_id: str,
                *,
                error_code: str,
                error_message: str,
                counts: RunCounts,
                source_url: str | None = None,
            ) -> bool:
                seen.update(transition="failed", run_id=run_id, source_url=source_url)
                return True

        monkeypatch.setattr(gcs, "_run_ledger", _Ledger)
        return seen

    def test_a_saved_game_records_its_source(self, recorded: dict[str, object]) -> None:
        gcs._close_saved_run(
            "run-1",
            None,
            saved=True,
            persist_cause=None,
            payload=FULL_DETAIL,
            source_url=REQUESTED,
        )

        assert recorded == {"transition": "success", "run_id": "run-1", "source_url": REQUESTED}

    def test_a_stored_but_incomplete_game_records_its_source(self, recorded: dict[str, object]) -> None:
        gcs._close_saved_run(
            "run-2",
            None,
            saved=True,
            persist_cause=None,
            payload={"hitters": {"away": [{}], "home": []}},
            source_url=REQUESTED,
        )

        assert recorded["transition"] == "partial"
        assert recorded["source_url"] == REQUESTED

    def test_a_failed_attempt_records_the_source_it_reached(self, recorded: dict[str, object]) -> None:
        gcs._close_failed_run(
            "run-3",
            None,
            failure_reason="timeout",
            extra_reason=None,
            source_url=REQUESTED,
        )

        assert recorded["transition"] == "failed"
        assert recorded["source_url"] == REQUESTED

    def test_an_unknown_source_is_passed_through_as_none(self, recorded: dict[str, object]) -> None:
        """`None` is the instruction to keep what the row inherited, not a blank."""
        gcs._close_failed_run(
            "run-4",
            None,
            failure_reason="timeout",
            extra_reason=None,
            source_url=None,
        )

        assert recorded["source_url"] is None


class TestTheLetterCarriesTheSameSource:
    """The queue is where an operator reads a failure, so it names the source.

    A letter with no URL says a game failed without saying where, and since a
    replay opens under the letter's spec, the value is also what the retry
    inherits when its own attempt learns nothing.
    """

    @pytest.fixture
    def letters(self, monkeypatch: pytest.MonkeyPatch) -> list[DeadLetterSpec]:
        sent: list[DeadLetterSpec] = []
        monkeypatch.setattr(gcs, "enqueue_failure", sent.append)
        return sent

    def test_a_queued_detail_game_names_its_source(self, letters: list[DeadLetterSpec]) -> None:
        gcs._enqueue_for_replay(
            GameCollectionTarget(game_id=GAME, game_date="20250501"),
            TerminalOutcome(status="failed", error_code="FETCH_TIMEOUT", run_id="run-1"),
            source_url=REQUESTED,
        )

        assert [spec.source_url for spec in letters] == [REQUESTED]

    def test_a_queued_relay_game_names_its_source(self, letters: list[DeadLetterSpec]) -> None:
        gcs._enqueue_relay_outcome(
            GameCollectionTarget(game_id=GAME, game_date="20250501"),
            RelayOutcome(status="failed", error_code="FETCH_HTTP_ERROR", run_id="run-2"),
            source_url=REQUESTED,
        )

        assert [spec.source_url for spec in letters] == [REQUESTED]

    def test_a_detail_failure_reads_the_record_the_crawler_kept(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The whole chain: crawler record -> close -> letter, in one call."""
        letters: list[DeadLetterSpec] = []
        monkeypatch.setattr(gcs, "enqueue_failure", letters.append)
        monkeypatch.setattr(gcs, "_run_ledger", _AcceptingLedger)
        monkeypatch.setattr(
            gcs,
            "_detail_payload_failure_reason",
            lambda _target, _payload, _crawler, _save: ("crawl_failed", "timeout", "no_detail_payload"),
        )
        target = GameCollectionTarget(game_id=GAME, game_date="20250501")
        result = GameCollectionResult()
        result.items = {GAME: GameCollectionItemResult(game_id=GAME, game_date="20250501")}
        ctx = MagicMock()
        ctx.cfg.log = MagicMock()
        ctx.cfg.should_save_detail = None
        ctx.detail_ready = set()
        ctx.result = result
        ctx.detail_crawler.get_requested_urls = MagicMock(return_value=(REQUESTED, SECONDARY))

        _process_detail_target(target, None, ctx, run_id="run-5", global_index=1, total_targets=1)

        assert [spec.source_url for spec in letters] == [f"{REQUESTED}|{SECONDARY}"]


class _AcceptingLedger:
    """A ledger that takes every transition, so the close path runs to its end."""

    def record_success(self, run_id: str, *, counts: RunCounts, source_url: str | None = None) -> bool:
        return True

    def record_partial(
        self,
        run_id: str,
        *,
        error_code: str,
        error_message: str,
        counts: RunCounts,
        source_url: str | None = None,
    ) -> bool:
        return True

    def record_failed(
        self,
        run_id: str,
        *,
        error_code: str,
        error_message: str,
        counts: RunCounts,
        source_url: str | None = None,
    ) -> bool:
        return True


class TestAReplayOpensWithTheLetterSURL:
    """A letter names its source, so the retry can inherit it.

    Detail replays already opened under `_spec_for`, which carries the letter's
    URL. Relay built its spec by hand and had dropped the value, so a relay
    replay that learned nothing new closed with no source at all -- losing the
    one fact the letter held.
    """

    def test_a_relay_replay_spec_carries_the_letters_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from types import SimpleNamespace

        from src.services import crawl_replay_dispatcher as dispatcher

        letter = SimpleNamespace(
            target_id=GAME,
            game_id=GAME,
            target_type=None,
            season=None,
            source_url=REQUESTED,
            original_run_id="run-a",
        )
        captured: list[CrawlRunSpec] = []
        monkeypatch.setattr(dispatcher, "_execute_relay_replay", lambda _game_id, spec: captured.append(spec))
        monkeypatch.setattr(dispatcher, "run_coro_blocking", lambda _value: None)
        monkeypatch.setattr(
            dispatcher,
            "_outcome_from_persisted_run",
            lambda run_id: dispatcher.ReplayOutcome(success=True, replay_run_id=run_id, status="success"),
        )

        dispatcher._replay_relay(letter, "run-b")

        assert [spec.source_url for spec in captured] == [REQUESTED]
