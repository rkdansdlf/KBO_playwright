"""The play-by-play crawler's reliability chain: read, ledger, dead letter.

`pbp_crawler` fills the tables the readiness gate, the SLA tracker, the gap
report and the RAG index all read, and it does not persist -- its callers own the
write. So the contract worth pinning is the split: the crawl says what the page
said, the run records it, and the replay handler is the only thing that writes.

The load-bearing distinction is `EMPTY` versus the failure statuses. A game with
no plays is the normal state before first pitch and for a cancelled game, while a
blocked crawl, a login redirect and a changed document are three different
answers with two different resolutions. The crawler used to return `None` for all
four, which made a blocked crawl look like a rained-out game.

The crawl is replaced at its own boundary rather than at `run`, so the ledger
rows and the dead letters are produced by the code under test.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.crawlers.pbp_crawler as pbp_module
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.pbp_crawler import PBP_CRAWLER_NAME, PBPCrawler
from src.crawlers.pbp_outcome import PbpGameRead, PbpStatus, classify_game_failure
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_RUNNING
from src.repositories.crawl_execution_repository import CrawlRunSpec

GAME_ID = "20260822LGHH0"


@pytest.fixture
def harness(monkeypatch):
    """Run the real `run` against a stub ledger and a stub crawl."""
    run = SimpleNamespace(
        run_id="pbp-run",
        status=RUN_STATUS_RUNNING,
        error_code=None,
        error_message=None,
        records_read=0,
        records_failed=0,
        checkpoint=None,
    )
    specs: list[CrawlRunSpec] = []

    @contextmanager
    def track_run(spec):
        specs.append(spec)
        yield run

    monkeypatch.setattr(pbp_module, "track_crawl_run", track_run)
    enqueue = MagicMock()
    monkeypatch.setattr(pbp_module, "enqueue_failure", enqueue)
    crawler = PBPCrawler(request_delay=0)
    return crawler, run, specs, enqueue


def _reads(crawler: PBPCrawler, status: PbpStatus, reason: str | None, events: list | None = None):
    """Make the crawl answer with one classified read, as the real one does."""
    payload = {"game_id": GAME_ID, "game_date": GAME_ID[:8], "events": events} if events else None

    async def crawl(game_id: str):
        crawler._record_read(status, reason, events)
        return payload

    crawler.crawl_game_events = crawl


class TestTheRunLedgerRecordsOneGame:
    def test_the_spec_names_the_game_and_its_season(self, harness):
        crawler, _, specs, _ = harness
        _reads(crawler, PbpStatus.SUCCESS, None, [{"event_seq": 1}])

        asyncio.run(crawler.run(GAME_ID))

        assert len(specs) == 1
        assert specs[0].crawler == PBP_CRAWLER_NAME
        assert specs[0].target_id == GAME_ID
        assert specs[0].game_id == GAME_ID
        assert specs[0].season == 2026

    def test_the_plays_read_are_recorded(self, harness):
        crawler, run, _, _ = harness
        _reads(crawler, PbpStatus.SUCCESS, None, [{"event_seq": 1}, {"event_seq": 2}])

        events = asyncio.run(crawler.run(GAME_ID))

        assert len(events) == 2
        assert run.records_read == 2
        assert run.status == RUN_STATUS_RUNNING
        assert run.checkpoint["status"] == str(PbpStatus.SUCCESS)


class TestAGameWithNoPlaysIsNotAFailure:
    def test_an_empty_read_succeeds_and_queues_nothing(self, harness):
        crawler, run, _, enqueue = harness
        _reads(crawler, PbpStatus.EMPTY, "no_events")

        events = asyncio.run(crawler.run(GAME_ID))

        assert events == []
        assert run.status == RUN_STATUS_RUNNING
        assert run.error_code is None
        enqueue.assert_not_called()


class TestAnUnreadablePageIsAFailure:
    @pytest.mark.parametrize(
        ("reason", "status", "expected_code"),
        [
            ("compliance_blocked", PbpStatus.FETCH_FAILED, FailureCode.FETCH_BLOCKED.value),
            ("auth_required", PbpStatus.AUTH_REQUIRED, FailureCode.FETCH_BLOCKED.value),
            ("crawl_error", PbpStatus.FETCH_FAILED, FailureCode.FETCH_HTTP_ERROR.value),
            ("pool_error", PbpStatus.FETCH_FAILED, FailureCode.FETCH_HTTP_ERROR.value),
        ],
    )
    def test_each_reason_reaches_the_ledger_with_its_own_code(
        self,
        harness,
        reason: str,
        status: PbpStatus,
        expected_code: str,
    ) -> None:
        crawler, run, _, enqueue = harness
        _reads(crawler, status, reason)

        events = asyncio.run(crawler.run(GAME_ID))

        assert events == []
        assert run.status == RUN_STATUS_FAILED
        assert run.error_code == expected_code
        enqueue.assert_called_once()
        letter = enqueue.call_args.args[0]
        assert letter.crawler == PBP_CRAWLER_NAME
        assert letter.target_id == GAME_ID
        assert letter.game_id == GAME_ID
        assert letter.error_code == expected_code

    def test_a_silent_crawl_is_a_failure_not_a_quiet_game(self, harness):
        """No recorded outcome means a path returned early without saying why.

        `None` from the crawl used to mean all four outcomes at once; treating a
        silent return as a quiet game is how an unreadable page becomes a
        missing play-by-play nobody looks for.
        """
        crawler, run, _, enqueue = harness
        crawler.crawl_game_events = AsyncMock(return_value=None)

        assert asyncio.run(crawler.run(GAME_ID)) == []
        assert run.status == RUN_STATUS_FAILED
        enqueue.assert_called_once()

    def test_a_replay_does_not_enqueue_a_second_letter(self, harness):
        crawler, run, specs, enqueue = harness
        _reads(crawler, PbpStatus.FETCH_FAILED, "pool_error")
        spec = CrawlRunSpec(
            crawler=PBP_CRAWLER_NAME,
            target_type="pbp_game",
            target_id=GAME_ID,
            run_id="replay-run",
            replay_of_run_id="original-run",
        )

        asyncio.run(crawler.run(GAME_ID, run_spec=spec, record_dead_letters=False))

        assert specs[0] is spec
        assert run.status == RUN_STATUS_FAILED
        enqueue.assert_not_called()


class TestTheReadVocabularyCarriesRetryability:
    def test_a_blocked_or_redirected_crawl_is_terminal(self) -> None:
        for reason in ("compliance_blocked", "auth_required", "document_changed"):
            read = PbpGameRead(status=PbpStatus.FETCH_FAILED, reason=reason)
            assert read.is_terminal, f"{reason} should not be retried"

    def test_a_transport_failure_stays_retryable(self) -> None:
        for reason in ("crawl_error", "pool_error"):
            read = PbpGameRead(status=PbpStatus.FETCH_FAILED, reason=reason)
            assert not read.is_terminal, f"{reason} should be retried"

    def test_an_unregistered_reason_degrades_to_retryable(self) -> None:
        """A new observation site must not turn 'unknown' into 'give up'."""
        code, terminal = classify_game_failure("something_new")

        assert code == FailureCode.UNKNOWN.value
        assert terminal is False

    def test_a_readable_game_is_not_terminal_and_carries_no_code(self) -> None:
        for status in (PbpStatus.SUCCESS, PbpStatus.EMPTY):
            read = PbpGameRead(status=status)

            assert read.error_code is None
            assert read.is_terminal is False


class TestTheDispatcherStoresWhatAReplayRefreshes:
    def test_the_handler_refuses_a_letter_without_a_game(self) -> None:
        from src.services import crawl_replay_dispatcher as dispatcher_module

        letter = SimpleNamespace(
            crawler=PBP_CRAWLER_NAME,
            target_type="pbp_game",
            target_id=None,
            game_id=None,
            season=None,
            source_url=None,
            original_run_id="original-run",
        )

        outcome = dispatcher_module._replay_pbp(letter, "replay-run")

        assert not outcome.success
        assert outcome.status == "unaddressable"

    def test_the_handler_persists_through_save_relay_data(self) -> None:
        from src.services import crawl_replay_dispatcher as dispatcher_module

        letter = SimpleNamespace(
            crawler=PBP_CRAWLER_NAME,
            target_type="pbp_game",
            target_id=GAME_ID,
            game_id=GAME_ID,
            season=2026,
            source_url=None,
            original_run_id="original-run",
        )
        stored = dispatcher_module.ReplayOutcome(
            success=True,
            replay_run_id="replay-run",
            status="success",
        )
        session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = session

        with (
            patch.object(dispatcher_module, "PBPCrawler") as crawler_class,
            patch.object(dispatcher_module, "save_relay_data") as save_relay,
            patch.object(dispatcher_module, "SessionLocal", session_factory),
            patch.object(dispatcher_module, "_outcome_from_persisted_run", return_value=stored),
        ):
            crawler_class.return_value.run = AsyncMock(return_value=[{"event_seq": 1}])
            save_relay.return_value = 1

            outcome = dispatcher_module._replay_pbp(letter, "replay-run")

        assert outcome is stored
        kwargs = save_relay.call_args.kwargs
        assert kwargs["events"] == [{"event_seq": 1}]
        assert kwargs["raise_on_error"] is True
        saved_call = crawler_class.return_value.run.await_args.kwargs
        assert saved_call["record_dead_letters"] is False

    def test_an_empty_replay_writes_nothing(self) -> None:
        from src.services import crawl_replay_dispatcher as dispatcher_module

        letter = SimpleNamespace(
            crawler=PBP_CRAWLER_NAME,
            target_type="pbp_game",
            target_id=GAME_ID,
            game_id=GAME_ID,
            season=2026,
            source_url=None,
            original_run_id="original-run",
        )

        with (
            patch.object(dispatcher_module, "PBPCrawler") as crawler_class,
            patch.object(dispatcher_module, "save_relay_data") as save_relay,
            patch.object(
                dispatcher_module,
                "_outcome_from_persisted_run",
                return_value=dispatcher_module.ReplayOutcome(
                    success=True,
                    replay_run_id="replay-run",
                    status="success",
                ),
            ),
        ):
            crawler_class.return_value.run = AsyncMock(return_value=[])
            dispatcher_module._replay_pbp(letter, "replay-run")

        save_relay.assert_not_called()


class TestTheRegistryAdvertisesTheHandler:
    def test_the_default_dispatcher_registers_pbp(self) -> None:
        from src.services.crawl_replay_dispatcher import build_default_dispatcher

        assert PBP_CRAWLER_NAME in build_default_dispatcher().registered_crawlers()
