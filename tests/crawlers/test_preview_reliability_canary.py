"""The preview crawler's reliability chain: ledger, dead letter, and verdict.

`preview_crawler` reads a whole day of pregame data and hands it back; the
preview batch owns the write because it also writes the manifest and decides
which previews are storable. So the contract worth pinning here is narrower
than a persisting crawler's, and the load-bearing part is the empty case: an
empty result is the normal state before first pitch *and* what an unreadable
page produces. Collapsing them either alerts every quiet morning or records a
failed fetch as a successful one, and both are silent.

The crawler is replaced at the fetch boundary rather than at `run`, so the
ledger entries and the dead letter are produced by the real code under test.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.crawlers.preview_crawler as preview_module
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.preview_crawler import PREVIEW_CRAWLER_NAME, PreviewCrawler
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_RUNNING
from src.repositories.crawl_execution_repository import CrawlRunSpec

GAME_DATE = "20260822"


@pytest.fixture
def harness(monkeypatch):
    """Run the real `run` against a stub ledger and a stub fetch."""
    run = SimpleNamespace(
        run_id="preview-run",
        status=RUN_STATUS_RUNNING,
        error_code=None,
        error_message=None,
        records_read=0,
        checkpoint=None,
    )
    specs: list[CrawlRunSpec] = []

    @contextmanager
    def track_run(spec):
        specs.append(spec)
        yield run

    monkeypatch.setattr(preview_module, "track_crawl_run", track_run)
    enqueue = MagicMock()
    monkeypatch.setattr(preview_module, "enqueue_failure", enqueue)
    crawler = PreviewCrawler(request_delay=0)
    monkeypatch.setattr(crawler, "crawl_preview_for_date", AsyncMock(return_value=[]))
    # The compliance check is a network read (robots.txt) the first time it runs
    # in a process. Allowed by default so the tests exercise the crawl path, and
    # the one that needs a block overrides this.
    monkeypatch.setattr(preview_module.compliance, "is_allowed", AsyncMock(return_value=True))
    return crawler, run, specs, enqueue


def _games_response(url: str = PreviewCrawler.GAME_LIST_URL) -> CrawlResult[object]:
    """A game list that holds one game, so the date is not confirmed empty."""
    return CrawlResult.success(
        {"game": [{"G_ID": "20260822LGHH0"}]},
        http_status=200,
        url=url,
        content_type="application/json",
    )


def _empty_games_response(url: str = PreviewCrawler.GAME_LIST_URL) -> CrawlResult[object]:
    return CrawlResult.success({}, http_status=200, url=url, content_type="application/json")


def _failed_games_response(url: str = PreviewCrawler.GAME_LIST_URL) -> CrawlResult[object]:
    return CrawlResult.failure(
        CrawlOutcome.RETRYABLE_ERROR,
        error="timed out",
        error_code=FailureCode.FETCH_TIMEOUT.value,
        url=url,
    )


class TestTheRunLedgerRecordsOneDate:
    def test_the_spec_names_the_date_as_the_replay_unit(self, harness):
        crawler, run, specs, _ = harness
        crawler.crawl_preview_for_date = AsyncMock(return_value=[{"game_id": "G1"}])

        asyncio.run(crawler.run(GAME_DATE))

        assert len(specs) == 1
        assert specs[0].crawler == PREVIEW_CRAWLER_NAME
        assert specs[0].target_id == GAME_DATE
        assert specs[0].source_url == PreviewCrawler.GAME_LIST_URL

    def test_the_rows_read_are_recorded(self, harness):
        crawler, run, _, _ = harness
        crawler.crawl_preview_for_date = AsyncMock(return_value=[{"game_id": "G1"}, {"game_id": "G2"}])

        previews = asyncio.run(crawler.run(GAME_DATE))

        assert len(previews) == 2
        assert run.records_read == 2
        assert run.status == RUN_STATUS_RUNNING


class TestAnEmptyResultIsToldApartFromAFailure:
    """The distinction the whole chain rests on."""

    def test_a_date_with_no_games_is_a_success_and_queues_nothing(self, harness):
        crawler, run, _, enqueue = harness
        crawler._http.post_json = AsyncMock(return_value=_empty_games_response())

        previews = asyncio.run(crawler.run(GAME_DATE))

        assert previews == []
        assert run.status == RUN_STATUS_RUNNING
        assert run.error_code is None
        enqueue.assert_not_called()

    def test_a_listed_day_with_no_preview_data_is_a_failure_that_queues(self, harness):
        """The games exist, so the missing pregame data is not a quiet day."""
        crawler, run, _, enqueue = harness
        crawler._http.post_json = AsyncMock(return_value=_games_response())

        previews = asyncio.run(crawler.run(GAME_DATE))

        assert previews == []
        assert run.status == RUN_STATUS_FAILED
        assert run.error_code == FailureCode.FETCH_HTTP_ERROR.value
        enqueue.assert_called_once()
        letter = enqueue.call_args.args[0]
        assert letter.crawler == PREVIEW_CRAWLER_NAME
        assert letter.target_id == GAME_DATE
        assert letter.game_id == GAME_DATE
        assert letter.original_run_id == "preview-run"

    def test_an_unreadable_game_list_cannot_confirm_an_empty_day(self, harness):
        """A blocked or failed list read means the day is unknown, not empty."""
        crawler, run, _, enqueue = harness
        crawler._http.post_json = AsyncMock(return_value=_failed_games_response())

        previews = asyncio.run(crawler.run(GAME_DATE))

        assert previews == []
        assert run.status == RUN_STATUS_FAILED
        enqueue.assert_called_once()

    def test_a_compliance_block_is_not_confirmation(self, harness):
        crawler, run, _, enqueue = harness
        crawler._http.post_json = AsyncMock(return_value=_empty_games_response())

        with patch.object(preview_module.compliance, "is_allowed", AsyncMock(return_value=False)):
            asyncio.run(crawler.run(GAME_DATE))

        assert run.status == RUN_STATUS_FAILED
        crawler._http.post_json.assert_not_awaited()
        enqueue.assert_called_once()

    def test_the_confirmation_read_does_not_run_the_playwright_fallback(self, harness):
        """Confirming uses the shared client only.

        A browser read here would turn a cheap check into a page launch on every
        quiet morning, which is the case the check exists to keep cheap.
        """
        crawler, _, _, _ = harness
        crawler._http.post_json = AsyncMock(return_value=_empty_games_response())

        with patch.object(crawler, "_fetch_game_list_with_playwright") as fallback:
            asyncio.run(crawler.run(GAME_DATE))

        fallback.assert_not_called()


class TestReplayReusesTheSameCodePath:
    def test_a_replay_spec_is_honoured_and_nested_dlq_is_disabled(self, harness):
        crawler, run, specs, enqueue = harness
        crawler._http.post_json = AsyncMock(return_value=_games_response())
        spec = CrawlRunSpec(
            crawler=PREVIEW_CRAWLER_NAME,
            target_type="preview_date",
            target_id=GAME_DATE,
            run_id="replay-run",
            replay_of_run_id="original-run",
        )

        asyncio.run(crawler.run(GAME_DATE, run_spec=spec, record_dead_letters=False))

        assert specs[0] is spec
        assert run.status == RUN_STATUS_FAILED
        enqueue.assert_not_called()


class TestTheDispatcherStoresWhatAReplayRefreshes:
    def test_the_handler_refuses_a_letter_without_a_date(self):
        from src.services import crawl_replay_dispatcher as dispatcher_module

        letter = SimpleNamespace(
            crawler=PREVIEW_CRAWLER_NAME,
            target_type="preview_date",
            target_id=None,
            game_id=None,
            season=None,
            source_url=None,
            original_run_id="original-run",
        )

        outcome = dispatcher_module._replay_preview(letter, "replay-run")

        assert not outcome.success
        assert outcome.status == "unaddressable"

    def test_the_handler_saves_through_the_batch_writer(self, tmp_path):
        from src.services import crawl_replay_dispatcher as dispatcher_module

        letter = SimpleNamespace(
            crawler=PREVIEW_CRAWLER_NAME,
            target_type="preview_date",
            target_id=GAME_DATE,
            game_id=GAME_DATE,
            season=None,
            source_url=PreviewCrawler.GAME_LIST_URL,
            original_run_id="original-run",
        )
        stored = dispatcher_module.ReplayOutcome(
            success=True,
            replay_run_id="replay-run",
            status="success",
        )

        with (
            patch.object(dispatcher_module, "PreviewCrawler") as crawler_class,
            patch.object(dispatcher_module, "save_preview_contexts") as save_contexts,
            patch.object(dispatcher_module, "_outcome_from_persisted_run", return_value=stored),
        ):
            crawler_class.return_value.run = AsyncMock(return_value=[{"game_id": "G1"}])
            save_contexts.return_value = ["G1"]

            outcome = dispatcher_module._replay_preview(letter, "replay-run")

        assert outcome is stored
        save_contexts.assert_called_once_with([{"game_id": "G1"}], GAME_DATE)
        kwargs = crawler_class.return_value.run.await_args.kwargs
        assert kwargs["record_dead_letters"] is False

    def test_an_empty_replay_writes_nothing(self):
        from src.services import crawl_replay_dispatcher as dispatcher_module

        letter = SimpleNamespace(
            crawler=PREVIEW_CRAWLER_NAME,
            target_type="preview_date",
            target_id=GAME_DATE,
            game_id=GAME_DATE,
            season=None,
            source_url=PreviewCrawler.GAME_LIST_URL,
            original_run_id="original-run",
        )

        with (
            patch.object(dispatcher_module, "PreviewCrawler") as crawler_class,
            patch.object(dispatcher_module, "save_preview_contexts") as save_contexts,
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
            dispatcher_module._replay_preview(letter, "replay-run")

        save_contexts.assert_not_called()


class TestTheRegistryAdvertisesTheHandler:
    def test_the_default_dispatcher_registers_preview(self):
        from src.services.crawl_replay_dispatcher import build_default_dispatcher

        assert PREVIEW_CRAWLER_NAME in build_default_dispatcher().registered_crawlers()
