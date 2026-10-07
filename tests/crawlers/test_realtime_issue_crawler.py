from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.crawlers.realtime_issue_crawler as crawler_module
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.realtime_issue_crawler import (
    MLBPARK_BULLPEN_TARGET_ID,
    NAVER_NEWS_TARGET_ID,
    RealtimeIssueCrawler,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_RUNNING
from src.repositories.crawl_execution_repository import CrawlRunSpec


@pytest.fixture
def harness(monkeypatch):
    run = SimpleNamespace(
        run_id="run-test",
        status=RUN_STATUS_RUNNING,
        error_code=None,
        error_message=None,
        records_read=0,
    )
    specs = []

    @contextmanager
    def track_run(spec):
        specs.append(spec)
        yield run

    monkeypatch.setattr(crawler_module, "track_crawl_run", track_run)
    enqueue = MagicMock()
    monkeypatch.setattr(crawler_module, "enqueue_failure", enqueue)
    http_client = AsyncMock()
    crawler = RealtimeIssueCrawler(timeout=5, http_client=http_client)
    return crawler, http_client, run, specs, enqueue


def _response(body: str, *, url: str, content_type: str = "text/html") -> CrawlResult[str]:
    return CrawlResult.success(body, http_status=200, url=url, content_type=content_type)


def _failure(code: FailureCode, *, url: str, status: int | None = None) -> CrawlResult[object]:
    outcome = CrawlOutcome.RETRYABLE_ERROR if code is FailureCode.FETCH_TIMEOUT else CrawlOutcome.PERMANENT_ERROR
    return CrawlResult.failure(
        outcome,
        error=f"{code.value} from {url}",
        error_code=code.value,
        http_status=status,
        url=url,
    )


def test_default_http_clients_have_independent_host_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    clients = [MagicMock(), MagicMock(), MagicMock()]
    factory = MagicMock(side_effect=clients)
    monkeypatch.setattr(crawler_module, "CrawlerHttpClient", factory)
    crawler = RealtimeIssueCrawler()

    hosts = (
        "https://api-gw.sports.naver.com/news",
        "https://sports.news.naver.com/kbaseball/news/index",
        "https://mlbpark.donga.com/mp/b.php?b=bullpen",
    )
    resolved = [crawler._http_for(url) for url in hosts]

    assert resolved == clients
    assert len({call.kwargs["name"] for call in factory.call_args_list}) == 3
    assert all(call.kwargs["policy"].max_attempts == 1 for call in factory.call_args_list)


class TestFetchNaverNewsHeadlines:
    def test_returns_parsed_articles_from_api(self, harness):
        crawler, http_client, run, specs, _ = harness
        api_url = crawler._naver_news_api_url()
        payload = {
            "result": {
                "newsList": [
                    {
                        "title": "KBO News",
                        "subContent": "Content",
                        "oid": "123",
                        "aid": "456",
                        "officeName": "Sports",
                        "datetime": "2024-01-01",
                    },
                ],
            },
        }
        http_client.fetch_text.return_value = _response(
            json.dumps(payload),
            url=api_url,
            content_type="application/json",
        )

        result = crawler.fetch_naver_news_headlines()

        assert len(result) == 1
        assert result[0]["title"] == "KBO News"
        assert "sports.news.naver.com" in result[0]["meta"]["source"]
        assert run.records_read == 1
        assert specs[0].crawler == "realtime_issue"
        assert specs[0].target_id == NAVER_NEWS_TARGET_ID
        http_client.fetch_text.assert_awaited_once()

    def test_falls_back_to_html_on_api_failure(self, harness):
        crawler, http_client, run, _, enqueue = harness
        api_url = crawler._naver_news_api_url()
        fallback_url = "https://sports.news.naver.com/kbaseball/news/index"
        http_client.fetch_text.side_effect = [
            _failure(FailureCode.FETCH_HTTP_ERROR, url=api_url, status=500),
            _response(
                '<html><body><a href="/kbaseball/news/read?oid=123&aid=456" title="Fallback News">link</a></body></html>',
                url=fallback_url,
            ),
        ]

        result = crawler.fetch_naver_news_headlines()

        assert len(result) == 1
        assert result[0]["title"] == "Fallback News"
        assert run.status == RUN_STATUS_RUNNING
        enqueue.assert_not_called()
        assert http_client.fetch_text.await_count == 2

    def test_both_sources_failing_records_one_source_dead_letter(self, harness):
        crawler, http_client, run, specs, enqueue = harness
        api_url = crawler._naver_news_api_url()
        fallback_url = "https://sports.news.naver.com/kbaseball/news/index"
        http_client.fetch_text.side_effect = [
            _failure(FailureCode.FETCH_TIMEOUT, url=api_url),
            _failure(FailureCode.FETCH_HTTP_ERROR, url=fallback_url, status=503),
        ]

        result = crawler.fetch_naver_news_headlines()

        assert result == []
        assert run.status == RUN_STATUS_FAILED
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value
        enqueue.assert_called_once()
        dead_letter = enqueue.call_args.args[0]
        assert dead_letter.original_run_id == "run-test"
        assert dead_letter.target_id == NAVER_NEWS_TARGET_ID
        assert dead_letter.source_url == api_url
        assert dead_letter.error_code == FailureCode.FETCH_TIMEOUT.value
        assert specs[0].target_id == NAVER_NEWS_TARGET_ID

    def test_malformed_api_payload_falls_back_instead_of_claiming_empty(self, harness):
        crawler, http_client, run, _, enqueue = harness
        api_url = crawler._naver_news_api_url()
        fallback_url = "https://sports.news.naver.com/kbaseball/news/index"
        http_client.fetch_text.side_effect = [
            _response("{}", url=api_url, content_type="application/json"),
            _response("<html><body></body></html>", url=fallback_url),
        ]

        assert crawler.fetch_naver_news_headlines() == []
        assert run.status == RUN_STATUS_RUNNING
        enqueue.assert_not_called()


class TestFetchMlbparkBullpenPosts:
    def test_returns_parsed_posts_and_deduplicates_urls(self, harness):
        crawler, http_client, run, specs, _ = harness
        url = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
        http_client.fetch_text.return_value = _response(
            "<html><body>"
            '<a href="/mp/b.php?b=bullpen&m=view&id=12345">Hot Topic [15]</a>'
            '<a href="/mp/b.php?b=bullpen&m=view&id=12345">Hot Topic [15]</a>'
            '<a href="/mp/b.php?b=bullpen&m=view&id=67890">Another Post</a>'
            "</body></html>",
            url=url,
        )

        result = crawler.fetch_mlbpark_bullpen_posts()

        assert len(result) == 2
        assert result[0]["title"] == "Hot Topic"
        assert result[1]["title"] == "Another Post"
        assert run.records_read == 2
        assert specs[0].target_id == MLBPARK_BULLPEN_TARGET_ID

    def test_transport_failure_is_recorded_and_returns_empty(self, harness):
        crawler, http_client, run, _, enqueue = harness
        url = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
        http_client.fetch_text.return_value = _failure(FailureCode.FETCH_TIMEOUT, url=url)

        assert crawler.fetch_mlbpark_bullpen_posts() == []

        assert run.status == RUN_STATUS_FAILED
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value
        enqueue.assert_called_once()
        assert enqueue.call_args.args[0].target_id == MLBPARK_BULLPEN_TARGET_ID

    def test_run_rejects_unknown_replay_targets(self, harness):
        crawler, _, _, _, _ = harness

        with pytest.raises(ValueError, match="unsupported realtime issue target"):
            asyncio.run(crawler.run(target_id="unknown"))

    def test_run_passes_the_replay_spec_and_disables_nested_dlq(self, harness):
        crawler, http_client, _, _, enqueue = harness
        http_client.fetch_text.return_value = _response(
            "<html><body></body></html>",
            url="https://mlbpark.donga.com/mp/b.php?b=bullpen",
        )
        spec = CrawlRunSpec(
            crawler="realtime_issue",
            target_type="realtime_issue_source",
            target_id=MLBPARK_BULLPEN_TARGET_ID,
            run_id="replay-run",
            replay_of_run_id="original-run",
        )

        assert (
            asyncio.run(
                crawler.run(
                    target_id=MLBPARK_BULLPEN_TARGET_ID,
                    run_spec=spec,
                    record_dead_letters=False,
                ),
            )
            == []
        )

        enqueue.assert_not_called()

    def test_raw_snapshot_write_failure_is_tracked_and_queued(self, harness, monkeypatch):
        crawler, http_client, run, _, enqueue = harness
        url = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
        http_client.fetch_text.return_value = _response("<html></html>", url=url)
        monkeypatch.setattr(crawler_module, "get_db_session", lambda: nullcontext(object()))

        def fail_snapshot_save(session, snapshots):
            raise RuntimeError("snapshot write failed")

        monkeypatch.setattr(crawler_module, "save_raw_snapshots", fail_snapshot_save)

        result = asyncio.run(crawler.run(target_id=MLBPARK_BULLPEN_TARGET_ID, save=True))

        assert result == []
        assert run.status == RUN_STATUS_FAILED
        assert run.error_code == FailureCode.PERSIST_CONNECTION.value
        dead_letter = enqueue.call_args.args[0]
        assert dead_letter.failure_stage == "persist"
        assert dead_letter.error_code == FailureCode.PERSIST_CONNECTION.value
