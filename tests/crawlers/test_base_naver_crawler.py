from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from pytest import mark

from src.crawlers.base_naver_crawler import NaverNewsCrawlerBase
from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.http_client import CrawlerHttpClient
from src.crawlers.result import CrawlOutcome, CrawlResult


class ConcreteCrawler(NaverNewsCrawlerBase):
    KEYWORDS = ["KBO", "baseball"]
    LABEL = "test_news"

    def _parse_article(self, article: dict) -> dict | None:
        title = article.get("title", "")
        if "KBO" in title:
            return {"title": title, "source": "test"}
        return None

    def _save_to_db(self, data: list[dict]) -> None:
        pass


@pytest.fixture(autouse=True)
def reset_circuit_breakers():
    """Keep the base class's process-wide breaker from leaking across tests."""
    circuit_registry.reset_all()
    yield
    circuit_registry.reset_all()


@pytest.fixture
def crawler():
    return ConcreteCrawler()


class TestFetchNews:
    @mark.asyncio
    async def test_returns_matching_articles(self, crawler):
        payload = {
            "result": {
                "newsList": [
                    {"title": "KBO news today", "content": "details"},
                    {"title": "Something else", "content": "irrelevant"},
                    {"title": "baseball KBO update", "content": "more"},
                ],
            },
        }
        crawler._fetch_day = AsyncMock(
            side_effect=[_success(payload), *[_failure() for _ in range(6)]],
        )

        result = await crawler._fetch_news()

        assert len(result) == 2
        assert result[0]["title"] == "KBO news today"
        assert result[1]["title"] == "baseball KBO update"
        assert crawler._fetch_day.await_count == 7

    @mark.asyncio
    async def test_a_failed_day_does_not_discard_the_other_days(self, crawler):
        """One unavailable day should not erase the six days that did answer."""
        payload = {"result": {"newsList": [{"title": "KBO news today"}]}}
        crawler._fetch_day = AsyncMock(
            side_effect=[_failure(status=503), *[_success(payload) for _ in range(6)]],
        )

        result = await crawler._fetch_news()

        assert len(result) == 6
        assert crawler._fetch_day.await_count == 7

    @mark.asyncio
    async def test_handles_an_unexpected_fetch_exception(self, crawler):
        crawler._fetch_day = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

        result = await crawler._fetch_news()

        assert result == []
        assert crawler._fetch_day.await_count == 7

    def test_the_base_uses_the_governed_client(self, crawler):
        """The old `BaseHttpCrawler.http_client()` only wrapped raw httpx."""
        assert isinstance(crawler._http, CrawlerHttpClient)


def _success(payload: dict) -> CrawlResult:
    """A successful Naver response, as `CrawlerHttpClient` returns one."""
    return CrawlResult.success(payload, http_status=200)


def _failure(status: int = 500) -> CrawlResult:
    """A failed Naver response, already classified by the transport."""
    return CrawlResult.failure(
        CrawlOutcome.PERMANENT_ERROR,
        error=f"HTTP {status}",
        error_code="FETCH_HTTP_PERMANENT",
        http_status=status,
        url="https://api-gw.sports.naver.com/news/articles/kbaseball",
    )


class TestRun:
    @mark.asyncio
    @patch.object(ConcreteCrawler, "_fetch_news")
    async def test_run_prints_dry_run(self, mock_fetch, crawler):
        mock_fetch.return_value = [{"title": "a"}, {"title": "b"}]
        await crawler.run(save=False)
        mock_fetch.assert_called_once()

    @mark.asyncio
    @patch.object(ConcreteCrawler, "_fetch_news")
    @patch.object(ConcreteCrawler, "_save_to_db")
    async def test_run_saves_when_requested(self, mock_save, mock_fetch, crawler):
        mock_fetch.return_value = [{"title": "a"}]
        await crawler.run(save=True)
        mock_save.assert_called_once_with([{"title": "a"}])


class TestAbstractEnforcement:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            NaverNewsCrawlerBase()
