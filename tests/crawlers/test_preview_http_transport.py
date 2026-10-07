from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.crawlers.http_client import CrawlerHttpClient
from src.crawlers.preview_crawler import PreviewCrawler
from src.crawlers.result import CrawlOutcome, CrawlResult


@pytest.fixture(autouse=True)
def _allow_source(monkeypatch) -> None:
    """Keep transport tests offline and avoid consulting robots.txt."""
    monkeypatch.setattr(
        "src.crawlers.preview_crawler.compliance.is_allowed",
        AsyncMock(return_value=True),
    )


@pytest.mark.asyncio
async def test_direct_post_uses_the_governed_client_and_preserves_payload_shape() -> None:
    payload = {"d": json.dumps([{"G_ID": "20260822LGSS0"}])}
    client = MagicMock(spec=CrawlerHttpClient)
    client.post_json = AsyncMock(return_value=CrawlResult.success(payload, http_status=200))
    crawler = PreviewCrawler(http_client=client)
    form = {"leId": "1", "srId": "0", "date": "20260822"}

    result = await crawler._fetch_api_json(
        crawler.GAME_LIST_URL,
        form,
        crawler.BASE_REFERER,
    )

    assert result == [{"G_ID": "20260822LGSS0"}]
    client.post_json.assert_awaited_once_with(
        crawler.GAME_LIST_URL,
        data=form,
        headers={**crawler.BASE_HEADERS, "Referer": crawler.BASE_REFERER},
    )


@pytest.mark.asyncio
async def test_empty_direct_response_does_not_trigger_playwright_fallback() -> None:
    client = MagicMock(spec=CrawlerHttpClient)
    client.post_json = AsyncMock(return_value=CrawlResult.empty(http_status=200))
    crawler = PreviewCrawler(http_client=client)
    page = MagicMock()
    page.request.post = AsyncMock()

    result = await crawler._fetch_api_json(crawler.GAME_LIST_URL, {}, crawler.BASE_REFERER, page=page)

    assert result == []
    page.request.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_failure_still_uses_playwright_fallback() -> None:
    payload = {"d": '[{"G_ID":"20260822LGSS0"}]'}
    client = MagicMock(spec=CrawlerHttpClient)
    client.post_json = AsyncMock(
        return_value=CrawlResult.failure(
            CrawlOutcome.RETRYABLE_ERROR,
            error="HTTP 503",
            error_code="FETCH_HTTP_ERROR",
            http_status=503,
        ),
    )
    crawler = PreviewCrawler(http_client=client)
    response = MagicMock(ok=True)
    response.json = AsyncMock(return_value=payload)
    page = MagicMock()
    page.request.post = AsyncMock(return_value=response)
    form = {"date": "20260822"}
    headers = {**crawler.BASE_HEADERS, "Referer": crawler.BASE_REFERER}

    result = await crawler._fetch_api_json(crawler.GAME_LIST_URL, form, crawler.BASE_REFERER, page=page)

    assert result == [{"G_ID": "20260822LGSS0"}]
    page.request.post.assert_awaited_once_with(crawler.GAME_LIST_URL, form=form, headers=headers)


@pytest.mark.asyncio
async def test_disallowed_source_does_not_call_either_transport(monkeypatch) -> None:
    client = MagicMock(spec=CrawlerHttpClient)
    client.post_json = AsyncMock()
    crawler = PreviewCrawler(http_client=client)
    allowed = AsyncMock(return_value=False)
    monkeypatch.setattr("src.crawlers.preview_crawler.compliance.is_allowed", allowed)
    page = MagicMock()
    page.request.post = AsyncMock()

    result = await crawler._fetch_api_json(crawler.GAME_LIST_URL, {}, crawler.BASE_REFERER, page=page)

    assert result is None
    client.post_json.assert_not_awaited()
    page.request.post.assert_not_awaited()


def test_preview_crawler_owns_a_governed_http_client() -> None:
    assert isinstance(PreviewCrawler()._http, CrawlerHttpClient)
