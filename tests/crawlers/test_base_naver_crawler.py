from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest
from pytest import mark

from src.crawlers.base_naver_crawler import NaverNewsCrawlerBase


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


@pytest.fixture
def crawler():
    return ConcreteCrawler()


class TestFetchNews:
    @mark.asyncio
    @patch("src.crawlers.base_naver_crawler.httpx.Client")
    async def test_returns_matching_articles(self, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "newsList": [
                    {"title": "KBO news today", "content": "details"},
                    {"title": "Something else", "content": "irrelevant"},
                    {"title": "baseball KBO update", "content": "more"},
                ],
            },
        }
        mock_client.get.side_effect = [mock_response] + [httpx.HTTPError("stop")] * 6

        result = await crawler._fetch_news()
        assert len(result) == 2
        assert result[0]["title"] == "KBO news today"
        assert result[1]["title"] == "baseball KBO update"

    @mark.asyncio
    @patch("src.crawlers.base_naver_crawler.httpx.Client")
    async def test_handles_non_200_response(self, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client.get.side_effect = [mock_response] + [httpx.HTTPError("stop")] * 6

        result = await crawler._fetch_news()
        assert result == []

    @mark.asyncio
    @patch("src.crawlers.base_naver_crawler.httpx.Client")
    async def test_handles_api_exception(self, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        result = await crawler._fetch_news()
        assert result == []


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
