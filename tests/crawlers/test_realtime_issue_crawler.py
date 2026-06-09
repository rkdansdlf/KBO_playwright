from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.crawlers.realtime_issue_crawler import RealtimeIssueCrawler


@pytest.fixture
def crawler():
    return RealtimeIssueCrawler(timeout=5)


class TestFetchNaverNewsHeadlines:
    @patch("src.crawlers.realtime_issue_crawler.httpx.Client")
    @patch("src.crawlers.realtime_issue_crawler.throttle.wait_sync")
    def test_returns_parsed_articles_from_api(self, mock_throttle, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"result":{"newsList":[{"title":"KBO News","subContent":"Content","oid":"123","aid":"456","officeName":"Sports","datetime":"2024-01-01"}]}}'
        mock_response.json.return_value = {
            "result": {
                "newsList": [
                    {"title": "KBO News", "subContent": "Content", "oid": "123", "aid": "456", "officeName": "Sports", "datetime": "2024-01-01"},
                ],
            },
        }
        mock_client.get.return_value = mock_response

        result = crawler.fetch_naver_news_headlines()

        assert len(result) == 1
        assert result[0]["title"] == "KBO News"
        assert "sports.news.naver.com" in result[0]["meta"]["source"]
        mock_client.get.assert_called_once()

    @patch("src.crawlers.realtime_issue_crawler.httpx.Client")
    @patch("src.crawlers.realtime_issue_crawler.throttle.wait_sync")
    def test_falls_back_to_html_on_api_failure(self, mock_throttle, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.side_effect = [mock_client, mock_client]
        mock_response_api = MagicMock()
        mock_response_api.status_code = 500
        mock_response_api.text = "error"

        mock_response_html = MagicMock()
        mock_response_html.status_code = 200
        mock_response_html.text = (
            '<html><body><a href="/kbaseball/news/read?oid=123&aid=456" title="Fallback News">link</a></body></html>'
        )

        mock_client.get.side_effect = [mock_response_api, mock_response_html]

        result = crawler.fetch_naver_news_headlines()

        assert len(result) >= 1
        assert "Fallback News" in result[0]["title"]

    @patch("src.crawlers.realtime_issue_crawler.httpx.Client")
    @patch("src.crawlers.realtime_issue_crawler.throttle.wait_sync")
    def test_returns_empty_on_all_failures(self, mock_throttle, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client.get.side_effect = httpx.HTTPError("network error")

        result = crawler.fetch_naver_news_headlines()
        assert result == []


class TestFetchMlbparkBullpenPosts:
    @patch("src.crawlers.realtime_issue_crawler.httpx.Client")
    @patch("src.crawlers.realtime_issue_crawler.throttle.wait_sync")
    def test_returns_parsed_posts(self, mock_throttle, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            '<html><body><a href="/mp/b.php?b=bullpen&m=view&id=12345">Hot Topic [15]</a>'
            '<a href="/mp/b.php?b=bullpen&m=view&id=67890">Another Post</a></body></html>'
        )
        mock_client.get.return_value = mock_response

        result = crawler.fetch_mlbpark_bullpen_posts()

        assert len(result) == 2
        assert "Hot Topic" in result[0]["title"]
        assert "Another Post" in result[1]["title"]

    @patch("src.crawlers.realtime_issue_crawler.httpx.Client")
    @patch("src.crawlers.realtime_issue_crawler.throttle.wait_sync")
    def test_deduplicates_urls(self, mock_throttle, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            '<html><body>'
            '<a href="/mp/b.php?b=bullpen&m=view&id=12345">Same</a>'
            '<a href="/mp/b.php?b=bullpen&m=view&id=12345">Same</a>'
            '</body></html>'
        )
        mock_client.get.return_value = mock_response

        result = crawler.fetch_mlbpark_bullpen_posts()
        assert len(result) == 1

    @patch("src.crawlers.realtime_issue_crawler.httpx.Client")
    @patch("src.crawlers.realtime_issue_crawler.throttle.wait_sync")
    def test_returns_empty_on_error(self, mock_throttle, mock_client_cls, crawler):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client.get.side_effect = httpx.HTTPError("timeout")

        result = crawler.fetch_mlbpark_bullpen_posts()
        assert result == []
