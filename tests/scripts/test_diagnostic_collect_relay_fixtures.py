from unittest.mock import AsyncMock, MagicMock, patch

from scripts.diagnostic.collect_relay_fixtures import _ensure_dirs, fetch_naver_relay, fetch_naver_schedule


class TestEnsureDirs:
    @patch("pathlib.Path.mkdir")
    def test_creates_dirs(self, mock_mkdir):
        _ensure_dirs()
        assert mock_mkdir.call_count == 4


class TestFetchNaverSchedule:
    @patch("scripts.diagnostic.collect_relay_fixtures.RelayCrawler")
    def test_no_games(self, mock_crawler_cls):
        import asyncio

        mock_crawler = AsyncMock(spec=["_schedule_query_dates", "_schedule_query_context", "_request_json"])
        mock_crawler_cls.return_value = mock_crawler
        mock_crawler._schedule_query_dates = MagicMock(return_value=[])
        mock_crawler._schedule_query_context = MagicMock(return_value={})
        mock_crawler._request_json = AsyncMock(return_value=({"result": {"games": []}}, None))

        result = asyncio.run(fetch_naver_schedule("test_id", mock_crawler))
        assert result is None


class TestFetchNaverRelay:
    @patch("scripts.diagnostic.collect_relay_fixtures.RelayCrawler")
    def test_no_data(self, mock_crawler_cls):
        import asyncio

        mock_crawler = AsyncMock()
        mock_crawler_cls.return_value = mock_crawler
        mock_crawler.api_base_url = "http://example.com/{game_id}"
        mock_crawler._request_json = AsyncMock(return_value=(None, None))

        result = asyncio.run(fetch_naver_relay("naver_id", mock_crawler))
        assert result is None
