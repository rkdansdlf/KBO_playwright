from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.sources.relay.base import NormalizedRelayResult
from src.sources.relay.naver import NaverRelayAdapter


class TestNaverRelayAdapter:
    def test_fetch_game_success(self):
        mock_crawler = MagicMock()
        mock_crawler.crawl_game_relay = AsyncMock(
            return_value={
                "events": [
                    {
                        "event_type": "hit",
                        "wpa": 0.1,
                        "win_expectancy_before": 0.5,
                        "win_expectancy_after": 0.6,
                        "inning": 1,
                        "inning_half": "top",
                        "outs": 1,
                        "description": "Single to left",
                        "home_score": 2,
                        "away_score": 1,
                        "base_state": 1,
                    }
                ],
                "raw_pbp_rows": [{"inning": 1}],
                "parser_version": "2.0",
            }
        )
        mock_crawler.get_last_failure_reason = MagicMock(return_value=None)

        adapter = NaverRelayAdapter(crawler=mock_crawler)

        import asyncio

        result = asyncio.run(adapter.fetch_game("20260412SKLG0"))

        assert isinstance(result, NormalizedRelayResult)
        assert result.game_id == "20260412SKLG0"
        assert result.source_name == "naver"
        assert len(result.events) == 1
        assert len(result.raw_pbp_rows) == 1
        assert result.has_event_state is True
        assert result.has_raw_pbp is True
        assert result.parser_version == "2.0"

    def test_fetch_game_empty_result(self):
        mock_crawler = MagicMock()
        mock_crawler.crawl_game_relay = AsyncMock(return_value=None)
        mock_crawler.get_last_failure_reason = MagicMock(return_value="timeout")

        adapter = NaverRelayAdapter(crawler=mock_crawler)

        import asyncio

        result = asyncio.run(adapter.fetch_game("20260412SKLG0"))

        assert result.events == []
        assert result.raw_pbp_rows == []
        assert result.notes == "timeout"

    def test_fetch_game_no_failure_reason_getter(self):
        mock_crawler = MagicMock(spec=[])
        mock_crawler.crawl_game_relay = AsyncMock(return_value={"events": []})

        adapter = NaverRelayAdapter(crawler=mock_crawler)

        import asyncio

        result = asyncio.run(adapter.fetch_game("20260412SKLG0"))

        assert result.events == []
        assert "No events" in (result.notes or "")

    def test_init_default_crawler(self):
        with patch("src.sources.relay.naver.RelayCrawler") as MockCrawler:
            adapter = NaverRelayAdapter()
            assert adapter.source_name == "naver"
            MockCrawler.assert_called_once()
