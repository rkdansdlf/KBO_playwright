from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.crawlers.relay_outcome import AttemptSeed, RelayStatus, build_attempt

from src.sources.relay.base import NormalizedRelayResult
from src.sources.relay.naver import NaverRelayAdapter


def _attempt_for(payload, *, status=RelayStatus.SUCCESS, reason=None, **seed):
    """The typed attempt the adapter now reads, built from the same vocabulary."""
    return build_attempt(
        "20260412SKLG0",
        AttemptSeed(status=status, result=payload, reason=reason, **seed),
    )


class TestNaverRelayAdapter:
    def test_fetch_game_success(self):
        mock_crawler = MagicMock()
        source_payload = [{"title": "1회초", "textOptions": []}]
        payload = {
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
                },
            ],
            "raw_pbp_rows": [{"inning": 1}],
            "parser_version": "2.0",
            "source_payload": source_payload,
        }
        # The adapter reads the typed attempt now, so a half-fetched game is
        # visible to it as partial rather than as a completed payload.
        mock_crawler.crawl_relay_attempt = AsyncMock(return_value=_attempt_for(payload))

        adapter = NaverRelayAdapter(crawler=mock_crawler)

        result = asyncio.run(adapter.fetch_game("20260412SKLG0"))

        assert isinstance(result, NormalizedRelayResult)
        assert result.game_id == "20260412SKLG0"
        assert result.source_name == "naver"
        assert len(result.events) == 1
        assert len(result.raw_pbp_rows) == 1
        assert result.has_event_state is True
        assert result.has_raw_pbp is True
        assert result.parser_version == "2.0"
        assert result.source_payload == source_payload

    def test_fetch_game_empty_result(self):
        mock_crawler = MagicMock()
        mock_crawler.crawl_relay_attempt = AsyncMock(
            return_value=_attempt_for(None, status=RelayStatus.FAILED, reason="relay_api_error"),
        )

        adapter = NaverRelayAdapter(crawler=mock_crawler)

        result = asyncio.run(adapter.fetch_game("20260412SKLG0"))

        assert result.events == []
        assert result.raw_pbp_rows == []
        assert result.notes == "relay request failed before a payload was parsed"

    def test_fetch_game_not_modified_preserves_status(self):
        mock_crawler = MagicMock()
        mock_crawler.crawl_relay_attempt = AsyncMock(
            return_value=_attempt_for(
                {"status": "not_modified", "payload_hash": "a" * 64}, status=RelayStatus.NOT_MODIFIED
            ),
        )

        result = asyncio.run(NaverRelayAdapter(crawler=mock_crawler).fetch_game("20260412SKLG0"))

        assert result.is_not_modified is True
        assert result.is_empty is False
        assert result.status == "not_modified"
        assert result.payload_hash == "a" * 64

    def test_a_crawler_that_reports_nothing_still_yields_a_note(self):
        """The adapter no longer reads a reason off the crawler.

        It reads the typed attempt instead, so an absence arrives as EMPTY with
        an explanation already attached. The note is what a later recovery reads
        to decide the game is genuinely absent rather than merely unprocessed.
        """
        mock_crawler = MagicMock(spec=[])
        # The production shape for an absence: a failure that classifies as one.
        mock_crawler.crawl_relay_attempt = AsyncMock(
            return_value=_attempt_for(None, status=RelayStatus.FAILED, reason="relay_not_found"),
        )

        adapter = NaverRelayAdapter(crawler=mock_crawler)

        result = asyncio.run(adapter.fetch_game("20260412SKLG0"))

        assert result.events == []
        assert "no games" in (result.notes or "")

    def test_init_default_crawler(self):
        with patch("src.sources.relay.naver.RelayCrawler") as MockCrawler:
            adapter = NaverRelayAdapter()
            assert adapter.source_name == "naver"
            MockCrawler.assert_called_once()
