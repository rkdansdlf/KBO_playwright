"""Unit tests for ``GameDetailCrawler._resolve_record_naver_id``.

The helper resolves the real Naver record id through the schedule lookup
before the record URL is built. Postseason/wild-card games carry a different
id prefix than the computed ``YYYYMMDD{away}{home}YYYY`` form, so a plain
computed id 404s for those games.

These tests exercise all three branches (resolved id / schedule miss /
schedule error) against a fake relay class, so no network traffic is made.
The fallback delegates to ``RelayCrawler._map_to_naver_id``, a pure
staticmethod.
"""

from __future__ import annotations

import logging

import pytest

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler

_COMPUTED_ID = "20261014OBLG02026"
_SCHEDULED_ID = "44441014OBLG02026"
_GAME_ID = "20261014OBLG0"


def _fake_relay_cls(
    resolved: str | None = None,
    *,
    raises: Exception | None = None,
) -> type:
    """Build a fake relay crawler class with a scripted resolver result."""

    class _FakeRelayCrawler:
        def __init__(self) -> None:
            pass

        async def _resolve_naver_game_id(self, game_id: str) -> str | None:
            if raises is not None:
                raise raises
            return resolved

        @staticmethod
        def _map_to_naver_id(game_id: str) -> str:
            return RelayCrawler._map_to_naver_id(game_id)

    return _FakeRelayCrawler


class TestTheResolvedScheduleIdWins:
    """When the schedule knows the game, its Naver id is used verbatim."""

    @pytest.mark.asyncio
    async def test_the_schedule_id_is_returned_unchanged(self) -> None:
        crawler = GameDetailCrawler()
        relay_cls = _fake_relay_cls(_SCHEDULED_ID)

        resolved = await crawler._resolve_record_naver_id(relay_cls, _GAME_ID)

        assert resolved == _SCHEDULED_ID


class TestAScheduleMissFallsBackToTheComputedId:
    """A ``None`` lookup result falls back to the computed Naver id."""

    @pytest.mark.asyncio
    async def test_none_falls_back_to_the_computed_id(self) -> None:
        crawler = GameDetailCrawler()
        relay_cls = _fake_relay_cls(None)

        resolved = await crawler._resolve_record_naver_id(relay_cls, _GAME_ID)

        assert resolved == _COMPUTED_ID


class TestAScheduleErrorFallsBackRatherThanPropagating:
    """A raising schedule lookup is logged and falls back, never raises."""

    @pytest.mark.asyncio
    async def test_an_exception_falls_back_and_is_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        crawler = GameDetailCrawler()
        relay_cls = _fake_relay_cls(raises=RuntimeError("schedule down"))

        with caplog.at_level(logging.ERROR):
            resolved = await crawler._resolve_record_naver_id(relay_cls, _GAME_ID)

        assert resolved == _COMPUTED_ID
        assert any("Schedule lookup raised for" in record.getMessage() for record in caplog.records)
