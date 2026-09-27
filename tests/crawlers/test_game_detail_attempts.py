"""The batch crawl keeps a slot for every game, and the old signature still works.

`crawl_games` filters failures out of its return value, so a caller that needs
to know which game failed has to diff the target list against the result and read
`_last_failure_reason`. That reconstruction cannot distinguish a game that
produced a degraded payload from a game that produced nothing at all -- both are
absent from the result.

These tests pin both halves of the change: `crawl_game_attempts` returns one
typed outcome per requested game, and `crawl_games` still returns payloads only,
so the seven existing callers are untouched.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.game_detail_outcome import GameDetailStatus

FULL_DETAIL: dict = {
    "hitters": {"away": [{}], "home": [{}]},
    "pitchers": {"away": [{}], "home": [{}]},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 5}},
}

DEGRADED_DETAIL: dict = {
    "hitters": {"away": [], "home": []},
    "pitchers": {"away": [], "home": []},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 5}},
}


def _crawler(payload_for) -> GameDetailCrawler:
    """Build a crawler whose browser path returns a payload per game, or None."""
    pool = MagicMock(max_pages=2)
    pool.start = AsyncMock()
    pool.acquire = AsyncMock(side_effect=[MagicMock(), MagicMock()])
    pool.release = AsyncMock()
    pool.close = AsyncMock()
    crawler = GameDetailCrawler(resolver=MagicMock(), pool=pool)
    crawler._crawl_naver_single = AsyncMock(return_value=None)

    async def _crawl_single(_page, game_id, _game_date, *, lightweight):
        return payload_for(game_id)

    crawler._crawl_single = AsyncMock(side_effect=_crawl_single)
    return crawler


def _games(*ids: str) -> list[dict[str, str]]:
    return [{"game_id": game_id, "game_date": "20250501"} for game_id in ids]


async def _run(crawler: GameDetailCrawler, games: list[dict[str, str]], *, lightweight: bool = False):
    with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=True)):
        return await crawler.crawl_game_attempts(games, concurrency=2, lightweight=lightweight)


class TestEveryGameGetsAnOutcome:
    @pytest.mark.asyncio
    async def test_a_successful_batch_returns_one_attempt_per_game(self) -> None:
        crawler = _crawler(lambda game_id: FULL_DETAIL)

        attempts = await _run(crawler, _games("20250501LGOB0", "20250502KTSS0"))

        assert [a.game_id for a in attempts] == ["20250501LGOB0", "20250502KTSS0"]
        assert all(a.status is GameDetailStatus.SUCCESS for a in attempts)

    @pytest.mark.asyncio
    async def test_a_failed_game_is_still_reported(self) -> None:
        """The bug this fixes: a failed game simply disappeared from the result."""
        crawler = _crawler(lambda game_id: FULL_DETAIL if game_id == "20250502KTSS0" else None)
        crawler._last_failure_reason["20250501LGOB0"] = "navigation_error"

        attempts = await _run(crawler, _games("20250501LGOB0", "20250502KTSS0"))

        by_id = {a.game_id: a for a in attempts}
        assert len(attempts) == 2
        assert by_id["20250501LGOB0"].status is GameDetailStatus.FAILED
        assert by_id["20250501LGOB0"].error_code == "FETCH_HTTP_ERROR"
        assert by_id["20250502KTSS0"].status is GameDetailStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_mixed_outcomes_stay_distinguishable(self) -> None:
        """A timeout, a degraded payload, and a full result must not collapse."""

        def _payload_for(game_id: str) -> dict | None:
            if game_id == "A":
                raise AssertionError
            if game_id == "B":
                return DEGRADED_DETAIL
            if game_id == "C":
                return FULL_DETAIL
            return None

        crawler = _crawler(_payload_for)
        crawler._last_failure_reason["A"] = "timeout"
        crawler._last_failure_reason["D"] = "incomplete_detail"

        attempts = await _run(crawler, _games("A", "B", "C", "D"))
        by_id = {a.game_id: a for a in attempts}

        assert by_id["A"].status is GameDetailStatus.FAILED
        assert by_id["A"].error_code == "FETCH_TIMEOUT"
        assert by_id["B"].status is GameDetailStatus.PARTIAL
        assert by_id["B"].error_code == "VALIDATION_QUALITY"
        assert by_id["C"].status is GameDetailStatus.SUCCESS
        assert by_id["D"].status is GameDetailStatus.FAILED
        assert by_id["D"].error_code == "VALIDATION_QUALITY"

    @pytest.mark.asyncio
    async def test_input_order_is_preserved(self) -> None:
        crawler = _crawler(lambda game_id: FULL_DETAIL)

        attempts = await _run(crawler, _games("20250503SSLG0", "20250501LGOB0", "20250502KTSS0"))

        assert [a.game_id for a in attempts] == ["20250503SSLG0", "20250501LGOB0", "20250502KTSS0"]

    @pytest.mark.asyncio
    async def test_an_empty_request_produces_no_attempts(self) -> None:
        crawler = _crawler(lambda game_id: FULL_DETAIL)

        assert await _run(crawler, []) == []

    @pytest.mark.asyncio
    async def test_lightweight_degraded_results_are_successes(self) -> None:
        crawler = _crawler(lambda game_id: DEGRADED_DETAIL)

        attempts = await _run(crawler, _games("20250501LGOB0"), lightweight=True)

        assert attempts[0].status is GameDetailStatus.SUCCESS
        assert attempts[0].needs_refetch is False

    @pytest.mark.asyncio
    async def test_full_mode_degraded_results_need_a_refetch(self) -> None:
        crawler = _crawler(lambda game_id: DEGRADED_DETAIL)

        attempts = await _run(crawler, _games("20250501LGOB0"), lightweight=False)

        assert attempts[0].status is GameDetailStatus.PARTIAL
        assert attempts[0].needs_refetch is True


class TestTheOldSignatureIsUnchanged:
    @pytest.mark.asyncio
    async def test_crawl_games_still_returns_payloads_only(self) -> None:
        crawler = _crawler(lambda game_id: FULL_DETAIL)

        with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=True)):
            payloads = await crawler.crawl_games(_games("A", "B"), concurrency=2)

        assert payloads == [FULL_DETAIL, FULL_DETAIL]

    @pytest.mark.asyncio
    async def test_crawl_games_still_drops_failures(self) -> None:
        """Existing callers rely on this: a failed game must not appear."""
        crawler = _crawler(lambda game_id: FULL_DETAIL if game_id == "B" else None)
        crawler._last_failure_reason["A"] = "timeout"

        with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=True)):
            payloads = await crawler.crawl_games(_games("A", "B"), concurrency=2)

        assert len(payloads) == 1

    @pytest.mark.asyncio
    async def test_crawl_games_still_returns_empty_for_no_games(self) -> None:
        crawler = _crawler(lambda game_id: FULL_DETAIL)

        with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=True)):
            assert await crawler.crawl_games([], concurrency=2) == []
