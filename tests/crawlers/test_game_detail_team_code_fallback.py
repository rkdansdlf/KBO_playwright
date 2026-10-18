"""Detail-page fallbacks must not leak GameCenter technical codes (issue #39)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.crawlers.game_detail_crawler import GameDetailCrawler, _season_detail_team_code


@pytest.mark.parametrize(
    ("code", "year", "expected"),
    [
        ("HT", 2026, "KIA"),
        ("OB", 2026, "DB"),
        ("SK", 2026, "SSG"),
        ("WO", 2026, "KH"),
        ("두산", 2026, "DB"),
        ("키움", 2026, "KH"),
        ("KIA", 2026, "KIA"),
        ("HT", 1995, "HT"),
        (None, 2026, None),
    ],
)
def test_season_detail_team_code(code, year, expected):
    assert _season_detail_team_code(code, year) == expected


async def test_scoreboard_fallback_codes_are_season_codes():
    page = AsyncMock()
    page.evaluate.return_value = None
    crawler = GameDetailCrawler()
    crawler._extract_live_scores = AsyncMock(return_value=None)
    crawler._fetch_scoreboard_inning_scores = AsyncMock(
        return_value={
            "away": {"name": "KIA", "code": "HT", "score": 5, "hits": 9, "errors": 0, "line_score": [5]},
            "home": {"name": "SSG", "code": "SK", "score": 2, "hits": 6, "errors": 1, "line_score": [2]},
        },
    )

    teams = await crawler._extract_team_info(page, "20260915HTSK0", 2026)

    assert (teams["away"]["code"], teams["home"]["code"]) == ("KIA", "SSG")
    # The ScoreBoard.aspx lookup still matches on the GameCenter tokens.
    crawler._fetch_scoreboard_inning_scores.assert_awaited_once_with(page, "20260915HTSK0", "HT", "SK")


async def test_live_score_fallback_codes_are_season_codes():
    page = AsyncMock()
    page.evaluate.return_value = None
    crawler = GameDetailCrawler()
    crawler._extract_live_scores = AsyncMock(
        return_value={
            "away": {"name": "키움", "code": "", "score": 1, "hits": 4, "errors": 0, "line_score": []},
            "home": {"name": "두산", "code": "OB", "score": 3, "hits": 8, "errors": 0, "line_score": []},
        },
    )
    crawler._fetch_scoreboard_inning_scores = AsyncMock(return_value=None)

    teams = await crawler._extract_team_info(page, "20260921WOOB0", 2026)

    assert (teams["away"]["code"], teams["home"]["code"]) == ("KH", "DB")
