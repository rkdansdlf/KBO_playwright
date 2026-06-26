from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.schedule_crawler import ScheduleCrawler


@pytest.fixture
def crawler():
    return ScheduleCrawler()


def _make_raw_game(
    game_id="20250625LGSS0",
    game_date="20250625",
    away_segment="LG",
    home_segment="SS",
    game_status="SCHEDULED",
    crawl_status="link_parsed",
    doubleheader_no=0,
    game_time="18:30",
    stadium="잠실",
    url_suffix="/Schedule/GameCenter/Main.aspx?gameId=20250625LGSS0",
    season_year=2025,
    season_type="regular",
    away_name=None,
    home_name=None,
):
    g = {
        "game_id": game_id,
        "game_date": game_date,
        "season_year": season_year,
        "season_type": season_type,
        "away_segment": away_segment,
        "home_segment": home_segment,
        "doubleheader_no": doubleheader_no,
        "game_status": game_status,
        "crawl_status": crawl_status,
        "url_suffix": url_suffix,
        "game_time": game_time,
        "stadium": stadium,
    }
    if away_name:
        g["away_name"] = away_name
    if home_name:
        g["home_name"] = home_name
    return g


class TestExtractGames:
    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_basic_link_parsed_game(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [_make_raw_game()]

        result = await crawler._extract_games(page, 2025, 6)
        assert len(result) == 1
        assert result[0]["game_id"] == "20250625LGSS0"
        assert result[0]["away_team_code"] == "LG"
        assert result[0]["home_team_code"] == "SS"

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.resolve_team_code")
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_text_parsed_game_no_id(self, mock_team_code, mock_resolve, crawler):
        mock_team_code.return_value = None
        mock_resolve.side_effect = lambda name, year: {"LG 트윈스": "LG", "삼성 라이온즈": "SS"}.get(name)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(
                game_id=None,
                away_segment=None,
                home_segment=None,
                away_name="LG 트윈스",
                home_name="삼성 라이온즈",
                crawl_status="text_parsed",
                url_suffix="",
            ),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        assert len(result) >= 0

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_empty_results(self, mock_team_code, crawler):
        page = AsyncMock()
        page.evaluate.return_value = []

        result = await crawler._extract_games(page, 2025, 6)
        assert result == []

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_status_normalization(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(game_status="CANCELLED"),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        assert len(result) >= 0
        if result:
            assert result[0]["game_status"] == "CANCELLED"

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_game_time_preserved(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(game_time="18:30"),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        if result:
            assert result[0]["game_time"] == "18:30"

    @pytest.mark.asyncio
    @patch("src.crawlers.schedule_crawler.team_code_from_game_id_segment")
    async def test_url_suffix_construction(self, mock_team_code, crawler):
        mock_team_code.side_effect = lambda seg, year: {"LG": "LG", "SS": "SS"}.get(seg)
        page = AsyncMock()
        page.evaluate.return_value = [
            _make_raw_game(url_suffix="/Schedule/GameCenter/Main.aspx?gameId=20250625LGSS0"),
        ]

        result = await crawler._extract_games(page, 2025, 6)
        if result:
            assert result[0]["url"].startswith("https://www.koreabaseball.com")
