from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.staff_register_crawler import (
    StaffRegisterCrawler,
    _parse_birth_date,
    _parse_hands,
    _parse_hw,
    _parse_player_id,
)


class TestParsePlayerId:
    def test_extracts_id(self):
        assert _parse_player_id("/Record/Player/HitterDetail/Basic.aspx?playerId=91350") == 91350

    def test_none_returns_none(self):
        assert _parse_player_id(None) is None

    def test_no_match_returns_none(self):
        assert _parse_player_id("/some/page.aspx") is None


class TestParseHw:
    def test_parses_height_weight(self):
        h, w = _parse_hw("185cm, 92kg")
        assert h == 185
        assert w == 92

    def test_no_match_returns_none(self):
        h, w = _parse_hw("No data")
        assert h is None
        assert w is None

    def test_empty_string(self):
        h, w = _parse_hw("")
        assert h is None
        assert w is None


class TestParseBirthDate:
    def test_parses_date(self):
        result = _parse_birth_date("1990-05-15")
        assert result == date(1990, 5, 15)

    def test_no_match_returns_none(self):
        assert _parse_birth_date("Unknown") is None

    def test_empty_returns_none(self):
        assert _parse_birth_date("") is None

    def test_invalid_calendar_date_returns_none(self):
        assert _parse_birth_date("2026-02-30") is None


class TestParseHands:
    def test_parses_right_throw_right_bat(self):
        throws, bats = _parse_hands("우투우타")
        assert throws == "R"
        assert bats == "R"

    def test_parses_left_throw_left_bat(self):
        throws, bats = _parse_hands("좌투좌타")
        assert throws == "L"
        assert bats == "L"

    def test_no_match_returns_none(self):
        throws, bats = _parse_hands("")
        assert throws is None
        assert bats is None


class TestStaffRegisterCrawler:
    @pytest.mark.asyncio
    async def test_crawl_team_normalizes_staff_rows_and_skips_daily_placeholder(self):
        crawler = StaffRegisterCrawler()
        crawler.policy.delay_async = AsyncMock()
        page = MagicMock()
        page.evaluate = AsyncMock(
            side_effect=[
                None,
                [
                    {
                        "href": "/Record/Player/HitterDetail/Basic.aspx?playerId=91350",
                        "name": " 염경엽 ",
                        "uniform_no": "85",
                        "hand_text": "우투좌타",
                        "birth_text": "1968-03-01",
                        "physical_text": "178cm, 75kg",
                        "staff_role": "manager",
                    },
                    {
                        "href": None,
                        "name": "당일 등록 없음",
                        "uniform_no": "",
                        "hand_text": "",
                        "birth_text": "",
                        "physical_text": "",
                        "staff_role": "coach",
                    },
                ],
            ],
        )
        page.wait_for_timeout = AsyncMock()

        records = await crawler.crawl_team(page, "LG")

        assert records == [
            {
                "player_id": 91350,
                "name": "염경엽",
                "uniform_no": "85",
                "team": "LG",
                "birth_date": "1968-03-01",
                "birth_date_date": date(1968, 3, 1),
                "height_cm": 178,
                "weight_kg": 75,
                "throws": "R",
                "bats": "L",
                "status": "staff",
                "staff_role": "manager",
                "status_source": "register",
            },
        ]
        crawler.policy.delay_async.assert_awaited_once_with(host="www.koreabaseball.com")
        assert page.evaluate.await_args_list[0].args == ("fnSearchChange('LG')",)

    @pytest.mark.asyncio
    @patch("src.crawlers.staff_register_crawler.async_playwright")
    async def test_crawl_all_teams_continues_after_team_error_and_closes_browser(self, mock_playwright):
        crawler = StaffRegisterCrawler()
        crawler.crawl_team = AsyncMock(side_effect=[[{"name": "manager"}], RuntimeError("team page failed")])

        page = MagicMock()
        page.goto = AsyncMock()
        page.wait_for_timeout = AsyncMock()
        context = MagicMock()
        context.new_page = AsyncMock(return_value=page)
        browser = MagicMock()
        browser.new_context = AsyncMock(return_value=context)
        browser.close = AsyncMock()
        playwright = MagicMock()
        playwright.chromium.launch = AsyncMock(return_value=browser)
        manager = MagicMock()
        manager.__aenter__ = AsyncMock(return_value=playwright)
        manager.__aexit__ = AsyncMock(return_value=None)
        mock_playwright.return_value = manager

        records = await crawler.crawl_all_teams(team_codes=["LG", "SS"])

        assert records == [{"name": "manager"}]
        assert crawler.crawl_team.await_count == 2
        page.goto.assert_awaited_once()
        browser.close.assert_awaited_once()

    def test_save_to_db_skips_invalid_records_without_creating_repository(self):
        crawler = StaffRegisterCrawler()

        assert crawler.save_to_db([{"name": "링크 없는 코치", "player_id": None}]) == 0
