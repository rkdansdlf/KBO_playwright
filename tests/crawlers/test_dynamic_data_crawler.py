from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.crawlers.dynamic_data_crawler import TEAM_TICKET_RULES, DynamicDataCrawler
from src.models.game import Game
from src.models.ticket_schedule import TicketSchedule


@pytest.fixture
def mock_session():
    return MagicMock(spec=Session)


@pytest.fixture
def crawler(mock_session):
    return DynamicDataCrawler(mock_session)


class TestDynamicDataCrawler:
    def test_init_creates_roster_crawler(self, mock_session):
        crawler = DynamicDataCrawler(mock_session)
        assert crawler.db_session is mock_session
        assert isinstance(crawler.roster_crawler, DailyRosterCrawler)

    @pytest.mark.asyncio
    async def test_crawl_roster_changes_success(self, crawler):
        mock_records = [{"player_id": "12345", "date": "20250101"}]
        with patch.object(crawler.roster_crawler, "crawl_date_range", return_value=mock_records) as mock_crawl:
            result = await crawler.crawl_roster_changes("20250101", "20250131")

            mock_crawl.assert_called_once_with("20250101", "20250131")
            assert result == mock_records

    @pytest.mark.asyncio
    async def test_crawl_roster_changes_error(self, crawler):
        with patch.object(crawler.roster_crawler, "crawl_date_range", side_effect=ValueError("network error")):
            with pytest.raises(ValueError, match="network error"):
                await crawler.crawl_roster_changes("20250101", "20250131")

    def test_crawl_and_update_ticket_times_no_games(self, crawler, mock_session):
        mock_session.scalars.return_value.all.return_value = []

        result = crawler.crawl_and_update_ticket_times(lookahead_days=14)

        assert result == []
        mock_session.commit.assert_called_once()

    def test_crawl_and_update_ticket_times_with_games(self, crawler, mock_session):
        today = datetime.now().date()
        home_team_name = list(TEAM_TICKET_RULES.keys())[0]
        game = MagicMock(spec=Game)
        game.game_date = today
        game.home_team = home_team_name
        game.away_team = "SomeTeam"
        game.stadium = "Some Stadium"
        mock_session.scalars.return_value.all.return_value = [game]
        mock_session.scalar.return_value = None

        result = crawler.crawl_and_update_ticket_times(lookahead_days=14)

        assert len(result) == 1
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        new_ticket = mock_session.add.call_args[0][0]
        assert isinstance(new_ticket, TicketSchedule)
        assert new_ticket.home_team == home_team_name
        assert new_ticket.away_team == "SomeTeam"

    def test_crawl_and_update_ticket_times_updates_existing(self, crawler, mock_session):
        today = datetime.now().date()
        home_team_name = list(TEAM_TICKET_RULES.keys())[0]
        game = MagicMock(spec=Game)
        game.game_date = today
        game.home_team = home_team_name
        game.away_team = "AwayTeam"
        game.stadium = "Stadium"
        mock_session.scalars.return_value.all.return_value = [game]

        existing_ticket = MagicMock(spec=TicketSchedule)
        existing_ticket.game_date = today
        existing_ticket.home_team = home_team_name
        existing_ticket.platform = TEAM_TICKET_RULES[home_team_name][2]
        mock_session.scalar.return_value = existing_ticket

        result = crawler.crawl_and_update_ticket_times(lookahead_days=14)

        assert len(result) == 1
        assert existing_ticket.away_team == "AwayTeam"
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called_once()

    def test_crawl_and_update_ticket_times_skip_unknown_home_team(self, crawler, mock_session):
        today = datetime.now().date()
        game = MagicMock(spec=Game)
        game.game_date = today
        game.home_team = "UnknownTeam"
        game.away_team = "AwayTeam"
        game.stadium = "Stadium"
        mock_session.scalars.return_value.all.return_value = [game]

        result = crawler.crawl_and_update_ticket_times(lookahead_days=14)

        assert result == []
        mock_session.commit.assert_called_once()

    def test_team_ticket_rules_have_all_teams(self):
        expected = {"두산", "키움", "LG", "KT", "SSG", "KIA", "삼성", "한화", "NC", "롯데"}
        assert set(TEAM_TICKET_RULES.keys()) == expected

    def test_team_ticket_rules_structure(self):
        for _team, (days_before, hour, platform, url) in TEAM_TICKET_RULES.items():
            assert isinstance(days_before, int) and days_before > 0
            assert isinstance(hour, int) and 0 <= hour <= 23
            assert isinstance(platform, str) and platform
            assert isinstance(url, str) and url.startswith("http")
