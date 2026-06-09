from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_p0_data import main


class TestCrawlP0DataCLI:
    def test_main_all_default(self):
        with patch("src.crawlers.team_event_crawler.TeamEventCrawler") as MockEvents, \
             patch("src.crawlers.roster_transaction_crawler.RosterTransactionCrawler") as MockRoster, \
             patch("src.crawlers.ticket_crawler.TicketCrawler") as MockTicket:
            mock_events = MagicMock()
            mock_events.run = AsyncMock(return_value=[])
            MockEvents.return_value = mock_events
            mock_roster = MagicMock()
            mock_roster.run = AsyncMock(return_value=[])
            MockRoster.return_value = mock_roster
            mock_ticket = MagicMock()
            mock_ticket.run = AsyncMock(return_value=[])
            MockTicket.return_value = mock_ticket

            result = main([])

            assert result == {"events": 0, "roster": 0, "ticket": 0}

    def test_main_events_only(self):
        with patch("src.crawlers.team_event_crawler.TeamEventCrawler") as MockEvents:
            mock_events = MagicMock()
            mock_events.run = AsyncMock(return_value=[{"id": 1}])
            MockEvents.return_value = mock_events

            result = main(["--type", "events", "--save"])

            assert result == {"events": 1}
            mock_events.run.assert_called_once_with(save=True, team_filter=None)

    def test_main_roster_with_date(self):
        with patch("src.crawlers.roster_transaction_crawler.RosterTransactionCrawler") as MockRoster:
            mock_roster = MagicMock()
            mock_roster.run = AsyncMock(return_value=[])
            MockRoster.return_value = mock_roster

            main(["--type", "roster", "--save", "--target-date", "20241015"])

            mock_roster.run.assert_called_once_with(save=True, target_date="2024-10-15")
