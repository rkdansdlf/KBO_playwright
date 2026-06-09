from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.discover_historical_players import main


class TestDiscoverHistoricalPlayersCLI:
    def test_main_default_args(self):
        with (
            patch("src.cli.discover_historical_players.RetiredPlayerListingCrawler") as MockCrawler,
            patch("src.cli.discover_historical_players.SessionLocal"),
        ):
            mock_instance = MagicMock()
            mock_instance.collect_historical_player_ids = AsyncMock(return_value={})
            mock_instance.collect_player_ids_for_year = AsyncMock(return_value={})
            MockCrawler.return_value = mock_instance

            main([])

            MockCrawler.assert_called_once_with(request_delay=1.0)

    def test_main_custom_range(self):
        with (
            patch("src.cli.discover_historical_players.RetiredPlayerListingCrawler") as MockCrawler,
            patch("src.cli.discover_historical_players.SessionLocal"),
        ):
            mock_instance = MagicMock()
            mock_instance.collect_historical_player_ids = AsyncMock(return_value={})
            mock_instance.collect_player_ids_for_year = AsyncMock(return_value={})
            MockCrawler.return_value = mock_instance

            main(["--start", "2000", "--end", "2010", "--active-year", "2025"])

            mock_instance.collect_historical_player_ids.assert_called_once_with(range(2000, 2011))
            mock_instance.collect_player_ids_for_year.assert_called_once_with(2025)

    def test_main_saves_players(self):
        with (
            patch("src.cli.discover_historical_players.RetiredPlayerListingCrawler") as MockCrawler,
            patch("src.cli.discover_historical_players.SessionLocal") as MockSessionLocal,
        ):
            mock_instance = MagicMock()
            mock_instance.collect_historical_player_ids = AsyncMock(return_value={"12345": "Kim"})
            mock_instance.collect_player_ids_for_year = AsyncMock(return_value={})
            MockCrawler.return_value = mock_instance

            mock_session = MagicMock()
            MockSessionLocal.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter_by.return_value.first.return_value = None

            main(["--start", "2020", "--end", "2020"])

            mock_session.add.assert_called()
            mock_session.commit.assert_called()
