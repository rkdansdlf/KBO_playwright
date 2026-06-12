from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.collect_games import main


class TestCollectGamesCLI:
    def test_main_year_default(self):
        with (
            patch("src.cli.collect_games.load_game_targets_from_db") as mock_load,
            patch("src.cli.collect_games.SessionLocal") as mock_sesh,
            patch("src.services.player_id_resolver.PlayerIdResolver"),
            patch("src.cli.collect_games.GameDetailCrawler"),
            patch("src.cli.collect_games.NaverRelayCrawler"),
            patch("src.cli.collect_games.crawl_and_save_game_details", new_callable=AsyncMock) as mock_crawl,
        ):
            mock_load.return_value = []
            mock_sesh.return_value = MagicMock()
            result = MagicMock(
                detail_saved=0,
                detail_failed=0,
                detail_skipped_existing=0,
                relay_saved_games=0,
                relay_rows_saved=0,
                relay_skipped_existing=0,
                processed_game_ids=set(),
                items={},
            )
            mock_crawl.return_value = result
            main(["--year", "2025"])
            mock_load.assert_called_once_with(2025, None)

    def test_main_with_month(self):
        with (
            patch("src.cli.collect_games.load_game_targets_from_db") as mock_load,
            patch("src.cli.collect_games.SessionLocal") as mock_sesh,
            patch("src.services.player_id_resolver.PlayerIdResolver"),
            patch("src.cli.collect_games.GameDetailCrawler"),
            patch("src.cli.collect_games.NaverRelayCrawler"),
            patch("src.cli.collect_games.crawl_and_save_game_details", new_callable=AsyncMock) as mock_crawl,
        ):
            mock_load.return_value = []
            mock_sesh.return_value = MagicMock()
            result = MagicMock(
                detail_saved=3,
                detail_failed=0,
                detail_skipped_existing=0,
                relay_saved_games=0,
                relay_rows_saved=0,
                relay_skipped_existing=0,
                processed_game_ids=set(),
                items={},
            )
            mock_crawl.return_value = result
            main(["--year", "2025", "--month", "10"])
            mock_load.assert_called_once_with(2025, 10)

    def test_main_with_game_ids(self):
        with (
            patch("src.cli.collect_games.load_game_targets_by_ids") as mock_load,
            patch("src.cli.collect_games.SessionLocal") as mock_sesh,
            patch("src.services.player_id_resolver.PlayerIdResolver"),
            patch("src.cli.collect_games.GameDetailCrawler"),
            patch("src.cli.collect_games.NaverRelayCrawler"),
            patch("src.cli.collect_games.crawl_and_save_game_details", new_callable=AsyncMock) as mock_crawl,
        ):
            mock_load.return_value = []
            mock_sesh.return_value = MagicMock()
            result = MagicMock(
                detail_saved=0,
                detail_failed=0,
                detail_skipped_existing=0,
                relay_saved_games=0,
                relay_rows_saved=0,
                relay_skipped_existing=0,
                processed_game_ids=set(),
                items={},
            )
            mock_crawl.return_value = result
            main(["--year", "2025", "--game-ids", "20250323SSHH0,20250323LGHH0"])
            mock_load.assert_called_once_with(["20250323SSHH0", "20250323LGHH0"])
