from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.fix_player_names import main


class TestFixPlayerNamesCLI:
    def test_main_no_crawl_flag(self):
        with patch("sys.argv", ["fix_player_names"]):
            result = main()
            assert result is None

    def test_main_crawl_no_players(self):
        with patch("sys.argv", ["fix_player_names", "--crawl"]), \
             patch("src.cli.fix_player_names.crawl_all_players", new_callable=AsyncMock) as mock_crawl, \
             patch("src.cli.fix_player_names.player_row_to_dict"), \
             patch("src.cli.fix_player_names.filter_valid_player_payloads"), \
             patch("src.cli.fix_player_names.init_db"), \
             patch("src.cli.fix_player_names.PlayerBasicRepository"):
            mock_crawl.return_value = []
            main()
            mock_crawl.assert_called_once_with(max_pages=None)

    def test_main_crawl_save(self):
        with patch("sys.argv", ["fix_player_names", "--crawl", "--save"]), \
             patch("src.cli.fix_player_names.crawl_all_players", new_callable=AsyncMock) as mock_crawl, \
             patch("src.cli.fix_player_names.player_row_to_dict") as mock_to_dict, \
             patch("src.cli.fix_player_names.filter_valid_player_payloads") as mock_filter, \
             patch("src.cli.fix_player_names.init_db"), \
             patch("src.cli.fix_player_names.PlayerBasicRepository") as MockRepo:
            mock_crawl.return_value = [{"player_id": "123"}]
            mock_to_dict.return_value = {"player_id": "123", "name": "Test"}
            mock_filter.return_value = ([{"player_id": "123", "name": "Test"}], {})
            mock_repo = MagicMock()
            mock_repo.upsert_players.return_value = 1
            MockRepo.return_value = mock_repo

            main()

            mock_repo.upsert_players.assert_called_once()
