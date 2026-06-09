from unittest.mock import patch, MagicMock

from src.cli.regenerate_game_stories import main


class TestRegenerateGameStories:
    def test_no_args_errors(self):
        try:
            main([])
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass

    def test_dry_run_with_game_id(self):
        with patch("src.cli.regenerate_game_stories.regenerate_game_stories") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0"])
            assert result == 0

    def test_with_apply(self):
        with patch("src.cli.regenerate_game_stories.regenerate_game_stories") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0", "--apply"])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.regenerate_game_stories.regenerate_game_stories") as mock:
            mock.return_value = []
            result = main(["--date", "20250401"])
            assert result == 0
