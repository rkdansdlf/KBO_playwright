from unittest.mock import AsyncMock, patch

from src.cli.auto_healer import main


class TestAutoHealer:
    def test_dry_run(self):
        with patch("src.cli.auto_healer.run_healer_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 0
            result = main(["--dry-run"])
            assert result == 0

    def test_pbp_dry_run(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--dry-run"])
            assert result == 0

    def test_pbp_with_game_id(self):
        with patch("src.cli.auto_healer.run_pbp_healer") as mock_pbp:
            mock_pbp.return_value = 0
            result = main(["--pbp", "--game-id", "20250401LGSS0"])
            assert result == 0
