from __future__ import annotations

from unittest.mock import AsyncMock, patch

from src.cli.fetch_kbo_pbp import main


class TestFetchKboPbpCLI:
    def test_main_no_args(self):
        result = main([])
        assert result == 1

    def test_main_with_season(self):
        with (
            patch("src.cli.fetch_kbo_pbp.load_relay_recovery_targets") as mock_load,
            patch("src.cli.fetch_kbo_pbp.recover_relay_data", new_callable=AsyncMock),
        ):
            mock_load.return_value = []
            result = main(["--season", "2025"])
            assert result == 0
            args, kwargs = mock_load.call_args
            assert kwargs["season"] == 2025
            assert kwargs["month"] is None
            assert kwargs["game_ids"] is None
            assert kwargs["missing_only"] is True

    def test_main_with_game_id(self):
        with (
            patch("src.cli.fetch_kbo_pbp.load_relay_recovery_targets") as mock_load,
            patch("src.cli.fetch_kbo_pbp.recover_relay_data", new_callable=AsyncMock) as mock_recover,
        ):
            mock_load.return_value = [{"game_id": "20250323SSHH0"}]
            result = main(["--game-id", "20250323SSHH0"])
            assert result == 0
            mock_load.assert_called_once()
            mock_recover.assert_called_once()
