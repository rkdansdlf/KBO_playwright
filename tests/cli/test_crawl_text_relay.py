from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_text_relay import main


class TestCrawlTextRelayCLI:
    def test_main_single_game_dry_run(self):
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 10
            result = main(["--game-id", "20260412SKLG0"])

            assert result == {"game_id": "20260412SKLG0", "rows": 10}
            mock_run.assert_called_once_with(
                game_id="20260412SKLG0",
                save=False,
                output_dir="data",
            )

    def test_main_single_game_save(self):
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 5
            result = main(["--game-id", "20260412SKLG0", "--save"])

            assert result == {"game_id": "20260412SKLG0", "rows": 5}
            mock_run.assert_called_once_with(
                game_id="20260412SKLG0",
                save=True,
                output_dir="data",
            )

    def test_main_single_game_custom_output_dir(self):
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 3
            result = main(["--game-id", "20260412SKLG0", "--output-dir", "/tmp/relay"])

            assert result["rows"] == 3
            mock_run.assert_called_once_with(
                game_id="20260412SKLG0",
                save=False,
                output_dir="/tmp/relay",
            )

    def test_main_season_dry_run(self):
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {"total": 5, "success": 4, "failed": 1}
            result = main(["--season", "2026"])

            assert result == {"total": 5, "success": 4, "failed": 1}
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args
            assert call_kwargs.kwargs["season"] == 2026
            assert call_kwargs.kwargs["month"] is None
            assert call_kwargs.kwargs["save"] is False

    def test_main_season_with_month(self):
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {"total": 2, "success": 2, "failed": 0}
            result = main(["--season", "2026", "--month", "4", "--save"])

            assert result["total"] == 2
            call_kwargs = mock_run.call_args
            assert call_kwargs.kwargs["season"] == 2026
            assert call_kwargs.kwargs["month"] == 4
            assert call_kwargs.kwargs["save"] is True
