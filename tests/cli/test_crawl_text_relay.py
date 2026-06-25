from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.crawl_text_relay import (
    build_arg_parser,
    main,
    run_from_args,
    run_season,
    run_single_game,
)


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


class TestBuildArgParser:
    def test_parser_creation(self):
        parser = build_arg_parser()
        assert parser is not None

    def test_parser_season_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--season", "2026"])
        assert args.season == 2026

    def test_parser_game_id_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--game-id", "20260412SKLG0"])
        assert args.game_id == "20260412SKLG0"


class TestRunFromArgs:
    async def test_run_single_game(self):
        args = MagicMock()
        args.game_id = "G1"
        args.season = None
        args.month = None
        args.save = False
        args.output_dir = "data"
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock:
            mock.return_value = 5
            result = await run_from_args(args)
            assert result["rows"] == 5

    async def test_run_season(self):
        args = MagicMock()
        args.game_id = None
        args.season = 2026
        args.month = None
        args.save = False
        args.output_dir = "data"
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock:
            mock.return_value = {"total": 10}
            result = await run_from_args(args)
            assert result["total"] == 10
