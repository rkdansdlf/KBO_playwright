"""Unit tests for calculate_sabermetrics CLI."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.cli.calculate_sabermetrics import main


class TestCalculateSabermetricsCLI:
    def test_main_dry_run(self) -> None:
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock_batch:
            result = main(["--years", "2025"])
            assert result == 0
            mock_batch.assert_called_once()

    def test_main_range(self) -> None:
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock_batch:
            result = main(["--years", "2024-2026"])
            assert result == 0
            args, _ = mock_batch.call_args
            assert args[0] == [2024, 2025, 2026]

    def test_main_single_year(self) -> None:
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock_batch:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock_batch.call_args
            assert args[0] == [2025]

    def test_main_with_sync(self) -> None:
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock_batch:
            result = main(["--years", "2025", "--sync"])
            assert result == 0
            _, kwargs = mock_batch.call_args
            assert kwargs.get("sync_oci") is True
