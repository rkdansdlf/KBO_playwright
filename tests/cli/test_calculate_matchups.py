from __future__ import annotations

from unittest.mock import patch

from src.cli.calculate_matchups import main


class TestCalculateMatchupsCLI:
    def test_main_default_years(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            main([])
            mock_batch.assert_called_once_with(list(range(2020, 2027)), sync_oci=False)

    def test_main_custom_year(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            main(["--years", "2025"])
            mock_batch.assert_called_once_with([2025], sync_oci=False)

    def test_main_custom_range(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            main(["--years", "2023-2025"])
            mock_batch.assert_called_once_with([2023, 2024, 2025], sync_oci=False)

    def test_main_with_sync(self):
        with patch("src.cli.calculate_matchups.batch_calculate_matchups") as mock_batch:
            main(["--sync"])
            mock_batch.assert_called_once_with(list(range(2020, 2027)), sync_oci=True)
