from __future__ import annotations

from unittest.mock import patch

from src.cli.seed_p1_data import main


class TestSeedP1DataCLI:
    def test_main_default_all(self):
        with (
            patch("src.cli.seed_p1_data.run_all") as mock_all,
            patch("src.cli.seed_p1_data.run_seat") as mock_seat,
            patch("src.cli.seed_p1_data.run_parking") as mock_parking,
            patch("src.cli.seed_p1_data.run_food") as mock_food,
        ):
            main([])
            mock_all.assert_called_once_with(dry_run=False)
            mock_seat.assert_not_called()
            mock_parking.assert_not_called()
            mock_food.assert_not_called()

    def test_main_seat(self):
        with (
            patch("src.cli.seed_p1_data.run_all") as mock_all,
            patch("src.cli.seed_p1_data.run_seat") as mock_seat,
        ):
            main(["--type", "seat"])
            mock_seat.assert_called_once_with(dry_run=False)
            mock_all.assert_not_called()

    def test_main_parking(self):
        with patch("src.cli.seed_p1_data.run_parking") as mock_parking:
            main(["--type", "parking"])
            mock_parking.assert_called_once_with(dry_run=False)

    def test_main_food(self):
        with patch("src.cli.seed_p1_data.run_food") as mock_food:
            main(["--type", "food"])
            mock_food.assert_called_once_with(dry_run=False)

    def test_main_dry_run(self):
        with patch("src.cli.seed_p1_data.run_all") as mock_all:
            main(["--dry-run"])
            mock_all.assert_called_once_with(dry_run=True)
