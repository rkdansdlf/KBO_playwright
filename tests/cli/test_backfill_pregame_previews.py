from unittest.mock import patch

from src.cli.backfill_pregame_previews import main


class TestBackfillPregamePreviews:
    def test_dry_run(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.return_value = 0
            result = main(["--dry-run"])
            assert result == 0

    def test_specific_dates(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.return_value = 0
            result = main(["--start-date", "20250401", "--end-date", "20250402", "--dry-run"])
            assert result == 0

    def test_with_include_complete(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.return_value = 0
            result = main(["--include-complete", "--days-ahead", "3", "--dry-run"])
            assert result == 0
