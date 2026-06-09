from unittest.mock import patch, MagicMock

from src.cli.backfill_starting_pitchers_from_stats import main


class TestBackfillStartingPitchersFromStats:
    def test_dry_run(self):
        with patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as mock_session:
            mock_session.return_value.__enter__.return_value = MagicMock()
            with patch("src.cli.backfill_starting_pitchers_from_stats.parse_args") as mock_args:
                mock_args.return_value = MagicMock(
                    start_date=None, end_date=None, dry_run=True,
                    overwrite=False, sync=False, sync_target_missing=False, limit=None,
                )
                result = main()
                assert result == 0

    def test_with_dates(self):
        with patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as mock_session:
            mock_session.return_value.__enter__.return_value = MagicMock()
            with patch("src.cli.backfill_starting_pitchers_from_stats.parse_args") as mock_args:
                args = MagicMock(start_date="20250101", end_date="20250131", dry_run=True, overwrite=False, sync=False, sync_target_missing=False, limit=None)
                mock_args.return_value = args
                result = main()
                assert result == 0
