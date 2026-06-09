from unittest.mock import patch

from src.cli.regenerate_review_summaries import main


class TestRegenerateReviewSummaries:
    def test_no_args_errors(self):
        try:
            main([])
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_dry_run_with_game_id(self):
        with patch("src.cli.regenerate_review_summaries.regenerate_review_summaries") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0"])
            assert result == 0

    def test_with_apply(self):
        with patch("src.cli.regenerate_review_summaries.regenerate_review_summaries") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0", "--apply"])
            assert result == 0

    def test_with_season(self):
        with patch("src.cli.regenerate_review_summaries.regenerate_review_summaries") as mock:
            mock.return_value = []
            result = main(["--season", "2025"])
            assert result == 0
