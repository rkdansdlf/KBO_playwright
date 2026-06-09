from unittest.mock import patch

from src.cli.ingest_schedule_html import main


class TestIngestScheduleHtml:
    def test_default_args(self):
        with patch("src.cli.ingest_schedule_html.Path") as mock_path:
            mock_path.return_value.exists.return_value = True
            mock_path.return_value.glob.return_value = []
            result = main([])
            assert result is None

    def test_custom_fixtures_dir(self):
        with patch("src.cli.ingest_schedule_html.Path") as mock_path:
            mock_path.return_value.exists.return_value = True
            mock_path.return_value.glob.return_value = []
            result = main(["--fixtures-dir", "/tmp/fixtures"])
            assert result is None

    def test_with_season_type(self):
        with patch("src.cli.ingest_schedule_html.Path") as mock_path:
            mock_path.return_value.exists.return_value = True
            mock_path.return_value.glob.return_value = []
            result = main(["--season-type", "postseason"])
            assert result is None
