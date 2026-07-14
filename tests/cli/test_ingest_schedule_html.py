from unittest.mock import MagicMock, patch

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

    def test_missing_fixtures_dir_raises(self):
        with patch("src.cli.ingest_schedule_html.Path") as mock_path:
            mock_path.return_value.exists.return_value = False
            try:
                main([])
                raise AssertionError("expected SystemExit")
            except SystemExit as exc:
                assert "not found" in str(exc)

    def test_ingest_saves_parsed_games(self):
        html_file = MagicMock()
        html_file.name = "2024_03.html"
        html_file.read_text.return_value = "<html>schedule</html>"
        with (
            patch("src.cli.ingest_schedule_html.Path") as mock_path,
            patch("src.cli.ingest_schedule_html.parse_schedule_html") as mock_parse,
            patch("src.cli.ingest_schedule_html.save_schedule_games") as mock_save,
        ):
            mock_path.return_value.exists.return_value = True
            mock_path.return_value.glob.return_value = [html_file]
            mock_parse.return_value = [{"game_id": "20240301_01"}]
            mock_save.return_value.saved = 1
            mock_save.return_value.failed = 0
            result = main(["--default-year", "2024", "--season-type", "regular"])
            assert result is None
            mock_parse.assert_called_once()
            mock_save.assert_called_once()

    def test_no_games_parsed_returns_early(self):
        html_file = MagicMock()
        html_file.name = "2024_03.html"
        html_file.read_text.return_value = "<html>empty</html>"
        with (
            patch("src.cli.ingest_schedule_html.Path") as mock_path,
            patch("src.cli.ingest_schedule_html.parse_schedule_html") as mock_parse,
            patch("src.cli.ingest_schedule_html.save_schedule_games") as mock_save,
        ):
            mock_path.return_value.exists.return_value = True
            mock_path.return_value.glob.return_value = [html_file]
            mock_parse.return_value = []
            result = main([])
            assert result is None
            mock_save.assert_not_called()
