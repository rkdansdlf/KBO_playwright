from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.run_pipeline_demo import main


class TestRunPipelineDemoCLI:
    def test_main_no_args(self):
        with patch("src.cli.run_pipeline_demo.SessionLocal") as mock_sesh:
            mock_session = MagicMock()
            mock_session.query.return_value.group_by.return_value.order_by.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session
            main([])

    def test_main_schedule_fixtures(self):
        with (
            patch("src.cli.run_pipeline_demo.Path") as MockPath,
            patch("src.cli.run_pipeline_demo.parse_schedule_html") as mock_parse,
            patch("src.cli.run_pipeline_demo.save_schedule_games") as mock_save,
        ):
            mock_path = MagicMock()
            mock_path.exists.return_value = True
            mock_path.glob.return_value = []
            MockPath.return_value = mock_path
            mock_parse.return_value = []
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--schedule-fixtures", "/tmp/fixtures"])

    def test_main_game_fixtures(self):
        with (
            patch("src.cli.run_pipeline_demo.Path") as MockPath,
            patch("src.cli.run_pipeline_demo.parse_game_detail_html") as mock_parse,
            patch("src.cli.run_pipeline_demo.save_game_detail") as mock_save,
        ):
            mock_path = MagicMock()
            mock_path.exists.return_value = True
            mock_file = MagicMock()
            mock_file.stem = "20251015LGHH0"
            mock_file.read_text.return_value = "<html>"
            mock_path.glob.return_value = [mock_file]
            MockPath.return_value = mock_path
            mock_parse.return_value = {}
            mock_save.return_value = True

            main(["--game-fixtures", "/tmp/fixtures"])
