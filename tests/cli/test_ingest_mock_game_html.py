from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.cli.ingest_mock_game_html import main


class TestIngestMockGameHtmlCLI:
    def test_main_default_fixtures_dir(self):
        with (
            patch("src.cli.ingest_mock_game_html.Path") as MockPath,
            patch("src.cli.ingest_mock_game_html.parse_game_detail_html"),
            patch("src.cli.ingest_mock_game_html.save_game_detail"),
        ):
            mock_path = MockPath.return_value
            mock_path.exists.return_value = True
            mock_path.glob.return_value = []

            main([])

            MockPath.assert_called_once_with("tests/fixtures/game_details")

    def test_main_limit(self):
        with TemporaryDirectory() as tmpdir:
            for i in range(5):
                (Path(tmpdir) / f"202510{i:02d}NCLG0.html").write_text("<html></html>", encoding="utf-8")

            with (
                patch.object(Path, "exists", return_value=True),
                patch("src.cli.ingest_mock_game_html.parse_game_detail_html") as mock_parse,
                patch("src.cli.ingest_mock_game_html.save_game_detail") as mock_save,
            ):
                mock_parse.return_value = {"game_id": "test"}
                mock_save.return_value = True

                main(["--fixtures-dir", tmpdir, "--limit", "3"])

                assert mock_parse.call_count == 3
                assert mock_save.call_count == 3

    def test_main_missing_dir(self):
        with patch("src.cli.ingest_mock_game_html.Path") as MockPath:
            mock_path = MockPath.return_value
            mock_path.exists.return_value = False

            try:
                main([])
            except SystemExit:
                pass

    def test_main_empty_dir(self):
        with TemporaryDirectory() as tmpdir:
            with (
                patch.object(Path, "exists", return_value=True),
                patch("src.cli.ingest_mock_game_html.parse_game_detail_html"),
                patch("src.cli.ingest_mock_game_html.save_game_detail"),
            ):
                main(["--fixtures-dir", tmpdir])
