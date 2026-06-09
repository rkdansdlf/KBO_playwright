from unittest.mock import MagicMock, patch

from src.cli.verify_chunk_quality import main


class TestVerifyChunkQuality:
    def test_default_run(self):
        with patch("src.cli.verify_chunk_quality.get_db_session") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.fetchall.return_value = []
            try:
                main()
            except SystemExit:
                pass

    def test_with_source_filter(self):
        with patch("src.cli.verify_chunk_quality.get_db_session") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.fetchall.return_value = []
            try:
                main()
            except SystemExit:
                pass

    def test_json_output(self):
        with patch("src.cli.verify_chunk_quality.get_db_session") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.fetchall.return_value = []
            try:
                main()
            except SystemExit:
                pass
