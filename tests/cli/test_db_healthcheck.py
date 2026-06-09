from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from src.cli.db_healthcheck import main


class TestDbHealthcheckCLI:
    def test_main_connectivity_and_tables(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar_one.return_value = 42

        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine.url.get_backend_name.return_value = "sqlite"

        with (
            patch("src.cli.db_healthcheck.Engine", mock_engine),
            patch("src.cli.db_healthcheck.inspect") as mock_inspect,
            patch("src.cli.db_healthcheck.text"),
        ):
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["players", "teams"]
            mock_inspect.return_value = mock_inspector

            main([])

    def test_main_connectivity_failure(self):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = SQLAlchemyError("connection failed")

        with patch("src.cli.db_healthcheck.Engine", mock_engine):
            main([])

    def test_main_respects_argv(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar_one.return_value = 42

        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine.url.get_backend_name.return_value = "sqlite"

        with (
            patch("src.cli.db_healthcheck.Engine", mock_engine),
            patch("src.cli.db_healthcheck.inspect") as mock_inspect,
            patch("src.cli.db_healthcheck.text"),
        ):
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["players"]
            mock_inspect.return_value = mock_inspector

            main(["--unexpected"])
