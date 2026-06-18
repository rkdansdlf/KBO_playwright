from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.data_quality_regression_pack import main


class TestDataQualityRegressionPackCLI:
    def test_main_success_stdout(self):
        with (
            patch("src.cli.data_quality_regression_pack.create_engine") as mock_create_engine,
            patch("src.cli.data_quality_regression_pack.run_regression_pack") as mock_run,
            patch("src.cli.data_quality_regression_pack.render_regression_report") as mock_render,
        ):
            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create_engine.return_value = mock_engine

            mock_report = MagicMock()
            mock_report.ok = True
            mock_run.return_value = mock_report
            mock_render.return_value = "rendered regression report"

            result = main(["--database-url", "sqlite:///:memory:"])

            assert result == 0
            mock_create_engine.assert_called_once_with("sqlite:///:memory:")
            mock_run.assert_called_once_with(mock_conn)
            mock_render.assert_called_once_with(mock_report)

    def test_main_missing_database_url_fails(self):
        with (
            patch("src.cli.data_quality_regression_pack.get_oci_url") as mock_get_oci,
            patch.dict("os.environ", {}, clear=True),
        ):
            mock_get_oci.return_value = None
            with pytest.raises(SystemExit):
                main([])

    def test_main_runs_with_env_vars(self):
        with (
            patch("src.cli.data_quality_regression_pack.create_engine") as mock_create_engine,
            patch("src.cli.data_quality_regression_pack.run_regression_pack") as mock_run,
            patch("src.cli.data_quality_regression_pack.render_regression_report") as mock_render,
            patch.dict("os.environ", {"DATABASE_URL": "sqlite:///from_env"}, clear=True),
        ):
            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create_engine.return_value = mock_engine

            mock_report = MagicMock()
            mock_report.ok = True
            mock_run.return_value = mock_report
            mock_render.return_value = "rendered"

            result = main([])

            assert result == 0
            mock_create_engine.assert_called_once_with("sqlite:///from_env")

    def test_main_with_json_and_failure(self, capsys):
        with (
            patch("src.cli.data_quality_regression_pack.create_engine") as mock_create_engine,
            patch("src.cli.data_quality_regression_pack.run_regression_pack") as mock_run,
            patch("src.cli.data_quality_regression_pack.report_to_json") as mock_json,
        ):
            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create_engine.return_value = mock_engine

            mock_report = MagicMock()
            mock_report.ok = False
            mock_run.return_value = mock_report
            mock_json.return_value = '{"ok": false}'

            result = main(["--database-url", "sqlite:///:memory:", "--json"])

            assert result == 1
            captured = capsys.readouterr()
            assert '{"ok": false}' in captured.out
            mock_json.assert_called_once_with(mock_report)
