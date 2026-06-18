from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.data_quality_regression_pack import main


def _make_report(*, ok: bool = True) -> MagicMock:
    report = MagicMock()
    report.ok = ok
    report.to_dict.return_value = {"ok": ok}
    return report


class TestDataQualityRegressionPack:
    def test_requires_db_url(self):
        with (
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value=None),
            patch("src.cli.data_quality_regression_pack.get_oci_url", return_value=None),
        ):
            try:
                main([])
            except SystemExit as exc:
                assert exc.code == 2

    def test_success_exit_code(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value="sqlite:///:memory:"),
            patch("src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=True)),
        ):
            result = main([])
            assert result == 0

    def test_failure_exit_code(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value="sqlite:///:memory:"),
            patch("src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=False)),
        ):
            result = main([])
            assert result == 1

    def test_json_output(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value="sqlite:///:memory:"),
            patch("src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=True)),
        ):
            result = main(["--json"])
            assert result == 0
