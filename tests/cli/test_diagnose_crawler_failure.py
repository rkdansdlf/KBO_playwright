from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.diagnose_crawler_failure import main


def _make_report(*, exit_code: int = 0) -> MagicMock:
    report = MagicMock()
    report.exit_code = exit_code
    report.to_dict.return_value = {"ok": exit_code == 0}
    return report


class TestDiagnoseCrawlerFailure:
    def test_success_exit_code(self):
        with (
            patch("src.cli.diagnose_crawler_failure._read_sources", return_value={"stdin": ""}),
            patch("src.cli.diagnose_crawler_failure.diagnose_sources", return_value=_make_report(exit_code=0)),
        ):
            result = main([])
            assert result == 0

    def test_failure_exit_code(self):
        with (
            patch("src.cli.diagnose_crawler_failure._read_sources", return_value={"stdin": ""}),
            patch("src.cli.diagnose_crawler_failure.diagnose_sources", return_value=_make_report(exit_code=1)),
        ):
            result = main(["--json"])
            assert result == 1

    def test_json_output(self):
        with (
            patch("src.cli.diagnose_crawler_failure._read_sources", return_value={"stdin": ""}),
            patch("src.cli.diagnose_crawler_failure.diagnose_sources", return_value=_make_report(exit_code=0)),
        ):
            result = main(["--json"])
            assert result == 0

    def test_reads_log_files(self):
        with (
            patch("src.cli.diagnose_crawler_failure._read_sources") as mock_read,
            patch("src.cli.diagnose_crawler_failure.diagnose_sources", return_value=_make_report(exit_code=0)),
        ):
            mock_read.return_value = {"test.log": "error info"}
            result = main(["test.log"])
            assert result == 0
            mock_read.assert_called_once_with(["test.log"])
