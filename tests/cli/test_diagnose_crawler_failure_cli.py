from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.diagnose_crawler_failure import main


def _make_report(*, exit_code: int = 0) -> MagicMock:
    report = MagicMock()
    report.exit_code = exit_code
    report.to_dict.return_value = {"ok": exit_code == 0}
    return report


class TestDiagnoseCrawlerFailureCLI:
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

    def test_main_with_files(self, tmp_path):
        log_file1 = tmp_path / "test1.log"
        log_file1.write_text("failure log line 1\n", encoding="utf-8")
        log_file2 = tmp_path / "test2.log"
        log_file2.write_text("failure log line 2\n", encoding="utf-8")

        with (
            patch("src.cli.diagnose_crawler_failure.diagnose_sources") as mock_diagnose,
            patch("src.cli.diagnose_crawler_failure.render_diagnosis_text") as mock_render,
        ):
            mock_report = MagicMock()
            mock_report.exit_code = 0
            mock_diagnose.return_value = mock_report
            mock_render.return_value = "diagnosis text report"

            result = main([str(log_file1), str(log_file2)])

            assert result == 0
            mock_diagnose.assert_called_once_with(
                {
                    str(log_file1): "failure log line 1\n",
                    str(log_file2): "failure log line 2\n",
                }
            )
            mock_render.assert_called_once_with(mock_report)

    def test_main_with_stdin(self, capsys):
        with (
            patch("src.cli.diagnose_crawler_failure.sys.stdin") as mock_stdin,
            patch("src.cli.diagnose_crawler_failure.diagnose_sources") as mock_diagnose,
            patch("src.cli.diagnose_crawler_failure.render_diagnosis_text") as mock_render,
        ):
            mock_stdin.read.return_value = "stdin log content\n"
            mock_report = MagicMock()
            mock_report.exit_code = 2
            mock_diagnose.return_value = mock_report
            mock_render.return_value = "rendered stdin failure report"

            result = main([])

            assert result == 2
            mock_diagnose.assert_called_once_with({"stdin": "stdin log content\n"})
            mock_render.assert_called_once_with(mock_report)

    def test_main_json_format(self, tmp_path, capsys):
        log_file = tmp_path / "test.log"
        log_file.write_text("dummy", encoding="utf-8")

        with (
            patch("src.cli.diagnose_crawler_failure.diagnose_sources") as mock_diagnose,
            patch("src.cli.diagnose_crawler_failure.report_to_json") as mock_json,
        ):
            mock_report = MagicMock()
            mock_report.exit_code = 0
            mock_diagnose.return_value = mock_report
            mock_json.return_value = '{"exit_code": 0}'

            result = main([str(log_file), "--json"])

            assert result == 0
            captured = capsys.readouterr()
            assert '{"exit_code": 0}' in captured.out
            mock_json.assert_called_once_with(mock_report)
