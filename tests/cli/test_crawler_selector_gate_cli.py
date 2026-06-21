from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.crawler_selector_gate import main


class TestCrawlerSelectorGateCLI:
    def test_requires_config(self):
        with patch("src.cli.crawler_selector_gate.argparse.ArgumentParser") as mock_parser:
            instance = mock_parser.return_value
            instance.parse_args.side_effect = SystemExit(2)
            try:
                main([])
            except SystemExit as exc:
                assert exc.code == 2

    def test_success_exit_code(self):
        with (
            patch("src.cli.crawler_selector_gate.load_selector_config") as mock_load,
            patch("src.cli.crawler_selector_gate.run_selector_gate") as mock_run,
        ):
            mock_load.return_value = []
            summary = MagicMock()
            summary.ok = True
            summary.to_dict.return_value = {"ok": True}
            mock_run.return_value = summary
            result = main(["--config", "/tmp/dummy.json"])
            assert result == 0

    def test_failure_exit_code(self):
        with (
            patch("src.cli.crawler_selector_gate.load_selector_config") as mock_load,
            patch("src.cli.crawler_selector_gate.run_selector_gate") as mock_run,
        ):
            mock_load.return_value = []
            summary = MagicMock()
            summary.ok = False
            mock_run.return_value = summary
            result = main(["--config", "/tmp/dummy.json"])
            assert result == 1

    def test_json_output(self):
        with (
            patch("src.cli.crawler_selector_gate.load_selector_config") as mock_load,
            patch("src.cli.crawler_selector_gate.run_selector_gate") as mock_run,
        ):
            mock_load.return_value = []
            summary = MagicMock()
            summary.ok = True
            summary.to_dict.return_value = {"ok": True}
            mock_run.return_value = summary
            result = main(["--config", "/tmp/dummy.json", "--json"])
            assert result == 0

    def test_main_success_stdout(self):
        with (
            patch("src.cli.crawler_selector_gate.load_selector_config") as mock_load,
            patch("src.cli.crawler_selector_gate.run_selector_gate") as mock_run,
            patch("src.cli.crawler_selector_gate.render_selector_summary") as mock_render,
        ):
            mock_load.return_value = []
            mock_summary = MagicMock()
            mock_summary.ok = True
            mock_run.return_value = mock_summary
            mock_render.return_value = "rendered summary"

            result = main(["--config", "dummy_config.json"])

            assert result == 0
            mock_load.assert_called_once_with("dummy_config.json")
            mock_run.assert_called_once_with([], output_dir=None)
            mock_render.assert_called_once_with(mock_summary)

    def test_main_with_output_dir_and_json(self, capsys):
        with (
            patch("src.cli.crawler_selector_gate.load_selector_config") as mock_load,
            patch("src.cli.crawler_selector_gate.run_selector_gate") as mock_run,
            patch("src.cli.crawler_selector_gate.render_selector_summary") as mock_render,
        ):
            mock_load.return_value = []
            mock_summary = MagicMock()
            mock_summary.ok = True
            mock_summary.to_dict.return_value = {"status": "ok"}
            mock_run.return_value = mock_summary

            result = main(["--config", "dummy.json", "--output-dir", "out_dir", "--json"])

            assert result == 0
            captured = capsys.readouterr()
            assert '{"status": "ok"}' in captured.out
            mock_render.assert_not_called()

    def test_main_failure_exit_code(self):
        with (
            patch("src.cli.crawler_selector_gate.load_selector_config") as mock_load,
            patch("src.cli.crawler_selector_gate.run_selector_gate") as mock_run,
            patch("src.cli.crawler_selector_gate.render_selector_summary") as mock_render,
        ):
            mock_load.return_value = []
            mock_summary = MagicMock()
            mock_summary.ok = False
            mock_run.return_value = mock_summary
            mock_render.return_value = "failure details"

            result = main(["--config", "dummy.json"])

            assert result == 1
