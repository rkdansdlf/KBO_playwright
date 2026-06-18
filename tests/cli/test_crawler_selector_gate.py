from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.crawler_selector_gate import main


class TestCrawlerSelectorGate:
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
