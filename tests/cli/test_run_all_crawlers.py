from argparse import Namespace
from unittest.mock import patch

from src.cli.run_all_crawlers import main


class TestRunAllCrawlers:
    def test_no_args_prints_help(self):
        try:
            main()
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_static_pipeline(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock,
        ):
            mock_parse.return_value = Namespace(type="static", pdf=None, daemon=False)
            mock.return_value = None
            result = main()
            assert result is None

    def test_dynamic_pipeline(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock,
        ):
            mock_parse.return_value = Namespace(type="dynamic", pdf=None, daemon=False)
            mock.return_value = None
            result = main()
            assert result is None

    def test_realtime_pipeline(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock,
        ):
            mock_parse.return_value = Namespace(type="realtime", pdf=None, daemon=False)
            mock.return_value = None
            result = main()
            assert result is None
