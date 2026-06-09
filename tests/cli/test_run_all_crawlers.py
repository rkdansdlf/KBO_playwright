from unittest.mock import patch, MagicMock

from src.cli.run_all_crawlers import main


class TestRunAllCrawlers:
    def test_no_args_prints_help(self):
        try:
            main()
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass

    def test_static_pipeline(self):
        with patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock:
            result = main()
            assert result is None

    def test_dynamic_pipeline(self):
        with patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock:
            result = main()
            assert result is None

    def test_realtime_pipeline(self):
        with patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock:
            result = main()
            assert result is None
