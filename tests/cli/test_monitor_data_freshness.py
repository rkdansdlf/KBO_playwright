from unittest.mock import patch

from src.cli.monitor_data_freshness import main


class TestMonitorDataFreshness:
    def test_default_run(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": [], "table_issues": [], "p0_issues": []}
            result = main([])
            assert result is None

    def test_no_alert(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": [], "table_issues": [], "p0_issues": []}
            result = main(["--no-alert"])
            assert result is None

    def test_dry_run(self):
        with patch("src.cli.monitor_data_freshness.run_monitor") as mock:
            mock.return_value = {"stale": [], "table_issues": [], "p0_issues": []}
            result = main(["--dry-run"])
            assert result is None
