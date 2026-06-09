from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.run_weekly_maintenance import main


class TestRunWeeklyMaintenanceCLI:
    def test_main_default(self):
        with patch("sys.argv", ["run_weekly_maintenance"]), \
             patch("src.cli.run_weekly_maintenance.collect_profiles", new_callable=AsyncMock) as mock_profiles, \
             patch("src.cli.run_weekly_maintenance.healthcheck_main") as mock_health, \
             patch("src.crawlers.team_event_crawler.TeamEventCrawler") as MockEvents, \
             patch("src.crawlers.fan_culture_crawler.FanCultureCrawler") as MockFC, \
             patch("src.cli.run_weekly_maintenance.cleanup_oci_duplicates") as mock_cleanup:
            mock_profiles.return_value = None
            mock_events = MagicMock()
            mock_events.run = AsyncMock()
            MockEvents.return_value = mock_events
            mock_fc = MagicMock()
            mock_fc.run = AsyncMock()
            MockFC.return_value = mock_fc
            mock_cleanup.return_value = {}

            main()

            mock_profiles.assert_called_once_with(limit=200)
            mock_health.assert_called_once_with([])
            MockEvents.return_value.run.assert_called_once_with(save=True)
            MockFC.return_value.run.assert_called_once_with(save=True)

    def test_main_with_sync(self):
        with patch("sys.argv", ["run_weekly_maintenance", "--profile-limit", "50", "--sync"]), \
             patch("src.cli.run_weekly_maintenance.collect_profiles", new_callable=AsyncMock), \
             patch("src.cli.run_weekly_maintenance.healthcheck_main"), \
             patch("src.crawlers.team_event_crawler.TeamEventCrawler") as MockEvents, \
             patch("src.crawlers.fan_culture_crawler.FanCultureCrawler") as MockFC, \
             patch("src.cli.run_weekly_maintenance.SessionLocal") as mock_sesh, \
             patch("src.cli.run_weekly_maintenance.OCISync") as MockSync, \
             patch("src.cli.run_weekly_maintenance.cleanup_oci_duplicates") as mock_cleanup, \
             patch("src.cli.run_weekly_maintenance.os.getenv", return_value="postgresql://fake"):
            mock_cleanup.return_value = {}
            mock_sync = MagicMock()
            MockSync.return_value.__enter__.return_value = mock_sync
            mock_events = MagicMock()
            mock_events.run = AsyncMock()
            MockEvents.return_value = mock_events
            mock_fc = MagicMock()
            mock_fc.run = AsyncMock()
            MockFC.return_value = mock_fc
            main()
            mock_cleanup.assert_called_once()
