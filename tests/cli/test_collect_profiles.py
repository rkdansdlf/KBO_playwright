from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.collect_profiles import main


class TestCollectProfilesCLI:
    def test_main_default(self):
        with (
            patch("src.cli.collect_profiles.SessionLocal") as mock_sesh,
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool"),
            patch("src.cli.collect_profiles.PlayerProfileCrawler"),
            patch("sys.argv", ["collect_profiles"]),
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            mock_sesh.return_value = mock_session
            main()

    def test_main_with_limit(self):
        with (
            patch("src.cli.collect_profiles.SessionLocal") as mock_sesh,
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool"),
            patch("src.cli.collect_profiles.PlayerProfileCrawler"),
            patch("sys.argv", ["collect_profiles", "--limit", "50"]),
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            mock_sesh.return_value = mock_session
            main()

    def test_main_with_ids(self):
        with (
            patch("src.cli.collect_profiles.SessionLocal") as mock_sesh,
            patch("src.cli.collect_profiles.PlayerRepository"),
            patch("src.cli.collect_profiles.AsyncPlaywrightPool"),
            patch("src.cli.collect_profiles.PlayerProfileCrawler"),
            patch("sys.argv", ["collect_profiles", "--ids", "12345,67890"]),
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            mock_sesh.return_value = mock_session
            main()
