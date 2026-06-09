from unittest.mock import patch

from src.cli.rebuild_relay_events import run


class TestRebuildRelayEvents:
    def test_dry_run(self):
        with patch("src.cli.rebuild_relay_events.rebuild_relay_events") as mock:
            result = run([])
            assert result == 0
            mock.assert_called_once()

    def test_with_season(self):
        with patch("src.cli.rebuild_relay_events.rebuild_relay_events") as mock:
            result = run(["--season", "2025"])
            assert result == 0
            _, kwargs = mock.call_args
            assert 2025 in kwargs.get("seasons", [])

    def test_with_apply(self):
        with patch("src.cli.rebuild_relay_events.rebuild_relay_events") as mock:
            result = run(["--apply"])
            assert result == 0
            _, kwargs = mock.call_args
            assert kwargs.get("apply") is True
