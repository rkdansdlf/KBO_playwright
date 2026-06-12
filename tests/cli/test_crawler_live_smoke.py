from unittest.mock import patch

from src.cli.crawler_live_smoke import main


def _run_and_close(coro, result):
    coro.close()
    return result


class TestCrawlerLiveSmoke:
    def test_network_not_allowed(self):
        with patch("src.cli.crawler_live_smoke._network_allowed", return_value=False):
            result = main(["--date", "20250101"])
            assert result == 2

    def test_network_allowed_schedule_scope(self):
        with patch("src.cli.crawler_live_smoke._network_allowed", return_value=True):
            with patch("src.cli.crawler_live_smoke.asyncio.run") as mock_run:
                mock_run.side_effect = lambda coro: _run_and_close(coro, {"ok": True})
                result = main(["--date", "20250101", "--allow-network", "--scope", "schedule"])
                assert result == 0

    def test_network_allowed_fail(self):
        with patch("src.cli.crawler_live_smoke._network_allowed", return_value=True):
            with patch("src.cli.crawler_live_smoke.asyncio.run") as mock_run:
                mock_run.side_effect = lambda coro: _run_and_close(coro, {"ok": False})
                result = main(["--date", "20250101", "--allow-network"])
                assert result == 1
