from unittest.mock import MagicMock, patch


def _close_coro(coro):
    coro.close()


class TestBackfillPlayerProfiles:
    def test_main(self):
        with (
            patch("scripts.backfill_player_profiles.asyncio.run") as mock_run,
            patch("sys.argv", ["script", "--limit", "3", "--delay", "0.5"]),
        ):
            mock_run.side_effect = _close_coro
            from scripts.backfill_player_profiles import main

            main()
            args, _ = mock_run.call_args
            assert args[0].cr_code.co_argcount == 3

    def test_main_with_ids(self):
        with (
            patch("scripts.backfill_player_profiles.asyncio.run") as mock_run,
            patch("sys.argv", ["script", "--ids", "10001,10002"]),
        ):
            mock_run.side_effect = _close_coro
            from scripts.backfill_player_profiles import main

            main()
            mock_run.assert_called_once()

    def test_backfill_no_targets(self):
        with patch("scripts.backfill_player_profiles.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.limit.return_value = mock_query
            mock_query.all.return_value = []

            import asyncio

            from scripts.backfill_player_profiles import backfill

            async def run_backfill():
                return await backfill(limit=5, delay=1.0, ids=None)

            asyncio.run(run_backfill())
