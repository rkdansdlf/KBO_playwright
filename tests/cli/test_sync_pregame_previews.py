import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

import src.cli.sync_pregame_previews as sync_module
from src.cli.sync_pregame_previews import PregameSyncTarget, main


class TestSyncPregamePreviews:
    def test_dry_run(self):
        with patch("src.cli.sync_pregame_previews.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.all.return_value = []
            result = main(["--dry-run", "--target-url", "postgresql://localhost/test"])
            assert result == 0

    def test_no_target_url(self):
        with patch("src.cli.sync_pregame_previews.get_oci_url", return_value=None):
            try:
                main(["--dry-run"])
                raise AssertionError("Should have raised SystemExit")
            except SystemExit:
                pass

    def test_with_dates(self):
        with patch("src.cli.sync_pregame_previews.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.all.return_value = []
            result = main(
                ["--start-date", "20250401", "--end-date", "20250402", "--target-url", "postgresql://localhost/test"],
            )
            assert result == 0

    def test_invalid_date_format_is_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="Invalid date"):
            sync_module._yyyymmdd("2025/04/01")

    def test_find_targets_maps_rows_and_query_parameters(self):
        rows = [
            SimpleNamespace(
                game_date="20250401",
                game_id="20250401LGSS0",
                away_pitcher=None,
                home_pitcher="Home Starter",
                has_preview=1,
            ),
            SimpleNamespace(
                game_date="20250402",
                game_id="20250402KTSS0",
                away_pitcher="Away Starter",
                home_pitcher="",
                has_preview=0,
            ),
        ]
        session_factory = MagicMock()
        session = session_factory.return_value.__enter__.return_value
        session.execute.return_value.all.return_value = rows

        with patch.object(sync_module, "SessionLocal", session_factory):
            result = sync_module.find_pregame_sync_targets("20250401", "20250402")

        assert result == [
            PregameSyncTarget("20250401", "20250401LGSS0", "", "Home Starter", True),
            PregameSyncTarget("20250402", "20250402KTSS0", "Away Starter", "", False),
        ]
        session.execute.assert_called_once()
        assert session.execute.call_args.args[1] == {"start_date": "20250401", "end_date": "20250402"}

    def test_defaults_are_passed_to_target_lookup(self):
        lookup = MagicMock(return_value=[])
        with (
            patch.object(sync_module, "find_pregame_sync_targets", lookup),
            patch.object(sync_module, "_default_start_date", return_value="20250410"),
            patch.object(sync_module, "_default_end_date", return_value="20250412") as default_end,
        ):
            result = main(["--target-url", "postgresql://localhost/test", "--days-ahead", "2"])

        assert result == 0
        lookup.assert_called_once_with("20250410", "20250412")
        default_end.assert_called_once_with(2)

    def test_syncs_each_target_and_closes_syncer(self):
        targets = [
            PregameSyncTarget("20250401", "20250401LGSS0", "Away", "Home", True),
            PregameSyncTarget("20250402", "20250402KTSS0", "", "Home", False),
        ]
        session_factory = MagicMock()
        session = session_factory.return_value.__enter__.return_value
        syncer = MagicMock()
        syncer.sync_specific_game.side_effect = ["synced", "already current"]

        with (
            patch.object(sync_module, "find_pregame_sync_targets", return_value=targets),
            patch.object(sync_module, "SessionLocal", session_factory),
            patch.object(sync_module, "OCISync", return_value=syncer) as syncer_class,
        ):
            result = main(
                [
                    "--start-date",
                    "20250401",
                    "--end-date",
                    "20250402",
                    "--target-url",
                    "postgresql://localhost/test",
                ],
            )

        assert result == 0
        syncer_class.assert_called_once_with("postgresql://localhost/test", session)
        assert syncer.sync_specific_game.call_args_list == [
            call("20250401LGSS0"),
            call("20250402KTSS0"),
        ]
        syncer.close.assert_called_once_with()

    def test_dry_run_does_not_construct_syncer_for_targets(self):
        target = PregameSyncTarget("20250401", "20250401LGSS0", "Away", "Home", True)

        with (
            patch.object(sync_module, "find_pregame_sync_targets", return_value=[target]),
            patch.object(sync_module, "OCISync") as syncer_class,
        ):
            result = main(["--dry-run", "--target-url", "postgresql://localhost/test"])

        assert result == 0
        syncer_class.assert_not_called()

    def test_syncer_is_closed_when_a_game_sync_fails(self):
        target = PregameSyncTarget("20250401", "20250401LGSS0", "Away", "Home", True)
        syncer = MagicMock()
        syncer.sync_specific_game.side_effect = RuntimeError("OCI unavailable")

        with (
            patch.object(sync_module, "find_pregame_sync_targets", return_value=[target]),
            patch.object(sync_module, "SessionLocal"),
            patch.object(sync_module, "OCISync", return_value=syncer),
        ):
            with pytest.raises(RuntimeError, match="OCI unavailable"):
                main(["--target-url", "postgresql://localhost/test"])

        syncer.close.assert_called_once_with()
