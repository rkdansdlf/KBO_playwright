from unittest.mock import MagicMock, patch

from scripts.backfill_birthdates import _parse_birth_date, backfill, main


class TestBackfillBirthdates:
    def test_parse_birth_date_iso(self):
        assert _parse_birth_date("1990-07-03") is not None

    def test_parse_birth_date_none(self):
        assert _parse_birth_date(None) is None

    def test_parse_birth_date_empty(self):
        assert _parse_birth_date("") is None

    def test_backfill_no_players(self):
        with patch("scripts.backfill_birthdates.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.scalars.return_value.all.return_value = []
            result = backfill()
            assert result == 0

    def test_backfill_dry_run(self):
        with patch("scripts.backfill_birthdates.SessionLocal") as mock_sf, \
             patch("scripts.backfill_birthdates._parse_birth_date", return_value="1990-07-03"):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_player = MagicMock()
            mock_player.player_id = 10001
            mock_player.name = "Test"
            mock_player.birth_date = "1990-07-03"
            mock_session.scalars.return_value.all.return_value = [mock_player]
            result = backfill(limit=1, dry_run=True)
            assert result == 1

    def test_main(self):
        with patch("scripts.backfill_birthdates.backfill") as mock_fn:
            with patch("sys.argv", ["script", "--limit", "5", "--dry-run"]):
                main()
            mock_fn.assert_called_once_with(limit=5, dry_run=True, verbose=False)
