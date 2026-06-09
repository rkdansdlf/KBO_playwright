from unittest.mock import patch, MagicMock

from src.cli.diagnose_coach_pitching import main


class TestDiagnoseCoachPitching:
    def test_no_date_or_game_id(self):
        try:
            main([])
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass

    def test_with_date(self):
        with patch("src.cli.diagnose_coach_pitching.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--date", "20250101"])
            assert result == 0

    def test_with_game_id(self):
        with patch("src.cli.diagnose_coach_pitching.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = ["20250401LGSS0"]
            result = main(["--game-id", "20250401LGSS0"])
            assert result == 0
