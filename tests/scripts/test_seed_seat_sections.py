from unittest.mock import MagicMock, patch


class TestSeedSeatSections:
    def test_run(self):
        with patch("scripts.seed_seat_sections.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            from scripts.seed_seat_sections import run
            run(dry_run=False)
            mock_session.commit.assert_called_once()

    def test_run_dry_run(self):
        with patch("scripts.seed_seat_sections.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            from scripts.seed_seat_sections import run
            run(dry_run=True)
            mock_session.commit.assert_not_called()
