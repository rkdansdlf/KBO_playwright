from unittest.mock import MagicMock, patch


class TestSeedStadiumFood:
    def test_run(self):
        with patch("scripts.seed_stadium_food.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            from scripts.seed_stadium_food import run

            run(dry_run=False)
            mock_session.commit.assert_called_once()

    def test_run_dry_run(self):
        with patch("scripts.seed_stadium_food.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            from scripts.seed_stadium_food import run

            run(dry_run=True)
            mock_session.commit.assert_not_called()
