from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError


class TestSeedStadiumInfo:
    def test_seed_stadium_info(self):
        with patch("scripts.seed_stadium_info.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value = mock_session
            from scripts.seed_stadium_info import seed_stadium_info

            seed_stadium_info()
            mock_session.commit.assert_called_once()

    def test_seed_stadium_info_rollback_on_error(self):
        with patch("scripts.seed_stadium_info.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value = mock_session
            mock_session.commit.side_effect = SQLAlchemyError("db error")
            from scripts.seed_stadium_info import seed_stadium_info

            try:
                seed_stadium_info()
            except SQLAlchemyError:
                pass
            mock_session.rollback.assert_called_once()
