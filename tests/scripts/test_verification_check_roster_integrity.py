from unittest.mock import MagicMock, patch

from scripts.verification.check_roster_integrity import check_integrity


class TestCheckIntegrity:
    @patch("scripts.verification.check_roster_integrity.SessionLocal")
    def test_no_rosters(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = []

        check_integrity(2025)
        # No error

    @patch("scripts.verification.check_roster_integrity.SessionLocal")
    def test_all_teams_present(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [
            ("LG",),
            ("HH",),
            ("SS",),
            ("KT",),
            ("OB",),
            ("LT",),
            ("HT",),
            ("NC",),
            ("SK",),
            ("WO",),
        ]

        check_integrity(2025)
        # No error
