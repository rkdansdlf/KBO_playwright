from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.reset_sqlite import reset_specific_range, reset_specific_year, reset_sqlite_data


class TestResetSqliteData:
    @patch("scripts.legacy.maintenance.reset_sqlite.SessionLocal")
    def test_already_empty(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.count.return_value = 0

        reset_sqlite_data(confirm=False)
        # No error

    @patch("builtins.input")
    @patch("scripts.legacy.maintenance.reset_sqlite.SessionLocal")
    def test_with_data_and_confirm_skips(self, mock_session_local, mock_input):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.count.side_effect = [10, 10]
        mock_input.return_value = "n"

        reset_sqlite_data(confirm=True)
        mock_input.assert_called_once()


class TestResetSpecificYear:
    @patch("scripts.legacy.maintenance.reset_sqlite.SessionLocal")
    def test_empty(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.count.return_value = 0

        reset_specific_year(2025, confirm=False)
        assert True


class TestResetSpecificRange:
    @patch("scripts.legacy.maintenance.reset_sqlite.SessionLocal")
    def test_empty(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter.return_value.count.return_value = 0

        reset_specific_range(2020, 2025, confirm=False)
        assert True
