from unittest.mock import MagicMock, patch


class TestFixLotteCode:
    def test_fix_lotte_code_already_lt(self):
        with patch("scripts.fix_lotte_code.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_result = MagicMock()
            mock_result.current_code = "LT"
            mock_session.execute.return_value.fetchone.return_value = mock_result
            from scripts.fix_lotte_code import fix_lotte_code
            fix_lotte_code()
            mock_session.commit.assert_not_called()

    def test_fix_lotte_code_not_found(self):
        with patch("scripts.fix_lotte_code.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.fetchone.return_value = None
            from scripts.fix_lotte_code import fix_lotte_code
            fix_lotte_code()
            mock_session.commit.assert_not_called()

    def test_fix_lotte_code_updates(self):
        with patch("scripts.fix_lotte_code.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_result_before = MagicMock()
            mock_result_before.current_code = "LOT"
            mock_result_after = MagicMock()
            mock_result_after.current_code = "LT"
            mock_session.execute.return_value.fetchone.side_effect = [
                mock_result_before,
                mock_result_after,
            ]
            from scripts.fix_lotte_code import fix_lotte_code
            fix_lotte_code()
            mock_session.commit.assert_called_once()
