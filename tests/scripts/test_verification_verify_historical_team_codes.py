from unittest.mock import patch


class TestTestResolution:
    @patch("scripts.verification.verify_historical_team_codes.resolve_team_code")
    def test_pass(self, mock_resolve):
        from scripts.verification.verify_historical_team_codes import test_resolution

        mock_resolve.return_value = "HT"
        result = test_resolution("해태 타이거즈", 2000, "HT", type="name")
        assert result is True

    @patch("scripts.verification.verify_historical_team_codes.resolve_team_code")
    def test_fail(self, mock_resolve):
        from scripts.verification.verify_historical_team_codes import test_resolution

        mock_resolve.return_value = "KIA"
        result = test_resolution("해태 타이거즈", 2000, "HT", type="name")
        assert result is False

    @patch("scripts.verification.verify_historical_team_codes.team_code_from_game_id_segment")
    def test_segment_pass(self, mock_segment):
        from scripts.verification.verify_historical_team_codes import test_resolution

        mock_segment.return_value = "HT"
        result = test_resolution("HT", 2000, "HT", type="segment")
        assert result is True


class TestMain:
    @patch("scripts.verification.verify_historical_team_codes.test_resolution")
    def test_all_pass(self, mock_test):
        from scripts.verification.verify_historical_team_codes import main

        mock_test.return_value = True
        main()
        assert mock_test.call_count > 0
