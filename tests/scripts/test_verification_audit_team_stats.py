from unittest.mock import MagicMock, patch

from scripts.verification.audit_team_stats import TeamStatAudit


class TestTeamStatAudit:
    @patch("scripts.verification.audit_team_stats.SessionLocal")
    def test_audit_batting_no_official(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.all.return_value = []

        TeamStatAudit.audit_batting(2025, "REGULAR")
        # No error

    @patch("scripts.verification.audit_team_stats.SessionLocal")
    def test_audit_batting_with_data(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session

        mock_official = MagicMock()
        mock_official.team_id = "LG"
        mock_official.team_name = "LG Twins"
        mock_official.games = 100
        mock_official.at_bats = 3000
        mock_official.hits = 800
        mock_official.home_runs = 100
        mock_official.runs = 400

        mock_session.query.return_value.filter_by.return_value.all.return_value = [mock_official]

        with patch(
            "scripts.verification.audit_team_stats.TeamStatAggregator.aggregate_team_batting",
            return_value=[{"team_id": "LG", "games": 100, "at_bats": 3000, "hits": 800, "home_runs": 100, "runs": 400}],
        ):
            TeamStatAudit.audit_batting(2025, "REGULAR")
        # No error

    @patch("scripts.verification.audit_team_stats.SessionLocal")
    def test_audit_pitching_no_official(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.all.return_value = []

        TeamStatAudit.audit_pitching(2025, "REGULAR")
        # No error
