from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.audit_game_stats import audit_game_stats


class TestAuditGameStats:
    @patch("scripts.legacy.maintenance.audit_game_stats.sessionmaker")
    @patch("scripts.legacy.maintenance.audit_game_stats.Engine")
    def test_no_games(self, mock_engine, mock_sessionmaker):
        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session
        mock_session.query.return_value.filter.return_value.all.return_value = []

        audit_game_stats(2025)
        # No error

    @patch("scripts.legacy.maintenance.audit_game_stats.sessionmaker")
    @patch("scripts.legacy.maintenance.audit_game_stats.Engine")
    def test_with_games_no_discrepancies(self, mock_engine, mock_sessionmaker):
        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session
        mock_game = MagicMock()
        mock_game.game_id = "G1"
        mock_game.home_team = "LG"
        mock_game.away_team = "SSG"
        mock_game.home_score = 5
        mock_game.away_score = 3

        mock_session.query.return_value.filter.return_value.all.side_effect = [
            [mock_game],
            [MagicMock(game_id="G1", team_side="home", total_runs=5, total_hits=8, player_count=10)],
            [],
            [],
        ]
        mock_session.query.return_value.filter.return_value.group_by.return_value.all.return_value = [
            MagicMock(game_id="G1", team_side="home", total_runs=5, total_hits=8, player_count=10)
        ]

        audit_game_stats(2025)
        # No error
