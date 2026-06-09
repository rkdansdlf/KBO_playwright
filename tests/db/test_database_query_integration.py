from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.db.database_query import get_player_defensive_stats


class TestGetPlayerDefensiveStats:
    def test_no_player_found_returns_empty(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.all.return_value = []
        with patch("src.db.database_query.SessionLocal", return_value=mock_session):
            result = get_player_defensive_stats("NonexistentPlayer", 2025)
            assert result == []

    def test_player_with_no_fielding_stats(self):
        mock_session = MagicMock()
        mock_session.execute.side_effect = [
            MagicMock(all=MagicMock(return_value=[(1,)])),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))),
        ]
        with patch("src.db.database_query.SessionLocal", return_value=mock_session):
            result = get_player_defensive_stats("Kim", 2025)
            assert result == []

    def test_returns_fielding_stats(self):
        mock_row = MagicMock()
        mock_row.player_id = 1
        mock_row.team_id = 10
        mock_row.year = 2025
        mock_row.position_id = 7
        mock_row.games = 50
        mock_row.games_started = 40
        mock_row.innings = 350.0
        mock_row.putouts = 80
        mock_row.assists = 120
        mock_row.errors = 3
        mock_row.double_plays = 15
        mock_row.fielding_pct = 0.985
        mock_row.pickoffs = 1
        mock_row.source = "crawl"

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.execute.side_effect = [
            MagicMock(all=MagicMock(return_value=[(1,)])),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_row])))),
        ]
        with patch("src.db.database_query.SessionLocal", return_value=mock_session):
            result = get_player_defensive_stats("Kim", 2025)
            assert len(result) == 1
            assert result[0]["player_id"] == 1
            assert result[0]["fielding_pct"] == 0.985
            assert result[0]["position_id"] == 7

    def test_multiple_players_same_name(self):
        mock_row1 = MagicMock()
        mock_row1.player_id = 1
        mock_row1.team_id = 10
        mock_row1.year = 2025
        mock_row1.position_id = 7
        mock_row1.games = 20
        mock_row1.games_started = 15
        mock_row1.innings = 120.0
        mock_row1.putouts = 30
        mock_row1.assists = 50
        mock_row1.errors = 2
        mock_row1.double_plays = 5
        mock_row1.fielding_pct = 0.975
        mock_row1.pickoffs = 0
        mock_row1.source = "crawl"

        mock_row2 = MagicMock()
        mock_row2.player_id = 2
        mock_row2.team_id = 20
        mock_row2.year = 2025
        mock_row2.position_id = 8
        mock_row2.games = 30
        mock_row2.games_started = 25
        mock_row2.innings = 200.0
        mock_row2.putouts = 60
        mock_row2.assists = 90
        mock_row2.errors = 1
        mock_row2.double_plays = 10
        mock_row2.fielding_pct = 0.993
        mock_row2.pickoffs = 2
        mock_row2.source = "crawl"

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.execute.side_effect = [
            MagicMock(all=MagicMock(return_value=[(1,), (2,)])),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_row1, mock_row2])))),
        ]
        with patch("src.db.database_query.SessionLocal", return_value=mock_session):
            result = get_player_defensive_stats("Kim", 2025)
            assert len(result) == 2
