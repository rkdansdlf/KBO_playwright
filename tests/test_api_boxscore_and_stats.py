"""Integration tests for Game Boxscore, Head-to-Head, and Player Stats API."""

from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import app
from src.models.game import Game, GameBattingStat, GameInningScore, GamePitchingStat
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

client = TestClient(app)
AUTH_HEADERS = {"X-API-Key": os.getenv("REST_API_KEY", "")}


def test_get_game_boxscore_api() -> None:
    """Test GET /api/v1/games/{game_id}/boxscore."""
    mock_game = Game(
        game_id="20260809LGKIA0",
        game_date=date(2026, 8, 9),
        stadium="광주",
        home_team="KIA",
        away_team="LG",
        home_score=6,
        away_score=3,
        game_status="FINAL",
    )
    mock_inning = GameInningScore(
        game_id="20260809LGKIA0",
        team_side="away",
        inning=1,
        runs=1,
    )
    mock_hitter = GameBattingStat(
        game_id="20260809LGKIA0",
        team_side="home",
        player_id=78224,
        player_name="김도영",
        batting_order=3,
        appearance_seq=1,
        at_bats=4,
        hits=2,
        runs=1,
        rbi=2,
        walks=1,
        strikeouts=0,
        avg=0.350,
    )
    mock_pitcher = GamePitchingStat(
        game_id="20260809LGKIA0",
        team_side="home",
        player_id=60181,
        player_name="양현종",
        appearance_seq=1,
        decision="승",
        innings_pitched=6.0,
        hits_allowed=4,
        runs_allowed=2,
        earned_runs=2,
        walks_allowed=1,
        strikeouts=6,
        era=3.10,
    )

    with patch("src.api.routers.games.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        empty_result = MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[]))))
        mock_session.execute.side_effect = [
            MagicMock(scalar_one_or_none=MagicMock(return_value=mock_game)),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_inning])))),
            empty_result,
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_hitter])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_pitcher])))),
            empty_result,
        ]

        res = client.get("/api/v1/games/20260809LGKIA0/boxscore", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert data["game_id"] == "20260809LGKIA0"
        assert data["home_team"] == "KIA"
        assert len(data["home_batters"]) == 1
        assert data["home_batters"][0]["player_name"] == "김도영"
        assert len(data["home_pitchers"]) == 1
        assert data["home_pitchers"][0]["player_name"] == "양현종"


def test_get_head_to_head_api() -> None:
    """Test GET /api/v1/games/head-to-head."""
    mock_game = Game(
        game_id="20260809LGKIA0",
        game_date=date(2026, 8, 9),
        home_team="KIA",
        away_team="LG",
        home_score=6,
        away_score=3,
        winning_team="KIA",
        season_id=2026,
    )

    with patch("src.api.routers.games.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_game]

        res = client.get("/api/v1/games/head-to-head?team1=KIA&team2=LG&season=2026", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert data["team1"] == "KIA"
        assert data["team2"] == "LG"
        assert data["team1_wins"] == 1
        assert data["team2_wins"] == 0
        assert data["total_games"] == 1
        assert len(data["recent_games"]) == 1


def test_head_to_head_excludes_incomplete_games_from_recent_results() -> None:
    """Test that scheduled games do not appear as zero-score completed games."""
    complete_game = Game(
        game_id="20250501LGKIA0",
        game_date=date(2025, 5, 1),
        home_team="KIA",
        away_team="LG",
        home_score=4,
        away_score=2,
        season_id=2025,
    )
    scheduled_game = Game(
        game_id="20250502LGKIA0",
        game_date=date(2025, 5, 2),
        home_team="KIA",
        away_team="LG",
        season_id=2025,
    )

    with patch("src.api.routers.games.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalars.return_value.all.return_value = [scheduled_game, complete_game]

        res = client.get("/api/v1/games/head-to-head?team1=KIA&team2=LG&season=2025", headers=AUTH_HEADERS)

    assert res.status_code == 200
    data = res.json()
    assert data["total_games"] == 1
    assert len(data["recent_games"]) == 1
    assert data["recent_games"][0]["game_id"] == "20250501LGKIA0"


def test_get_player_stats_api() -> None:
    """Test GET /api/v1/players/{player_id}/stats."""
    mock_player = PlayerBasic(
        player_id=78224,
        name="김도영",
        position="내야수",
        team="KIA",
    )
    mock_batting = PlayerSeasonBatting(
        player_id=78224,
        season=2026,
        canonical_team_code="KIA",
        games=100,
        plate_appearances=450,
        at_bats=400,
        hits=140,
        home_runs=25,
        rbi=80,
        avg=0.350,
    )

    with patch("src.api.routers.players.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = mock_player

        mock_session.execute.side_effect = [
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[mock_batting])))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))),
        ]

        res = client.get("/api/v1/players/78224/stats", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert data["player_id"] == "78224"
        assert data["player_name"] == "김도영"
        assert len(data["batting_seasons"]) == 1
        assert data["batting_seasons"][0]["season"] == 2026
        assert data["batting_seasons"][0]["hr"] == 25


def test_get_player_sabermetrics_api() -> None:
    """Test GET /api/v1/players/{player_id}/sabermetrics."""
    mock_player = PlayerBasic(player_id=78224, name="김도영")
    mock_batting = PlayerSeasonBatting(
        player_id=78224,
        season=2026,
        plate_appearances=450,
        at_bats=400,
        hits=140,
        doubles=25,
        triples=3,
        home_runs=25,
        walks=45,
        hbp=5,
        sacrifice_flies=3,
        babip=0.360,
        iso=0.255,
    )

    with patch("src.api.routers.players.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = mock_player

        mock_session.query.return_value.filter.return_value.first.side_effect = [
            mock_batting,
            None,  # pitching stat
        ]

        with patch("src.aggregators.sabermetrics_calculator.SabermetricsCalculator.get_league_constants") as mock_lg:
            mock_lg.return_value = {
                "lg_woba": 0.330,
                "woba_scale": 1.15,
                "lg_r_per_pa": 0.12,
                "fip_constant": 3.80,
                "rpw": 10.0,
                "lg_era": 4.20,
                "lg_obp": 0.340,
                "lg_slg": 0.400,
            }

            res = client.get("/api/v1/players/78224/sabermetrics?season=2026", headers=AUTH_HEADERS)
            assert res.status_code == 200
            data = res.json()
            assert data["player_id"] == "78224"
            assert data["player_name"] == "김도영"
            assert data["season"] == 2026
            assert data["woba"] is not None
            assert data["babip"] == 0.360
            assert data["isop"] == 0.255


def test_get_pitcher_sabermetrics_api() -> None:
    """Test pitcher-only sabermetrics use innings_outs and fip_adj."""
    mock_player = PlayerBasic(player_id=60181, name="양현종", position="투수")
    mock_pitching = PlayerSeasonPitching(
        player_id=60181,
        season=2026,
        innings_outs=18,
        innings_pitched=6.0,
        strikeouts=6,
        earned_runs=2,
    )

    with patch("src.api.routers.players.get_db_session") as mock_get_db:
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session
        mock_session.get.return_value = mock_player
        mock_session.query.return_value.filter.return_value.first.side_effect = [None, mock_pitching]

        with (
            patch(
                "src.aggregators.sabermetrics_calculator.SabermetricsCalculator.get_league_constants",
                return_value={"fip_constant": 3.8, "lg_era": 4.2, "rpw": 10.0},
            ),
            patch(
                "src.aggregators.sabermetrics_calculator.SabermetricsCalculator.calculate_pitching_metrics",
                return_value={"fip_adj": 3.21, "lob_pct": 0.75, "war": 0.12},
            ),
        ):
            res = client.get("/api/v1/players/60181/sabermetrics?season=2026", headers=AUTH_HEADERS)

    assert res.status_code == 200
    data = res.json()
    assert data["fip"] == 3.21
    assert data["pitching_war"] == 0.12
    assert data["war"] == 0.12
