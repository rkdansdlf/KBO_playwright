"""Tests for standings_integrity validator."""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.game import Game
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily
from src.validators.standings_integrity import validate_standings_integrity

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    KboSeason.__table__.create(bind=engine)
    TeamStandingsDaily.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_season(session: Session, season_id=1, year=2025, code=0, name="Regular Season"):
    session.add(KboSeason(season_id=season_id, season_year=year, league_type_code=code, league_type_name=name))
    session.commit()


def _add_game(
    session: Session,
    game_id="G1",
    game_date=date(2025, 10, 1),
    home_team="LG",
    away_team="SSG",
    home_score=5,
    away_score=3,
    game_status="COMPLETED",
    season_id=1,
):
    session.add(
        Game(
            game_id=game_id,
            game_date=game_date,
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
            game_status=game_status,
            season_id=season_id,
        )
    )
    session.commit()


def _add_standings(
    session: Session,
    standings_date=date(2025, 10, 1),
    team_code="LG",
    games_played=1,
    wins=1,
    losses=0,
    draws=0,
    runs_scored=5,
    runs_allowed=3,
    win_pct=1.0,
):
    session.add(
        TeamStandingsDaily(
            standings_date=standings_date,
            team_code=team_code,
            games_played=games_played,
            wins=wins,
            losses=losses,
            draws=draws,
            runs_scored=runs_scored,
            runs_allowed=runs_allowed,
            win_pct=win_pct,
            games_behind=0.0,
            current_streak=1,
            run_differential=2,
        )
    )
    session.commit()


# ── Tests ────────────────────────────────────────────────────────────────────


class TestEarlyExit:
    def test_skips_year_before_2020(self, session):
        result = validate_standings_integrity(session, date(2019, 10, 1))
        assert result["ok"] is True
        assert "note" in result

    def test_skips_post_season_date(self, session):
        _add_season(session)
        _add_game(session)
        result = validate_standings_integrity(session, date(2025, 12, 1))
        assert result["ok"] is True
        assert "Post-season" in result.get("note", "")


class TestAggregation:
    def test_empty_games_no_standings(self, session):
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is True
        assert result["checked_teams"] == 0

    def test_aggregates_single_game(self, session):
        _add_season(session)
        _add_game(session)
        _add_standings(session, team_code="LG", wins=1, runs_scored=5, runs_allowed=3)
        _add_standings(
            session, team_code="SSG", games_played=1, wins=0, losses=1, runs_scored=3, runs_allowed=5, win_pct=0.0
        )
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is True

    def test_missing_scores_added_to_missing_list(self, session):
        _add_season(session)
        _add_game(session, game_id="MISSING", home_score=None, away_score=None)
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert len(result["missing_score_games"]) == 1
        assert "MISSING" in result["missing_score_games"]


class TestMismatchDetection:
    def test_matches_when_standings_agree(self, session):
        _add_season(session)
        _add_game(session)
        _add_standings(session, team_code="LG")
        _add_standings(
            session, team_code="SSG", games_played=1, wins=0, losses=1, runs_scored=3, runs_allowed=5, win_pct=0.0
        )
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is True
        assert result["mismatches"] == []

    def test_mismatch_wins(self, session):
        _add_season(session)
        _add_game(session)
        _add_standings(session, team_code="LG", wins=0)
        _add_standings(
            session, team_code="SSG", games_played=1, wins=0, losses=1, runs_scored=3, runs_allowed=5, win_pct=0.0
        )
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is False
        lg_mismatch = [m for m in result["mismatches"] if m["team_code"] == "LG"]
        assert len(lg_mismatch) == 1
        assert "wins" in lg_mismatch[0]["differences"]

    def test_mismatch_runs_scored(self, session):
        _add_season(session)
        _add_game(session)
        _add_standings(session, team_code="LG", runs_scored=99)
        _add_standings(
            session, team_code="SSG", games_played=1, wins=0, losses=1, runs_scored=3, runs_allowed=5, win_pct=0.0
        )
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is False
        lg_mismatch = [m for m in result["mismatches"] if m["team_code"] == "LG"]
        assert len(lg_mismatch) == 1
        diff = lg_mismatch[0]["differences"]
        assert diff["runs_scored"]["expected"] == 5
        assert diff["runs_scored"]["actual"] == 99

    def test_missing_standings_row_reported(self, session):
        _add_season(session)
        _add_game(session)
        _add_standings(
            session, team_code="SSG", games_played=1, wins=0, losses=1, runs_scored=3, runs_allowed=5, win_pct=0.0
        )
        # LG standings row is missing
        result = validate_standings_integrity(session, date(2025, 10, 1))
        mismatches = [m for m in result["mismatches"] if m["issue"] == "missing_standings_row"]
        assert len(mismatches) == 1
        assert mismatches[0]["team_code"] == "LG"

    def test_extra_standings_row_reported(self, session):
        _add_season(session)
        _add_standings(session, team_code="XYZ")
        result = validate_standings_integrity(session, date(2025, 10, 1))
        mismatches = [m for m in result["mismatches"] if m["issue"] == "extra_standings_row"]
        assert len(mismatches) >= 1
        assert mismatches[0]["team_code"] == "XYZ"

    def test_multiple_games_both_teams_match(self, session):
        _add_season(session)
        _add_game(session, game_id="G1", home_team="LG", away_team="SSG", home_score=5, away_score=3)
        _add_game(session, game_id="G2", home_team="SSG", away_team="LG", home_score=2, away_score=4)
        _add_standings(session, team_code="LG", games_played=2, wins=2, runs_scored=9, runs_allowed=5)
        _add_standings(
            session, team_code="SSG", games_played=2, wins=0, losses=2, runs_scored=5, runs_allowed=9, win_pct=0.0
        )
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is True


class TestFiltering:
    def test_excludes_all_star_teams_ea_we(self, session):
        _add_season(session)
        _add_game(session, game_id="G1", home_team="EA", away_team="WE", home_score=10, away_score=8)
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is True
        assert result["checked_teams"] == 0

    def test_only_regular_season_games_count(self, session):
        _add_season(session)
        _add_game(session, game_id="G1", season_id=1)
        _add_game(session, game_id="G2", season_id=2, home_team="LG", away_team="KT", home_score=2, away_score=1)
        session.add(KboSeason(season_id=2, season_year=2025, league_type_code=2, league_type_name="Postseason"))
        session.commit()
        _add_standings(
            session, team_code="SSG", games_played=1, wins=0, losses=1, runs_scored=3, runs_allowed=5, win_pct=0.0
        )
        result = validate_standings_integrity(session, date(2025, 10, 1))
        mismatches = [m for m in result["mismatches"] if m["issue"] == "missing_standings_row"]
        lg_mismatches = [m for m in mismatches if m["team_code"] == "LG"]
        # Only game G1 (season 1, regular) counts for LG
        # G2 is postseason (season 2) so KT should not be expected
        assert len(lg_mismatches) == 1

    def test_non_terminal_games_do_not_affect_standings(self, session):
        _add_season(session)
        _add_game(session, game_status="SCHEDULED")
        result = validate_standings_integrity(session, date(2025, 10, 1))
        assert result["ok"] is True
        assert result["checked_teams"] == 0
