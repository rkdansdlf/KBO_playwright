"""Validate daily standings snapshots against completed game results."""
from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.models.game import Game
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


REGULAR_SEASON_NAMES = ("정규시즌", "Regular Season", "regular")
STANDINGS_FIELDS = (
    "games_played",
    "wins",
    "losses",
    "draws",
    "runs_scored",
    "runs_allowed",
)


def validate_standings_integrity(session: Session, target_date: date) -> dict[str, Any]:
    """Compare one daily standings snapshot with an independent game-result rollup."""
    expected, missing_score_games = _aggregate_regular_season_results(session, target_date)
    snapshot_rows = (
        session.query(TeamStandingsDaily)
        .filter(TeamStandingsDaily.standings_date == target_date)
        .all()
    )
    actual = {row.team_code: row for row in snapshot_rows}

    mismatches: list[dict[str, Any]] = []
    for team_code, expected_values in sorted(expected.items()):
        row = actual.get(team_code)
        if row is None:
            mismatches.append(
                {
                    "team_code": team_code,
                    "issue": "missing_standings_row",
                    "expected": expected_values,
                }
            )
            continue

        differences = {}
        for field in STANDINGS_FIELDS:
            actual_value = int(getattr(row, field) or 0)
            expected_value = int(expected_values[field])
            if actual_value != expected_value:
                differences[field] = {
                    "expected": expected_value,
                    "actual": actual_value,
                }

        if differences:
            mismatches.append(
                {
                    "team_code": team_code,
                    "issue": "value_mismatch",
                    "differences": differences,
                }
            )

    for team_code, row in sorted(actual.items()):
        if team_code in expected:
            continue
        mismatches.append(
            {
                "team_code": team_code,
                "issue": "extra_standings_row",
                "actual": {field: int(getattr(row, field) or 0) for field in STANDINGS_FIELDS},
            }
        )

    return {
        "ok": not mismatches and not missing_score_games,
        "checked_date": target_date.isoformat(),
        "checked_teams": len(expected),
        "mismatches": mismatches,
        "missing_score_games": missing_score_games,
    }


def _regular_season_filter():
    return or_(
        KboSeason.league_type_code == 0,
        KboSeason.league_type_name.in_(REGULAR_SEASON_NAMES),
    )


def _empty_team_state() -> dict[str, int]:
    return {
        "games_played": 0,
        "wins": 0,
        "losses": 0,
        "draws": 0,
        "runs_scored": 0,
        "runs_allowed": 0,
    }


def _aggregate_regular_season_results(
    session: Session,
    target_date: date,
) -> tuple[dict[str, dict[str, int]], list[str]]:
    games = (
        session.query(Game)
        .join(KboSeason, Game.season_id == KboSeason.season_id)
        .filter(
            KboSeason.season_year == target_date.year,
            _regular_season_filter(),
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            Game.game_date <= target_date,
        )
        .order_by(Game.game_date.asc(), Game.game_id.asc())
        .all()
    )

    standings: dict[str, dict[str, int]] = {}
    missing_score_games: list[str] = []
    for game in games:
        if game.home_score is None or game.away_score is None:
            missing_score_games.append(game.game_id)
            continue
        if not game.home_team or not game.away_team:
            continue

        home = standings.setdefault(game.home_team, _empty_team_state())
        away = standings.setdefault(game.away_team, _empty_team_state())
        home_score = int(game.home_score)
        away_score = int(game.away_score)

        home["games_played"] += 1
        away["games_played"] += 1
        home["runs_scored"] += home_score
        home["runs_allowed"] += away_score
        away["runs_scored"] += away_score
        away["runs_allowed"] += home_score

        if home_score > away_score:
            home["wins"] += 1
            away["losses"] += 1
        elif away_score > home_score:
            away["wins"] += 1
            home["losses"] += 1
        else:
            home["draws"] += 1
            away["draws"] += 1

    return standings, missing_score_games
