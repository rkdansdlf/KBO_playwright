"""The ranking qualification threshold must survive the 1982~2000 status rewrite.

1982~2000 hold a score and no body, and a date-ordered crawl sweep rewrote
``game_status`` on them. The threshold was derived from the count of
``COMPLETED``/``DRAW`` game dates, so it read 1~16 games for a season that
ran 80~133 games per team and collapsed onto its 30 PA / 90 out floor.

A floor that low is not a qualification criterion. It admitted a 30-PA
batter as the 1989 batting champion, which is the extreme value the
completion criterion forbids.

The schedule was not rewritten, so the era's length comes from the schedule.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.calc import calculate_rankings as rankings
from src.models.game import Game


#: The real 1989 season: 7 teams, 120 games per team, 420 game rows.
ERA_SEASON = 1989
ERA_TEAMS = 7
ERA_GAMES_PER_TEAM = 120


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(engine)
    return sessionmaker(bind=engine)


def _add(session, game_id: str, game_date: date, status: str, home: str, away: str) -> None:
    """Add a game row with the given status and teams."""
    session.add(
        Game(
            game_id=game_id,
            game_date=game_date,
            home_team=home,
            away_team=away,
            home_score=3,
            away_score=1,
            game_status=status,
        ),
    )


def _add_schedule(
    session,
    season: int,
    *,
    teams: int,
    games_per_team: int,
    status: str,
    distinct_dates: bool = False,
) -> int:
    """Add a schedule of ``teams`` teams and ``games_per_team`` games each.

    A team's games per team is ``rows * 2 / teams``, so the row count is
    ``games_per_team * teams / 2``.

    Args:
        session: Session.
        season: Season year.
        teams: Number of teams in the season.
        games_per_team: Games each team played.
        status: Status written on every row.
        distinct_dates: Give every game its own date instead of one shared date.

    Returns:
        The number of games added.

    """
    team_codes = [f"T{index}" for index in range(teams)]
    rows = games_per_team * teams // 2
    for index in range(rows):
        home = team_codes[index % teams]
        away = team_codes[(index + 1) % teams]
        game_date = date(season, 1, 1)
        if distinct_dates:
            game_date = date(season, 1, 1) + timedelta(days=index)
        _add(session, f"{season}{index:05d}{home}{away}0", game_date, status, home, away)
    return rows


class TestTheSourceLimitedEraUsesTheSchedule:
    def test_a_swept_season_measures_its_schedule_not_its_statuses(self, session_factory) -> None:
        """A 120-game season reads 120 games per team even when no row says COMPLETED."""
        with session_factory() as session:
            rows = _add_schedule(
                session,
                ERA_SEASON,
                teams=ERA_TEAMS,
                games_per_team=ERA_GAMES_PER_TEAM,
                status="UNRESOLVED_MISSING",
            )
            session.commit()

            assert rows == 420
            assert rankings._games_played_in_season(session, ERA_SEASON) == 0
            assert rankings._qualification_games_played(session, ERA_SEASON) == ERA_GAMES_PER_TEAM

    def test_the_threshold_uses_the_schedule_length(self, session_factory) -> None:
        """The threshold scales with the real season length instead of the floor."""
        with session_factory() as session:
            _add_schedule(
                session,
                ERA_SEASON,
                teams=ERA_TEAMS,
                games_per_team=ERA_GAMES_PER_TEAM,
                status="UNRESOLVED_MISSING",
            )
            session.commit()

            assert rankings._compute_min_pa(session, ERA_SEASON) == int(
                ERA_GAMES_PER_TEAM * rankings._MIN_PA_PER_GAME,
            )
            assert rankings._compute_min_ip_outs(session, ERA_SEASON) == int(
                ERA_GAMES_PER_TEAM * rankings._MIN_IP_PER_GAME,
            )

    def test_a_swept_season_does_not_fall_to_the_floor(self, session_factory) -> None:
        """A real season must not qualify on 30 PA."""
        with session_factory() as session:
            _add_schedule(
                session,
                ERA_SEASON,
                teams=ERA_TEAMS,
                games_per_team=ERA_GAMES_PER_TEAM,
                status="UNRESOLVED_MISSING",
            )
            session.commit()

            assert rankings._compute_min_pa(session, ERA_SEASON) > rankings._MIN_PA_FLOOR
            assert rankings._compute_min_ip_outs(session, ERA_SEASON) > rankings._MIN_IP_FLOOR

    def test_a_collected_season_still_uses_its_completed_games(self, session_factory) -> None:
        """2001 onward is untouched: the status says which games were played."""
        with session_factory() as session:
            _add_schedule(
                session,
                2001,
                teams=8,
                games_per_team=133,
                status="COMPLETED",
                distinct_dates=True,
            )
            session.commit()

            completed_games = rankings._games_played_in_season(session, 2001)
            assert completed_games > 0
            assert rankings._qualification_games_played(session, 2001) == completed_games

    def test_an_empty_season_reports_no_games(self, session_factory) -> None:
        """A season with no schedule rows has no length to scale by."""
        with session_factory() as session:
            session.commit()

            assert rankings._scheduled_games_per_team(session, ERA_SEASON) == 0
            assert rankings._qualification_games_played(session, ERA_SEASON) == 0

    def test_the_era_boundary_is_the_first_collected_season(self) -> None:
        """The boundary is a fact about the data, not a magic year in this module."""
        assert rankings._is_source_limited_season(1982) is True
        assert rankings._is_source_limited_season(2000) is True
        assert rankings._is_source_limited_season(2001) is False
