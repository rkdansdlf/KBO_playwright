"""Standings must count a franchise once, whatever code its games were stored with.

Issue #39: from late June 2026 some KIA/두산/SSG/키움 games were stored with the
KBO technical code (HT/OB/SK/WO) instead of the season code. Grouping by the raw
string split each franchise into two rows, so KIA showed 106 of its 143 games and
league wins stopped equalling losses.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.calc.calculate_standings import StandingsCalculator, _build_daily_snapshots
from src.models.game import Game
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily
from src.utils.team_codes import STANDARD_TEAM_CODES

LEGACY_TO_SEASON_2026 = {"HT": "KIA", "OB": "DB", "SK": "SSG", "WO": "KH"}
SEASON_ID_2026 = 264


def _game(game_id: str, day: date, away: str, home: str, away_score: int, home_score: int) -> Game:
    status = "DRAW" if away_score == home_score else "COMPLETED"
    return Game(
        game_id=game_id,
        game_date=day,
        away_team=away,
        home_team=home,
        away_score=away_score,
        home_score=home_score,
        game_status=status,
        season_id=SEASON_ID_2026,
    )


def _mixed_code_season() -> list[Game]:
    """Round-robin of the 10 franchises where every other game uses legacy codes."""
    teams = sorted(STANDARD_TEAM_CODES)
    season_to_legacy = {v: k for k, v in LEGACY_TO_SEASON_2026.items()}
    games: list[Game] = []
    day = date(2026, 4, 1)
    serial = 0
    for i, away in enumerate(teams):
        for home in teams[i + 1 :]:
            for leg in range(2):
                use_legacy = (serial % 2) == 1
                a = season_to_legacy.get(away, away) if use_legacy else away
                h = season_to_legacy.get(home, home) if use_legacy else home
                away_score, home_score = [(3, 1), (2, 5), (4, 4)][serial % 3]
                games.append(_game(f"G{serial:04d}", day, a, h, away_score, home_score))
                serial += 1
                if leg:
                    day += timedelta(days=1)
    return games


def _expected_records(games: list[Game]) -> dict[str, tuple[int, int, int]]:
    wins, losses, draws = Counter(), Counter(), Counter()
    for g in games:
        away = LEGACY_TO_SEASON_2026.get(g.away_team, g.away_team)
        home = LEGACY_TO_SEASON_2026.get(g.home_team, g.home_team)
        if g.away_score == g.home_score:
            draws[away] += 1
            draws[home] += 1
        elif g.away_score > g.home_score:
            wins[away] += 1
            losses[home] += 1
        else:
            wins[home] += 1
            losses[away] += 1
    return {t: (wins[t], losses[t], draws[t]) for t in STANDARD_TEAM_CODES}


def _final_day(snapshots: list[TeamStandingsDaily]) -> list[TeamStandingsDaily]:
    last = max(s.standings_date for s in snapshots)
    return [s for s in snapshots if s.standings_date == last]


def test_legacy_codes_are_counted_for_their_franchise():
    games = _mixed_code_season()
    assert {g.home_team for g in games} & set(LEGACY_TO_SEASON_2026), "fixture must mix legacy codes"

    final = _final_day(_build_daily_snapshots(games))

    codes = [row.team_code for row in final]
    assert sorted(codes) == sorted(STANDARD_TEAM_CODES)
    expected = _expected_records(games)
    for row in final:
        assert (row.wins, row.losses, row.draws) == expected[row.team_code]
        assert row.games_played == sum(expected[row.team_code])
    assert sum(r.wins for r in final) == sum(r.losses for r in final)
    assert sum(r.draws for r in final) % 2 == 0
    assert sorted(r.rank for r in final) == list(range(1, 11))


def test_ht_game_counts_for_kia_home_and_away_splits():
    games = [
        _game("20260701HTLT0", date(2026, 7, 1), "HT", "LT", 5, 2),
        _game("20260702LTKIA0", date(2026, 7, 2), "LT", "KIA", 1, 6),
    ]

    final = {row.team_code: row for row in _final_day(_build_daily_snapshots(games))}

    assert set(final) == {"KIA", "LT"}
    kia = final["KIA"]
    assert (kia.wins, kia.losses, kia.games_played) == (2, 0, 2)
    assert (kia.home_wins, kia.away_wins) == (1, 1)
    assert kia.current_streak == 2


def test_historical_season_keeps_its_own_brand():
    games = [_game("19950501HTOB0", date(1995, 5, 1), "HT", "OB", 3, 2)]

    final = {row.team_code for row in _final_day(_build_daily_snapshots(games))}

    assert final == {"HT", "OB"}


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for model in (KboSeason, Game, TeamStandingsDaily):
        model.__table__.create(engine)
    db = sessionmaker(bind=engine)()
    db.add(
        KboSeason(season_id=SEASON_ID_2026, season_year=2026, league_type_code=0, league_type_name="정규시즌"),
    )
    db.commit()
    yield db
    db.close()


def test_calculate_year_persists_one_row_per_franchise(session):
    games = _mixed_code_season()
    session.add_all(games)
    session.commit()

    StandingsCalculator(session).calculate_year(2026)

    rows = session.query(TeamStandingsDaily).all()
    last = max(r.standings_date for r in rows)
    final = [r for r in rows if r.standings_date == last]
    assert sorted(r.team_code for r in final) == sorted(STANDARD_TEAM_CODES)
    assert not {r.team_code for r in rows} & set(LEGACY_TO_SEASON_2026)
    assert sum(r.wins for r in final) == sum(r.losses for r in final)
    expected = _expected_records(games)
    assert {r.team_code: (r.wins, r.losses, r.draws) for r in final} == expected
