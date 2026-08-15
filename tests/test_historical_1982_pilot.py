"""Unit tests for Historical1982PilotService."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.services.historical_1982_pilot_service import (
    HISTORICAL_1982_EXPECTED_TOTAL_GAMES,
    HISTORICAL_1982_TEAMS,
    Historical1982PilotService,
)


def _get_in_memory_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_generate_1982_schedule_fixtures() -> None:
    """Fixture generation should produce exactly 240 games across 6 teams."""
    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    fixtures = service.generate_1982_schedule_fixtures()
    assert len(fixtures) == HISTORICAL_1982_EXPECTED_TOTAL_GAMES

    team_games = dict.fromkeys(HISTORICAL_1982_TEAMS, 0)
    for f in fixtures:
        assert f["season_year"] == 1982
        team_games[f["home_team_code"]] += 1
        team_games[f["away_team_code"]] += 1

    # Each team must play 80 games
    for team, count in team_games.items():
        assert count == 80, f"Team {team} played {count} games, expected 80"


def test_seed_and_verify_1982_pilot() -> None:
    """Seeding fixtures and running audit should confirm season integrity."""
    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    saved_count = service.seed_1982_fixtures()
    session.commit()
    assert saved_count == HISTORICAL_1982_EXPECTED_TOTAL_GAMES

    report = service.verify_1982_season_integrity()
    assert report.total_games == HISTORICAL_1982_EXPECTED_TOTAL_GAMES
    assert report.is_count_valid is True
    assert report.missing_games_count == 0
    assert report.standings_match is True


def test_generated_game_ids_are_unique() -> None:
    """All 240 fixture game_ids must be unique."""
    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    fixtures = service.generate_1982_schedule_fixtures()
    game_ids = [f["game_id"] for f in fixtures]
    assert len(game_ids) == len(set(game_ids))


def test_each_ordered_pair_plays_exactly_8_games() -> None:
    """Every ordered (home, away) pairing must occur exactly 8 times."""
    from collections import Counter

    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    fixtures = service.generate_1982_schedule_fixtures()
    pair_counts = Counter((f["home_team_code"], f["away_team_code"]) for f in fixtures)
    assert len(pair_counts) == 30
    assert all(count == 8 for count in pair_counts.values())


def test_fixture_dates_are_valid_1982_dates() -> None:
    """Every fixture date must parse as a real 1982 calendar date."""
    from datetime import date

    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    for f in service.generate_1982_schedule_fixtures():
        parsed = date.fromisoformat(f["game_date"])
        assert parsed.year == 1982


def test_seed_is_idempotent() -> None:
    """Re-seeding the same fixtures must not create duplicate rows."""
    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    assert service.seed_1982_fixtures() == HISTORICAL_1982_EXPECTED_TOTAL_GAMES
    session.commit()
    assert service.seed_1982_fixtures() == 0


def test_verify_on_empty_db_reports_missing_games() -> None:
    """An empty database should fail the 1982 integrity audit."""
    session = _get_in_memory_session()
    service = Historical1982PilotService(session)

    report = service.verify_1982_season_integrity()
    assert report.total_games == 0
    assert report.is_count_valid is False
    assert report.missing_games_count == HISTORICAL_1982_EXPECTED_TOTAL_GAMES
