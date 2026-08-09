"""Tests for newly added Repositories and CLI modules."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.cli.crawl_futures_schedule import build_arg_parser as futures_parser
from src.cli.crawl_milestones import build_arg_parser as milestone_parser
from src.cli.crawl_press_releases import build_arg_parser as press_parser
from src.models.base import Base
from src.repositories.futures_schedule_repository import FuturesScheduleRepository
from src.repositories.milestone_repository import MilestoneRepository
from src.repositories.press_release_repository import KboPressReleaseRepository


@pytest.fixture
def db_session() -> Session:
    """Fixture providing an in-memory SQLite session with initialized tables."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


def test_press_release_repository(db_session: Session) -> None:
    """Test saving and retrieving KBO press releases."""
    repo = KboPressReleaseRepository(db_session)
    data = {
        "notice_id": "PR_101",
        "published_date": date(2026, 4, 15),
        "category": "공시",
        "title": "2026 KBO 상벌위원회 징계 결과 공시",
        "source_url": "https://www.koreabaseball.com/News/Notice/101",
    }
    rec = repo.save_press_release(data)
    db_session.commit()

    assert rec.notice_id == "PR_101"
    recent = repo.get_recent_releases(limit=10)
    assert len(recent) == 1
    assert recent[0].title == "2026 KBO 상벌위원회 징계 결과 공시"


def test_milestone_repository(db_session: Session) -> None:
    """Test saving and retrieving player milestones."""
    repo = MilestoneRepository(db_session)
    data = {
        "season": 2026,
        "player_id": "79101",
        "player_name": "김현수",
        "team_code": "LG",
        "milestone_category": "2500안타",
        "current_val": 2485,
        "target_val": 2500,
        "remaining_val": 15,
        "is_achieved": False,
    }
    rec = repo.save_milestone(data)
    db_session.commit()

    assert rec.remaining_val == 15
    upcoming = repo.get_upcoming_milestones(season=2026)
    assert len(upcoming) == 1
    assert upcoming[0].player_name == "김현수"


def test_futures_schedule_repository(db_session: Session) -> None:
    """Test saving Futures game schedule and standings."""
    repo = FuturesScheduleRepository(db_session)
    sched_data = {
        "season": 2026,
        "game_date": date(2026, 5, 2),
        "game_id": "FUT_20260502_SSG_LG",
        "away_team": "SSG",
        "home_team": "LG",
        "stadium": "이천",
        "game_status": "SCHEDULED",
    }
    rec_game = repo.save_game_schedule(sched_data)

    stand_data = {
        "season": 2026,
        "division": "북부",
        "team_code": "LG",
        "games_played": 15,
        "wins": 10,
        "losses": 5,
        "draws": 0,
        "win_pct": 0.667,
        "games_behind": 0.0,
        "rank": 1,
    }
    rec_stand = repo.save_team_standings(stand_data)
    db_session.commit()

    assert rec_game.game_id == "FUT_20260502_SSG_LG"
    assert rec_stand.rank == 1


def test_cli_arg_parsers() -> None:
    """Test CLI argument parser configurations."""
    p1 = press_parser()
    args1 = p1.parse_args(["--save", "--max-pages", "3"])
    assert args1.save is True
    assert args1.max_pages == 3

    p2 = milestone_parser()
    args2 = p2.parse_args(["--save", "--season", "2026"])
    assert args2.save is True
    assert args2.season == 2026

    p3 = futures_parser()
    args3 = p3.parse_args(["--year", "2026", "--month", "4"])
    assert args3.year == 2026
    assert args3.month == 4
