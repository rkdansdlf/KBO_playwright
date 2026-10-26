"""Tests for MilestoneRepository milestone target handling."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
from src.repositories.milestone_repository import MilestoneRepository


@pytest.fixture
def session() -> Session:
    """Provide an in-memory SQLite database session."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as sess:
        yield sess


def test_recrawl_keeps_target_val_and_recomputes_remaining(session: Session) -> None:
    """Re-crawl of an existing row preserves target_val and updates remaining_val."""
    repo = MilestoneRepository(session)
    existing = PlayerMilestone(
        season=2026,
        player_id="1001",
        player_name="원래이름",
        team_code="KIA",
        milestone_category="2000안타",
        current_val=1990,
        target_val=2000,
        remaining_val=10,
        is_achieved=0,
        achieved_date=None,
    )
    session.add(existing)
    session.commit()

    # Re-crawl data attempts to creep target_val to 2005 and set is_achieved
    recrawl_data = {
        "season": 2026,
        "player_id": "1001",
        "player_name": "새이름",
        "team_code": "HT",
        "milestone_category": "2000안타",
        "current_val": 1995,
        "target_val": 2005,
        "remaining_val": 10,
        "is_achieved": True,
        "achieved_date": date(2026, 1, 1),
    }
    saved = repo.save_milestone(recrawl_data)
    session.commit()

    assert saved.target_val == 2000
    assert saved.current_val == 1995
    assert saved.player_name == "새이름"
    assert saved.team_code == "HT"
    assert saved.remaining_val == 5
    assert saved.is_achieved == 0
    assert saved.achieved_date is None


def test_recrawl_remaining_val_floors_at_zero(session: Session) -> None:
    """When current_val exceeds target_val on re-crawl, remaining_val is 0."""
    repo = MilestoneRepository(session)
    existing = PlayerMilestone(
        season=2026,
        player_id="1001",
        player_name="선수",
        team_code="KIA",
        milestone_category="2000안타",
        current_val=1990,
        target_val=2000,
        remaining_val=10,
        is_achieved=0,
        achieved_date=None,
    )
    session.add(existing)
    session.commit()

    recrawl_data = {
        "season": 2026,
        "player_id": "1001",
        "player_name": "선수",
        "team_code": "KIA",
        "milestone_category": "2000안타",
        "current_val": 2010,
        "target_val": 2020,
    }
    saved = repo.save_milestone(recrawl_data)
    session.commit()

    assert saved.target_val == 2000
    assert saved.current_val == 2010
    assert saved.remaining_val == 0
    assert saved.is_achieved == 0
    assert saved.achieved_date is None


def test_new_row_honors_positive_data_target_val(session: Session) -> None:
    """For a new row, data['target_val'] is used when it is a positive integer."""
    repo = MilestoneRepository(session)
    data = {
        "season": 2026,
        "player_id": "1002",
        "player_name": "홍길동",
        "team_code": "LG",
        "milestone_category": "통산 안타",
        "current_val": 350,
        "target_val": 500,
    }
    saved = repo.save_milestone(data)
    session.commit()

    assert saved.target_val == 500
    assert saved.current_val == 350
    assert saved.remaining_val == 150
    assert saved.is_achieved == 0


def test_new_row_derives_target_from_initial_milestone_target(session: Session) -> None:
    """For a new row without a positive target_val, initial_milestone_target is used."""
    repo = MilestoneRepository(session)

    # Category starting with integer
    data_cat_int = {
        "season": 2026,
        "player_id": "1003",
        "player_name": "타자A",
        "team_code": "SSG",
        "milestone_category": "2000안타",
        "current_val": 1990,
        "target_val": 0,
    }
    saved_cat = repo.save_milestone(data_cat_int)

    # Category without integer
    data_round = {
        "season": 2026,
        "player_id": "1004",
        "player_name": "타자B",
        "team_code": "NC",
        "milestone_category": "통산 안타",
        "current_val": 351,
        "target_val": 0,
    }
    saved_round = repo.save_milestone(data_round)
    session.commit()

    assert saved_cat.target_val == 2000
    assert saved_cat.remaining_val == 10
    assert saved_cat.is_achieved == 0

    assert saved_round.target_val == 400
    assert saved_round.remaining_val == 49
    assert saved_round.is_achieved == 0
