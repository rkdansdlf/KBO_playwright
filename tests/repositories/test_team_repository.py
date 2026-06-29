from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.repositories.team_repository import TeamRepository


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


class TestTeamRepository:
    def test_save_daily_rosters(self, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 12345,
                "player_name": "홍길동",
                "position": "투수",
                "back_number": "18",
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1

    def test_save_daily_rosters_dedup(self, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 12345,
                "player_name": "홍길동",
                "position": "투수",
                "back_number": "18",
            },
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 12345,
                "player_name": "홍길동2",
                "position": "투수",
                "back_number": "99",
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1

    def test_save_daily_rosters_empty(self, session):
        repo = TeamRepository(session)
        assert repo.save_daily_rosters([]) == 0

    def test_save_daily_rosters_person_type_staff(self, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 1,
                "player_name": "김감독",
                "position": "감독",
                "back_number": None,
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1

    def test_save_daily_rosters_upsert(self, session):
        repo = TeamRepository(session)
        roster = {
            "roster_date": date(2025, 4, 1),
            "team_code": "LG",
            "player_id": 100,
            "player_name": "홍길동",
            "position": "투수",
            "back_number": "18",
        }
        repo.save_daily_rosters([roster])
        roster["back_number"] = "99"
        repo.save_daily_rosters([roster])
        from src.models.team import TeamDailyRoster

        record = (
            session.query(TeamDailyRoster)
            .filter_by(roster_date=date(2025, 4, 1), team_code="LG", player_id=100)
            .first()
        )
        assert record.back_number == "99"
