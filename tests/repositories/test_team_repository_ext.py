from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.repositories.team_repository import (
    PLAYER_ROSTER_POSITIONS,
    STAFF_ROSTER_POSITIONS,
    TeamRepository,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


class TestPersonTypeForPosition:
    def test_player_positions(self):
        repo = TeamRepository(MagicMock())
        for pos in PLAYER_ROSTER_POSITIONS:
            assert repo._person_type_for_position(pos) == "player"

    def test_staff_positions(self):
        repo = TeamRepository(MagicMock())
        for pos in STAFF_ROSTER_POSITIONS:
            assert repo._person_type_for_position(pos) == "staff"

    def test_unknown_position(self):
        repo = TeamRepository(MagicMock())
        assert repo._person_type_for_position("트레이너") == "unknown"

    def test_none_position(self):
        repo = TeamRepository(MagicMock())
        assert repo._person_type_for_position(None) == "unknown"

    def test_empty_position(self):
        repo = TeamRepository(MagicMock())
        assert repo._person_type_for_position("") == "unknown"


class TestSaveDailyRostersAdvanced:
    def test_staff_roster_does_not_lookup_player_basic(self, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 99999,
                "player_name": "김코치",
                "position": "코치",
                "back_number": "77",
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1

    def test_player_not_in_player_basic_table(self, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "SS",
                "player_id": 88888,
                "player_name": "이신인",
                "position": "투수",
                "back_number": "1",
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1
        from src.models.team import TeamDailyRoster

        record = session.query(TeamDailyRoster).first()
        assert record.player_basic_id is None

    def test_explicit_person_type_override(self, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "OB",
                "player_id": 77777,
                "player_name": "박트레이너",
                "position": "트레이너",
                "person_type": "staff",
                "back_number": "55",
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1

    def test_dedup_keeps_last_entry(self, session):
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
                "player_name": "홍길동갱신",
                "position": "투수",
                "back_number": "99",
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1

    @patch.object(TeamRepository, "_person_type_for_position", return_value="player")
    def test_player_with_player_basic_id_explicit(self, mock_pt, session):
        repo = TeamRepository(session)
        rosters = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 12345,
                "player_name": "테스트",
                "position": "투수",
                "back_number": "18",
                "player_basic_id": 12345,
            },
        ]
        count = repo.save_daily_rosters(rosters)
        assert count == 1
