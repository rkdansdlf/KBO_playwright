from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game  # noqa: F401 register FK target for team_events.game_id
from src.models.team_event import TeamEvent
from src.repositories.team_event_repository import TeamEventRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


class TestTeamEventRepository:
    def test_save_and_query(self, session):
        repo = TeamEventRepository(session)
        data = {
            "event_scope": "team",
            "team_id": "LG",
            "title": "팬 사인회 이벤트",
            "event_type": "fan_participation",
            "source_url": "https://example.com/event1",
            "status": "scheduled",
        }
        event = repo.save(data)
        assert event.title == "팬 사인회 이벤트"
        assert event.team_id == "LG"

    def test_save_dedup_by_source_url(self, session):
        repo = TeamEventRepository(session)
        data = {"team_id": "LG", "title": "중복 이벤트", "source_url": "https://example.com/dup", "status": "scheduled"}
        e1 = repo.save(data)
        e2 = repo.save({**data, "status": "open"})
        assert e1.id == e2.id
        assert e2.status == "open"

    def test_save_dedup_by_team_title(self, session):
        repo = TeamEventRepository(session)
        data = {"team_id": "SS", "title": "서울 이벤트", "status": "scheduled"}
        e1 = repo.save(data)
        e2 = repo.save({"team_id": "SS", "title": "서울 이벤트", "status": "open"})
        assert e1.id == e2.id
        assert e2.status == "open"

    def test_get_by_team(self, session):
        repo = TeamEventRepository(session)
        repo.save(
            {
                "event_scope": "team",
                "team_id": "LG",
                "title": "LG 이벤트1",
                "source_url": "u1",
                "published_at": datetime.now(UTC).replace(tzinfo=None),
            }
        )
        repo.save(
            {
                "event_scope": "team",
                "team_id": "LG",
                "title": "LG 이벤트2",
                "source_url": "u2",
                "published_at": datetime.now(UTC).replace(tzinfo=None),
            }
        )
        repo.save({"event_scope": "team", "team_id": "SS", "title": "SS 이벤트", "source_url": "u3"})
        session.commit()

        lg_events = repo.get_by_team("LG")
        assert len(lg_events) == 2

        ss_events = repo.get_by_team("SS")
        assert len(ss_events) == 1

    def test_get_upcoming(self, session):
        repo = TeamEventRepository(session)
        future = datetime(2099, 6, 15)
        past = datetime(2020, 1, 1)
        repo.save({"title": "지난 이벤트", "status": "ended", "event_end_at": past, "source_url": "u1"})
        repo.save({"title": "다가오는 이벤트", "status": "scheduled", "event_end_at": future, "source_url": "u2"})
        session.commit()

        upcoming = repo.get_upcoming()
        titles = [e.title for e in upcoming]
        assert "다가오는 이벤트" in titles
        assert "지난 이벤트" not in titles

    def test_update_status(self, session):
        repo = TeamEventRepository(session)
        e = repo.save({"team_id": "LG", "title": "상태 변경", "source_url": "u1", "status": "scheduled"})
        session.commit()
        repo.update_status(e.id, "closed")
        session.commit()

        updated = session.get(TeamEvent, e.id)
        assert updated.status == "closed"

    def test_unique_constraint(self, session):
        repo = TeamEventRepository(session)
        repo.save({"team_id": "LG", "title": "유니크 테스트", "source_url": "https://example.com/uq"})
        session.commit()

        with pytest.raises(IntegrityError):
            direct = TeamEvent(team_id="LG", title="유니크 테스트", source_url="https://example.com/uq")
            session.add(direct)
            session.commit()
