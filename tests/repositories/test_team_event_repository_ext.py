from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.team_event import TeamEvent
from src.repositories.team_event_repository import TeamEventRepository


class TestTeamEventRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        TeamEvent.__table__.create(engine)

    def test_save_by_source_url_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        event = repo.save(
            {
                "source_url": "https://example.com/event/1",
                "title": "팬 사인회",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        session.commit()
        assert event.id is not None
        assert event.title == "팬 사인회"

    def test_save_upsert_by_source_url(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        r1 = repo.save(
            {
                "source_url": "https://example.com/event/1",
                "title": "팬 사인회",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "source_url": "https://example.com/event/1",
                "title": "팬 사인회 (수정)",
                "team_id": "LG",
                "event_scope": "team",
                "status": "open",
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.status == "open"

    def test_save_upsert_by_team_title(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        r1 = repo.save(
            {
                "title": "팬 사인회",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "title": "팬 사인회",
                "team_id": "LG",
                "event_scope": "team",
                "status": "open",
                "description": "Updated description",
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.status == "open"

    def test_get_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        repo.save(
            {
                "source_url": "https://example.com/1",
                "title": "A",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
                "published_at": datetime(2024, 1, 2),
            }
        )
        repo.save(
            {
                "source_url": "https://example.com/2",
                "title": "B",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
                "published_at": datetime(2024, 1, 1),
            }
        )
        repo.save(
            {
                "source_url": "https://example.com/3",
                "title": "C",
                "team_id": "SSG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        session.commit()

        results = repo.get_by_team("LG", limit=10)
        assert len(results) == 2
        assert results[0].published_at >= results[1].published_at

    def test_get_by_game(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        repo.save(
            {
                "source_url": "https://example.com/g1",
                "title": "Game Event",
                "game_id": "GAME001",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        repo.save(
            {
                "source_url": "https://example.com/g2",
                "title": "Other",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        session.commit()

        results = repo.get_by_game("GAME001")
        assert len(results) == 1

    def test_update_status(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        event = repo.save(
            {
                "source_url": "https://example.com/1",
                "title": "Test",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
            }
        )
        session.commit()

        repo.update_status(event.id, "closed")
        session.commit()
        session.refresh(event)

        assert event.status == "closed"

    def test_get_upcoming(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TeamEventRepository(session)

        future = datetime.now(UTC).replace(tzinfo=None) + timedelta(days=7)
        past = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=7)

        repo.save(
            {
                "source_url": "https://example.com/future",
                "title": "Upcoming",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
                "event_end_at": future,
            }
        )
        repo.save(
            {
                "source_url": "https://example.com/past",
                "title": "Past",
                "team_id": "LG",
                "event_scope": "team",
                "status": "scheduled",
                "event_end_at": past,
            }
        )
        session.commit()

        results = repo.get_upcoming(limit=10)
        assert len(results) == 1
        assert results[0].title == "Upcoming"
