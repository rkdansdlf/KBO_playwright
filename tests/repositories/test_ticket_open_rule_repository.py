from __future__ import annotations

from datetime import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.ticket_open_rule import TicketOpenRule
from src.repositories.ticket_open_rule_repository import TicketOpenRuleRepository


class TestTicketOpenRuleRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        TicketOpenRule.__table__.create(engine)

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketOpenRuleRepository(session)

        rule = repo.save({
            "team_id": "LG",
            "platform": "Ticketlink",
            "open_offset_days": 7,
            "open_time": time(11, 0),
            "max_tickets_per_user": 4,
        })
        session.commit()

        assert rule.id is not None
        assert rule.team_id == "LG"
        assert rule.max_tickets_per_user == 4

    def test_save_upsert_same_key_updates(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketOpenRuleRepository(session)

        r1 = repo.save({
            "team_id": "LG", "platform": "Ticketlink",
            "open_offset_days": 7, "open_time": time(11, 0),
            "max_tickets_per_user": 4,
        })
        session.commit()

        r2 = repo.save({
            "team_id": "LG", "platform": "Ticketlink",
            "open_offset_days": 7, "open_time": time(11, 0),
            "max_tickets_per_user": 6, "note": "updated",
        })
        session.commit()

        assert r1.id == r2.id
        assert r2.max_tickets_per_user == 6

    def test_get_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketOpenRuleRepository(session)

        repo.save({"team_id": "LG", "platform": "A", "open_offset_days": 7, "open_time": time(11, 0)})
        repo.save({"team_id": "LG", "platform": "B", "open_offset_days": 3, "open_time": time(10, 0)})
        repo.save({"team_id": "SSG", "platform": "A", "open_offset_days": 7, "open_time": time(11, 0)})
        session.commit()

        results = repo.get_by_team("LG")
        assert len(results) == 2

    def test_get_by_team_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketOpenRuleRepository(session)

        assert repo.get_by_team("NONE") == []

    def test_get_all_active(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketOpenRuleRepository(session)

        repo.save({"team_id": "LG", "platform": "A", "open_offset_days": 7, "open_time": time(11, 0)})
        repo.save({"team_id": "SSG", "platform": "B", "open_offset_days": 3, "open_time": time(10, 0)})
        session.commit()

        results = repo.get_all_active()
        assert len(results) == 2

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketOpenRuleRepository(session)

        count = repo.bulk_save([
            {"team_id": "LG", "platform": "A", "open_offset_days": 7, "open_time": time(11, 0)},
            {"team_id": "SSG", "platform": "B", "open_offset_days": 3, "open_time": time(10, 0)},
        ])
        session.commit()

        assert count == 2
        assert len(repo.get_all_active()) == 2
