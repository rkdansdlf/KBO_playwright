from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.foreign_player import ForeignPlayerChange
from src.repositories.foreign_player_repository import ForeignPlayerRepository


class TestForeignPlayerRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        ForeignPlayerChange.__table__.create(engine)

    def test_save_change_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        c = repo.save_change({
            "player_name": "Smith",
            "team_id": "LG",
            "season": 2025,
            "change_type": "SIGNED",
            "note": "New signing",
        })
        session.commit()

        assert c.id is not None
        assert c.player_name == "Smith"
        assert c.change_type == "SIGNED"

    def test_save_change_upsert_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        c1 = repo.save_change({
            "player_name": "Smith", "team_id": "LG", "season": 2025, "change_type": "SIGNED", "note": "v1",
        })
        session.commit()

        c2 = repo.save_change({
            "player_name": "Smith", "team_id": "LG", "season": 2025, "change_type": "SIGNED", "note": "v2",
        })
        session.commit()

        assert c1.id == c2.id
        assert c2.note == "v2"

    def test_get_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        repo.save_change({"player_name": "A", "team_id": "LG", "season": 2025, "change_type": "SIGNED",
                           "announcement_date": date(2025, 1, 15)})
        repo.save_change({"player_name": "B", "team_id": "LG", "season": 2025, "change_type": "RELEASED",
                           "announcement_date": date(2025, 6, 1)})
        repo.save_change({"player_name": "C", "team_id": "SSG", "season": 2025, "change_type": "SIGNED"})
        session.commit()

        results = repo.get_by_team("LG")
        assert len(results) == 2

    def test_get_by_team_with_season_filter(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        repo.save_change({"player_name": "A", "team_id": "LG", "season": 2024, "change_type": "SIGNED"})
        repo.save_change({"player_name": "B", "team_id": "LG", "season": 2025, "change_type": "SIGNED"})
        session.commit()

        results = repo.get_by_team("LG", season=2025)
        assert len(results) == 1
        assert results[0].season == 2025

    def test_get_by_team_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        assert repo.get_by_team("NONE") == []

    def test_get_by_season(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        repo.save_change({"player_name": "A", "team_id": "LG", "season": 2025, "change_type": "SIGNED",
                           "announcement_date": date(2025, 1, 10)})
        repo.save_change({"player_name": "B", "team_id": "SSG", "season": 2025, "change_type": "SIGNED",
                           "announcement_date": date(2025, 2, 20)})
        repo.save_change({"player_name": "C", "team_id": "KT", "season": 2024, "change_type": "SIGNED"})
        session.commit()

        results = repo.get_by_season(2025)
        assert len(results) == 2

    def test_get_by_season_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ForeignPlayerRepository(session)

        assert repo.get_by_season(2025) == []
