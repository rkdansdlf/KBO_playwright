from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.manager_change import ManagerChange
from src.repositories.manager_change_repository import ManagerChangeRepository


class TestManagerChangeRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        ManagerChange.__table__.create(engine)

    def test_save_change_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ManagerChangeRepository(session)

        c = repo.save_change(
            {
                "team_id": "LG",
                "season": 2025,
                "previous_manager": "Kim",
                "new_manager": "Park",
                "change_date": date(2025, 5, 1),
                "change_reason": "FIRED",
            },
        )
        session.commit()

        assert c.id is not None
        assert c.new_manager == "Park"
        assert c.team_id == "LG"

    def test_save_change_upsert_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ManagerChangeRepository(session)

        c1 = repo.save_change(
            {
                "team_id": "LG",
                "season": 2025,
                "new_manager": "Park",
                "note": "v1",
            },
        )
        session.commit()

        c2 = repo.save_change(
            {
                "team_id": "LG",
                "season": 2025,
                "new_manager": "Park",
                "note": "v2",
            },
        )
        session.commit()

        assert c1.id == c2.id
        assert c2.note == "v2"

    def test_get_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ManagerChangeRepository(session)

        repo.save_change({"team_id": "LG", "season": 2024, "new_manager": "A", "change_date": date(2024, 6, 1)})
        repo.save_change({"team_id": "LG", "season": 2025, "new_manager": "B", "change_date": date(2025, 5, 1)})
        repo.save_change({"team_id": "SSG", "season": 2025, "new_manager": "C"})
        session.commit()

        results = repo.get_by_team("LG")
        assert len(results) == 2

    def test_get_by_team_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ManagerChangeRepository(session)

        assert repo.get_by_team("NONE") == []

    def test_get_by_season(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ManagerChangeRepository(session)

        repo.save_change({"team_id": "LG", "season": 2025, "new_manager": "A"})
        repo.save_change({"team_id": "SSG", "season": 2025, "new_manager": "B"})
        repo.save_change({"team_id": "KT", "season": 2024, "new_manager": "C"})
        session.commit()

        results = repo.get_by_season(2025)
        assert len(results) == 2

    def test_get_by_season_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ManagerChangeRepository(session)

        assert repo.get_by_season(2025) == []
