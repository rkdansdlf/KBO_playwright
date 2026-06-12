from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.injury import InjuryEntry
from src.repositories.injury_repository import InjuryRepository


class TestInjuryRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        InjuryEntry.__table__.create(engine)

    def test_save_injury_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        injury = repo.save_injury(
            {
                "player_id": 1,
                "player_name": "Kim",
                "team_id": "LG",
                "injury_type": "elbow strain",
                "il_placement_date": date(2025, 4, 1),
                "status": "15_IL",
            }
        )
        session.commit()

        assert injury.id is not None
        assert injury.player_name == "Kim"
        assert injury.status == "15_IL"

    def test_save_injury_upsert_by_player_id_and_placement_date(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        i1 = repo.save_injury(
            {
                "player_id": 1,
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "15_IL",
                "note": "initial",
            }
        )
        session.commit()

        i2 = repo.save_injury(
            {
                "player_id": 1,
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "60_IL",
                "note": "updated",
            }
        )
        session.commit()

        assert i1.id == i2.id
        assert i2.status == "60_IL"

    def test_save_injury_without_player_id_creates_new(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        i1 = repo.save_injury(
            {
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "ACTIVE",
            }
        )
        session.commit()

        i2 = repo.save_injury(
            {
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "ACTIVE",
            }
        )
        session.commit()

        assert i1.id != i2.id  # no player_id → different records

    def test_get_active_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        repo.save_injury(
            {
                "player_id": 1,
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "ACTIVE",
            }
        )
        repo.save_injury(
            {
                "player_id": 2,
                "player_name": "Park",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 5),
                "status": "15_IL",
            }
        )
        repo.save_injury(
            {
                "player_id": 3,
                "player_name": "Lee",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 10),
                "status": "RETURNED",
            }
        )
        session.commit()

        results = repo.get_active_by_team("LG")
        assert len(results) == 2
        assert all(r.status in ("ACTIVE", "15_IL", "60_IL") for r in results)

    def test_get_active_by_team_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        assert repo.get_active_by_team("NONE") == []

    def test_get_all_active(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        repo.save_injury(
            {
                "player_id": 1,
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "ACTIVE",
            }
        )
        repo.save_injury(
            {
                "player_id": 2,
                "player_name": "Park",
                "team_id": "SSG",
                "il_placement_date": date(2025, 4, 5),
                "status": "RETURNED",
            }
        )
        session.commit()

        results = repo.get_all_active()
        assert len(results) == 1
        assert results[0].status == "ACTIVE"

    def test_get_all_active_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        assert repo.get_all_active() == []

    def test_mark_returned(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = InjuryRepository(session)

        injury = repo.save_injury(
            {
                "player_id": 1,
                "player_name": "Kim",
                "team_id": "LG",
                "il_placement_date": date(2025, 4, 1),
                "status": "15_IL",
            }
        )
        session.commit()

        repo.mark_returned(injury.id, return_date=date(2025, 5, 1))
        session.commit()

        session.refresh(injury)
        assert injury.status == "RETURNED"
        assert injury.actual_return_date == date(2025, 5, 1)
