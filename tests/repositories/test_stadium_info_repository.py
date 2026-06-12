from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_info import StadiumInfo, StadiumRegulation
from src.repositories.stadium_info_repository import StadiumInfoRepository


class TestStadiumInfoRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        StadiumInfo.__table__.create(engine)
        StadiumRegulation.__table__.create(engine)

    # --- StadiumInfo ---

    def test_save_stadium_info_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        info = repo.save_stadium_info(
            {
                "stadium_code": "JAMSIL",
                "name_kr": "잠실종합운동장",
                "name_en": "Jamsil Baseball Stadium",
                "home_team_id": "LG",
                "capacity": 25000,
            }
        )
        session.commit()

        assert info.stadium_code == "JAMSIL"
        assert info.name_kr == "잠실종합운동장"

    def test_save_stadium_info_upsert_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        i1 = repo.save_stadium_info(
            {
                "stadium_code": "JAMSIL",
                "name_kr": "Jamsil",
                "home_team_id": "LG",
                "capacity": 20000,
            }
        )
        session.commit()

        i2 = repo.save_stadium_info(
            {
                "stadium_code": "JAMSIL",
                "name_kr": "Jamsil Updated",
                "home_team_id": "LG",
                "capacity": 25000,
            }
        )
        session.commit()

        assert i1.stadium_code == i2.stadium_code
        assert i2.capacity == 25000
        assert i2.name_kr == "Jamsil Updated"

    def test_get_all(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        repo.save_stadium_info({"stadium_code": "JAMSIL", "name_kr": "Jamsil", "home_team_id": "LG"})
        repo.save_stadium_info({"stadium_code": "MUNHAK", "name_kr": "Munhak", "home_team_id": "SSG"})
        session.commit()

        results = repo.get_all()
        assert len(results) == 2

    def test_get_all_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        assert repo.get_all() == []

    def test_get_by_code_found(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        repo.save_stadium_info({"stadium_code": "JAMSIL", "name_kr": "Jamsil", "home_team_id": "LG"})
        repo.save_stadium_info({"stadium_code": "MUNHAK", "name_kr": "Munhak", "home_team_id": "SSG"})
        session.commit()

        result = repo.get_by_code("JAMSIL")
        assert result is not None
        assert result.stadium_code == "JAMSIL"

    def test_get_by_code_not_found(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        assert repo.get_by_code("NONEXISTENT") is None

    # --- StadiumRegulation ---

    def test_save_regulation_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        reg = repo.save_regulation(
            {
                "stadium_code": "JAMSIL",
                "regulation_type": "GROUND_RULE",
                "title": "Foul Pole",
                "description": "Ball hitting foul pole is a home run.",
            }
        )
        session.commit()

        assert reg.id is not None
        assert reg.stadium_code == "JAMSIL"
        assert reg.title == "Foul Pole"

    def test_save_regulation_no_upsert_always_creates(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        r1 = repo.save_regulation(
            {
                "stadium_code": "JAMSIL",
                "regulation_type": "GROUND_RULE",
                "title": "Foul Pole",
                "description": "v1",
            }
        )
        session.commit()

        r2 = repo.save_regulation(
            {
                "stadium_code": "JAMSIL",
                "regulation_type": "GROUND_RULE",
                "title": "Foul Pole",
                "description": "v2",
            }
        )
        session.commit()

        assert r1.id != r2.id  # always inserts

    def test_get_regulations_by_stadium(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        repo.save_regulation({"stadium_code": "JAMSIL", "regulation_type": "A", "title": "T1", "description": "D1"})
        repo.save_regulation({"stadium_code": "JAMSIL", "regulation_type": "B", "title": "T2", "description": "D2"})
        repo.save_regulation({"stadium_code": "MUNHAK", "regulation_type": "A", "title": "T3", "description": "D3"})
        session.commit()

        results = repo.get_regulations_by_stadium("JAMSIL")
        assert len(results) == 2

    def test_get_regulations_by_stadium_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumInfoRepository(session)

        assert repo.get_regulations_by_stadium("NONE") == []
