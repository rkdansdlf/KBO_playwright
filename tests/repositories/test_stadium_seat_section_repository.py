from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_seat_section import StadiumSeatSection
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository


class TestStadiumSeatSectionRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        StadiumSeatSection.__table__.create(engine)

    def test_save_with_code_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        sec = repo.save({
            "stadium_id": "JAMSIL", "section_code": "101B",
            "section_name": "1루 블루석", "seat_grade": "블루석",
        })
        session.commit()
        assert sec.id is not None
        assert sec.section_name == "1루 블루석"

    def test_save_without_code_uses_name(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        sec = repo.save({
            "stadium_id": "JAMSIL", "section_name": "1루 블루석",
            "seat_grade": "블루석",
        })
        session.commit()
        assert sec.id is not None

    def test_save_upsert_by_code(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        r1 = repo.save({
            "stadium_id": "JAMSIL", "section_code": "101B",
            "section_name": "1루 블루석", "seat_grade": "블루석",
        })
        session.commit()

        r2 = repo.save({
            "stadium_id": "JAMSIL", "section_code": "101B",
            "section_name": "1루 블루석", "seat_grade": "프리미엄",
            "is_home_cheering": True,
        })
        session.commit()

        assert r1.id == r2.id
        assert r2.seat_grade == "프리미엄"
        assert r2.is_home_cheering is True

    def test_get_by_stadium(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        repo.save({"stadium_id": "JAMSIL", "section_code": "A1", "section_name": "1루"})
        repo.save({"stadium_id": "JAMSIL", "section_code": "B2", "section_name": "3루"})
        repo.save({"stadium_id": "MUNH", "section_code": "C1", "section_name": "중앙"})
        session.commit()

        results = repo.get_by_stadium("JAMSIL")
        assert len(results) == 2

    def test_get_cheering_sections(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        repo.save({"stadium_id": "JAMSIL", "section_code": "H1", "section_name": "홈응원",
                         "is_home_cheering": True})
        repo.save({"stadium_id": "JAMSIL", "section_code": "A1", "section_name": "어웨이응원",
                    "is_away_cheering": True})
        repo.save({"stadium_id": "JAMSIL", "section_code": "N1", "section_name": "중립"})
        session.commit()

        results = repo.get_cheering_sections("JAMSIL")
        assert len(results) == 2

    def test_get_cheering_sections_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        repo.save({"stadium_id": "JAMSIL", "section_code": "N1", "section_name": "중립"})
        session.commit()

        assert repo.get_cheering_sections("JAMSIL") == []

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        count = repo.bulk_save([
            {"stadium_id": "JAMSIL", "section_code": "A1", "section_name": "1루"},
            {"stadium_id": "JAMSIL", "section_code": "B2", "section_name": "3루"},
        ])
        session.commit()
        assert count == 2
