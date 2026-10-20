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

        sec = repo.save(
            {
                "stadium_id": "JAMSIL",
                "section_code": "101B",
                "section_name": "1루 블루석",
                "seat_grade": "블루석",
            },
        )
        session.commit()
        assert sec.id is not None
        assert sec.section_name == "1루 블루석"

    def test_save_without_code_uses_name(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        sec = repo.save(
            {
                "stadium_id": "JAMSIL",
                "section_name": "1루 블루석",
                "seat_grade": "블루석",
            },
        )
        session.commit()
        assert sec.id is not None

    def test_save_upsert_by_code(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumSeatSectionRepository(session)

        r1 = repo.save(
            {
                "stadium_id": "JAMSIL",
                "section_code": "101B",
                "section_name": "1루 블루석",
                "seat_grade": "블루석",
            },
        )
        session.commit()

        r2 = repo.save(
            {
                "stadium_id": "JAMSIL",
                "section_code": "101B",
                "section_name": "1루 블루석",
                "seat_grade": "프리미엄",
                "is_home_cheering": True,
            },
        )
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

        repo.save({"stadium_id": "JAMSIL", "section_code": "H1", "section_name": "홈응원", "is_home_cheering": True})
        repo.save(
            {"stadium_id": "JAMSIL", "section_code": "A1", "section_name": "어웨이응원", "is_away_cheering": True},
        )
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

        count = repo.bulk_save(
            [
                {"stadium_id": "JAMSIL", "section_code": "A1", "section_name": "1루"},
                {"stadium_id": "JAMSIL", "section_code": "B2", "section_name": "3루"},
            ],
        )
        session.commit()
        assert count == 2


class TestSaveNeverRollsBackTheCallersTransaction:
    """The repository resolves key conflicts in the statement, never with its own rollback."""

    @staticmethod
    def _repo():
        engine = create_engine("sqlite:///:memory:")
        StadiumSeatSection.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        return session, StadiumSeatSectionRepository(session)

    def test_same_name_under_a_new_code_updates_the_one_row(self):
        session, repo = self._repo()
        first = repo.save({"stadium_id": "JAMSIL", "section_code": "A", "section_name": "1루 블루석"})
        session.commit()

        # The code lookup misses (new code), so the insert hits the (stadium_id, section_name) key.
        second = repo.save(
            {"stadium_id": "JAMSIL", "section_code": "B", "section_name": "1루 블루석", "seat_grade": "프리미엄"},
        )
        session.commit()

        assert second.id == first.id
        assert session.query(StadiumSeatSection).count() == 1
        assert second.section_code == "B"
        assert second.seat_grade == "프리미엄"

    def test_a_conflict_leaves_earlier_pending_work_in_the_callers_transaction(self):
        session, repo = self._repo()
        repo.save({"stadium_id": "JAMSIL", "section_code": "A", "section_name": "1루 블루석"})
        session.commit()
        repo.save({"stadium_id": "MUNH", "section_code": "C", "section_name": "중앙"})  # pending, not committed

        repo.save({"stadium_id": "JAMSIL", "section_code": "B", "section_name": "1루 블루석"})  # conflicts

        pending = session.query(StadiumSeatSection).filter_by(stadium_id="MUNH").one_or_none()
        assert pending is not None, "a conflict must not discard what the caller had not yet committed"

    def test_the_callers_rollback_discards_the_inserted_row(self):
        session, repo = self._repo()
        repo.save({"stadium_id": "JAMSIL", "section_code": "A", "section_name": "1루 블루석"})
        session.rollback()

        assert session.query(StadiumSeatSection).count() == 0

    def test_a_payload_with_only_the_key_fields_is_stored_once(self):
        session, repo = self._repo()
        first = repo.save({"stadium_id": "JAMSIL", "section_name": "1루 블루석"})
        second = repo.save({"stadium_id": "JAMSIL", "section_name": "1루 블루석"})
        session.commit()

        assert second.id == first.id
        assert session.query(StadiumSeatSection).count() == 1
