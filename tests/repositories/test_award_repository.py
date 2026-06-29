from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.award import Award
from src.repositories.award_repository import AwardRepository


class TestAwardRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        Award.__table__.create(engine)

    def test_save_award_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = AwardRepository(session)

        award = repo.save_award(
            {
                "year": 2024,
                "award_type": "MVP",
                "category": "Pitcher",
                "player_name": "Kim",
                "team_name": "LG",
            },
        )
        session.commit()

        assert award.id is not None
        assert award.year == 2024
        assert award.award_type == "MVP"

    def test_save_award_upsert_same_key_returns_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = AwardRepository(session)

        a1 = repo.save_award(
            {
                "year": 2024,
                "award_type": "MVP",
                "category": None,
                "player_name": "Kim",
                "team_name": "LG",
            },
        )
        session.commit()

        a2 = repo.save_award(
            {
                "year": 2024,
                "award_type": "MVP",
                "category": None,
                "player_name": "Kim",
                "team_name": "LG",
            },
        )
        session.commit()

        assert a1.id == a2.id

    def test_get_awards_by_year(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = AwardRepository(session)

        repo.save_award({"year": 2024, "award_type": "MVP", "category": None, "player_name": "Kim", "team_name": "LG"})
        repo.save_award({"year": 2024, "award_type": "GG", "category": "1B", "player_name": "Park", "team_name": "SSG"})
        repo.save_award({"year": 2023, "award_type": "MVP", "category": None, "player_name": "Lee", "team_name": "KIW"})
        session.commit()

        results = repo.get_awards_by_year(2024)
        assert len(results) == 2

    def test_get_awards_by_year_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = AwardRepository(session)

        assert repo.get_awards_by_year(2024) == []

    def test_clear_awards_by_year(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = AwardRepository(session)

        repo.save_award({"year": 2024, "award_type": "MVP", "category": None, "player_name": "Kim", "team_name": "LG"})
        repo.save_award({"year": 2023, "award_type": "MVP", "category": None, "player_name": "Lee", "team_name": "KIW"})
        session.commit()

        repo.clear_awards_by_year(2024)

        assert len(repo.get_awards_by_year(2024)) == 0
        assert len(repo.get_awards_by_year(2023)) == 1
