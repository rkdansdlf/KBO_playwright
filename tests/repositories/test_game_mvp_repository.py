from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game_mvp import GameMvp
from src.repositories.game_mvp_repository import GameMvpRepository


class TestGameMvpRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        GameMvp.__table__.create(engine)

    def test_save_mvp_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = GameMvpRepository(session)

        mvp = repo.save_mvp(
            {
                "game_id": "20241015_LGvSSG",
                "player_name": "Kim Hyun-soo",
                "team_id": "LG",
            }
        )
        session.commit()

        assert mvp.id is not None
        assert mvp.game_id == "20241015_LGvSSG"
        assert mvp.mvp_type == "GAME"

    def test_save_mvp_upsert_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = GameMvpRepository(session)

        m1 = repo.save_mvp(
            {
                "game_id": "G1",
                "mvp_type": "GAME",
                "player_name": "Kim",
                "team_id": None,
            }
        )
        session.commit()

        m2 = repo.save_mvp(
            {
                "game_id": "G1",
                "mvp_type": "GAME",
                "player_name": "Kim",
                "team_id": "LG",
            }
        )
        session.commit()

        assert m1.id == m2.id
        assert m2.team_id == "LG"

    def test_get_by_game(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = GameMvpRepository(session)

        repo.save_mvp({"game_id": "G1", "player_name": "Kim", "team_id": "LG"})
        repo.save_mvp({"game_id": "G1", "player_name": "Park", "team_id": "SSG", "mvp_type": "WEEKLY"})
        repo.save_mvp({"game_id": "G2", "player_name": "Lee", "team_id": "KIW"})
        session.commit()

        results = repo.get_by_game("G1")
        assert len(results) == 2

    def test_get_by_game_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = GameMvpRepository(session)

        assert repo.get_by_game("NONEXISTENT") == []

    def test_delete_by_game(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = GameMvpRepository(session)

        repo.save_mvp({"game_id": "G1", "player_name": "Kim"})
        repo.save_mvp({"game_id": "G2", "player_name": "Park"})
        session.commit()

        repo.delete_by_game("G1")

        assert len(repo.get_by_game("G1")) == 0
        assert len(repo.get_by_game("G2")) == 1
