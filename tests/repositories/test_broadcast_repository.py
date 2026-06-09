from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.broadcast import GameBroadcast
from src.repositories.broadcast_repository import BroadcastRepository


class TestBroadcastRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        GameBroadcast.__table__.create(engine)

    def test_save_broadcast_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = BroadcastRepository(session)

        bc = repo.save_broadcast({"game_id": "20241015_LGvSSG", "broadcaster": "MBC", "channel_name": "MBC Sports+"})
        session.commit()

        assert bc.id is not None
        assert bc.game_id == "20241015_LGvSSG"
        assert bc.broadcaster == "MBC"

    def test_save_broadcast_upsert_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = BroadcastRepository(session)

        b1 = repo.save_broadcast({"game_id": "G1", "broadcaster": "SBS", "channel_name": None})
        session.commit()

        b2 = repo.save_broadcast({"game_id": "G1", "broadcaster": "SBS", "channel_name": "SBS Sports"})
        session.commit()

        assert b1.id == b2.id
        assert b2.channel_name == "SBS Sports"

    def test_get_by_game(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = BroadcastRepository(session)

        repo.save_broadcast({"game_id": "G1", "broadcaster": "MBC"})
        repo.save_broadcast({"game_id": "G1", "broadcaster": "SBS"})
        repo.save_broadcast({"game_id": "G2", "broadcaster": "KBS"})
        session.commit()

        results = repo.get_by_game("G1")
        assert len(results) == 2

    def test_get_by_game_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = BroadcastRepository(session)

        assert repo.get_by_game("NONEXISTENT") == []

    def test_delete_by_game(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = BroadcastRepository(session)

        repo.save_broadcast({"game_id": "G1", "broadcaster": "MBC"})
        repo.save_broadcast({"game_id": "G2", "broadcaster": "KBS"})
        session.commit()

        repo.delete_by_game("G1")

        assert len(repo.get_by_game("G1")) == 0
        assert len(repo.get_by_game("G2")) == 1
