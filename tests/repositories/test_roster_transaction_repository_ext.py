from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.roster_transaction import RosterTransaction
from src.repositories.roster_transaction_repository import RosterTransactionRepository


class TestRosterTransactionRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        RosterTransaction.__table__.create(engine)

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        trx = repo.save(
            {
                "dedupe_key": "20241015-LG-Kim-registered",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        session.commit()
        assert trx.id is not None
        assert trx.player_name == "Kim"

    def test_save_upsert_same_dedupe_key(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        r1 = repo.save(
            {
                "dedupe_key": "20241015-LG-Kim-registered",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        session.commit()

        r2 = repo.save(
            {
                "dedupe_key": "20241015-LG-Kim-registered",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "second_team",
                "source_type": "snapshot_diff",
            },
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.roster_level == "second_team"

    def test_get_by_team_date(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        repo.save(
            {
                "dedupe_key": "k1",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        repo.save(
            {
                "dedupe_key": "k2",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Park",
                "action": "deregistered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        repo.save(
            {
                "dedupe_key": "k3",
                "transaction_date": date(2024, 10, 15),
                "team_id": "SSG",
                "player_name": "Choi",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        session.commit()

        results = repo.get_by_team_date("LG", date(2024, 10, 15))
        assert len(results) == 2

    def test_get_by_date(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        repo.save(
            {
                "dedupe_key": "k1",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        repo.save(
            {
                "dedupe_key": "k2",
                "transaction_date": date(2024, 10, 16),
                "team_id": "LG",
                "player_name": "Park",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        session.commit()

        results = repo.get_by_date(date(2024, 10, 15))
        assert len(results) == 1

    def test_get_by_player(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        repo.save(
            {
                "dedupe_key": "k1",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
                "player_id": 1,
            },
        )
        session.commit()
        repo.save(
            {
                "dedupe_key": "k2",
                "transaction_date": date(2024, 10, 16),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "deregistered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
                "player_id": 1,
            },
        )
        session.commit()
        repo.save(
            {
                "dedupe_key": "k3",
                "transaction_date": date(2024, 10, 15),
                "team_id": "SSG",
                "player_name": "Choi",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
                "player_id": 2,
            },
        )
        session.commit()

        results = repo.get_by_player(1)
        assert len(results) == 2

    def test_exists(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        repo.save(
            {
                "dedupe_key": "k1",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
            },
        )
        session.commit()

        assert repo.exists("k1") is True
        assert repo.exists("nonexistent") is False

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        count = repo.bulk_save(
            [
                {
                    "dedupe_key": "k1",
                    "transaction_date": date(2024, 10, 15),
                    "team_id": "LG",
                    "player_name": "Kim",
                    "action": "registered",
                    "roster_level": "first_team",
                    "source_type": "kbo_today_page",
                },
                {
                    "dedupe_key": "k2",
                    "transaction_date": date(2024, 10, 15),
                    "team_id": "LG",
                    "player_name": "Park",
                    "action": "deregistered",
                    "roster_level": "first_team",
                    "source_type": "kbo_today_page",
                },
            ],
        )
        session.commit()
        assert count == 2

    def test_save_preserves_player_id_when_player_basic_table_missing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RosterTransactionRepository(session)

        trx = repo.save(
            {
                "dedupe_key": "k1",
                "transaction_date": date(2024, 10, 15),
                "team_id": "LG",
                "player_name": "Kim",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
                "player_id": 999,
            },
        )
        session.commit()

        assert trx.player_id == 999  # player_basic table missing -> assume valid
