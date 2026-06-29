from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic
from src.models.roster_transaction import RosterTransaction
from src.repositories.roster_transaction_repository import RosterTransactionRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    RosterTransaction.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


@pytest.fixture
def sample_data():
    return {
        "transaction_date": date(2025, 5, 15),
        "team_id": "LG",
        "player_id": 12345,
        "player_name": "홍길동",
        "action": "registered",
        "roster_level": "first_team",
        "source_type": "kbo_today_page",
        "confidence": "high",
        "dedupe_key": "2025-05-15_LG_홍길동_registered",
    }


class TestRosterTransactionRepository:
    def test_save_and_query(self, session, sample_data):
        repo = RosterTransactionRepository(session)
        txn = repo.save(sample_data)
        assert txn.player_name == "홍길동"
        assert txn.dedupe_key == "2025-05-15_LG_홍길동_registered"

    def test_save_dedup_by_dedupe_key(self, session, sample_data):
        repo = RosterTransactionRepository(session)
        e1 = repo.save(sample_data)
        e2 = repo.save({**sample_data, "confidence": "medium"})
        assert e1.id == e2.id
        assert e2.confidence == "medium"

    def test_get_by_team_date(self, session):
        repo = RosterTransactionRepository(session)
        repo.save(
            {
                "transaction_date": date(2025, 5, 15),
                "team_id": "LG",
                "player_name": "A",
                "action": "registered",
                "dedupe_key": "k1",
            },
        )
        repo.save(
            {
                "transaction_date": date(2025, 5, 15),
                "team_id": "LG",
                "player_name": "B",
                "action": "deregistered",
                "dedupe_key": "k2",
            },
        )
        repo.save(
            {
                "transaction_date": date(2025, 5, 15),
                "team_id": "SS",
                "player_name": "C",
                "action": "registered",
                "dedupe_key": "k3",
            },
        )
        session.commit()

        lg_txns = repo.get_by_team_date("LG", date(2025, 5, 15))
        assert len(lg_txns) == 2

    def test_get_by_date(self, session):
        repo = RosterTransactionRepository(session)
        repo.save(
            {
                "transaction_date": date(2025, 5, 15),
                "team_id": "LG",
                "player_name": "A",
                "action": "registered",
                "dedupe_key": "k1",
            },
        )
        repo.save(
            {
                "transaction_date": date(2025, 5, 16),
                "team_id": "LG",
                "player_name": "B",
                "action": "registered",
                "dedupe_key": "k2",
            },
        )
        session.commit()

        day1 = repo.get_by_date(date(2025, 5, 15))
        assert len(day1) == 1

    def test_get_by_player(self, session):
        repo = RosterTransactionRepository(session)
        repo.save(
            {
                "transaction_date": date(2025, 5, 15),
                "team_id": "LG",
                "player_id": 100,
                "player_name": "A",
                "action": "registered",
                "dedupe_key": "k1",
            },
        )
        repo.save(
            {
                "transaction_date": date(2025, 6, 1),
                "team_id": "LG",
                "player_id": 100,
                "player_name": "A",
                "action": "deregistered",
                "dedupe_key": "k2",
            },
        )
        session.commit()

        txns = repo.get_by_player(100)
        assert len(txns) == 2

    def test_exists(self, session, sample_data):
        repo = RosterTransactionRepository(session)
        repo.save(sample_data)
        session.commit()
        assert repo.exists("2025-05-15_LG_홍길동_registered") is True
        assert repo.exists("nonexistent_key") is False

    def test_get_recent_by_team(self, session):
        repo = RosterTransactionRepository(session)
        repo.save(
            {
                "transaction_date": date.today(),
                "team_id": "LG",
                "player_name": "A",
                "action": "registered",
                "dedupe_key": "k1",
            },
        )
        repo.save(
            {
                "transaction_date": date.today(),
                "team_id": "LG",
                "player_name": "B",
                "action": "deregistered",
                "dedupe_key": "k2",
            },
        )
        session.commit()

        recent = repo.get_recent_by_team("LG", days=7)
        assert len(recent) == 2

    def test_bulk_save(self, session, sample_data):
        repo = RosterTransactionRepository(session)
        count = repo.bulk_save([sample_data, {**sample_data, "dedupe_key": "k2", "player_name": "김철수"}])
        assert count == 2

    def test_save_nulls_missing_player_basic_fk(self):
        engine = create_engine("sqlite:///:memory:")
        PlayerBasic.__table__.create(engine)
        RosterTransaction.__table__.create(engine)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            repo = RosterTransactionRepository(session)

            txn = repo.save(
                {
                    "transaction_date": date(2026, 5, 30),
                    "team_id": "WO",
                    "player_id": 56305,
                    "player_name": "히우라",
                    "action": "registered",
                    "dedupe_key": "2026-05-30_WO_히우라_registered",
                },
            )

            assert txn.player_id is None
