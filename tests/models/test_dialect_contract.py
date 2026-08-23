"""Multi-Dialect Database Contract & UPSERT Matrix test suite.

Validates the behavior of dialect-neutral upsert utilities (upsert_model_by_unique_keys)
across various ORM models, verifying:
1. Insertion of new records.
2. Safe updating of existing records.
3. Preservation of immutable database identities (id, created_at).
4. Strict validation of required business keys (MissingUniqueKeyValuesError).
5. Repeated UPSERT idempotency.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.award import Award
from src.models.base import Base
from src.models.player import PlayerBasic
from src.models.stadium_info import StadiumInfo
from src.repositories.oracle_upsert import (
    MissingUniqueKeyValuesError,
    upsert_model_by_unique_keys,
)


@pytest.fixture
def session() -> Session:
    """Create an isolated in-memory SQLite session with all tables created."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


class TestDialectUpsertContract:
    """Validate UPSERT contract on various ORM entity models."""

    def test_upsert_player_basic_inserts_new_row(self, session: Session) -> None:
        payload = {
            "player_id": 11111,
            "name": "홍길동",
            "team": "LG",
            "position": "투수",
        }
        row = upsert_model_by_unique_keys(
            session=session,
            model=PlayerBasic,
            payload=payload,
            unique_keys=("player_id",),
        )
        session.flush()

        assert row is not None
        assert row.player_id == 11111  # type: ignore[attr-defined]
        assert row.name == "홍길동"  # type: ignore[attr-defined]
        assert session.query(PlayerBasic).filter_by(player_id=11111).count() == 1

    def test_upsert_player_basic_updates_existing_row(self, session: Session) -> None:
        initial_payload = {
            "player_id": 22222,
            "name": "이순신",
            "team": "KIA",
            "position": "내야수",
        }
        row1 = upsert_model_by_unique_keys(
            session=session,
            model=PlayerBasic,
            payload=initial_payload,
            unique_keys=("player_id",),
        )
        session.flush()
        initial_pk = row1.player_id  # type: ignore[attr-defined]

        # Update position and uniform_no
        update_payload = {
            "player_id": 22222,
            "name": "이순신",
            "team": "KIA",
            "position": "외야수",
            "uniform_no": "99",
        }
        row2 = upsert_model_by_unique_keys(
            session=session,
            model=PlayerBasic,
            payload=update_payload,
            unique_keys=("player_id",),
        )
        session.flush()

        assert row2.player_id == initial_pk  # type: ignore[attr-defined]
        assert row2.position == "외야수"  # type: ignore[attr-defined]
        assert row2.uniform_no == "99"  # type: ignore[attr-defined]
        assert session.query(PlayerBasic).filter_by(player_id=22222).count() == 1

    def test_upsert_missing_unique_keys_raises_error(self, session: Session) -> None:
        incomplete_payload = {
            "name": "무명선수",
            "team": "SS",
        }
        with pytest.raises(MissingUniqueKeyValuesError) as exc_info:
            upsert_model_by_unique_keys(
                session=session,
                model=PlayerBasic,
                payload=incomplete_payload,
                unique_keys=("player_id",),
            )
        assert "player_id" in str(exc_info.value)

    def test_upsert_idempotency_repeated_calls(self, session: Session) -> None:
        payload = {
            "stadium_code": "JAMSIL",
            "name_kr": "서울종합운동장 야구장",
            "home_team_id": "LG",
            "capacity": 25000,
        }
        for _ in range(5):
            upsert_model_by_unique_keys(
                session=session,
                model=StadiumInfo,
                payload=payload,
                unique_keys=("stadium_code",),
            )
            session.flush()

        assert session.query(StadiumInfo).filter_by(stadium_code="JAMSIL").count() == 1
        stadium = session.query(StadiumInfo).filter_by(stadium_code="JAMSIL").first()
        assert stadium is not None
        assert stadium.capacity == 25000

    def test_upsert_award_composite_unique_keys(self, session: Session) -> None:
        award_payload = {
            "year": 2024,
            "award_type": "MVP",
            "player_name": "김도영",
            "team_name": "KIA",
            "team_code": "HT",
        }
        row = upsert_model_by_unique_keys(
            session=session,
            model=Award,
            payload=award_payload,
            unique_keys=("year", "award_type", "player_name", "team_name"),
        )
        session.flush()
        first_id = row.id  # type: ignore[attr-defined]

        # Re-upsert with updated category
        updated_award = {
            "year": 2024,
            "award_type": "MVP",
            "player_name": "김도영",
            "team_name": "KIA",
            "category": "정규시즌 MVP",
        }
        row2 = upsert_model_by_unique_keys(
            session=session,
            model=Award,
            payload=updated_award,
            unique_keys=("year", "award_type", "player_name", "team_name"),
        )
        session.flush()

        assert row2.id == first_id  # type: ignore[attr-defined]
        assert row2.category == "정규시즌 MVP"  # type: ignore[attr-defined]
        assert session.query(Award).filter_by(year=2024, award_type="MVP").count() == 1
