"""Tests for Player Draft History and Situational Splits Crawlers & Repositories."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.repositories.player_draft_repository import PlayerDraftRepository
from src.repositories.player_splits_repository import PlayerSplitsRepository

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    yield session
    session.close()


def test_player_draft_repository(db_session: Session) -> None:
    """Test PlayerDraftRepository save and query operations."""
    repo = PlayerDraftRepository(db_session)
    item = {
        "season": 2026,
        "draft_type": "2차",
        "round_num": 1,
        "pick_seq": 1,
        "team_code": "키움",
        "player_name": "정현우",
        "player_id": "draft_2026_1",
        "position": "투수",
        "school": "덕수고",
        "sign_fee": "5억원",
    }
    record = repo.save_draft_entry(item)
    assert record.id is not None
    assert record.player_name == "정현우"

    records = repo.get_draft_by_season(2026)
    assert len(records) == 1
    assert records[0].pick_seq == 1


def test_player_splits_repository(db_session: Session) -> None:
    """Test PlayerSplitsRepository save and query operations."""
    repo = PlayerSplitsRepository(db_session)
    item = {
        "season": 2026,
        "player_id": "78224",
        "player_name": "김도영",
        "team_code": "KIA",
        "split_type": "scoring_position",
        "split_key": "득점권시",
        "ab": 120,
        "hits": 45,
        "hr": 10,
        "rbi": 40,
        "avg": 0.375,
        "ops": 1.050,
    }
    record = repo.save_splits_entry(item)
    assert record.id is not None
    assert record.avg == 0.375

    records = repo.get_splits_by_player("78224", season=2026)
    assert len(records) == 1
    assert records[0].split_key == "득점권시"
