"""Regression tests for KnowledgeIndexer domain chunk builders."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.futures_schedule import FuturesGameSchedule
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat
from src.rag.indexer.knowledge_indexer import KnowledgeIndexer


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        sess.add(
            PlayerMilestone(
                season=2025,
                player_id="78224",
                player_name="김도영",
                team_code="KIA",
                milestone_category="2000안타",
                current_val=1990,
                target_val=2000,
                remaining_val=10,
                is_achieved=False,
                achieved_date=None,
            )
        )
        sess.add(
            FuturesGameSchedule(
                season=2025,
                game_date=date(2025, 4, 1),
                game_id="20250401FUTURE0",
                away_team="LG",
                home_team="KIA",
                game_status="SCHEDULED",
            )
        )
        sess.add(
            PlayerSplitsStat(
                season=2025,
                player_id="78224",
                player_name="김도영",
                team_code="KIA",
                split_type="scoring_position",
                split_key="득점권",
            )
        )
        sess.commit()
        yield sess
    finally:
        sess.close()


def _indexer_with_fake_repo(db_session) -> tuple[KnowledgeIndexer, MagicMock]:
    indexer = KnowledgeIndexer(db_session)
    fake_repo = MagicMock()
    fake_repo.upsert_chunks.side_effect = len
    indexer.rag_repo = fake_repo
    return indexer, fake_repo


def test_index_milestones_uses_real_columns(db_session) -> None:
    """Regression: milestone indexing must not reference missing model attributes."""
    indexer, fake_repo = _indexer_with_fake_repo(db_session)
    assert indexer.index_milestones() == 1
    chunks = fake_repo.upsert_chunks.call_args[0][0]
    assert "2000안타" in chunks[0]["content"]
    assert chunks[0]["meta"]["milestone_category"] == "2000안타"


def test_index_futures_schedule_without_game_time(db_session) -> None:
    """Regression: futures schedule indexing must not reference missing game_time."""
    indexer, fake_repo = _indexer_with_fake_repo(db_session)
    assert indexer.index_futures_schedule() == 1
    chunks = fake_repo.upsert_chunks.call_args[0][0]
    assert "퓨처스리그" in chunks[0]["content"]


def test_index_player_splits_uses_split_key(db_session) -> None:
    """Regression: player splits indexing must use split_key, not split_value."""
    indexer, fake_repo = _indexer_with_fake_repo(db_session)
    assert indexer.index_player_splits() == 1
    chunks = fake_repo.upsert_chunks.call_args[0][0]
    assert "득점권" in chunks[0]["content"]
    assert chunks[0]["meta"]["split_key"] == "득점권"
