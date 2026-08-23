"""Tests for advanced pipeline recalculation, RAG indexing, and context aggregator alerts."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.services.context_aggregator import ContextAggregator
from src.services.rag_indexer import RagKnowledgeIndexer


class FakeEmbeddingService:
    """Offline provider returning a fixed-size vector per text."""

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


from src.services.stat_recalculator import StatRecalculator


@pytest.fixture
def session() -> Session:
    """In-memory SQLite session fixture."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sess = Session(engine)
    try:
        yield sess
    finally:
        sess.close()


def test_stat_recalculator(session: Session) -> None:
    """Test milestone and splits recalculation."""
    m = PlayerMilestone(
        season=2026,
        player_id="1001",
        player_name="최형우",
        team_code="KIA",
        milestone_category="2000안타",
        current_val=1980,
        target_val=2000,
        remaining_val=20,
        is_achieved=False,
    )
    session.add(m)
    session.commit()

    recalc = StatRecalculator(session)
    count = recalc.recalc_player_milestones(season=2026)
    assert count == 0  # No game batting stats added yet, remains unchanged
    assert m.remaining_val == 20


def test_rag_knowledge_indexer(session: Session) -> None:
    """Test RAG knowledge indexer for press releases and milestones."""
    pr = KboPressRelease(
        notice_id="N100",
        published_date=date(2026, 4, 10),
        category="공시",
        title="2026 KBO 상벌위원회 징계 결과",
        source_url="https://www.koreabaseball.com/Notice/N100",
    )
    m = PlayerMilestone(
        season=2026,
        player_id="1002",
        player_name="김현수",
        team_code="LG",
        milestone_category="2500안타",
        current_val=2480,
        target_val=2500,
        remaining_val=20,
        is_achieved=False,
    )
    session.add(pr)
    session.add(m)
    session.commit()

    indexer = RagKnowledgeIndexer(session, embedding_service=FakeEmbeddingService())
    pr_count = indexer.index_press_releases()
    ms_count = indexer.index_milestones(season=2026)

    assert pr_count == 1
    assert ms_count == 1


def test_context_aggregator_alerts(session: Session) -> None:
    """Test ContextAggregator milestone alerts and recent notices."""
    pr = KboPressRelease(
        notice_id="N200",
        published_date=date(2026, 5, 1),
        category="보도자료",
        title="2026 KBO 정규시즌 일정 변경 안내",
        source_url="https://www.koreabaseball.com/Notice/N200",
    )
    m = PlayerMilestone(
        season=2026,
        player_id="1003",
        player_name="최정",
        team_code="SSG",
        milestone_category="500홈런",
        current_val=495,
        target_val=500,
        remaining_val=5,
        is_achieved=False,
    )
    session.add(pr)
    session.add(m)
    session.commit()

    agg = ContextAggregator(session)
    alerts = agg.get_milestone_alerts(away_team="SSG", home_team="KIA", season=2026)
    notices = agg.get_recent_notices(limit=5)

    assert len(alerts) == 1
    assert alerts[0]["player_name"] == "최정"
    assert "500홈런" in alerts[0]["alert_message"]

    assert len(notices) == 1
    assert notices[0]["notice_id"] == "N200"
