"""Unit tests for src.pipeline.orchestrator."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.pipeline.dto import DefectReport, PipelineRunSummary
from src.pipeline.orchestrator import DailyPipelineOrchestrator
from src.services.quality_hub import UnifiedQualityReport


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


def test_orchestrator_run_pipeline(db_session) -> None:
    orchestrator = DailyPipelineOrchestrator(db_session)

    # Mock components for deterministic pipeline testing
    mock_detector = MagicMock()
    mock_detector.detect_all.return_value = DefectReport(
        target_date="2025-06-15",
        defects=[],
    )
    orchestrator.detector = mock_detector

    mock_quality_hub = MagicMock()
    mock_quality_hub.run_full_audit.return_value = UnifiedQualityReport(
        timestamp="2025-06-15T00:00:00",
        overall_status="PASS",
        quality_score=98,
    )
    orchestrator.quality_hub = mock_quality_hub

    mock_standings = MagicMock()
    mock_standings.calculate_year.return_value = None
    orchestrator.standings_calc = mock_standings

    mock_indexer = MagicMock()
    mock_indexer.index_all.return_value = {"press_releases": 2}
    orchestrator.knowledge_indexer = mock_indexer

    summary = orchestrator.run_pipeline(
        target_date=date(2025, 6, 15),
        auto_heal=True,
        skip_rag=False,
    )

    assert isinstance(summary, PipelineRunSummary)
    assert summary.target_date == "2025-06-15"
    assert summary.overall_status == "SUCCESS"
    assert len(summary.stages) == 4
    assert summary.stages[0].stage_name == "Stage 1: Finalize & Status Sync"
    assert summary.stages[1].stage_name == "Stage 2: Standings & Aggregation"
    assert summary.stages[2].stage_name == "Stage 3: Validate & Self-Heal"
    assert summary.stages[3].stage_name == "Stage 4: Advanced Sync & RAG Indexing"
