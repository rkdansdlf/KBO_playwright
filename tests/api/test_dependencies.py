"""Unit tests for src.api.dependencies."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.analytics.matchup import MatchupAnalyticsEngine
from src.analytics.sabermetrics import SabermetricsEngine
from src.api.dependencies import (
    get_auto_healer,
    get_hybrid_retriever,
    get_matchup_engine,
    get_pipeline_detector,
    get_pipeline_orchestrator,
    get_quality_hub,
    get_sabermetrics_engine,
)
from src.pipeline.auto_healer_service import AutoHealerService
from src.pipeline.defect_detector import PipelineDefectDetector
from src.pipeline.orchestrator import DailyPipelineOrchestrator
from src.rag.retrievers.hybrid import UnifiedHybridRetriever
from src.services.quality_hub import QualityHub


def test_dependency_providers() -> None:
    mock_session = MagicMock()

    saber = get_sabermetrics_engine(mock_session)
    assert isinstance(saber, SabermetricsEngine)

    matchup = get_matchup_engine(mock_session)
    assert isinstance(matchup, MatchupAnalyticsEngine)

    retriever = get_hybrid_retriever(mock_session)
    assert isinstance(retriever, UnifiedHybridRetriever)

    detector = get_pipeline_detector(mock_session)
    assert isinstance(detector, PipelineDefectDetector)

    healer = get_auto_healer(mock_session)
    assert isinstance(healer, AutoHealerService)

    qhub = get_quality_hub(mock_session)
    assert isinstance(qhub, QualityHub)

    orchestrator = get_pipeline_orchestrator(mock_session)
    assert isinstance(orchestrator, DailyPipelineOrchestrator)
