"""FastAPI dependency injection container providing database sessions and domain service engines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends

from src.analytics.matchup import MatchupAnalyticsEngine
from src.analytics.sabermetrics import SabermetricsEngine
from src.db.engine import get_db_session
from src.pipeline.auto_healer_service import AutoHealerService
from src.pipeline.defect_detector import PipelineDefectDetector
from src.pipeline.orchestrator import DailyPipelineOrchestrator
from src.rag.retrievers.hybrid import UnifiedHybridRetriever
from src.services.quality_hub import QualityHub

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.orm import Session


def get_db() -> Generator[Session, None, None]:
    """Provide a request-scoped database session."""
    with get_db_session() as session:
        yield session


SessionDep = Annotated["Session", Depends(get_db)]


def get_sabermetrics_engine(session: SessionDep) -> SabermetricsEngine:
    """Provide a SabermetricsEngine instance bound to the request session."""
    return SabermetricsEngine(session)


def get_matchup_engine(session: SessionDep) -> MatchupAnalyticsEngine:
    """Provide a MatchupAnalyticsEngine instance bound to the request session."""
    return MatchupAnalyticsEngine(session)


def get_hybrid_retriever(session: SessionDep) -> UnifiedHybridRetriever:
    """Provide a UnifiedHybridRetriever instance bound to the request session."""
    return UnifiedHybridRetriever(session=session)


def get_pipeline_detector(session: SessionDep) -> PipelineDefectDetector:
    """Provide a PipelineDefectDetector instance bound to the request session."""
    return PipelineDefectDetector(session)


def get_auto_healer(session: SessionDep) -> AutoHealerService:
    """Provide an AutoHealerService instance bound to the request session."""
    return AutoHealerService(session)


def get_quality_hub(session: SessionDep) -> QualityHub:
    """Provide a QualityHub instance bound to the request session."""
    return QualityHub(session)


def get_pipeline_orchestrator(session: SessionDep) -> DailyPipelineOrchestrator:
    """Provide a DailyPipelineOrchestrator instance bound to the request session."""
    return DailyPipelineOrchestrator(session)
