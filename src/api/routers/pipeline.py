"""FastAPI Router for KBO Data Pipeline Management, Self-Healing, and Quality Hub Audits."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.api.auth import get_api_key
from src.api.dependencies import (
    get_auto_healer,
    get_pipeline_detector,
    get_pipeline_orchestrator,
    get_quality_hub,
)
from src.api.schemas import (
    PipelineDefectReportResponse,
    PipelineHealingActionResponse,
    PipelineRunResponse,
    QualityHubSummaryResponse,
)

if TYPE_CHECKING:
    from src.pipeline.auto_healer_service import AutoHealerService
    from src.pipeline.defect_detector import PipelineDefectDetector
    from src.pipeline.orchestrator import DailyPipelineOrchestrator
    from src.services.quality_hub import QualityHub

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["KBO Pipeline & Self-Healing"])

DetectorDep = Annotated["PipelineDefectDetector", Depends(get_pipeline_detector)]
HealerDep = Annotated["AutoHealerService", Depends(get_auto_healer)]
QualityHubDep = Annotated["QualityHub", Depends(get_quality_hub)]
OrchestratorDep = Annotated["DailyPipelineOrchestrator", Depends(get_pipeline_orchestrator)]


class PipelineHealRequest(BaseModel):
    """Request schema for POST /api/pipeline/heal."""

    game_id: str | None = Field(default=None, example="20250615LGSS0", description="특정 경기 ID")
    target_date: str | None = Field(default=None, example="2025-06-15", description="특정 대상 일자 (YYYY-MM-DD)")


class PipelineRunRequest(BaseModel):
    """Request schema for POST /api/pipeline/run."""

    target_date: str | None = Field(default=None, example="2025-06-15", description="실행 대상 일자")
    auto_heal: bool = Field(default=True, description="결함 자동 치유 여부")
    skip_rag: bool = Field(default=False, description="RAG 증분 색인 스킵 여부")


@router.get(
    "/defects",
    response_model=PipelineDefectReportResponse,
    dependencies=[Depends(get_api_key)],
    summary="KBO 파이프라인 결함 및 이상치 진단 리포트 조회",
)
def get_pipeline_defects(
    detector: DetectorDep,
    target_date: Annotated[str | None, Query(description="진단 대상 일자 (YYYY-MM-DD)")] = None,
) -> PipelineDefectReportResponse:
    """Run defect detection rules and return anomaly report."""
    try:
        t_date = date.fromisoformat(target_date) if target_date else None
        report = detector.detect_all(t_date)
        return PipelineDefectReportResponse(**report.to_dict())
    except Exception as exc:
        logger.exception("Failed to detect pipeline defects")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post(
    "/heal",
    response_model=list[PipelineHealingActionResponse],
    dependencies=[Depends(get_api_key)],
    summary="결함 경기 또는 일자별 자가 치유(Self-Healing) 실행",
)
def execute_pipeline_healing(
    req: PipelineHealRequest,
    detector: DetectorDep,
    healer: HealerDep,
) -> list[PipelineHealingActionResponse]:
    """Execute automated self-healing on a specific game or for a target date."""
    try:
        if req.game_id:
            action = healer.heal_stuck_game(req.game_id)
            return [PipelineHealingActionResponse(**action.to_dict())]

        t_date = date.fromisoformat(req.target_date) if req.target_date else None
        defect_report = detector.detect_all(t_date)
        healed_actions = healer.heal_from_defect_report(defect_report)
        return [PipelineHealingActionResponse(**a.to_dict()) for a in healed_actions]
    except Exception as exc:
        logger.exception("Failed to execute pipeline healing")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/quality",
    response_model=QualityHubSummaryResponse,
    dependencies=[Depends(get_api_key)],
    summary="통합 품질 허브(QualityHub) 종합 감사 진단 리포트 조회",
)
def get_quality_hub_report(
    quality_hub: QualityHubDep,
    season: Annotated[int, Query(ge=1982, le=2100, description="시즌 연도")] = 2025,
    target_date: Annotated[str | None, Query(description="특정 일자 (YYYY-MM-DD)")] = None,
) -> QualityHubSummaryResponse:
    """Run QualityHub composite evaluation and return quality score with remediation hints."""
    try:
        t_date = date.fromisoformat(target_date) if target_date else None
        report = quality_hub.run_full_audit(target_date=t_date, season=season)
        return QualityHubSummaryResponse(**report.to_dict())
    except Exception as exc:
        logger.exception("Failed to run quality hub audit")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post(
    "/run",
    response_model=PipelineRunResponse,
    dependencies=[Depends(get_api_key)],
    summary="4단계 KBO 일일 파이프라인 수동 실행",
)
def run_daily_pipeline(
    req: PipelineRunRequest,
    orchestrator: OrchestratorDep,
) -> PipelineRunResponse:
    """Trigger the 4-stage daily data pipeline."""
    try:
        t_date = date.fromisoformat(req.target_date) if req.target_date else None
        summary = orchestrator.run_pipeline(
            target_date=t_date,
            auto_heal=req.auto_heal,
            skip_rag=req.skip_rag,
        )
        return PipelineRunResponse(**summary.to_dict())
    except Exception as exc:
        logger.exception("Failed to run daily pipeline")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
