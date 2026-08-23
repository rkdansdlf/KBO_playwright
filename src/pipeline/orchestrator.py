"""Daily Pipeline Orchestrator coordinating collection, aggregation, validation, and self-healing."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING

from src.cli.calc.calculate_standings import StandingsCalculator
from src.constants import KST
from src.pipeline.auto_healer_service import AutoHealerService
from src.pipeline.defect_detector import PipelineDefectDetector
from src.pipeline.dto import (
    HealingActionSummary,
    PipelineRunSummary,
    PipelineStageResult,
)
from src.rag.indexer.knowledge_indexer import KnowledgeIndexer
from src.repositories.game_status import refresh_game_status_for_date
from src.services.quality_hub import QualityHub

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class DailyPipelineOrchestrator:
    """Coordinates the 4-stage daily data pipeline with integrated validation and self-healing."""

    def __init__(self, session: Session) -> None:
        """Initialize the daily pipeline orchestrator."""
        self.session = session
        self.detector = PipelineDefectDetector(session)
        self.healer = AutoHealerService(session)
        self.quality_hub = QualityHub(session)
        self.standings_calc = StandingsCalculator(session)
        self.knowledge_indexer = KnowledgeIndexer(session)

    def execute_stage_1_finalize(self, target_date: date) -> PipelineStageResult:
        """Stage 1: Refresh game statuses and finalize completed games."""
        t0 = time.perf_counter()
        errors: list[str] = []
        metrics: dict[str, int] = {}
        try:
            status_res = refresh_game_status_for_date(target_date.isoformat(), session=self.session)
            metrics = {
                "updated_count": status_res.get("updated", 0),
                "total_games": status_res.get("total", 0),
            }
            self.session.flush()
            stage_status = "SUCCESS"
        except Exception as exc:
            logger.exception("Stage 1 Finalize failed for %s", target_date)
            errors.append(str(exc))
            stage_status = "FAILED"

        return PipelineStageResult(
            stage_name="Stage 1: Finalize & Status Sync",
            status=stage_status,
            duration_seconds=time.perf_counter() - t0,
            metrics=metrics,
            errors=errors,
        )

    def execute_stage_2_aggregate(self, target_date: date) -> PipelineStageResult:
        """Stage 2: Compute standings and aggregate team/player stats."""
        t0 = time.perf_counter()
        errors: list[str] = []
        metrics: dict[str, int] = {}
        try:
            season = target_date.year
            self.standings_calc.calculate_year(season)
            metrics = {"calculated_season": season}
            self.session.flush()
            stage_status = "SUCCESS"
        except Exception as exc:
            logger.exception("Stage 2 Aggregate failed for %s", target_date)
            errors.append(str(exc))
            stage_status = "FAILED"

        return PipelineStageResult(
            stage_name="Stage 2: Standings & Aggregation",
            status=stage_status,
            duration_seconds=time.perf_counter() - t0,
            metrics=metrics,
            errors=errors,
        )

    def execute_stage_3_validate_and_heal(
        self,
        target_date: date,
        *,
        auto_heal: bool = True,
    ) -> tuple[PipelineStageResult, list[HealingActionSummary]]:
        """Stage 3: Detect anomalies and execute automated self-healing."""
        t0 = time.perf_counter()
        errors: list[str] = []
        healed_actions: list[HealingActionSummary] = []
        metrics: dict[str, int] = {}

        try:
            defect_report = self.detector.detect_all(target_date)
            metrics["defects_detected"] = defect_report.total_defects

            if auto_heal and defect_report.has_defects:
                healed_actions = self.healer.heal_from_defect_report(defect_report)
                metrics["defects_healed"] = len(healed_actions)
                self.session.flush()

            # Execute quality gate evaluation
            quality_report = self.quality_hub.run_full_audit(target_date=target_date, season=target_date.year)
            metrics["quality_score"] = quality_report.quality_score
            metrics["overall_quality_status"] = 1 if quality_report.overall_status == "PASS" else 0

            stage_status = "SUCCESS" if quality_report.overall_status in ("PASS", "WARN") else "WARNING"
        except Exception as exc:
            logger.exception("Stage 3 Validate & Heal failed for %s", target_date)
            errors.append(str(exc))
            stage_status = "FAILED"

        return (
            PipelineStageResult(
                stage_name="Stage 3: Validate & Self-Heal",
                status=stage_status,
                duration_seconds=time.perf_counter() - t0,
                metrics=metrics,
                errors=errors,
            ),
            healed_actions,
        )

    def execute_stage_4_advanced_sync_and_rag(self) -> PipelineStageResult:
        """Stage 4: Incrementally index knowledge chunks into RAG corpus."""
        t0 = time.perf_counter()
        errors: list[str] = []
        metrics: dict[str, int] = {}

        try:
            indexed_counts = self.knowledge_indexer.index_all()
            metrics = {f"indexed_{k}": v for k, v in indexed_counts.items()}
            self.session.flush()
            stage_status = "SUCCESS"
        except Exception as exc:
            logger.exception("Stage 4 Advanced Sync & RAG failed")
            errors.append(str(exc))
            stage_status = "FAILED"

        return PipelineStageResult(
            stage_name="Stage 4: Advanced Sync & RAG Indexing",
            status=stage_status,
            duration_seconds=time.perf_counter() - t0,
            metrics=metrics,
            errors=errors,
        )

    def run_pipeline(
        self,
        *,
        target_date: date | None = None,
        auto_heal: bool = True,
        skip_rag: bool = False,
    ) -> PipelineRunSummary:
        """Execute all 4 stages of the daily KBO pipeline in sequence."""
        t_start = time.perf_counter()
        now = datetime.now(KST)
        run_date = target_date or now.date()
        run_id = f"pipeline_{run_date.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"

        stages: list[PipelineStageResult] = []
        all_healed: list[HealingActionSummary] = []

        # Stage 1
        s1 = self.execute_stage_1_finalize(run_date)
        stages.append(s1)

        # Stage 2
        s2 = self.execute_stage_2_aggregate(run_date)
        stages.append(s2)

        # Stage 3
        s3, healed = self.execute_stage_3_validate_and_heal(run_date, auto_heal=auto_heal)
        stages.append(s3)
        all_healed.extend(healed)

        # Stage 4
        if not skip_rag:
            s4 = self.execute_stage_4_advanced_sync_and_rag()
            stages.append(s4)

        has_failed = any(s.status == "FAILED" for s in stages)
        has_warning = any(s.status == "WARNING" for s in stages)
        overall_status = "FAILED" if has_failed else ("WARNING" if has_warning else "SUCCESS")

        total_duration = time.perf_counter() - t_start

        return PipelineRunSummary(
            run_id=run_id,
            target_date=run_date.isoformat(),
            overall_status=overall_status,
            stages=stages,
            healed_defects=all_healed,
            total_duration_seconds=total_duration,
            timestamp=now.isoformat(),
        )
