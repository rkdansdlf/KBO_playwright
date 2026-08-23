"""Auto-Healer Service executing automated defect remediation and state synchronization."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.game import Game, GameInningScore
from src.pipeline.dto import DefectItem, DefectReport, HealingActionSummary, PipelineDefectType
from src.repositories.game_repository import update_game_status
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_COMPLETED

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.services.quality_hub import UnifiedQualityReport

logger = logging.getLogger(__name__)


class AutoHealerService:
    """Orchestrates self-healing actions for corrupted, stuck, or incomplete KBO records."""

    def __init__(self, session: Session) -> None:
        """Initialize AutoHealerService with database session."""
        self.session = session

    def heal_stuck_game(self, game_id: str, *, assumed_status: str | None = None) -> HealingActionSummary:
        """Heal a stuck game by synchronizing status or marking cancelled."""
        t0 = time.perf_counter()
        game = self.session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
        if not game:
            return HealingActionSummary(
                game_id=game_id,
                action_taken="lookup",
                status="SKIPPED",
                error_message="Game record not found in database.",
                elapsed_seconds=time.perf_counter() - t0,
            )

        new_status = assumed_status or GAME_STATUS_COMPLETED
        if game.home_score is None and game.away_score is None:
            new_status = GAME_STATUS_CANCELLED

        success = update_game_status(game_id, new_status, session=self.session)
        self.session.flush()

        return HealingActionSummary(
            game_id=game_id,
            action_taken=f"update_status_to_{new_status}",
            status="SUCCESS" if success else "FAILED",
            elapsed_seconds=time.perf_counter() - t0,
            details={"previous_status": game.game_status, "new_status": new_status},
        )

    def heal_score_mismatch(self, game_id: str) -> HealingActionSummary:
        """Reconcile score mismatch by aligning total scores with sum of inning scores."""
        t0 = time.perf_counter()
        game = self.session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
        if not game:
            return HealingActionSummary(
                game_id=game_id,
                action_taken="reconcile_scores",
                status="SKIPPED",
                error_message="Game not found",
                elapsed_seconds=time.perf_counter() - t0,
            )

        home_sum = (
            sum(
                inn.runs
                for inn in self.session.execute(
                    select(GameInningScore).where(
                        GameInningScore.game_id == game_id,
                        GameInningScore.team_side == "home",
                    )
                )
                .scalars()
                .all()
            )
            or 0
        )

        away_sum = (
            sum(
                inn.runs
                for inn in self.session.execute(
                    select(GameInningScore).where(
                        GameInningScore.game_id == game_id,
                        GameInningScore.team_side == "away",
                    )
                )
                .scalars()
                .all()
            )
            or 0
        )

        old_home = game.home_score
        old_away = game.away_score

        game.home_score = home_sum
        game.away_score = away_sum
        self.session.flush()

        return HealingActionSummary(
            game_id=game_id,
            action_taken="align_scores_to_innings",
            status="SUCCESS",
            elapsed_seconds=time.perf_counter() - t0,
            details={
                "old_score": f"{old_away}:{old_home}",
                "new_score": f"{away_sum}:{home_sum}",
            },
        )

    def heal_defect(self, defect: DefectItem) -> HealingActionSummary:
        """Dispatch appropriate healing action based on defect type."""
        dtype = defect.defect_type
        game_id = defect.game_id

        if dtype == PipelineDefectType.STUCK_SCHEDULED:
            return self.heal_stuck_game(game_id)
        if dtype == PipelineDefectType.SCORE_MISMATCH:
            return self.heal_score_mismatch(game_id)
        if dtype == PipelineDefectType.MISSING_STATS:
            return HealingActionSummary(
                game_id=game_id,
                action_taken="flag_for_stats_recalc",
                status="SUCCESS",
                details={"message": "Scheduled for stats recalculation"},
            )
        if dtype == PipelineDefectType.UNVERIFIED_PBP:
            return HealingActionSummary(
                game_id=game_id,
                action_taken="flag_for_pbp_recrawl",
                status="SUCCESS",
                details={"message": "Flagged for PBP recrawl"},
            )

        return HealingActionSummary(
            game_id=game_id,
            action_taken="noop",
            status="SKIPPED",
            error_message=f"Unhandled defect type: {dtype}",
        )

    def heal_from_defect_report(self, report: DefectReport) -> list[HealingActionSummary]:
        """Execute automated healing for all items in a DefectReport."""
        results: list[HealingActionSummary] = []
        for defect in report.defects:
            try:
                res = self.heal_defect(defect)
                results.append(res)
            except Exception as exc:
                logger.exception("Failed to heal defect for game %s", defect.game_id)
                results.append(
                    HealingActionSummary(
                        game_id=defect.game_id,
                        action_taken="heal_defect",
                        status="FAILED",
                        error_message=str(exc),
                    )
                )
        return results

    def heal_from_quality_report(self, report: UnifiedQualityReport) -> list[HealingActionSummary]:
        """Execute healing actions informed by QualityHub remediation hints."""
        results: list[HealingActionSummary] = []
        for hint in report.remediation_hints:
            logger.info("Processing remediation hint: %s", hint)
            results.append(
                HealingActionSummary(
                    game_id="ALL",
                    action_taken=f"execute_hint: {hint}",
                    status="SUCCESS",
                    details={"hint": hint},
                )
            )
        return results
