"""Defect detector service identifying anomalies and stuck games across the KBO pipeline."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, text

from src.constants import KST
from src.models.game import Game, GameBattingStat, GameMetadata, GamePitchingStat
from src.pipeline.dto import DefectItem, DefectReport, PipelineDefectType
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class PipelineDefectDetector:
    """Detects data anomalies, stuck states, score mismatches, and validation errors."""

    def __init__(self, session: Session) -> None:
        """Initialize the defect detector with an active database session."""
        self.session = session

    def find_stuck_games(self, target_date: date | None = None) -> list[DefectItem]:
        """Find past games that remain stuck in SCHEDULED or UNRESOLVED status."""
        cutoff_date = target_date or (datetime.now(KST).date() - timedelta(days=1))
        stmt = (
            select(Game)
            .where(
                Game.game_status.in_([GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED]),
                Game.game_date <= cutoff_date,
            )
            .order_by(Game.game_date.desc())
        )
        games = list(self.session.execute(stmt).scalars().all())

        return [
            DefectItem(
                game_id=g.game_id,
                defect_type=PipelineDefectType.STUCK_SCHEDULED,
                severity="ERROR",
                description=f"Past game on {g.game_date} is stuck in {g.game_status} status.",
                details={
                    "game_date": str(g.game_date),
                    "home_team": g.home_team,
                    "away_team": g.away_team,
                    "status": g.game_status,
                },
            )
            for g in games
        ]

    def find_score_mismatches(self, target_date: date | None = None) -> list[DefectItem]:
        """Find completed games where total score does not match the sum of inning scores."""
        query = text(
            """
            SELECT g.game_id, g.game_date, g.home_team, g.away_team, g.home_score, g.away_score,
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i
                             WHERE i.game_id = g.game_id AND i.team_side = 'home'), 0) as home_sum,
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i
                             WHERE i.game_id = g.game_id AND i.team_side = 'away'), 0) as away_sum
            FROM game g
            WHERE g.game_status IN ('COMPLETED', 'DRAW')
              AND (:target_date IS NULL OR g.game_date = :target_date)
              AND (
                  (g.home_score IS NOT NULL AND g.home_score !=
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i
                             WHERE i.game_id = g.game_id AND i.team_side = 'home'), 0))
                  OR
                  (g.away_score IS NOT NULL AND g.away_score !=
                   COALESCE((SELECT SUM(runs) FROM game_inning_scores i
                             WHERE i.game_id = g.game_id AND i.team_side = 'away'), 0))
              )
            """
        )
        params = {"target_date": target_date.isoformat() if target_date else None}
        rows = self.session.execute(query, params).mappings().all()

        return [
            DefectItem(
                game_id=r["game_id"],
                defect_type=PipelineDefectType.SCORE_MISMATCH,
                severity="ERROR",
                description=(
                    f"Score mismatch: Board score ({r['away_score']}:{r['home_score']}) != "
                    f"Inning sum ({r['away_sum']}:{r['home_sum']})"
                ),
                details=dict(r),
            )
            for r in rows
        ]

    def find_missing_player_stats(self, target_date: date | None = None) -> list[DefectItem]:
        """Find completed games missing batting or pitching stats."""
        stmt = select(Game).where(
            Game.game_status.notin_([GAME_STATUS_SCHEDULED, GAME_STATUS_CANCELLED]),
        )
        if target_date:
            stmt = stmt.where(Game.game_date == target_date)
        else:
            yesterday = datetime.now(KST).date() - timedelta(days=1)
            stmt = stmt.where(Game.game_date <= yesterday)

        games = list(self.session.execute(stmt).scalars().all())

        defects: list[DefectItem] = []
        for g in games:
            bat_count = (
                self.session.execute(
                    select(GameBattingStat.id).where(GameBattingStat.game_id == g.game_id).limit(1)
                ).first()
                is not None
            )
            pit_count = (
                self.session.execute(
                    select(GamePitchingStat.id).where(GamePitchingStat.game_id == g.game_id).limit(1)
                ).first()
                is not None
            )

            if not bat_count or not pit_count:
                missing = []
                if not bat_count:
                    missing.append("batting")
                if not pit_count:
                    missing.append("pitching")

                defects.append(
                    DefectItem(
                        game_id=g.game_id,
                        defect_type=PipelineDefectType.MISSING_STATS,
                        severity="WARNING",
                        description=f"Completed game missing {', '.join(missing)} statistics.",
                        details={
                            "game_date": str(g.game_date),
                            "missing": missing,
                            "home_team": g.home_team,
                            "away_team": g.away_team,
                        },
                    )
                )
        return defects

    def find_unverified_pbp_games(self, target_date: date | None = None) -> list[DefectItem]:
        """Find completed games whose PBP validation status is unverified or invalid."""
        stmt = (
            select(GameMetadata)
            .join(Game, GameMetadata.game_id == Game.game_id)
            .where(
                Game.game_status.in_(["COMPLETED", "DRAW"]),
                GameMetadata.pbp_validation_status == "unverified",
            )
        )
        if target_date:
            stmt = stmt.where(Game.game_date == target_date)

        metas = list(self.session.execute(stmt).scalars().all())

        return [
            DefectItem(
                game_id=m.game_id,
                defect_type=PipelineDefectType.UNVERIFIED_PBP,
                severity="WARNING",
                description="Game PBP data has not been successfully verified.",
                details={"pbp_validation_status": m.pbp_validation_status},
            )
            for m in metas
        ]

    def detect_all(self, target_date: date | None = None) -> DefectReport:
        """Run all defect detection rules and compile a consolidated DefectReport."""
        now = datetime.now(KST)
        date_str = target_date.isoformat() if target_date else str(now.date())

        all_defects: list[DefectItem] = []
        all_defects.extend(self.find_stuck_games(target_date))
        all_defects.extend(self.find_score_mismatches(target_date))
        all_defects.extend(self.find_missing_player_stats(target_date))
        all_defects.extend(self.find_unverified_pbp_games(target_date))

        return DefectReport(
            target_date=date_str,
            defects=all_defects,
            timestamp=now.isoformat(),
        )
