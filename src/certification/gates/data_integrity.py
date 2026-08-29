"""G05: Critical Data Invariants Certification Gate."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import create_engine, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.certification.models import GateResult, GateStatus
from src.db.engine import get_db_session
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


class DataIntegrityGate:
    """G05: Validates critical mathematical and logical data invariants across KBO batting and pitching records."""

    gate_id: str = "data_invariants"
    name: str = "Critical Data Invariants"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = ["schema_migration"]

    def _query_violations(self, session: Session) -> tuple[int, int]:
        """Query invalid records from batting and pitching tables."""
        batting_neg_stmt = select(PlayerSeasonBatting).where(
            or_(
                PlayerSeasonBatting.at_bats < 0,
                PlayerSeasonBatting.hits < 0,
                PlayerSeasonBatting.home_runs < 0,
                PlayerSeasonBatting.walks < 0,
                PlayerSeasonBatting.hits > PlayerSeasonBatting.at_bats,  # H > AB is mathematically impossible
            )
        )
        invalid_batting = list(session.scalars(batting_neg_stmt).all())

        pitching_neg_stmt = select(PlayerSeasonPitching).where(
            or_(
                PlayerSeasonPitching.innings_pitched < 0,
                PlayerSeasonPitching.earned_runs < 0,
                PlayerSeasonPitching.strikeouts < 0,
                PlayerSeasonPitching.hits_allowed < 0,
            )
        )
        invalid_pitching = list(session.scalars(pitching_neg_stmt).all())
        return len(invalid_batting), len(invalid_pitching)

    def run(self, context: CertificationContext) -> GateResult:
        """Inspect database records against impossibility invariants."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        try:
            try:
                with get_db_session() as session:
                    bat_count, pitch_count = self._query_violations(session)
            except Exception:  # noqa: BLE001
                if context.target == "local":
                    local_engine = create_engine("sqlite:///./data/kbo_dev.db")
                    with Session(local_engine) as local_session:
                        bat_count, pitch_count = self._query_violations(local_session)
                else:
                    raise

            metrics["invalid_batting_records"] = bat_count
            metrics["invalid_pitching_records"] = pitch_count

            total_violations = bat_count + pitch_count
            metrics["total_critical_violations"] = total_violations
            evidence["batting_violations"] = bat_count
            evidence["pitching_violations"] = pitch_count

            duration_ms = (time.perf_counter() - start) * 1000.0

            if total_violations > 0:
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.FAIL,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=f"{total_violations} critical data invariant violation(s) detected",
                )

            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.PASS,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence=evidence,
                message="Critical data invariants 100% verified (0 violations)",
            )

        except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"Data invariant inspection error: {err}",
            )


__all__ = [
    "DataIntegrityGate",
]
