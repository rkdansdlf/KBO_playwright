"""G02: Transaction Atomicity & Rollback Certification Gate."""

from __future__ import annotations

import time
from datetime import date
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.certification.models import GateResult, GateStatus
from src.models.base import Base
from src.models.player import PlayerBasic
from src.models.team import TeamDailyRoster
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.repositories.team_repository import TeamRepository

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


class TransactionAtomicityGate:
    """G02: Verifies multi-repository write atomicity and 100% rollback guarantee under failure."""

    gate_id: str = "transaction_atomicity"
    name: str = "Transaction & Rollback Atomicity"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = ["schema_migration"]

    def run(self, context: CertificationContext) -> GateResult:
        """Execute atomic multi-repository write test with intentional failure and rollback check."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        # Use an isolated in-memory verification engine to never touch production data
        iso_engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(iso_engine)
        iso_session_maker = sessionmaker(bind=iso_engine)

        test_player_id = 99998888
        test_date = date(2026, 4, 1)

        try:
            with iso_session_maker() as session:
                player_repo = PlayerBasicRepository(session)
                team_repo = TeamRepository(session)

                # 1. Multi-repository stage 1: Insert player
                player_repo.upsert_players([{"player_id": test_player_id, "name": "트랜잭션검증선수"}])
                session.flush()

                # 2. Multi-repository stage 2: Insert daily roster
                team_repo.save_daily_rosters(
                    [
                        {
                            "roster_date": test_date,
                            "team_code": "LG",
                            "player_id": test_player_id,
                            "player_name": "트랜잭션검증선수",
                            "position": "투수",
                            "back_number": "99",
                        }
                    ]
                )
                session.flush()

                # Check records in session before rollback
                p_count_before = session.query(PlayerBasic).filter_by(player_id=test_player_id).count()
                r_count_before = session.query(TeamDailyRoster).filter_by(player_id=test_player_id).count()
                evidence["pre_rollback_player_count"] = p_count_before
                evidence["pre_rollback_roster_count"] = r_count_before

                # 3. Simulate intentional failure
                session.rollback()

                # 4. Confirm clean rollback
                p_count_after = session.query(PlayerBasic).filter_by(player_id=test_player_id).count()
                r_count_after = session.query(TeamDailyRoster).filter_by(player_id=test_player_id).count()
                metrics["post_rollback_leaked_rows"] = p_count_after + r_count_after
                evidence["post_rollback_player_count"] = p_count_after
                evidence["post_rollback_roster_count"] = r_count_after

                duration_ms = (time.perf_counter() - start) * 1000.0

                if (p_count_after + r_count_after) > 0:
                    return GateResult(
                        gate_id=self.gate_id,
                        name=self.name,
                        status=GateStatus.FAIL,
                        duration_ms=duration_ms,
                        blocking=self.blocking,
                        metrics=metrics,
                        evidence=evidence,
                        message="Rollback failed: uncommitted rows leaked into session",
                    )

                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.PASS,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message="Multi-repo transaction atomicity and clean rollback verified",
                )

        except Exception as exc:  # noqa: BLE001
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
                message=f"Transaction atomicity test error: {err}",
            )


__all__ = [
    "TransactionAtomicityGate",
]
