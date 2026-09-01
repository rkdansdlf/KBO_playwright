"""G03: Upsert Idempotency & Duplicate Prevention Certification Gate."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.certification.models import GateResult, GateStatus
from src.models.base import Base
from src.models.player import PlayerBasic
from src.repositories.player_basic_repository import PlayerBasicRepository

if TYPE_CHECKING:
    from src.certification.context import CertificationContext

EXPECTED_PAYLOAD_COUNT = 2
logger = logging.getLogger(__name__)


class UpsertIdempotencyGate:
    """G03: Verifies that identical data payloads upserted multiple times produce 0 duplicate rows and 0 drift."""

    gate_id: str = "upsert_idempotency"
    name: str = "Repository Upsert Idempotency"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = ["schema_migration"]

    def run(self, context: CertificationContext) -> GateResult:
        """Run idempotent repeat-write test on isolated verification session."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        iso_engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(iso_engine)
        iso_session_maker = sessionmaker(bind=iso_engine)

        payload = [
            {"player_id": 777111, "name": "멱등성검증A", "team_name": "KIA"},
            {"player_id": 777222, "name": "멱등성검증B", "team_name": "LG"},
        ]

        try:
            with iso_session_maker() as session:
                repo = PlayerBasicRepository(session)

                # 1. Run #1
                repo.upsert_players(payload)
                session.flush()
                count_run1 = session.query(PlayerBasic).filter(PlayerBasic.player_id.in_([777111, 777222])).count()
                evidence["count_after_run1"] = count_run1

                # 2. Run #2 with identical payload
                repo.upsert_players(payload)
                session.flush()
                count_run2 = session.query(PlayerBasic).filter(PlayerBasic.player_id.in_([777111, 777222])).count()
                evidence["count_after_run2"] = count_run2

                delta = count_run2 - count_run1
                metrics["row_delta_after_repeat"] = delta
                metrics["total_verified_rows"] = count_run2

                duration_ms = (time.perf_counter() - start) * 1000.0

                if delta != 0 or count_run2 != EXPECTED_PAYLOAD_COUNT:
                    return GateResult(
                        gate_id=self.gate_id,
                        name=self.name,
                        status=GateStatus.FAIL,
                        duration_ms=duration_ms,
                        blocking=self.blocking,
                        metrics=metrics,
                        evidence=evidence,
                        message=f"Idempotency violation: row count changed on re-upsert (delta={delta})",
                    )

                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.PASS,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message="Upsert operations verified 100% idempotent (0 duplicates)",
                )

        except Exception as exc:  # noqa: BLE001
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            logger.warning("Idempotency verification error: %s", err)
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"Idempotency verification error: {err}",
            )


__all__ = [
    "EXPECTED_PAYLOAD_COUNT",
    "UpsertIdempotencyGate",
]
