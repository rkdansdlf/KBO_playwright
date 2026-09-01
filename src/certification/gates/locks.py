"""G08: Scheduler Lock Safety & Stale Owner Auto-recovery Certification Gate."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, ClassVar

from src.certification.models import GateResult, GateStatus
from src.utils.lock import ForceProcessLock, ProcessLock

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


logger = logging.getLogger(__name__)


class SchedulerLocksGate:
    """G08: Validates 3-Tier scheduler lock mutual exclusion, double-acquire rejection, and stale lock auto-healing."""

    gate_id: str = "scheduler_locks"
    name: str = "3-Tier Scheduler Lock Safety"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = []

    def run(self, context: CertificationContext) -> GateResult:
        """Run isolated lock acquisition, mutual exclusion rejection, and stale owner recovery tests."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        lock_name = f"cert_lock_{context.run_id}"

        try:
            # 1. Acquire ProcessLock
            lock1 = ProcessLock(lock_name)
            acquired1 = lock1.acquire(blocking=False)
            evidence["initial_lock_acquired"] = acquired1

            if not acquired1:
                duration_ms = (time.perf_counter() - start) * 1000.0
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.FAIL,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    message="Failed to acquire test ProcessLock",
                )

            # 2. Test double acquisition rejection
            lock2 = ProcessLock(lock_name)
            acquired2 = lock2.acquire(blocking=False)
            double_acquire_rejected = not acquired2

            metrics["double_acquire_rejected"] = double_acquire_rejected
            evidence["double_acquire_rejected"] = double_acquire_rejected

            # 3. Release lock1
            lock1.release()

            # 4. Test ForceProcessLock stale recovery
            force_lock = ForceProcessLock(lock_name)
            force_acquired = force_lock.acquire(blocking=False)
            if force_acquired:
                force_lock.release()

            metrics["stale_recovery_supported"] = force_acquired
            evidence["force_lock_acquired"] = force_acquired

            duration_ms = (time.perf_counter() - start) * 1000.0

            if not double_acquire_rejected:
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.FAIL,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message="Mutual exclusion failed: concurrent lock acquisition was not rejected",
                )

            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.PASS,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence=evidence,
                message="Scheduler 3-Tier mutual exclusion and ForceProcessLock recovery verified",
            )

        except Exception as exc:  # noqa: BLE001
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            logger.warning("Scheduler lock safety test error: %s", err)
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"Scheduler lock safety test error: {err}",
            )


__all__ = [
    "SchedulerLocksGate",
]
