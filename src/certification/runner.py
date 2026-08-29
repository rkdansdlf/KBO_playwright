"""Main orchestration runner for evaluating all production certification gates."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from src.certification.models import CertificationReport, GateResult, GateStatus
from src.certification.registry import GateRegistry
from src.certification.reporter import CertificationReporter

if TYPE_CHECKING:
    from src.certification.context import CertificationContext
    from src.certification.models import CertificationGate

logger = logging.getLogger(__name__)


def _compute_overall_status(
    gate_results: list[GateResult],
    target: str,
) -> tuple[str, int, int]:
    """Compute aggregated certification status, blocking failures, and warning counts."""
    blocking_failures = 0
    warnings = 0

    for r in gate_results:
        if r.status == GateStatus.FAIL and r.blocking:
            blocking_failures += 1
        elif r.status == GateStatus.SKIP and r.blocking and target == "production":
            # In production, skipping a required gate is considered a certification failure
            blocking_failures += 1
        elif r.status == GateStatus.WARN:
            warnings += 1

    if blocking_failures > 0:
        overall_status = "NOT_CERTIFIED"
    elif warnings > 0:
        overall_status = "CERTIFIED_WITH_WARNINGS"
    else:
        overall_status = "CERTIFIED"

    return overall_status, blocking_failures, warnings


def _execute_single_gate(
    gate: CertificationGate,
    context: CertificationContext,
    status_by_id: dict[str, GateStatus],
) -> GateResult:
    """Execute a single gate with dependency resolution and exception isolation."""
    dep_failed = any(status_by_id.get(dep) in {GateStatus.FAIL, GateStatus.SKIP} for dep in gate.dependencies)
    if dep_failed:
        return GateResult(
            gate_id=gate.gate_id,
            name=gate.name,
            status=GateStatus.SKIP,
            duration_ms=0.0,
            blocking=gate.blocking,
            message=f"Skipped due to unmet dependency: {', '.join(gate.dependencies)}",
        )

    gate_start = time.perf_counter()
    try:
        return gate.run(context)
    except Exception as exc:
        gate_elapsed = (time.perf_counter() - gate_start) * 1000.0
        err_msg = context.redact(str(exc))
        logger.exception("Unhandled exception in gate %s: %s", gate.gate_id, err_msg)
        return GateResult(
            gate_id=gate.gate_id,
            name=gate.name,
            status=GateStatus.FAIL,
            duration_ms=gate_elapsed,
            blocking=gate.blocking,
            message=f"Unhandled error: {err_msg}",
            evidence={"exception": err_msg},
        )


class CertificationRunner:
    """Orchestrates sequential execution of certification gates with dependency and fault isolation."""

    def __init__(
        self,
        registry: GateRegistry | None = None,
        reporter: CertificationReporter | None = None,
    ) -> None:
        """Initialize runner with gate registry and reporter."""
        self.registry = registry or GateRegistry.create_default()
        self.reporter = reporter or CertificationReporter()

    def run_certification(self, context: CertificationContext) -> CertificationReport:
        """Execute full certification sweep and return verified report."""
        gates = self.registry.list_gates(context.filter_gate)
        gate_results: list[GateResult] = []
        status_by_id: dict[str, GateStatus] = {}

        start_time = time.perf_counter()
        started_iso = datetime.now(UTC).isoformat()

        for gate in gates:
            result = _execute_single_gate(gate, context, status_by_id)
            gate_results.append(result)
            status_by_id[gate.gate_id] = result.status

            if context.fail_fast and result.status == GateStatus.FAIL and result.blocking:
                logger.warning("Fail-fast triggered by gate %s", gate.gate_id)
                break

        total_elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        finished_iso = datetime.now(UTC).isoformat()

        overall_status, blocking_failures, warnings = _compute_overall_status(gate_results, context.target)

        return CertificationReport(
            run_id=context.run_id,
            target=context.target,
            status=overall_status,
            started_at=started_iso,
            finished_at=finished_iso,
            git_revision=context.git_revision,
            total_duration_ms=total_elapsed_ms,
            blocking_failures=blocking_failures,
            warnings=warnings,
            gates=gate_results,
        )


__all__ = [
    "CertificationRunner",
]
