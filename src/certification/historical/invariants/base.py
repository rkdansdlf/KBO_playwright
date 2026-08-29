"""Base Invariant execution protocol, metadata binding, and SQL aggregate audit utilities."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.certification.historical.exceptions import HistoricalExceptionRegistry
from src.certification.historical.manifest import SeasonManifestRegistry
from src.certification.historical.models import (
    ComparisonMode,
    DataDisposition,
    InvariantMetadata,
    InvariantResult,
    InvariantSeverity,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from src.certification.context import CertificationContext

_AGG_COL_MIN_COUNT = 2


@dataclass
class InvariantEvalContext:
    """Context parameters for creating an InvariantResult."""

    season: int
    violations: int
    checked: int
    samples: list[dict[str, Any]]
    duration_ms: float
    message: str = ""


class BaseHistoricalInvariant:
    """Base class for all historical certification invariant checks."""

    invariant_id: str = "BASE"
    name: str = "Base Invariant"
    layer: str = "H00"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="BASE",
        name="Base Invariant",
        layer="H00",
        severity=InvariantSeverity.BLOCKER,
        comparison_mode=ComparisonMode.EXACT,
    )

    def evaluate_seasons(
        self,
        engine: Engine,
        seasons: list[int],
        context: CertificationContext,
    ) -> list[InvariantResult]:
        """Evaluate invariant across a list of seasons using efficient SQL aggregation.

        Must be implemented by subclasses.
        """
        raise NotImplementedError

    def _check_schema_capability(self, engine: Engine) -> str | None:
        """Verify that required tables for this invariant exist on the database.

        Returns missing table name if unsupported, or None if fully capable.
        """
        if not self.metadata or not self.metadata.required_tables:
            return None
        try:
            inspector = inspect(engine)
            existing_tables = set(inspector.get_table_names())
            for req_table in self.metadata.required_tables:
                if req_table not in existing_tables:
                    return req_table
        except Exception:  # noqa: BLE001
            return None
        return None

    def _create_result(self, eval_ctx: InvariantEvalContext) -> InvariantResult:
        """Construct InvariantResult matching declared exceptions and season status."""
        season = eval_ctx.season
        violations = eval_ctx.violations
        checked = eval_ctx.checked
        samples = eval_ctx.samples
        duration_ms = eval_ctx.duration_ms
        message = eval_ctx.message

        manifest = SeasonManifestRegistry.get_manifest(season)
        declared_exc = HistoricalExceptionRegistry.get_exception(self.invariant_id, season)

        if declared_exc:
            if declared_exc.disposition == DataDisposition.NOT_APPLICABLE:
                return InvariantResult(
                    invariant_id=self.invariant_id,
                    name=self.name,
                    layer=self.layer,
                    season=season,
                    status="N_A",
                    severity=self.severity,
                    violation_count=0,
                    checked_count=checked,
                    message=f"N/A: {declared_exc.reason}",
                    duration_ms=duration_ms,
                    metadata=self.metadata,
                )
            if declared_exc.disposition in {DataDisposition.CONDITIONAL, DataDisposition.AS_OF_CUTOFF}:
                status = "PASS_WITH_EXCEPTION" if violations > 0 else "PASS"
                return InvariantResult(
                    invariant_id=self.invariant_id,
                    name=self.name,
                    layer=self.layer,
                    season=season,
                    status=status,
                    severity=InvariantSeverity.WARNING if violations > 0 else self.severity,
                    violation_count=violations,
                    checked_count=checked,
                    samples=samples,
                    message=f"{declared_exc.reason} ({violations} noted)" if violations > 0 else "Verified",
                    duration_ms=duration_ms,
                    metadata=self.metadata,
                )

        if manifest.status == manifest.status.ACTIVE and self.invariant_id.startswith("H01-SCHEDULE"):
            return InvariantResult(
                invariant_id=self.invariant_id,
                name=self.name,
                layer=self.layer,
                season=season,
                status="AS_OF_CUTOFF",
                severity=self.severity,
                violation_count=violations,
                checked_count=checked,
                samples=samples,
                message=f"Active season evaluated as-of cutoff (scheduled/played={checked})",
                duration_ms=duration_ms,
                metadata=self.metadata,
            )

        status = "FAIL" if violations > 0 else "PASS"
        msg = f"{violations} violation(s) found" if violations > 0 else message or "Verified 100% compliant"

        return InvariantResult(
            invariant_id=self.invariant_id,
            name=self.name,
            layer=self.layer,
            season=season,
            status=status,
            severity=self.severity,
            violation_count=violations,
            checked_count=checked,
            samples=samples,
            message=msg,
            duration_ms=duration_ms,
            metadata=self.metadata,
        )

    def _execute_aggregate_query(
        self,
        engine: Engine,
        agg_sql: str,
        sample_sql: str | None,
        seasons: list[int],
    ) -> list[InvariantResult]:
        """Execute a season aggregate query and fetch top-20 samples on failure."""
        start = time.perf_counter()

        # Check schema capability first
        missing_table = self._check_schema_capability(engine)
        if missing_table:
            duration_ms = (time.perf_counter() - start) * 1000.0
            return [
                InvariantResult(
                    invariant_id=self.invariant_id,
                    name=self.name,
                    layer=self.layer,
                    season=s,
                    status="N_A",
                    severity=self.severity,
                    violation_count=0,
                    checked_count=0,
                    message=f"Schema capability: table '{missing_table}' not present in database",
                    duration_ms=duration_ms / max(len(seasons), 1),
                    metadata=self.metadata,
                )
                for s in seasons
            ]

        results: list[InvariantResult] = []

        try:
            with engine.connect() as conn:
                cursor = conn.execute(text(agg_sql))
                rows = cursor.fetchall()
                agg_data: dict[int, tuple[int, int]] = {}
                for r in rows:
                    sn = int(r[0])
                    v_count = int(r[1])
                    c_count = int(r[2]) if len(r) > _AGG_COL_MIN_COUNT else 0
                    agg_data[sn] = (v_count, c_count)

                total_duration_ms = (time.perf_counter() - start) * 1000.0
                per_season_duration = total_duration_ms / max(len(seasons), 1)

                for s in seasons:
                    v_count, c_count = agg_data.get(s, (0, 0))
                    samples: list[dict[str, Any]] = []

                    if v_count > 0 and sample_sql:
                        try:
                            s_cursor = conn.execute(text(sample_sql), {"season": s})
                            s_rows = s_cursor.mappings().fetchmany(20)
                            samples = [dict(sr) for sr in s_rows]
                        except Exception:  # noqa: BLE001
                            samples = []

                    eval_ctx = InvariantEvalContext(
                        season=s,
                        violations=v_count,
                        checked=c_count,
                        samples=samples,
                        duration_ms=per_season_duration,
                    )
                    results.append(self._create_result(eval_ctx))

        except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            err_msg = f"Query error: {exc}"
            results.extend(
                [
                    InvariantResult(
                        invariant_id=self.invariant_id,
                        name=self.name,
                        layer=self.layer,
                        season=s,
                        status="FAIL",
                        severity=self.severity,
                        violation_count=1,
                        message=err_msg,
                        duration_ms=duration_ms / len(seasons),
                        metadata=self.metadata,
                    )
                    for s in seasons
                ]
            )

        return results


__all__ = [
    "BaseHistoricalInvariant",
    "InvariantEvalContext",
]
