"""Historical Data Certification Data Models and Status Contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class SeasonStatus(StrEnum):
    """Lifecycle status of a KBO season."""

    FINAL = "FINAL"  # Historical finalized season (1982 ~ previous year)
    ACTIVE = "ACTIVE"  # Ongoing / in-progress season (current year e.g. 2026)
    PARTIAL_SOURCE = "PARTIAL_SOURCE"  # Finalized but official historical source has known limitations
    UNSUPPORTED_DETAIL = "UNSUPPORTED_DETAIL"  # Pre-digital records with no boxscore detail


class DataDisposition(StrEnum):
    """Expected data availability disposition for a specific domain layer in a season."""

    REQUIRED = "REQUIRED"  # Must exist and pass 100% verification
    CONDITIONAL = "CONDITIONAL"  # Required if game/schedule indicates availability
    UNAVAILABLE = "UNAVAILABLE"  # Known not available in official records
    AS_OF_CUTOFF = "AS_OF_CUTOFF"  # Evaluated up to the latest completed game date
    NOT_APPLICABLE = "NOT_APPLICABLE"  # N/A with documented justification
    UNKNOWN = "UNKNOWN"  # Availability not yet determined by empirical source investigation


class ComparisonMode(StrEnum):
    """Comparison relationship mode for boxscore and aggregate reconciliation."""

    EXACT = "EXACT"  # Mathematical equality must strictly hold (e.g. Sum of Player Hits = Team Hits)
    DERIVED = "DERIVED"  # Computed transformation (e.g. Outs = 3 * Full Innings + Remainder)
    CONDITIONAL = "CONDITIONAL"  # Holds only when game was official/completed without exception
    NOT_COMPARABLE = "NOT_COMPARABLE"  # Source data model differences preclude direct comparison


class HistoricalSeasonVerdict(StrEnum):
    """Certification verdict for an individual season."""

    PASS = "PASS"  # noqa: S105
    PASS_WITH_DECLARED_EXCEPTIONS = "PASS_WITH_DECLARED_EXCEPTIONS"  # noqa: S105
    FAIL = "FAIL"
    SKIP = "SKIP"


class InvariantSeverity(StrEnum):
    """Severity of a detected invariant failure."""

    BLOCKER = "BLOCKER"  # Impossible baseball math, orphan key, duplicate game
    WARNING = "WARNING"  # Discrepancy explained by known source limitation or external recalc difference
    INFO = "INFO"  # Informational observation


@dataclass(frozen=True)
class InvariantMetadata:
    """Explicit metadata and schema capability requirements for an invariant rule."""

    invariant_id: str
    name: str
    layer: str  # H01 ~ H07
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    required_tables: list[str] = field(default_factory=list)
    required_columns: dict[str, list[str]] = field(default_factory=dict)
    applicability: str = "ALL_SEASONS"
    comparison_mode: ComparisonMode = ComparisonMode.EXACT
    source_scope: str = "OFFICIAL_BOXSCORE"

    def to_dict(self) -> dict[str, Any]:
        """Convert invariant metadata to dictionary."""
        return {
            "invariant_id": self.invariant_id,
            "name": self.name,
            "layer": self.layer,
            "severity": self.severity.value,
            "required_tables": self.required_tables,
            "required_columns": self.required_columns,
            "applicability": self.applicability,
            "comparison_mode": self.comparison_mode.value,
            "source_scope": self.source_scope,
        }


@dataclass
class InvariantResult:
    """Result of evaluating a single historical invariant across one or more seasons."""

    invariant_id: str
    name: str
    layer: str  # H01 ~ H07
    season: int
    status: str  # PASS / FAIL / N_A / SKIP / AS_OF_CUTOFF
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    violation_count: int = 0
    checked_count: int = 0
    samples: list[dict[str, Any]] = field(default_factory=list)
    message: str = ""
    duration_ms: float = 0.0
    metadata: InvariantMetadata | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert invariant result to dictionary."""
        return {
            "invariant_id": self.invariant_id,
            "name": self.name,
            "layer": self.layer,
            "season": self.season,
            "status": self.status,
            "severity": self.severity.value,
            "violation_count": self.violation_count,
            "checked_count": self.checked_count,
            "samples": self.samples[:20],
            "message": self.message,
            "duration_ms": round(self.duration_ms, 2),
            "metadata": self.metadata.to_dict() if self.metadata else None,
        }


@dataclass
class SeasonManifestItem:
    """Declarative data contract and expectations for a single KBO season."""

    season: int
    status: SeasonStatus
    expected_games_min: int
    expected_games_max: int
    pbp_disposition: DataDisposition = DataDisposition.UNKNOWN
    lineup_disposition: DataDisposition = DataDisposition.UNKNOWN
    boxscore_disposition: DataDisposition = DataDisposition.REQUIRED
    season_totals_disposition: DataDisposition = DataDisposition.REQUIRED
    source_evidence: str = "KBO Official Record Archive"
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest item to dictionary."""
        return {
            "season": self.season,
            "status": self.status.value,
            "expected_games_range": [self.expected_games_min, self.expected_games_max],
            "pbp_disposition": self.pbp_disposition.value,
            "lineup_disposition": self.lineup_disposition.value,
            "boxscore_disposition": self.boxscore_disposition.value,
            "season_totals_disposition": self.season_totals_disposition.value,
            "source_evidence": self.source_evidence,
            "notes": self.notes,
        }


@dataclass
class SeasonAuditResult:
    """Consolidated historical certification result for a single season."""

    season: int
    status: SeasonStatus
    verdict: HistoricalSeasonVerdict
    layer_status: dict[str, str] = field(default_factory=dict)  # H01 -> PASS, H02 -> PASS, ...
    total_violations: int = 0
    declared_exceptions: int = 0
    invariants: list[InvariantResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert season audit result to dictionary."""
        return {
            "season": self.season,
            "status": self.status.value,
            "verdict": self.verdict.value,
            "layer_status": self.layer_status,
            "total_violations": self.total_violations,
            "declared_exceptions": self.declared_exceptions,
            "invariants": [inv.to_dict() for inv in self.invariants],
        }


@dataclass
class HistoricalAuditReport:
    """Top-level historical certification report spanning 45 seasons (1982~2026)."""

    schema_version: str = "1.0"
    contract: str = "historical-v1"
    run_id: str = ""
    started_at: str = ""
    finished_at: str = ""
    target: str = "local"
    git_revision: str = ""
    start_season: int = 1982
    end_season: int = 2026
    total_seasons: int = 45
    passed_seasons: int = 0
    passed_with_exceptions: int = 0
    failed_seasons: int = 0
    total_violations: int = 0
    blocking_violations: int = 0
    total_declared_exceptions: int = 0
    undeclared_exceptions: int = 0
    required_checks_skipped: int = 0
    total_duration_ms: float = 0.0
    overall_verdict: str = "NOT_CERTIFIED"  # CERTIFIED / CERTIFIED_WITH_EXCEPTIONS / NOT_CERTIFIED
    is_certified: bool = False
    seasons: list[SeasonAuditResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert historical audit report to serializable dictionary."""
        return {
            "schema_version": self.schema_version,
            "contract": self.contract,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "target": self.target,
            "git_revision": self.git_revision,
            "start_season": self.start_season,
            "end_season": self.end_season,
            "total_seasons": self.total_seasons,
            "passed_seasons": self.passed_seasons,
            "passed_with_exceptions": self.passed_with_exceptions,
            "failed_seasons": self.failed_seasons,
            "total_violations": self.total_violations,
            "blocking_violations": self.blocking_violations,
            "total_declared_exceptions": self.total_declared_exceptions,
            "undeclared_exceptions": self.undeclared_exceptions,
            "required_checks_skipped": self.required_checks_skipped,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "overall_verdict": self.overall_verdict,
            "is_certified": self.is_certified,
            "seasons": [s.to_dict() for s in self.seasons],
        }


__all__ = [
    "ComparisonMode",
    "DataDisposition",
    "HistoricalAuditReport",
    "HistoricalSeasonVerdict",
    "InvariantMetadata",
    "InvariantResult",
    "InvariantSeverity",
    "SeasonAuditResult",
    "SeasonManifestItem",
    "SeasonStatus",
]
