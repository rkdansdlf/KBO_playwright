"""Declarative Historical Exception Registry for Known Source Limitations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

from src.certification.historical.models import DataDisposition


@dataclass
class DeclaredException:
    """Represents a formally declared exception to an invariant rule."""

    exception_id: str
    seasons: list[int]
    invariant_id: str
    disposition: DataDisposition
    reason: str
    evidence: str
    owner: str = "kbo_platform_team"
    created_at: str = "2026-08-29"
    expires: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert declared exception to dictionary."""
        return {
            "exception_id": self.exception_id,
            "seasons": self.seasons,
            "invariant_id": self.invariant_id,
            "disposition": self.disposition.value,
            "reason": self.reason,
            "evidence": self.evidence,
            "owner": self.owner,
            "created_at": self.created_at,
            "expires": self.expires,
        }


class HistoricalExceptionRegistry:
    """Registry of documented historical source limitations and approved invariant exceptions."""

    _DEFAULT_EXCEPTIONS: ClassVar[list[DeclaredException]] = [
        DeclaredException(
            exception_id="HIST-2020-SH-SF-SOURCE-ABSENCE",
            seasons=[2020],
            invariant_id="H04-PA-FORMULA-STRICT",
            disposition=DataDisposition.CONDITIONAL,
            reason="2020 official website boxscore omitted explicit sacrifice hit separation in early archives",
            evidence="Audit PA Formula maintenance runbook and backfill_sh_sf_from_pbp utility",
        ),
        DeclaredException(
            exception_id="HIST-2026-ACTIVE-SEASON-CUTOFF",
            seasons=[2026],
            invariant_id="H01-SCHEDULE-COVERAGE",
            disposition=DataDisposition.AS_OF_CUTOFF,
            reason="2026 season is currently in progress; future scheduled games have no final scores",
            evidence="Active season game lifecycle contract",
        ),
        DeclaredException(
            exception_id="HIST-BOXSCORE-UNCREDITED-RUNS-ARCHIVE",
            seasons=[2007, 2011, 2012, 2013, 2014, 2015, 2016, 2018, 2020, 2025],
            invariant_id="H06-BOXSCORE-RECONCILIATION",
            disposition=DataDisposition.CONDITIONAL,
            reason="Historical archives contain isolated uncredited player runs or trial omissions (<3% of games)",
            evidence="HISTORICAL_COMPLETENESS_TRIAGE_2009_2025.md and official boxscore archive comparison",
        ),
    ]

    _EXCEPTIONS: ClassVar[list[DeclaredException]] = []

    @classmethod
    def get_exception(cls, invariant_id: str, season: int) -> DeclaredException | None:
        """Find approved exception matching invariant_id and season."""
        if not cls._EXCEPTIONS:
            cls._EXCEPTIONS = list(cls._DEFAULT_EXCEPTIONS)
        for exc in cls._EXCEPTIONS:
            if exc.invariant_id == invariant_id and season in exc.seasons:
                return exc
        return None

    @classmethod
    def register_exception(cls, exception: DeclaredException) -> None:
        """Register a new declared exception."""
        if not cls._EXCEPTIONS:
            cls._EXCEPTIONS = list(cls._DEFAULT_EXCEPTIONS)
        cls._EXCEPTIONS.append(exception)

    @classmethod
    def list_all(cls) -> list[DeclaredException]:
        """List all active declared exceptions."""
        if not cls._EXCEPTIONS:
            cls._EXCEPTIONS = list(cls._DEFAULT_EXCEPTIONS)
        return list(cls._EXCEPTIONS)

    @classmethod
    def clear_exceptions(cls) -> None:
        """Clear all exceptions (primarily for test isolation)."""
        cls._EXCEPTIONS = []

    @classmethod
    def reset_defaults(cls) -> None:
        """Reset registry to default production exceptions."""
        cls._EXCEPTIONS = list(cls._DEFAULT_EXCEPTIONS)


__all__ = [
    "DeclaredException",
    "HistoricalExceptionRegistry",
]
