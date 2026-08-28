"""Standard Data Transfer Objects (DTOs) for Unified Reporting and Quality Intelligence."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ReportFormat(StrEnum):
    """Output rendering formats for executive reports."""

    MARKDOWN = "markdown"
    JSON = "json"
    HTML = "html"
    TEXT = "text"


class ReportCategory(StrEnum):
    """Domain categories for system reports."""

    QUALITY_GATE = "quality_gate"
    GAP_ANALYSIS = "gap_analysis"
    FRESHNESS = "freshness"
    EXECUTIVE_DASHBOARD = "executive_dashboard"
    SCOUTING = "scouting"


@dataclass
class ReportSection:
    """Represents a discrete section within a report."""

    title: str
    content_markdown: str
    metrics: dict[str, Any] = field(default_factory=dict)
    status: str = "PASS"  # PASS, WARN, FAIL

    def to_dict(self) -> dict[str, Any]:
        """Convert section to dictionary."""
        return {
            "title": self.title,
            "content_markdown": self.content_markdown,
            "metrics": self.metrics,
            "status": self.status,
        }


@dataclass
class UnifiedExecutiveReport:
    """Comprehensive multi-section executive report."""

    report_id: str
    category: ReportCategory
    title: str
    generated_at: str
    overall_status: str  # PASS, WARN, FAIL
    summary_metrics: dict[str, Any] = field(default_factory=dict)
    sections: list[ReportSection] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "report_id": self.report_id,
            "category": self.category.value,
            "title": self.title,
            "generated_at": self.generated_at,
            "overall_status": self.overall_status,
            "summary_metrics": self.summary_metrics,
            "sections": [s.to_dict() for s in self.sections],
        }
