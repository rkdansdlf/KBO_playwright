"""Rule-based diagnosis for crawler and pipeline failure logs."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

_SEVERITY_RANK = {"info": 0, "warning": 1, "high": 2}


@dataclass(frozen=True)
class DiagnosisFinding:
    """DiagnosisFinding class."""

    category: str
    severity: str
    message: str
    evidence: str
    suggested_commands: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "category": self.category,
            "severity": self.severity,
            "message": self.message,
            "evidence": self.evidence,
            "suggested_commands": list(self.suggested_commands),
        }


@dataclass(frozen=True)
class DiagnosisReport:
    """DiagnosisReport class."""

    sources: tuple[str, ...]
    findings: tuple[DiagnosisFinding, ...]

    @property
    def source_count(self) -> int:
        """Handle the source count operation.

        Returns:
            Integer result.

        """
        return len(self.sources)

    @property
    def highest_severity(self) -> str:
        """Handle the highest severity operation.

        Returns:
            String result.

        """
        if not self.findings:
            return "info"
        return max((finding.severity for finding in self.findings), key=lambda value: _SEVERITY_RANK[value])

    @property
    def exit_code(self) -> int:
        """Handle the exit code operation.

        Returns:
            Integer result.

        """
        return 1 if any(finding.severity == "high" for finding in self.findings) else 0

    @property
    def suggested_commands(self) -> tuple[str, ...]:
        """Handle the suggested commands operation.

        Returns:
            Tuple result.

        """
        commands: list[str] = []

        for finding in self.findings:
            for command in finding.suggested_commands:
                if command not in commands:
                    commands.append(command)
        return tuple(commands)

    def to_dict(self) -> dict[str, Any]:
        """Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "source_count": self.source_count,
            "sources": list(self.sources),
            "highest_severity": self.highest_severity,
            "exit_code": self.exit_code,
            "findings": [finding.to_dict() for finding in self.findings],
            "suggested_commands": list(self.suggested_commands),
        }


@dataclass(frozen=True)
class _DiagnosisRule:
    category: str
    severity: str
    pattern: re.Pattern[str]
    message: str
    suggested_commands: tuple[str, ...]


_RULES: tuple[_DiagnosisRule, ...] = (
    _DiagnosisRule(
        category="selector",
        severity="high",
        pattern=re.compile(
            r"waiting for selector|strict mode violation|selector .*not found|locator\(|no node found|selector",
            re.IGNORECASE,
        ),
        message="Crawler selector or DOM contract likely changed.",
        suggested_commands=(
            "venv/bin/python -m src.cli.crawler_selector_gate "
            "--config Docs/references/crawler_selector_gate.json --json",
        ),
    ),
    _DiagnosisRule(
        category="auth",
        severity="high",
        pattern=re.compile(
            r"authentication failed|login failed|invalid .*credential|KBO_USER_ID|KBO_USER_PWD",
            re.IGNORECASE,
        ),
        message="KBO authentication or credential configuration failed.",
        suggested_commands=("venv/bin/python -m src.cli.health_check --json",),
    ),
    _DiagnosisRule(
        category="sqlite_corruption",
        severity="high",
        pattern=re.compile(
            r"malformed database schema|invalid rootpage|database disk image is malformed|file is not a database",
            re.IGNORECASE,
        ),
        message="SQLite database file corruption detected.",
        suggested_commands=(
            "venv/bin/python -m src.cli.sqlite_integrity_guard "
            "--database-url sqlite:///data/kbo_dev.db --action quarantine --json",
            "venv/bin/python -m src.cli.db_healthcheck",
        ),
    ),
    _DiagnosisRule(
        category="database",
        severity="high",
        pattern=re.compile(
            r"IntegrityError|FOREIGN KEY|UNIQUE constraint|database is locked|OperationalError",
            re.IGNORECASE,
        ),
        message="Database write, constraint, or lock failure detected.",
        suggested_commands=("venv/bin/python -m src.cli.db_healthcheck",),
    ),
    _DiagnosisRule(
        category="quality_gate",
        severity="high",
        pattern=re.compile(
            r"quality gate|freshness gate|exceeds baseline|null_player_id|completeness audit",
            re.IGNORECASE,
        ),
        message="Data quality gate or freshness invariant failed.",
        suggested_commands=(
            "venv/bin/python -m src.cli.data_quality_regression_pack --json",
            "venv/bin/python -m src.cli.quality_gate_check --year YYYY",
        ),
    ),
    _DiagnosisRule(
        category="scheduler_lock",
        severity="warning",
        pattern=re.compile(r"LIVE_LOCK|DAILY_LOCK|MAINTENANCE_LOCK|lock .*held|already held", re.IGNORECASE),
        message="Scheduler lock prevented concurrent job execution.",
        suggested_commands=("venv/bin/python scripts/scheduler.py --help",),
    ),
    _DiagnosisRule(
        category="network",
        severity="high",
        pattern=re.compile(
            r"ConnectTimeout|ReadTimeout|HTTPStatusError|ERR_NAME_NOT_RESOLVED|net::|ConnectionError",
            re.IGNORECASE,
        ),
        message="Network, HTTP, or upstream availability failure detected.",
        suggested_commands=("venv/bin/python -m src.cli.freshness_gate --days 1 --json",),
    ),
    _DiagnosisRule(
        category="playwright",
        severity="high",
        pattern=re.compile(r"TargetClosedError|Browser.*closed|playwright\._impl\._errors\.Error", re.IGNORECASE),
        message="Playwright browser/runtime failure detected.",
        suggested_commands=("venv/bin/python -m src.cli.crawler_live_smoke",),
    ),
)


def diagnose_text(text: str, *, source: str = "stdin") -> DiagnosisReport:
    """Diagnose one log text blob.

    Args:
        text: Text.
        source: Source.
        text: Text.
        source: Source.

    """
    return diagnose_sources({source: text})


def diagnose_sources(sources: Mapping[str, str]) -> DiagnosisReport:
    """Diagnose one or more named log sources.

    Args:
        sources: Sources.
        sources: Sources.

    """
    findings: list[DiagnosisFinding] = []

    seen_categories: set[str] = set()
    for source_name, text in sources.items():
        for line in text.splitlines():
            evidence = line.strip()
            if not evidence:
                continue
            for rule in _RULES:
                if rule.category in seen_categories:
                    continue
                if rule.pattern.search(evidence):
                    findings.append(
                        DiagnosisFinding(
                            category=rule.category,
                            severity=rule.severity,
                            message=f"{rule.message} Source: {source_name}",
                            evidence=evidence,
                            suggested_commands=rule.suggested_commands,
                        ),
                    )
                    seen_categories.add(rule.category)

    return DiagnosisReport(sources=tuple(sources.keys()), findings=tuple(findings))


def render_diagnosis_text(report: DiagnosisReport) -> str:
    """Render a human-readable diagnosis report.

    Args:
        report: Report.
        report: Report.

    """
    lines = [
        f"Failure diagnosis: {'PASS' if not report.findings else 'ISSUES'}",
        f"Sources: {report.source_count}",
        f"Highest severity: {report.highest_severity}",
    ]
    for finding in report.findings:
        lines.extend(
            [
                f"- {finding.category} [{finding.severity}]",
                f"  {finding.message}",
                f"  Evidence: {finding.evidence}",
            ],
        )
    if report.suggested_commands:
        lines.append("Suggested commands:")
        lines.extend(f"- {command}" for command in report.suggested_commands)
    return "\n".join(lines)


def report_to_json(report: DiagnosisReport) -> str:
    """Report to json.

    Args:
        report: Report.
        report: Report.
        report: Report.

    Returns:
        String result.

    """
    return json.dumps(report.to_dict(), ensure_ascii=False)
