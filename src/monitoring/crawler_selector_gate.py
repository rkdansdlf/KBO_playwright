"""Selector stability checks for crawler source pages and captured fixtures."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


@dataclass(frozen=True)
class SelectorCheck:
    """A single selector contract for a crawler target."""

    name: str
    selector: str
    min_count: int = 1
    max_count: int | None = None
    required_text: str | None = None
    required_attrs: tuple[str, ...] = ()


@dataclass(frozen=True)
class SelectorTarget:
    """A page or fixture that should satisfy a set of selector contracts."""

    name: str
    source: str
    source_type: str = "file"
    checks: Sequence[SelectorCheck] = field(default_factory=tuple)
    wait_until: Literal["commit", "domcontentloaded", "load", "networkidle"] | None = "networkidle"
    timeout_ms: int = 30_000


@dataclass(frozen=True)
class SelectorIssue:
    """A selector contract violation."""

    category: str
    check_name: str
    selector: str
    message: str
    observed_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "category": self.category,
            "check_name": self.check_name,
            "selector": self.selector,
            "message": self.message,
            "observed_count": self.observed_count,
        }


@dataclass(frozen=True)
class SelectorGateResult:
    """Selector gate result for one target."""

    target: str
    source: str
    source_type: str
    checks: dict[str, dict[str, Any]]
    issues: list[SelectorIssue]

    @property
    def ok(self) -> bool:
        """
        Handle the ok operation.

        Returns:
            True if successful, False otherwise.

        """
        return not self.issues

    @property
    def issue_count(self) -> int:
        """
        Handle the issue count operation.

        Returns:
            Integer result.

        """
        return len(self.issues)

    def to_dict(self) -> dict[str, Any]:
        """
        Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "target": self.target,
            "source": self.source,
            "source_type": self.source_type,
            "ok": self.ok,
            "issue_count": self.issue_count,
            "checks": self.checks,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class SelectorGateSummary:
    """Aggregate selector gate result."""

    targets: list[SelectorGateResult]
    report_path: str | None = None

    @property
    def ok(self) -> bool:
        """
        Handle the ok operation.

        Returns:
            True if successful, False otherwise.

        """
        return all(target.ok for target in self.targets)

    @property
    def target_count(self) -> int:
        """
        Handle the target count operation.

        Returns:
            Integer result.

        """
        return len(self.targets)

    @property
    def issue_count(self) -> int:
        """
        Handle the issue count operation.

        Returns:
            Integer result.

        """
        return sum(target.issue_count for target in self.targets)

    def to_dict(self) -> dict[str, Any]:
        """
        Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "ok": self.ok,
            "target_count": self.target_count,
            "issue_count": self.issue_count,
            "report_path": self.report_path,
            "targets": [target.to_dict() for target in self.targets],
        }


def evaluate_html_target(target: SelectorTarget, html: str) -> SelectorGateResult:
    """
    Evaluate one target against already-captured HTML.

    Args:
        target: Target.
        html: Html.
        target: Target.
        html: Html.

    """
    soup = BeautifulSoup(html, "html.parser")

    check_payloads: dict[str, dict[str, Any]] = {}
    issues: list[SelectorIssue] = []

    for check in target.checks:
        matches = soup.select(check.selector)
        count = len(matches)
        texts = [match.get_text(" ", strip=True) for match in matches]
        attrs = {attr: sum(1 for match in matches if match.has_attr(attr)) for attr in check.required_attrs}
        check_payloads[check.name] = {
            "selector": check.selector,
            "count": count,
            "min_count": check.min_count,
            "max_count": check.max_count,
            "required_text": check.required_text,
            "required_attrs": list(check.required_attrs),
            "matched_text": texts[:5],
            "matched_attrs": attrs,
        }

        if count < check.min_count:
            issues.append(
                SelectorIssue(
                    category="selector_missing",
                    check_name=check.name,
                    selector=check.selector,
                    message=f"Expected at least {check.min_count} match(es), found {count}",
                    observed_count=count,
                ),
            )
            continue

        if check.max_count is not None and count > check.max_count:
            issues.append(
                SelectorIssue(
                    category="selector_too_many",
                    check_name=check.name,
                    selector=check.selector,
                    message=f"Expected at most {check.max_count} match(es), found {count}",
                    observed_count=count,
                ),
            )

        if check.required_text and not any(check.required_text in text for text in texts):
            issues.append(
                SelectorIssue(
                    category="text_missing",
                    check_name=check.name,
                    selector=check.selector,
                    message=f"Required text not found: {check.required_text}",
                    observed_count=count,
                ),
            )

        for attr, attr_count in attrs.items():
            if attr_count == 0:
                issues.append(
                    SelectorIssue(
                        category="attribute_missing",
                        check_name=check.name,
                        selector=check.selector,
                        message=f"Required attribute not found on any matched node: {attr}",
                        observed_count=count,
                    ),
                )

    return SelectorGateResult(
        target=target.name,
        source=target.source,
        source_type=target.source_type,
        checks=check_payloads,
        issues=issues,
    )


def load_selector_config(path: str | Path) -> list[SelectorTarget]:
    """
    Load selector targets from a JSON config file.

    Args:
        path: Path.
        path: Path.

    """
    config_path = Path(path)

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    targets = payload.get("targets", [])
    if not isinstance(targets, list):
        msg = "selector gate config must contain a list at 'targets'"
        raise TypeError(msg)

    return [_target_from_dict(target, config_path.parent) for target in targets]


def run_selector_gate(
    targets: Iterable[SelectorTarget],
    *,
    output_dir: str | Path | None = None,
) -> SelectorGateSummary:
    """
    Run selector checks for all targets and optionally write a JSON report.

    Args:
        targets: Targets.
        output_dir: Output Dir.
        targets: Targets.
        output_dir: Output Dir.

    """
    output_path = Path(output_dir) if output_dir else None

    if output_path:
        output_path.mkdir(parents=True, exist_ok=True)

    results = [_evaluate_target(target, output_path) for target in targets]
    report_path: str | None = None
    summary = SelectorGateSummary(targets=results)

    if output_path:
        report_file = output_path / "selector_gate_report.json"
        report_file.write_text(
            json.dumps(summary.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        report_path = str(report_file)
        summary = SelectorGateSummary(targets=results, report_path=report_path)
        report_file.write_text(
            json.dumps(summary.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return summary


def render_selector_summary(summary: SelectorGateSummary) -> str:
    """
    Render a compact text report for terminal use.

    Args:
        summary: Summary.
        summary: Summary.

    """
    lines = [
        f"Selector gate: {'PASS' if summary.ok else 'FAIL'}",
        f"Targets: {summary.target_count}",
        f"Issues: {summary.issue_count}",
    ]
    if summary.report_path:
        lines.append(f"Report: {summary.report_path}")
    for target in summary.targets:
        lines.append(f"- {target.target}: {'PASS' if target.ok else 'FAIL'} ({target.issue_count} issue(s))")
        lines.extend(
            f"  - {issue.category}: {issue.check_name} {issue.selector} - {issue.message}" for issue in target.issues
        )
    return "\n".join(lines)


def _target_from_dict(payload: dict[str, Any], config_dir: Path) -> SelectorTarget:
    checks = tuple(_check_from_dict(check) for check in payload.get("checks", []))
    source_type = str(payload.get("source_type", "file"))
    source = str(payload["source"])
    if source_type == "file":
        source_path = Path(source)
        if not source_path.is_absolute():
            source = str((config_dir / source_path).resolve())
    return SelectorTarget(
        name=str(payload["name"]),
        source=source,
        source_type=source_type,
        checks=checks,
        wait_until=payload.get("wait_until", "networkidle"),
        timeout_ms=int(payload.get("timeout_ms", 30_000)),
    )


def _check_from_dict(payload: dict[str, Any]) -> SelectorCheck:
    required_attrs = payload.get("required_attrs", ())
    if isinstance(required_attrs, str):
        required_attrs = (required_attrs,)
    return SelectorCheck(
        name=str(payload["name"]),
        selector=str(payload["selector"]),
        min_count=int(payload.get("min_count", 1)),
        max_count=int(payload["max_count"]) if payload.get("max_count") is not None else None,
        required_text=payload.get("required_text"),
        required_attrs=tuple(str(attr) for attr in required_attrs),
    )


def _evaluate_target(target: SelectorTarget, output_dir: Path | None) -> SelectorGateResult:
    if target.source_type == "inline":
        return evaluate_html_target(target, target.source)
    if target.source_type == "file":
        return evaluate_html_target(target, Path(target.source).read_text(encoding="utf-8"))
    if target.source_type == "url":
        html = asyncio.run(_capture_url_html(target, output_dir))
        return evaluate_html_target(target, html)
    msg = f"Unsupported selector target source_type: {target.source_type}"
    raise ValueError(msg)


async def _capture_url_html(target: SelectorTarget, output_dir: Path | None) -> str:
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(target.source, wait_until=target.wait_until, timeout=target.timeout_ms)
        html = await page.content()
        if output_dir:
            safe_name = _safe_artifact_name(target.name)
            await page.screenshot(path=str(output_dir / f"{safe_name}.png"), full_page=True)
            (output_dir / f"{safe_name}.html").write_text(html, encoding="utf-8")
        await browser.close()
        return html


def _safe_artifact_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "selector_target"
