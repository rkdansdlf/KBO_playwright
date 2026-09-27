"""Custom lint check: forbid application code from calling alert transports directly.

`src/utils/alerting.py` holds the Telegram/Slack/generic-webhook transport
adapters. Application code that imports them bypasses incident lifecycle,
deduplication, cooldown, severity routing and delivery metrics — the exact
controls that made alerting noisy before convergence.

The only sanctioned callers are the transport module itself and
`src/notifications/dispatcher.py`. Everything else must publish an
`AlertEvent` through `src.notifications.publisher.AlertPublisher`.

Files are grandfathered while they migrate. A new violation fails immediately,
and the goal is an empty `GRANDFATHERED` set.

Usage: python scripts/lint_alert_transport_bypass.py [FILE ...]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

SCAN_ROOTS = (Path("src"), Path("scripts"))

#: The single sanctioned consumers of the transport adapters.
ALLOWED_FILES = frozenset(
    {
        "src/utils/alerting.py",
        "src/notifications/dispatcher.py",
    }
)

#: Files still importing a transport directly while they migrate to AlertPublisher.
#: Each entry should disappear as its call site adopts the incident pipeline.
GRANDFATHERED = frozenset(
    {
        "scripts/scheduler.py",
        "scripts/verification/audit_fallback_stats.py",
        "src/cli/backfill/auto_healer.py",
        "src/cli/live/live_crawler.py",
        "src/cli/pipelines/run_daily_update.py",
        "src/cli/reports/dashboard_report.py",
        "src/cli/reports/freshness_gate.py",
        "src/cli/reports/gap_report.py",
        "src/cli/reports/generate_quality_report.py",
        "src/cli/reports/morning_pbp_report.py",
        "src/cli/sync/sqlite_integrity_guard.py",
        "src/services/notification_service.py",
    }
)

#: Explicit, reviewable escape hatch for a legitimate raw-transport need.
BYPASS_MARKER = "alert-transport-bypass"

#: Migration classification for every grandfathered file. Frozen here so the
#: remaining work cannot drift into "mechanically port everything to AlertPublisher".
#:
#:   A = stateful alert -> AlertPublisher (needs OPEN / RECOVERED / cooldown)
#:   B = stateless notification -> NotificationDispatcher / send_notification
#:   C = dead, duplicate or pure wrapper -> delete or unwrap
CLASSIFICATION: dict[str, str] = {
    # A: stateful alerts
    "src/cli/backfill/auto_healer.py": "A",
    "src/cli/live/live_crawler.py": "A",
    "src/cli/sync/sqlite_integrity_guard.py": "A",
    "src/cli/reports/freshness_gate.py": "A",
    "src/cli/pipelines/run_daily_update.py": "A",
    # B: stateless notifications / digests
    "src/cli/reports/dashboard_report.py": "B",
    "src/cli/reports/generate_quality_report.py": "B",
    "src/cli/reports/morning_pbp_report.py": "B",
    "src/cli/reports/gap_report.py": "B",
    "scripts/verification/audit_fallback_stats.py": "B",
    # B: domain service that composes messages and must delegate delivery
    "src/services/notification_service.py": "B",
    # C: wrapper / bootstrap re-export
    "scripts/scheduler.py": "C",
}

TRANSPORT_MODULE = "src.utils.alerting"
TRANSPORT_CLASSES = frozenset({"TelegramBotClient", "SlackWebhookClient", "GenericWebhookClient"})


def _normalize(path: Path) -> str:
    """Return a repo-relative POSIX path for comparison."""
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _targets(explicit: list[str]) -> list[Path]:
    if explicit:
        return [Path(arg) for arg in explicit]
    files: list[Path] = []
    for root in SCAN_ROOTS:
        try:
            result = subprocess.run(
                ["git", "ls-files", str(root)],
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.SubprocessError, OSError):
            files.extend(sorted(root.rglob("*.py")))
            continue
        files.extend(Path(line) for line in result.stdout.splitlines() if line.endswith(".py"))
    return files


class _TransportImportVisitor(ast.NodeVisitor):
    """Collect imports of alert transport clients."""

    def __init__(self) -> None:
        self.lines: list[int] = []

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == TRANSPORT_MODULE and any(alias.name in TRANSPORT_CLASSES for alias in node.names):
            self.lines.append(node.lineno)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        if any(alias.name == TRANSPORT_MODULE for alias in node.names):
            self.lines.append(node.lineno)
        self.generic_visit(node)


def scan(path: Path) -> list[int]:
    """Return the line numbers of direct transport imports in a file."""
    source = path.read_text(encoding="utf-8")
    if BYPASS_MARKER in source:
        return []
    visitor = _TransportImportVisitor()
    visitor.visit(ast.parse(source, filename=str(path)))
    return visitor.lines


def main(argv: list[str] | None = None) -> int:
    """Report application files that import an alert transport directly.

    Returns:
        0 when no new violation exists, 1 otherwise.

    """
    args = list(sys.argv[1:] if argv is None else argv)
    issues: list[str] = []
    grandfathered_present: set[str] = set()

    for path in _targets(args):
        if not path.is_file() or path.suffix != ".py":
            continue
        relative = _normalize(path)
        if relative in ALLOWED_FILES:
            continue
        lines = scan(path)
        if not lines:
            continue
        if relative in GRANDFATHERED:
            grandfathered_present.add(relative)
            continue
        for line in lines:
            issues.append(
                f"{relative}:{line}: imports alert transport directly; "
                f"publish an AlertEvent via src.notifications.publisher.AlertPublisher, "
                f"or add `# {BYPASS_MARKER}` with a reason",
            )

    for relative in sorted(grandfathered_present):
        print(f"[grandfathered] {relative}")

    for issue in issues:
        print(f"ERROR: {issue}")

    if issues:
        print(
            f"\n{len(issues)} alert transport bypass(es) outside the allowlist. "
            f"Existing exemptions live in scripts/lint_alert_transport_bypass.py:GRANDFATHERED.",
        )
        return 1

    print("No new alert transport bypasses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
