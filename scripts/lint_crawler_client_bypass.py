"""Custom lint check: forbid crawlers from constructing their own HTTP client.

A crawler that builds its own `httpx.AsyncClient` silently opts out of the
URL validation, adaptive throttling, retry, `Retry-After` handling, and
circuit breaking that `src.crawlers.http_client.CrawlerHttpClient` provides.
That is how 17 of 18 client constructions in `src/crawlers/` ended up
bypassing every one of those controls.

Grandfathered files are listed in `GRANDFATHERED` so the existing crawlers can
migrate one at a time. A new violation fails immediately.

Usage: python scripts/lint_crawler_client_bypass.py [FILE ...]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

SCAN_ROOT = Path("src/crawlers")

#: The single sanctioned place an httpx client may be constructed.
#: `base.py` hosts the legacy `BaseHttpCrawler.http_client()` helper that
#: `base_naver_crawler.py` still uses; it retires with that helper.
ALLOWED_FILES = frozenset(
    {
        "src/crawlers/base.py",
        "src/crawlers/http_client.py",
    }
)

#: Files allowed to still build their own client while they migrate.
#: Each entry should disappear as its crawler adopts CrawlerHttpClient.
GRANDFATHERED = frozenset(
    {
        "src/crawlers/award_crawler.py",
        "src/crawlers/external_stats_crawler.py",
        "src/crawlers/game_detail_crawler.py",
        "src/crawlers/game_mvp_crawler.py",
        "src/crawlers/operation_notice_lg_crawler.py",
        "src/crawlers/preview_crawler.py",
        "src/crawlers/realtime_issue_crawler.py",
        "src/crawlers/relay_crawler.py",
        "src/crawlers/roster_transaction_crawler.py",
        "src/crawlers/schedule_crawler.py",
        "src/crawlers/seat_crawler.py",
        "src/crawlers/team_event_crawler.py",
        "src/crawlers/ticket_crawler.py",
    }
)

#: Explicit, reviewable escape hatch for a file that genuinely needs a
#: raw client (for example to drive a streaming download).
BYPASS_MARKER = "crawler-client-bypass"

CLIENT_FACTORIES = {"AsyncClient", "Client"}
CLIENT_MODULES = {"httpx"}


def _normalize(path: Path) -> str:
    """Return a repo-relative POSIX path for comparison."""
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _targets(explicit: list[str]) -> list[Path]:
    if explicit:
        return [Path(arg) for arg in explicit]
    try:
        result = subprocess.run(
            ["git", "ls-files", str(SCAN_ROOT)],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.SubprocessError, OSError):
        return sorted(SCAN_ROOT.rglob("*.py"))
    return [Path(line) for line in result.stdout.splitlines() if line.endswith(".py")]


class _ClientFactoryVisitor(ast.NodeVisitor):
    """Collect call sites that construct an httpx client."""

    def __init__(self) -> None:
        self.lines: list[int] = []
        self.has_marker = False

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr in CLIENT_FACTORIES
            and isinstance(func.value, ast.Name)
            and func.value.id in CLIENT_MODULES
            and BYPASS_MARKER in ast.dump(node)
        ):
            self.has_marker = True
            return
        if (
            isinstance(func, ast.Attribute)
            and func.attr in CLIENT_FACTORIES
            and isinstance(func.value, ast.Name)
            and func.value.id in CLIENT_MODULES
        ):
            self.lines.append(node.lineno)
        self.generic_visit(node)


def scan(path: Path) -> list[int]:
    """Return the line numbers of direct client constructions in a file."""
    source = path.read_text(encoding="utf-8")
    if BYPASS_MARKER in source:
        return []
    visitor = _ClientFactoryVisitor()
    visitor.visit(ast.parse(source, filename=str(path)))
    return visitor.lines


def main(argv: list[str] | None = None) -> int:
    """Report crawler files that construct their own HTTP client.

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
                f"{relative}:{line}: constructs httpx client directly; "
                f"use src.crawlers.http_client.CrawlerHttpClient, or add "
                f"`# {BYPASS_MARKER}` on the call with a reason",
            )

    for relative in sorted(grandfathered_present):
        print(f"[grandfathered] {relative}")

    for issue in issues:
        print(f"ERROR: {issue}")

    if issues:
        print(
            f"\n{len(issues)} crawler client bypass(es) outside the allowlist. "
            f"Existing exemptions live in scripts/lint_crawler_client_bypass.py:GRANDFATHERED.",
        )
        return 1

    print("No new crawler HTTP client bypasses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
