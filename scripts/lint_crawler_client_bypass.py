"""Custom lint check: forbid crawlers from constructing their own HTTP client.

A crawler that builds its own `httpx.AsyncClient` silently opts out of the
URL validation, adaptive throttling, retry, `Retry-After` handling, and
circuit breaking that `src.crawlers.http_client.CrawlerHttpClient` provides.

Two sanctioned places remain, listed in `ALLOWED_FILES`. Everything else has
to route through the shared client, so a new violation fails immediately.

A file that genuinely needs a raw client (for example to drive a streaming
download) declares it on the call itself with ``# crawler-client-bypass: why``.
The marker is read from the comment tokens around that call, not from the file
as a whole: a file-wide marker would exempt every later bypass in the same
file, which is the quiet exemption this check exists to prevent. A marker
without a reason after the colon is not accepted either, because a bypass
nobody can review is indistinguishable from one that was overlooked.

Usage: python scripts/lint_crawler_client_bypass.py [FILE ...]
"""

from __future__ import annotations

import ast
import io
import re
import subprocess
import sys
import tokenize
from pathlib import Path

SCAN_ROOT = Path("src/crawlers")

#: The sanctioned places an httpx client may be constructed.
#: `http_client.py` is `CrawlerHttpClient` itself. `base.py` hosts the legacy
#: `BaseHttpCrawler.http_client()` helper, which no crawler in `src/` or
#: `scripts/` calls any more; it exists only for
#: `tests/crawlers/test_base_crawler.py` and retires with that helper.
ALLOWED_FILES = frozenset(
    {
        "src/crawlers/base.py",
        "src/crawlers/http_client.py",
    }
)

#: The marker that exempts a single call, with its required reason.
BYPASS_MARKER = "crawler-client-bypass"
_MARKER_REASON_RE = re.compile(rf"{BYPASS_MARKER}\s*:\s*\S")

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
    except subprocess.SubprocessError, OSError:
        return sorted(SCAN_ROOT.rglob("*.py"))
    return [Path(line) for line in result.stdout.splitlines() if line.endswith(".py")]


def _marked_lines(source: str) -> set[int]:
    """Return the lines whose own comment declares a bypass and gives a reason.

    Comments never reach the AST, so the marker has to be read from the token
    stream. A source file that cannot be tokenized yields no markers, which
    means the AST scan that follows reports its violations rather than the
    whole file quietly passing.
    """
    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
        comments = [token for token in tokens if token.type == tokenize.COMMENT]
    except tokenize.TokenError, IndentationError, SyntaxError:
        return set()
    return {
        token.start[0] for token in comments if BYPASS_MARKER in token.string and _MARKER_REASON_RE.search(token.string)
    }


class _ClientFactoryVisitor(ast.NodeVisitor):
    """Collect call sites that construct an httpx client and are not marked."""

    def __init__(self, marked: frozenset[int]) -> None:
        self.lines: list[int] = []
        self.marked: list[int] = []
        self._marked = marked

    def visit_Call(self, node: ast.Call) -> None:
        if self._is_client_call(node):
            if self._is_marked(node):
                self.marked.append(node.lineno)
            else:
                self.lines.append(node.lineno)
        self.generic_visit(node)

    @staticmethod
    def _is_client_call(node: ast.Call) -> bool:
        func = node.func
        return (
            isinstance(func, ast.Attribute)
            and func.attr in CLIENT_FACTORIES
            and isinstance(func.value, ast.Name)
            and func.value.id in CLIENT_MODULES
        )

    def _is_marked(self, node: ast.Call) -> bool:
        """Return whether a marked comment sits on this call.

        The search spans the call plus the line above it, which is where both
        the leading form and the trailing `)  # marker` form land.
        """
        end = node.end_lineno or node.lineno
        return any(line in self._marked for line in range(node.lineno - 1, end + 1))


def scan(path: Path) -> tuple[list[int], list[int]]:
    """Return the unmarked and marked client constructions in a file."""
    source = path.read_text(encoding="utf-8")
    visitor = _ClientFactoryVisitor(frozenset(_marked_lines(source)))
    visitor.visit(ast.parse(source, filename=str(path)))
    return visitor.lines, visitor.marked


def main(argv: list[str] | None = None) -> int:
    """Report crawler files that construct their own HTTP client.

    Returns:
        0 when no unmarked violation exists, 1 otherwise.

    """
    args = list(sys.argv[1:] if argv is None else argv)
    issues: list[str] = []
    allowed_seen: list[str] = []

    for path in _targets(args):
        if not path.is_file() or path.suffix != ".py":
            continue
        relative = _normalize(path)
        lines, _ = scan(path)
        if relative in ALLOWED_FILES:
            if lines:
                allowed_seen.append(f"[allowed] {relative} ({len(lines)} sanctioned construction(s))")
            continue
        if not lines:
            continue
        for line in lines:
            issues.append(
                f"{relative}:{line}: constructs httpx client directly; "
                f"use src.crawlers.http_client.CrawlerHttpClient, or add "
                f"`# {BYPASS_MARKER}: <reason>` on the call",
            )

    for line in allowed_seen:
        print(line)

    for issue in issues:
        print(f"ERROR: {issue}")

    if issues:
        print(
            f"\n{len(issues)} crawler client bypass(es) outside the allowlist. "
            f"Sanctioned constructions live in scripts/lint_crawler_client_bypass.py:ALLOWED_FILES.",
        )
        return 1

    print("No new crawler HTTP client bypasses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
