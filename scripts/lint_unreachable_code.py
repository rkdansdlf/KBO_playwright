"""Custom lint check: detect statements that follow a function's final return or raise.

Dead code hides a second copy of logic that no test exercises and no tool reports. Ruff's
RET/B012 rules do not cover statements after a terminator inside a function body, so this
checker closes that gap.

Usage: python scripts/lint_unreachable_code.py [FILE ...]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

DEFAULT_SCAN_DIRS = (Path("src"), Path("scripts"), Path("tools"))
EXCLUDED_PARTS = ("investigations",)
TERMINATORS = (ast.Return, ast.Raise)


def _default_files() -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files", *[str(directory) for directory in DEFAULT_SCAN_DIRS]],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.SubprocessError, OSError):
        return sorted(path for scan_dir in DEFAULT_SCAN_DIRS for path in scan_dir.rglob("*.py"))
    return [Path(line) for line in result.stdout.splitlines() if line.endswith(".py")]


def _is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_PARTS for part in path.parts)


def unreachable_statements(path: Path) -> list[tuple[int, str]]:
    """Return (line, enclosing function) for statements unreachable in a function body.

    A top-level ``return``/``raise`` always exits, so everything after the *first* one is
    unreachable even when the dead block ends in another return. ``yield`` is deliberately
    not a terminator because execution resumes after it in a generator.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError):
        return []
    issues: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        terminator_indexes = [index for index, stmt in enumerate(node.body) if isinstance(stmt, TERMINATORS)]
        if not terminator_indexes:
            continue
        for stmt in node.body[min(terminator_indexes) + 1 :]:
            issues.append((stmt.lineno, node.name))
    return issues


def scan_file(path: Path) -> list[tuple[int, str]]:
    """Return unreachable statements for one file, honoring scan exclusions."""
    if _is_excluded(path):
        return []
    return unreachable_statements(path)


def main() -> int:
    args = sys.argv[1:]
    if args:
        files = [Path(argument) for argument in args if argument.endswith(".py")]
    else:
        files = _default_files()
    total = 0
    for path in files:
        for line, function in scan_file(path):
            print(f"{path}:{line}: unreachable statement after terminator in {function}()")
            total += 1
    print(f"\nTotal: {total} unreachable statements in {len(files)} files")
    return total


if __name__ == "__main__":
    sys.exit(main())
