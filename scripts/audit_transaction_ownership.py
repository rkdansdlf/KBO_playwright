"""Audit SQLAlchemy transaction ownership across the codebase.

Read-only static analysis that scans ``src/`` and ``scripts/`` for transaction
lifecycle calls and classifies every module against the repository contract:

* ``src/**/repositories/**`` must never commit, rollback, close, or create a
  session, except for explicitly allowed compatibility facades.
* Services, CLIs, and scripts own transaction boundaries (caller layer) and
  are allowed to commit/rollback/close sessions they create.

Classifications:
    OK                no transaction lifecycle markers.
    COMPAT_FACADE     repository with a session-optional API (``get_db_session``
                      / ``get_rag_index_session`` fallback) or an allowlisted
                      compatibility file.
    CALLER            caller-layer module (service/CLI/script) that owns a
                      transaction boundary.
    STRICT_VIOLATION  repository outside the allowlist with lifecycle control.

The compatibility facades in ``ALLOWED_COMPATIBILITY_FACADES`` are covered by
dedicated contract tests; the allowlist guards against lifecycle control being
re-introduced in any other repository file.

Exit codes:
    0  contract satisfied (no strict violations)
    1  at least one strict violation detected
    2  usage error
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Repository files allowed to perform guarded transaction lifecycle control.
# Each entry is a documented compatibility facade (session-optional API or
# ``_managed_session`` seam) covered by dedicated contract tests.
ALLOWED_COMPATIBILITY_FACADES = {
    "game_save.py",
    "game_relay.py",
    "game_status.py",
    "safe_batting_repository.py",
    "player_season_pitching_repository.py",
    "rag_chunk_repository.py",
}

# Markers that transfer transaction ownership away from the repository.
HARD_MARKERS = ("commit", "rollback", "close", "begin")

# Session scope markers used by compatibility facades.
SCOPE_MARKERS = ("get_db_session", "get_rag_index_session")

# Session-creation markers (repository contract violation candidates).
CREATION_MARKERS = ("SessionLocal", "Session", "sessionmaker")

DEFAULT_ROOTS = ("src", "scripts")


@dataclass
class FunctionEntry:
    """Transaction markers found inside one function."""

    name: str
    markers: list[str] = field(default_factory=list)


@dataclass
class FileReport:
    """Transaction marker report for a single Python file."""

    path: Path
    functions: list[FunctionEntry]
    classification: str


def _is_repository_path(path: Path) -> bool:
    """Return True when the file lives inside a repositories package."""
    return any(part == "repositories" for part in path.parts)


def _marker_for_call(node: ast.Call) -> str | None:
    """Return the marker name for a transaction call node, or None."""
    func = node.func
    if isinstance(func, ast.Attribute):
        if func.attr in HARD_MARKERS or func.attr in SCOPE_MARKERS:
            return func.attr
        if func.attr == "SessionLocal":
            return "SessionLocal"
        return None
    if isinstance(func, ast.Name):
        if func.id in SCOPE_MARKERS:
            return func.id
        if func.id in CREATION_MARKERS:
            return func.id
        return None
    return None


def _enclosing_function(node: ast.AST, tree: ast.AST) -> str:
    """Return the name of the innermost enclosing function for a node."""
    for parent in ast.walk(tree):
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in ast.walk(parent):
                if child is node:
                    return parent.name
    return "<module>"


def scan_file(path: Path) -> FileReport:
    """Scan one Python file and collect transaction markers per function."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return FileReport(path=path, functions=[], classification="PARSE_ERROR")

    by_function: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        marker = _marker_for_call(node)
        if marker is None:
            continue
        func_name = _enclosing_function(node, tree)
        markers = by_function.setdefault(func_name, [])
        if marker not in markers:
            markers.append(marker)

    functions = [FunctionEntry(name=name, markers=markers) for name, markers in sorted(by_function.items())]
    classification = _classify(path, functions)
    return FileReport(path=path, functions=functions, classification=classification)


def _classify(path: Path, functions: list[FunctionEntry]) -> str:
    """Classify a file against the transaction ownership contract."""
    if not functions:
        return "OK"
    if not _is_repository_path(path):
        return "CALLER"
    hard = _collect_hard_markers(functions)
    scopes = _collect_scope_markers(functions)
    if not hard and not scopes:
        return "OK"
    if path.name in ALLOWED_COMPATIBILITY_FACADES:
        return "COMPAT_FACADE"
    if hard:
        return "STRICT_VIOLATION"
    return "COMPAT_FACADE"


def _collect_hard_markers(functions: list[FunctionEntry]) -> list[str]:
    """Collect commit/rollback/close/begin/session-creation markers."""
    markers: list[str] = []
    for entry in functions:
        for marker in entry.markers:
            if marker in HARD_MARKERS or marker in CREATION_MARKERS:
                if marker not in markers:
                    markers.append(marker)
    return markers


def _collect_scope_markers(functions: list[FunctionEntry]) -> list[str]:
    """Collect session-scope fallback markers (facade pattern)."""
    markers: list[str] = []
    for entry in functions:
        for marker in entry.markers:
            if marker in SCOPE_MARKERS and marker not in markers:
                markers.append(marker)
    return markers


def _format_report(report: FileReport, *, verbose: bool) -> list[str]:
    """Render a file report into printable lines."""
    lines = [f"{report.path}"]
    for entry in report.functions:
        marker_text = ", ".join(entry.markers)
        owner = {
            "COMPAT_FACADE": "compat_facade",
            "CALLER": "caller",
            "STRICT_VIOLATION": "repository",
        }.get(report.classification, "caller")
        commit = "yes" if "commit" in entry.markers else "no"
        rollback = "yes" if "rollback" in entry.markers else "no"
        lines.append(f"  function: {entry.name}")
        lines.append(f"  session_owner: {owner}")
        lines.append(f"  commit: {commit}")
        lines.append(f"  rollback: {rollback}")
        lines.append(f"  markers: {marker_text}")
    lines.append(f"  classification: {report.classification}")
    if not verbose and report.classification in ("OK", "CALLER"):
        return []
    return lines


def _iter_python_files(roots: list[Path]) -> list[Path]:
    """Collect all Python files below the given roots, sorted."""
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts))
    return files


def audit(roots: list[Path], *, verbose: bool) -> tuple[int, list[str]]:
    """Run the ownership audit and return (exit_code, output_lines)."""
    reports = [scan_file(path) for path in _iter_python_files(roots)]
    violations = [report for report in reports if report.classification == "STRICT_VIOLATION"]

    lines: list[str] = []
    for report in reports:
        lines.extend(_format_report(report, verbose=verbose))

    facade_count = sum(1 for report in reports if report.classification == "COMPAT_FACADE")
    caller_count = sum(1 for report in reports if report.classification == "CALLER")
    lines.append("-" * 60)
    lines.append(f"Scanned {len(reports)} files: {facade_count} compatibility facades, {caller_count} caller modules.")
    if violations:
        lines.append(f"RESULT: {len(violations)} strict violation(s) found:")
        for report in violations:
            lines.append(f"  - {report.path}")
        return 1, lines
    lines.append("RESULT: contract satisfied (no strict violations)")
    return 0, lines


def main(argv: list[str] | None = None) -> int:
    """Run the transaction ownership audit CLI."""
    parser = argparse.ArgumentParser(description="Audit SQLAlchemy transaction ownership across the codebase.")
    parser.add_argument(
        "roots",
        nargs="*",
        default=list(DEFAULT_ROOTS),
        help="Directories to scan (default: src scripts)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Show OK/CALLER files too.")
    args = parser.parse_args(argv)
    roots = [Path(root) for root in args.roots]
    exit_code, lines = audit(roots, verbose=args.verbose)
    for line in lines:
        print(line)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
