"""Custom lint check: detect bare except Exception without logging or re-raise.
Usage: python scripts/lint_bare_except.py [--fix FILE]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

DEFAULT_SCAN_DIRS = (Path("src"), Path("scripts"))


def _default_files() -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "src", "scripts"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.SubprocessError, OSError):
        return sorted(path for scan_dir in DEFAULT_SCAN_DIRS for path in scan_dir.rglob("*.py"))
    return [Path(line) for line in result.stdout.splitlines() if line.endswith(".py")]


class BareExceptVisitor(ast.NodeVisitor):
    def __init__(self):
        self.issues = []
        self._import_logger = False

    def visit_ExceptHandler(self, node):
        if node.type is None or (isinstance(node.type, ast.Name) and node.type.id == "Exception"):
            # Check if body does: log, print, or raise
            body_str = ast.dump(node)
            has_log = any(
                isinstance(stmt, ast.Call)
                and isinstance(stmt.func, ast.Attribute)
                and "logger" in body_str
                and "exc" in body_str
                for stmt in node.body
            )
            has_print = any(
                isinstance(stmt, ast.Call) and isinstance(stmt.func, ast.Name) and stmt.func.id == "print"
                for stmt in node.body
            )
            has_raise = any(isinstance(stmt, ast.Raise) for stmt in node.body)
            has_logger_call = "logger" in body_str and any(
                w in body_str for w in ("exception", "error", "warning", "info")
            )

            if not (has_raise or has_logger_call or (has_log and has_print)):
                name = node.name if node.name else "(unnamed)"
                self.issues.append((node.lineno, name))

    def visit_FunctionDef(self, node):
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self.generic_visit(node)


def scan_file(path: Path) -> list:
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError:
        return []
    visitor = BareExceptVisitor()
    visitor.visit(tree)
    return [(path, lineno, name) for lineno, name in visitor.issues]


def main():
    args = sys.argv[1:]
    if args:
        files = [Path(a) for a in args if a.endswith(".py")]
    else:
        files = _default_files()
    total = 0
    for f in files:
        issues = scan_file(f)
        for path, lineno, name in issues:
            print(f"{path}:{lineno}: bare except Exception (name={name})")
            total += 1
    print(f"\nTotal: {total} bare except Exception in {len(files)} files")
    return total


if __name__ == "__main__":
    sys.exit(main())
