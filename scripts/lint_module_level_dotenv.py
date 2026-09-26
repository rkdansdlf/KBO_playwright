"""Custom lint check: forbid module-level `.env` loading in `src/`.

A module that calls `load_dotenv()` at import time mutates `os.environ` for
whoever imports it. That has two consequences worth preventing mechanically:

* a test's outcome depends on whether some unrelated module was imported first,
  so the same commit fails and passes on different runs;
* a developer's real credentials land in every pytest worker.

Use `src.config.env_loader.load_project_env()` instead. It is a no-op when
`KBO_ENV_FILE_LOADING` is disabled, which is what `tests/conftest.py` sets.

Calls inside a function are allowed: an explicit CLI entry point loading its own
environment on demand is a deliberate act, and it is still guarded once it goes
through `load_project_env`.

Usage: python scripts/lint_module_level_dotenv.py [--fix FILE]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

SCAN_DIRS = (Path("src"), Path("scripts"))

#: The one module allowed to touch python-dotenv directly.
ALLOWED_FILES = frozenset({"src/config/env_loader.py"})

#: Name of the guarded replacement; any other env loader is still a violation.
GUARDED_CALL = "load_project_env"


def _default_files() -> list[Path]:
    """Return the tracked Python files under the scan directories."""
    try:
        result = subprocess.run(
            ["git", "ls-files", *[str(d) for d in SCAN_DIRS]],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.SubprocessError, OSError):
        return sorted(p for d in SCAN_DIRS for p in d.rglob("*.py"))
    return [Path(line) for line in result.stdout.splitlines() if line.endswith(".py")]


def _relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _is_load_dotenv(node: ast.expr) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "load_dotenv"
    return isinstance(node, ast.Attribute) and node.attr == "load_dotenv"


def scan(path: Path) -> list[tuple[int, str]]:
    """Return `(line, callee)` for module-level `.env` loads in a file."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    issues: list[tuple[int, str]] = []
    for node in tree.body:
        # Direct call: load_dotenv(...)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and _is_load_dotenv(node.value.func):
            callee = ast.unparse(node.value.func)
            issues.append((node.lineno, callee))
            continue
        # Assignment: something = load_dotenv(...)
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if isinstance(value, ast.Call) and _is_load_dotenv(value.func):
                issues.append((node.lineno, ast.unparse(value.func)))
    return issues


def main(argv: list[str] | None = None) -> int:
    """Report module-level `.env` loads outside the allowed module.

    Returns:
        0 when clean, 1 when a violation exists.

    """
    args = list(sys.argv[1:] if argv is None else argv)
    targets = [Path(arg) for arg in args] if args else _default_files()

    issues: list[str] = []
    for path in targets:
        if not path.is_file() or path.suffix != ".py":
            continue
        relative = _relative(path)
        if relative in ALLOWED_FILES:
            continue
        for line, callee in scan(path):
            if callee == GUARDED_CALL:
                continue
            issues.append(
                f"{relative}:{line}: module-level `{callee}()` call; "
                f"use src.config.env_loader.{GUARDED_CALL}() so the load can be disabled",
            )

    for issue in issues:
        print(f"ERROR: {issue}")
    if issues:
        print(
            f"\n{len(issues)} module-level .env load(s). Loading on import makes test "
            f"outcomes depend on import order and injects real credentials into test workers.",
        )
        return 1

    print("No module-level .env loads outside src/config/env_loader.py.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
