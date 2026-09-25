"""Custom lint check: detect public definitions that nothing in the repository references.

`lint_unreachable_code.py` catches statements that follow a terminator. This catches the
other half of dead code: a public function, class, or method that is defined, documented,
and never called. Such a symbol still looks like part of the contract while proving
nothing.

Only Python call and attribute sites count as references, so a name mentioned in a
docstring or comment does not keep dead API alive.

Scope and deliberate exclusions:

* Class-level annotated attributes are NOT linted. They are consumed through `asdict()`
  and serialized dict keys rather than attribute access, so an attribute-based reference
  counter reports real dataclass and DTO fields as dead. Linting them would push people
  to delete real contract fields, so the check stays where it is decidable and those
  attributes stay a review responsibility.
* Names in `ALLOWLIST` are skipped. Each entry needs a reason so the exclusion is a
  recorded decision rather than a silent hole.

Usage: python scripts/lint_unused_defs.py [PATH ...]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

DEFAULT_SCAN_DIRS = (Path("tools/agent_harness"),)
REFERENCE_ROOTS = ("tools", "src", "scripts", "tests", ".github")
REFERENCE_SUFFIXES = (".py",)
SCAN_SUFFIXES = (".py",)
EXCLUDED_PARTS = ("investigations",)
#: Public names kept for typing, protocol, or export contracts, with the reason.
ALLOWLIST: dict[str, str] = {
    "to_dict": "serialization entrypoint invoked reflectively by evidence writers",
    "from_dict": "deserialization entrypoint paired with to_dict",
}


def _is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_PARTS for part in path.parts)


def _reference_files() -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files", *REFERENCE_ROOTS],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.SubprocessError, OSError):
        return [path for root in REFERENCE_ROOTS for path in Path(root).rglob("*") if path.is_file()]
    return [Path(line) for line in result.stdout.splitlines() if line.endswith(REFERENCE_SUFFIXES)]


def _reference_counts() -> dict[str, int]:
    """Count Python identifier references and attribute accesses per name.

    Assignment targets are excluded: binding a name is not a use of it, so
    `FLAG: tuple = ()` must not keep its own name alive.
    """
    counts: dict[str, int] = {}
    for path in _reference_files():
        if _is_excluded(path) or path.suffix not in SCAN_SUFFIXES:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            name: str | None = None
            if isinstance(node, ast.Name):
                if isinstance(node.ctx, ast.Store):
                    continue
                name = node.id
            elif isinstance(node, ast.Attribute):
                name = node.attr
            if name is not None:
                counts[name] = counts.get(name, 0) + 1
    return counts


def _definitions(tree: ast.Module) -> list[tuple[str, int]]:
    """Return (qualified name, line) for public functions, classes, and methods.

    Class-level annotated attributes are intentionally not collected. They are consumed
    through `asdict()` and serialized dict keys rather than attribute access, so an
    attribute-based counter reports real dataclass and DTO fields as dead. Those need
    review, not a heuristic, so the gate stays where it is decidable.
    """
    found: list[tuple[str, int]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found.append((node.name, node.lineno))
        elif isinstance(node, ast.ClassDef):
            found.append((node.name, node.lineno))
            for member in node.body:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    found.append((f"{node.name}.{member.name}", member.lineno))
    return found


def _declared_names(path: Path) -> list[tuple[str, int]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError):
        return []
    return [
        (name, line)
        for name, line in _definitions(tree)
        if not name.split(".")[-1].startswith("_") and name.split(".")[-1] not in ALLOWLIST
    ]


def scan_file(path: Path, counts: dict[str, int]) -> list[tuple[int, str]]:
    """Return (line, symbol) for public definitions with zero references."""
    return [(line, name) for name, line in _declared_names(path) if counts.get(name.split(".")[-1], 0) == 0]


def main(argv: Sequence[str] | None = None) -> int:
    """Lint the default scan dirs, or the ``.py`` paths given in ``argv``."""
    args = list(sys.argv[1:] if argv is None else argv)
    if args:
        files = [Path(argument) for argument in args if argument.endswith(".py")]
    else:
        files = sorted(
            path for scan_dir in DEFAULT_SCAN_DIRS for path in scan_dir.rglob("*.py") if not _is_excluded(path)
        )
    counts = _reference_counts()
    total = 0
    for path in files:
        for line, symbol in scan_file(path, counts):
            print(f"{path}:{line}: public symbol {symbol} is never referenced")
            total += 1
    print(f"\nTotal: {total} unreferenced public symbols in {len(files)} files")
    return total


if __name__ == "__main__":
    sys.exit(main())
