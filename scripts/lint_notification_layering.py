"""Custom lint check: keep the notification package's layers acyclic.

``src/notifications/`` is the single stack the alerting path converged onto.
Ranks, where a higher rank sits closer to the composition root::

    0  alert_dto, dto         pure contracts
    1  formatter, policy      presentation and routing helpers
    2  incident, recorder     state: the incident ledger and the delivery audit
    2  retention              ledger maintenance
    3  dispatcher             transport dispatch
    4  publisher              composition root
    5  bridge, standalone     entry points composing the root

An import may travel downward (a higher rank composing a lower one) or
sideways. An import that travels upward makes a lower layer depend on the layer
that is supposed to compose it. That is how the convergence came apart before:
call sites reached past the pipeline into the raw transport, and the delivery
path could have been dragged into the ledger.

Three invariants are enforced:

1. Every module in the package declares a rank, and no import may target a
   higher rank. A new module without a rank fails instead of silently joining
   at the wrong level, and a rank with no module fails as stale.
2. The transport (``src/utils/alerting.py``) may import only the pure contract
   modules. It must never learn about incident lifecycle or orchestration.
3. ``src/services/notification_service.py`` composes messages and delegates
   delivery; it must not drive the ledger itself.

Detection is by directory and filename so the rules stay testable on synthetic
files: ``<root>/notifications/<module>.py``, ``<root>/utils/alerting.py`` and
``<root>/services/notification_service.py``.

Usage: python scripts/lint_notification_layering.py [FILE ...]
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

PACKAGE = "src.notifications"
PACKAGE_DIR = Path("src/notifications")
FACADE_FILENAME = "__init__.py"

#: Rank per package module. Higher ranks may import lower ones, never the reverse.
MODULE_RANKS: dict[str, int] = {
    # 0 — pure contracts, no intra-package dependencies
    "alert_dto": 0,
    "dto": 0,
    # 1 — presentation and routing helpers
    "formatter": 1,
    "policy": 1,
    # 2 — state: the incident ledger, the delivery audit and its maintenance
    "incident": 2,
    "recorder": 2,
    "retention": 2,
    # 3 — transport dispatch
    "dispatcher": 3,
    # 4 — composition root
    "publisher": 4,
    # 5 — entry points composing the root
    "bridge": 5,
    "standalone": 5,
}

#: The transport adapters may import only these pure notification modules.
TRANSPORT_FILENAME = "alerting.py"
TRANSPORT_PARENT = "utils"
TRANSPORT_ALLOWED_IMPORTS = frozenset({"alert_dto", "policy"})

#: Services that compose messages and must delegate delivery instead of driving
#: the incident lifecycle themselves.
DELEGATION_FILENAME = "notification_service.py"
DELEGATION_PARENT = "services"
DELEGATION_FORBIDDEN_IMPORTS = frozenset({"incident"})

#: Explicit, reviewable escape hatch when an inversion is genuinely intended.
BYPASS_MARKER = "notification-layering-bypass"


def _package_imports(source: str, filename: str = "<unknown>") -> list[tuple[int, str]]:
    """Return ``(line, module)`` for every ``src.notifications`` submodule import.

    Only submodule imports are collected. ``from src.notifications import Symbol``
    is a facade import and is deliberately ignored: this check is about layer
    direction, not about facade cycles.
    """
    imports: list[tuple[int, str]] = []
    prefix = f"{PACKAGE}."

    for node in ast.walk(ast.parse(source, filename=filename)):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith(prefix):
                imports.append((node.lineno, node.module[len(prefix) :].split(".")[0]))
            elif node.module == PACKAGE:
                for alias in node.names:
                    if alias.name in MODULE_RANKS:
                        imports.append((node.lineno, alias.name))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(prefix):
                    imports.append((node.lineno, alias.name[len(prefix) :].split(".")[0]))

    return imports


def package_violations(module: str, imports: Sequence[tuple[int, str]]) -> list[str]:
    """Return layer inversions for one package module.

    Args:
        module: The importing module's basename, without ``.py``.
        imports: ``(line, target module)`` pairs collected from the file.

    Returns:
        One message per inversion or undeclared target rank.

    """
    own_rank = MODULE_RANKS.get(module)
    if own_rank is None:
        return [f"module '{module}' has no declared rank in MODULE_RANKS"]

    issues: list[str] = []
    for line, target in imports:
        target_rank = MODULE_RANKS.get(target)
        if target_rank is None:
            issues.append(f"{line}: imports '{target}', which has no declared rank in MODULE_RANKS")
        elif target_rank > own_rank:
            issues.append(
                f"{line}: layer inversion — '{module}' (rank {own_rank}) imports "
                f"'{target}' (rank {target_rank}); an import may only travel downward",
            )
    return issues


def transport_violations(imports: Sequence[tuple[int, str]]) -> list[str]:
    """Return the notification imports the transport is not allowed to make.

    Args:
        imports: ``(line, target module)`` pairs collected from the transport.

    Returns:
        One message per disallowed import.

    """
    allowed = ", ".join(sorted(TRANSPORT_ALLOWED_IMPORTS))
    return [
        f"{line}: the transport must not import '{target}' (only {allowed} are pure contracts)"
        for line, target in imports
        if target not in TRANSPORT_ALLOWED_IMPORTS
    ]


def delegation_violations(imports: Sequence[tuple[int, str]]) -> list[str]:
    """Return ledger imports that a delegating service must not make.

    Args:
        imports: ``(line, target module)`` pairs collected from the service.

    Returns:
        One message per forbidden import.

    """
    forbidden = ", ".join(sorted(DELEGATION_FORBIDDEN_IMPORTS))
    return [
        f"{line}: a delegating service must not import '{target}' (forbidden: {forbidden})"
        for line, target in imports
        if target in DELEGATION_FORBIDDEN_IMPORTS
    ]


def unranked_modules(modules: Sequence[str]) -> list[str]:
    """Return package modules missing a declared rank.

    Args:
        modules: Module basenames present on disk.

    Returns:
        One message per module without a rank.

    """
    return [
        f"{PACKAGE_DIR / f'{module}.py'}: no declared rank in MODULE_RANKS; "
        "add one so the module cannot silently join at the wrong level"
        for module in sorted(set(modules) - set(MODULE_RANKS))
    ]


def stale_ranks(modules: Sequence[str]) -> list[str]:
    """Return declared ranks with no module behind them.

    Args:
        modules: Module basenames present on disk.

    Returns:
        One message per stale rank.

    """
    return [
        f"MODULE_RANKS declares '{module}', but {PACKAGE_DIR / f'{module}.py'} does not exist"
        for module in sorted(set(MODULE_RANKS) - set(modules))
    ]


def _is_package_module(path: Path) -> bool:
    return path.parent.name == PACKAGE_DIR.name and path.name != FACADE_FILENAME


def _is_transport(path: Path) -> bool:
    return path.name == TRANSPORT_FILENAME and path.parent.name == TRANSPORT_PARENT


def _is_delegating_service(path: Path) -> bool:
    return path.name == DELEGATION_FILENAME and path.parent.name == DELEGATION_PARENT


def scan(path: Path) -> list[str]:
    """Return the layering violations in one file.

    Args:
        path: The file to inspect.

    Returns:
        One message per violation; empty when the file is clean or out of scope.

    """
    source = path.read_text(encoding="utf-8")
    if BYPASS_MARKER in source:
        return []

    imports = _package_imports(source, str(path))

    if _is_transport(path):
        return transport_violations(imports)
    if _is_delegating_service(path):
        return delegation_violations(imports)
    if _is_package_module(path):
        return package_violations(path.stem, imports)
    return []


def _default_targets() -> list[Path]:
    """Return the files this check owns: the package plus its two boundaries."""
    package = sorted(PACKAGE_DIR.glob("*.py"))
    boundaries = [
        Path("src") / TRANSPORT_PARENT / TRANSPORT_FILENAME,
        Path("src") / DELEGATION_PARENT / DELEGATION_FILENAME,
    ]
    return package + boundaries


def main(argv: list[str] | None = None) -> int:
    """Report notification layer inversions and rank drift.

    Args:
        argv: Optional explicit file list; defaults to the owned files.

    Returns:
        0 when the layering is intact, 1 otherwise.

    """
    args = list(sys.argv[1:] if argv is None else argv)
    explicit = [Path(arg) for arg in args]
    targets = explicit or _default_targets()

    issues: list[str] = []
    for path in targets:
        if not path.is_file():
            continue
        issues.extend(f"{path.as_posix()}:{message}" for message in scan(path))

    if not explicit and PACKAGE_DIR.is_dir():
        modules = [path.stem for path in PACKAGE_DIR.glob("*.py") if path.name != FACADE_FILENAME]
        issues.extend(f"{PACKAGE_DIR.as_posix()}:{message}" for message in unranked_modules(modules))
        issues.extend(f"{PACKAGE_DIR.as_posix()}:{message}" for message in stale_ranks(modules))

    for issue in issues:
        print(f"ERROR: {issue}")

    if issues:
        print(
            f"\n{len(issues)} notification layering violation(s). "
            "Ranks live in scripts/lint_notification_layering.py:MODULE_RANKS; "
            f"use `# {BYPASS_MARKER}: reason` only when an inversion is genuinely intended.",
        )
        return 1

    print("No notification layering violations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
