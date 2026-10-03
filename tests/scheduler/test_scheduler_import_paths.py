"""Guard against scheduler jobs importing modules that do not exist.

``aggregate_team_defense_job`` imported ``src.aggregators.team_defense_aggregator``,
which has never existed in this repository. Because the import was lazy (inside the
function body) and no test executed the job, the resulting ``ModuleNotFoundError``
escaped the job's own handler every night and nothing noticed.

This scan resolves every ``src.``/``scripts.`` import path in the scheduler package
statically, so a mistyped or deleted module fails the suite instead of a nightly job.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEDULER_ROOT = REPO_ROOT / "src" / "scheduler"


def _import_paths() -> set[tuple[str, str]]:
    """Return ``(module, location)`` for every absolute first-party import."""
    paths: set[tuple[str, str]] = set()
    for source in sorted(SCHEDULER_ROOT.rglob("*.py")):
        if "__pycache__" in source.parts:
            continue
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and node.module
                and node.module.startswith(("src.", "scripts."))
            ):
                paths.add((node.module, f"{source.relative_to(REPO_ROOT)}:{node.lineno}"))
    return paths


def _resolves(module: str) -> bool:
    relative = module.replace(".", "/")
    return (REPO_ROOT / f"{relative}.py").exists() or (REPO_ROOT / relative).is_dir()


def test_scheduler_import_paths_resolve() -> None:
    """Every module the scheduler package imports must exist."""
    unresolved = sorted(
        f"{module} (imported at {location})" for module, location in _import_paths() if not _resolves(module)
    )

    assert unresolved == [], "Scheduler imports point at missing modules:\n" + "\n".join(unresolved)


def test_the_scan_actually_finds_imports() -> None:
    """Guard the guard: a broken glob must not silently pass everything."""
    assert len(_import_paths()) > 20
