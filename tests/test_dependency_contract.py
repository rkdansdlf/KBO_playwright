"""Dependency source-of-truth contract.

`pyproject.toml` is the only place runtime dependencies are declared. These
tests fail when a second dependency list reappears, when a distribution that
`src/` imports is missing from the declared set, or when `uv.lock` no longer
describes the declared dependencies.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
SRC = ROOT / "src"
LOCKFILE = ROOT / "uv.lock"

# Distribution names that differ from their import name.
IMPORT_TO_DISTRIBUTION = {
    "apscheduler": "APScheduler",
    "attr": "attrs",
    "bs4": "beautifulsoup4",
    "dateutil": "python-dateutil",
    "dotenv": "python-dotenv",
    "jwt": "PyJWT",
    "kakaobrain": "kakaobrainsdk",
    "lxml": "lxml",
    "opentelemetry": "opentelemetry-api",
    "PIL": "pillow",
    "playwright": "playwright",
    "psycopg2": "psycopg2-binary",
    "pydantic": "pydantic",
    "sentry_sdk": "sentry-sdk",
    "sklearn": "scikit-learn",
    "sqlalchemy": "SQLAlchemy",
    "tiktoken": "tiktoken",
    "yaml": "PyYAML",
}

# Declared locally or provided by the runtime, not a pip distribution.
IGNORED_ROOTS = frozenset({"src", "scripts", "kbo_playwright", "__future__"})


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _project_table() -> dict[str, object]:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]


def _declared_distributions() -> set[str]:
    """Return every distribution name declared across all dependency tables."""
    project = _project_table()
    raw: list[str] = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        raw.extend(extra)
    return {_normalize(re.split(r"[<>=!~\[; ]", spec, maxsplit=1)[0]) for spec in raw}


def _locked_distributions() -> set[str]:
    if not LOCKFILE.exists():
        pytest.fail("uv.lock is missing; regenerate it with `uv lock`")
    return set(re.findall(r'^name = "([^"]+)"', LOCKFILE.read_text(encoding="utf-8"), re.MULTILINE))


def _imported_roots() -> set[str]:
    """Return the top-level module names imported anywhere under `src/`."""
    roots: set[str] = set()
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                roots.add(node.module.split(".")[0])
    return roots


def _stdlib_roots() -> frozenset[str]:
    return frozenset(sys.stdlib_module_names)


DECLARED = _declared_distributions()
LOCKED = {_normalize(name) for name in _locked_distributions()}
STDLIB = _stdlib_roots()


def test_requirements_files_are_not_reintroduced() -> None:
    """A second dependency list silently drifts from pyproject.toml."""
    for name in ("requirements.txt", "requirements-dev.txt"):
        assert not (ROOT / name).exists(), f"{name} duplicates pyproject.toml; declare dependencies in pyproject.toml"


@pytest.mark.parametrize(
    "relative_path",
    [
        ".github/actions/python-env/action.yml",
        ".github/actions/kbo-job-setup/action.yml",
        ".github/workflows/test_suite.yml",
        ".github/workflows/docker_build.yml",
        "Dockerfile",
        "Dockerfile.playwright",
    ],
)
def test_ci_and_docker_do_not_reference_requirements_files(relative_path: str) -> None:
    """CI and Docker must resolve dependencies from pyproject.toml."""
    text = (ROOT / relative_path).read_text(encoding="utf-8")
    assert "requirements.txt" not in text, f"{relative_path} still installs from a requirements file"
    assert "requirements-dev.txt" not in text, f"{relative_path} still installs from a requirements file"


def test_every_distribution_imported_by_src_is_declared() -> None:
    """`pip install .` alone must be enough to import the application."""
    undeclared: list[str] = []
    for root in sorted(_imported_roots()):
        if root in STDLIB or root in IGNORED_ROOTS:
            continue
        distribution = _normalize(IMPORT_TO_DISTRIBUTION.get(root, root))
        if distribution not in DECLARED:
            undeclared.append(f"{root} (expected distribution {distribution})")
    assert not undeclared, f"Undeclared runtime dependencies: {', '.join(undeclared)}"


def test_declared_dependencies_are_present_in_the_lockfile() -> None:
    """uv.lock must describe every declared distribution, or it is a trap."""
    missing = sorted(name for name in DECLARED if name not in LOCKED)
    assert not missing, f"uv.lock is missing declared dependencies: {', '.join(missing)}"


def test_optional_dependency_groups_are_declared() -> None:
    """PostgreSQL and dev tooling stay explicit, discoverable extras."""
    extras = set(_project_table()["optional-dependencies"])
    assert {"postgres", "dev"} <= extras, f"missing extras: {sorted({'postgres', 'dev'} - extras)}"


def test_shipped_packages_cover_runtime_first_party_imports() -> None:
    """`pip install .` must ship every first-party package src/ imports.

    src/scheduler/registry.py imports scripts.maintenance.*, so excluding
    scripts/ from the distribution makes an installed scheduler fail to start
    even though it works from a repository checkout.
    """
    include = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["tool"]["setuptools"]["packages"]["find"]["include"]
    first_party = {root for root in _imported_roots() if root in IGNORED_ROOTS and root not in STDLIB}
    shipped = {root for root in first_party if any(pattern.rstrip("*") == root for pattern in include)}
    assert first_party <= shipped, f"not shipped: {sorted(first_party - shipped)}"


def test_postgres_only_dependencies_are_not_in_the_runtime_set() -> None:
    """PostgreSQL is acceptance-only; shipping it in core hides that boundary."""
    runtime = {_normalize(re.split(r"[<>=!~\[; ]", spec, maxsplit=1)[0]) for spec in _project_table()["dependencies"]}
    assert "psycopg2-binary" not in runtime
    assert "pgvector" not in runtime
