"""Coverage contracts for the affected-pytest gate's subsystem tables.

`src/crawlers`, `src/orchestration`, `src/cli`, and hundreds of other files used to resolve
to no affected test target at all, so the gate named "affected pytest" ran the Harness'
own suite and reported success. Worse, a crawler change did not run `tests/crawlers`, and
`src/validators` was routed to the database suite.

These tests make the tables fail loudly when they rot: an unmapped source file, a target
path that does not exist, or a prefix pointing at a subsystem with no gates.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.agent_harness.project_adapter import (
    SUBSYSTEM_PREFIXES,
    SUBSYSTEM_PYTEST_TARGETS,
    KBOProjectAdapter,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
ADAPTER = KBOProjectAdapter()

#: Source files with no dedicated test directory, and why. Adding an entry here is a
#: decision to accept a coverage gap, so each one carries its reason.
#: `src/streaming` is deliberately absent: `tests/streaming/` was removed as premature in
#: 61ba0738 and the module is covered by two real test files instead.
UNMAPPED_SOURCE_FILES: dict[str, str] = {
    "src/__init__.py": "package marker, no executable behaviour",
    "src/constants.py": "shared root module; 22 test modules import it, no dedicated suite",
    "src/urls.py": "shared root module; exercised by tests that use it, no dedicated suite",
}


def _source_files() -> list[Path]:
    return sorted(SRC.rglob("*.py"))


def test_every_source_file_resolves_to_an_affected_target() -> None:
    """The gate is only meaningful if a change always reaches a real test suite."""
    unmapped = {
        str(path.relative_to(ROOT)): ADAPTER.pytest_targets([str(path.relative_to(ROOT))])
        for path in _source_files()
        if len(ADAPTER.pytest_targets([str(path.relative_to(ROOT))])) == 1
    }

    assert unmapped == {path: ["tests/agent_harness"] for path in UNMAPPED_SOURCE_FILES}


def test_documented_exceptions_are_still_needed() -> None:
    """An exception that became mappable is a silent coverage hole, so fail here."""
    assert set(UNMAPPED_SOURCE_FILES) == {
        "src/__init__.py",
        "src/constants.py",
        "src/urls.py",
    }
    for path, reason in UNMAPPED_SOURCE_FILES.items():
        assert reason.strip(), f"{path} needs a reason"
        assert (ROOT / path).is_file(), f"{path} no longer exists"


@pytest.mark.parametrize("target", sorted({t for targets in SUBSYSTEM_PYTEST_TARGETS.values() for t in targets}))
def test_every_declared_target_exists(target: str) -> None:
    """A missing path makes pytest fail on collection, not skip quietly."""
    assert (ROOT / target).exists(), f"verification target does not exist: {target}"


def test_no_prefix_points_at_a_subsystem_without_gates() -> None:
    """Adding a prefix must not create a subsystem whose changes verify nothing."""
    names = {name for _, name in SUBSYSTEM_PREFIXES}
    ungated = sorted(
        name
        for name in names
        if name not in SUBSYSTEM_PYTEST_TARGETS
        # The Harness suite is the base target, and these carry no extra target by design.
        and name not in {"harness", "dependencies", "platform"}
    )

    assert ungated == []


def test_crawler_change_runs_the_crawler_suites() -> None:
    """Regression: a crawler change ran only the 43-test selector-gate module."""
    targets = ADAPTER.pytest_targets(["src/crawlers/game_boxscore_crawler.py"])

    assert "tests/crawlers" in targets
    assert "tests/parsers" in targets
    assert "tests/monitoring/test_crawler_selector_gate.py" in targets


@pytest.mark.parametrize(
    ("source", "expected_target"),
    [
        ("src/validators/quality_gate.py", "tests/validators"),
        ("src/aggregators/season_stat_aggregator.py", "tests/aggregators"),
        ("src/sync/table_dag.py", "tests/sync"),
        ("src/reporting/scouting_engine.py", "tests/reporting"),
        ("src/notifications/dispatcher.py", "tests/notifications"),
        ("src/diagnostics/engine.py", "tests/diagnostics"),
        ("src/models/game.py", "tests/models"),
        ("src/lineage/tracker.py", "tests/lineage"),
    ],
)
def test_source_area_runs_its_own_suite_not_a_neighbouring_one(source: str, expected_target: str) -> None:
    """These areas were bundled into database/analytics/observability suites."""
    assert expected_target in ADAPTER.pytest_targets([source]), source


def test_schema_areas_still_get_the_migration_and_repository_contracts() -> None:
    """Not every area should be narrowed; schema changes genuinely need the wide net."""
    targets = ADAPTER.pytest_targets(["src/repositories/game_repository.py"])

    assert {"tests/migrations", "tests/db", "tests/repositories"} <= set(targets)
