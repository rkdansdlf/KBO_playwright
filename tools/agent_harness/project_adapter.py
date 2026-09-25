"""Map Harness verification roles to existing KBO quality gates.

The adapter is the single place that answers "what did this change touch, and
which existing project gate proves it still works". It describes project
commands; it never reimplements their logic.

Mappings are additive: a path that matches nothing is left to the caller's
default rather than being forced into a category.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Changed-file prefixes and the subsystem they belong to.
#: Order matters only in that the first match wins for a single path, so the
#: more specific prefixes come first.
SUBSYSTEM_PREFIXES: tuple[tuple[str, str], ...] = (
    ("src/crawlers/", "crawler"),
    ("src/parsers/", "crawler"),
    ("src/selenium/", "crawler"),
    ("tests/crawlers/", "crawler"),
    ("tests/parsers/", "crawler"),
    ("tests/monitoring/", "observability"),
    ("tests/api/", "api"),
    ("tests/services/", "services"),
    ("src/rag/", "analytics"),
    ("src/analytics/", "analytics"),
    ("src/simulation/", "analytics"),
    ("src/reporting/", "analytics"),
    ("src/monitoring/", "observability"),
    ("src/notifications/", "observability"),
    ("src/diagnostics/", "observability"),
    ("src/orchestration/", "orchestration"),
    ("src/scheduler/", "orchestration"),
    ("src/api/", "api"),
    ("src/services/", "services"),
    ("src/repositories/", "database"),
    ("src/models/", "database"),
    ("src/db/", "database"),
    ("src/sync/", "database"),
    ("src/aggregators/", "database"),
    ("src/validators/", "database"),
    ("migrations/", "database"),
    (".github/workflows/", "ci"),
    (".github/actions/", "ci"),
    (".github/dependabot.yml", "ci"),
    (".agent-harness/", "harness"),
    ("tools/agent_harness/", "harness"),
    ("tests/agent_harness/", "harness"),
)

#: Whole-file (not prefix) paths that carry infrastructure contracts.
SUBSYSTEM_FILES: dict[str, str] = {
    "pyproject.toml": "dependencies",
    "uv.lock": "dependencies",
    "requirements.txt": "dependencies",
    "Dockerfile": "ci",
    "Dockerfile.playwright": "ci",
    "docker-compose.yml": "ci",
    "docker-compose.dev.yml": "ci",
    "docker-compose.prod.yml": "ci",
    "Docs/references/COMPOSE_IMAGE_LOCK.md": "ci",
    "docker-compose.text-relay.yml": "ci",
    ".pre-commit-config.yaml": "ci",
    "pytest.ini": "ci",
    ".github/CODEOWNERS": "ci",
}

#: Extra pytest targets per subsystem, run on top of the base target.
SUBSYSTEM_PYTEST_TARGETS: dict[str, tuple[str, ...]] = {
    "crawler": ("tests/monitoring/test_crawler_selector_gate.py",),
    "analytics": ("tests/analytics",),
    "api": ("tests/api",),
    "observability": ("tests/monitoring",),
    "services": ("tests/services",),
    # A schema change cannot be trusted on unit tests alone: it needs the
    # migration contract, the dialect contract, and the repository layer.
    "database": (
        "tests/migrations",
        "tests/db",
        "tests/repositories",
    ),
}

#: Subsystems that always require certification-grade review.
#: Ordered (subsystem, profile) pairs used to pick a profile from a change set.
#: Order is the priority: the first subsystem present in the change wins.
SUBSYSTEM_PROFILE_PRIORITY: tuple[tuple[str, str], ...] = (
    ("crawler", "crawler-bug"),
    ("analytics", "analytics"),
    ("database", "refactor"),
    ("dependencies", "refactor"),
    ("orchestration", "architecture"),
    ("ci", "architecture"),
    ("api", "architecture"),
    ("harness", "feature"),
)

#: Subsystems that always require certification-gated review.
#: `database` covers schema and write-path changes; `dependencies` covers the
#: packaging surface, where drift silently breaks an installed runtime.
CERTIFICATION_SUBSYSTEMS = frozenset({"database", "dependencies"})


@dataclass(frozen=True)
class KBOProjectAdapter:
    """Describe existing project commands without reimplementing their logic."""

    verification_profiles: tuple[str, ...] = ("project", "crawler", "analytics", "research", "full")

    def source_of_truth(self, profile: str) -> str:
        """Return the existing project gate represented by a verifier profile."""
        mapping = {
            "project": "narrow pytest + Ruff",
            "crawler": "crawler selector tests + narrow pytest + Ruff",
            "analytics": "analytics pytest + Ruff",
            "research": "Harness manifest and permission doctor",
            "full": "repository pytest + Ruff",
        }
        if profile not in mapping:
            msg = f"Unknown project verification profile: {profile}"
            raise ValueError(msg)
        return mapping[profile]

    @staticmethod
    def _normalize(changed_files: list[str] | tuple[str, ...]) -> list[str]:
        return [path.replace("\\", "/").removeprefix("./") for path in changed_files]

    def subsystem_of(self, path: str) -> str | None:
        """Return the subsystem a single repository-relative path belongs to."""
        normalized = path.replace("\\", "/").removeprefix("./")
        if normalized in SUBSYSTEM_FILES:
            return SUBSYSTEM_FILES[normalized]
        for prefix, subsystem in SUBSYSTEM_PREFIXES:
            if normalized.startswith(prefix):
                return subsystem
        return None

    def infer_profile_from_files(self, changed_files: list[str] | tuple[str, ...]) -> str | None:
        """Infer a Harness profile from changed-file prefixes without LLM calls.

        Subsystems are resolved in declaration order, so the most specific
        match wins: a change touching both a crawler and a migration routes as
        a crawler bug, because that is the failure the author is looking at.
        """
        subsystems = self.affected_subsystems(changed_files)
        for subsystem, profile in SUBSYSTEM_PROFILE_PRIORITY:
            if subsystem in subsystems:
                return profile
        return None

    def affected_subsystems(self, changed_files: list[str] | tuple[str, ...]) -> set[str]:
        """Return the subsystem names touched by changed files."""
        subsystems: set[str] = set()
        for path in self._normalize(changed_files):
            subsystem = self.subsystem_of(path)
            if subsystem is not None:
                subsystems.add(subsystem)
            elif path.startswith("src/"):
                subsystems.add("platform")
            elif path.startswith("tests/"):
                subsystems.add("tests")
        return subsystems

    def pytest_targets(self, changed_files: list[str] | tuple[str, ...]) -> list[str]:
        """Return minimal pytest targets covering the changed files.

        The Harness' own suite always runs: the routing and permission
        behaviour under test is what produced this target list in the first
        place.
        """
        if not changed_files:
            return ["tests/agent_harness"]
        targets: set[str] = {"tests/agent_harness"}
        for subsystem in self.affected_subsystems(changed_files):
            targets.update(SUBSYSTEM_PYTEST_TARGETS.get(subsystem, ()))
        return sorted(targets)

    def needs_crawler_gate(self, changed_files: list[str] | tuple[str, ...]) -> bool:
        """Return whether the crawler selector gate is required for these files."""
        return "crawler" in self.affected_subsystems(changed_files)

    def needs_certification(self, changed_files: list[str] | tuple[str, ...], profile: str = "") -> bool:
        """Return whether certification-gated review is required (approval path)."""
        if profile in {"refactor", "architecture"}:
            return True
        return bool(self.affected_subsystems(changed_files) & CERTIFICATION_SUBSYSTEMS)


__all__ = [
    "CERTIFICATION_SUBSYSTEMS",
    "SUBSYSTEM_FILES",
    "SUBSYSTEM_PREFIXES",
    "SUBSYSTEM_PROFILE_PRIORITY",
    "SUBSYSTEM_PYTEST_TARGETS",
    "KBOProjectAdapter",
]
