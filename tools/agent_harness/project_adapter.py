"""Map Harness verification roles to existing KBO quality gates."""

from __future__ import annotations

from dataclasses import dataclass


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

    def infer_profile_from_files(self, changed_files: list[str] | tuple[str, ...]) -> str | None:
        """Infer a Harness profile from changed-file prefixes without LLM calls."""
        normalized = [path.replace("\\", "/").removeprefix("./") for path in changed_files]
        if any(path.startswith(("src/crawlers/", "src/parsers/")) for path in normalized):
            return "crawler-bug"
        if any("crawler" in path or "parser" in path for path in normalized if path.startswith("tests/")):
            return "crawler-bug"
        if any(path.startswith(("src/rag/", "src/analytics/")) for path in normalized):
            return "analytics"
        if any(path.startswith(("src/orchestration/", ".github/workflows/")) for path in normalized):
            return "architecture"
        if any(path.startswith(("tools/agent_harness/", ".agent-harness/")) for path in normalized):
            return "feature"
        return None

    def affected_subsystems(self, changed_files: list[str] | tuple[str, ...]) -> set[str]:
        """Return the subsystem names touched by changed files."""
        subsystems: set[str] = set()
        for path in (p.replace("\\", "/").removeprefix("./") for p in changed_files):
            if path.startswith(("src/crawlers/", "src/parsers/")):
                subsystems.add("crawler")
            elif path.startswith(("src/rag/", "src/analytics/")):
                subsystems.add("analytics")
            elif path.startswith(("src/orchestration/", ".github/workflows/")):
                subsystems.add("orchestration")
            elif path.startswith(("tools/agent_harness/", ".agent-harness/")):
                subsystems.add("harness")
            elif path.startswith("src/"):
                subsystems.add("platform")
            elif path.startswith("tests/"):
                subsystems.add("tests")
        return subsystems

    def pytest_targets(self, changed_files: list[str] | tuple[str, ...]) -> list[str]:
        """Return minimal pytest targets covering the changed files."""
        if not changed_files:
            return ["tests/agent_harness"]
        normalized = [path.replace("\\", "/").removeprefix("./") for path in changed_files]
        targets: set[str] = {"tests/agent_harness"}
        if any(path.startswith(("src/crawlers/", "src/parsers/")) for path in normalized):
            targets.add("tests/monitoring/test_crawler_selector_gate.py")
        if any(path.startswith(("src/rag/", "src/analytics/")) for path in normalized):
            targets.add("tests/analytics")
        return sorted(targets)

    def needs_crawler_gate(self, changed_files: list[str] | tuple[str, ...]) -> bool:
        """Return whether the crawler selector gate is required for these files."""
        normalized = [path.replace("\\", "/").removeprefix("./") for path in changed_files]
        return any(
            path.startswith(("src/crawlers/", "src/parsers/"))
            or (path.startswith("tests/") and ("crawler" in path or "parser" in path))
            for path in normalized
        )

    def needs_certification(self, changed_files: list[str] | tuple[str, ...], profile: str = "") -> bool:
        """Return whether certification-gated review is required (approval path)."""
        if profile in {"refactor", "architecture"}:
            return True
        normalized = [path.replace("\\", "/").removeprefix("./") for path in changed_files]
        return any(path.startswith(("migrations/", "src/models/")) for path in normalized)


__all__ = ["KBOProjectAdapter"]
