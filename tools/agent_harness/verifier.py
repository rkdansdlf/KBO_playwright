"""Execute existing KBO verification commands and capture evidence."""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.registry import _mapping, load_yaml_mapping, project_root

if TYPE_CHECKING:
    from pathlib import Path

    from tools.agent_harness.command_runner import CommandRunner
    from tools.agent_harness.evidence import EvidenceStore


@dataclass(frozen=True)
class CommandResult:
    """Record one project verification command result."""

    argv: tuple[str, ...]
    exit_code: int
    duration_ms: float
    stdout: str
    stderr: str

    def to_dict(self) -> dict[str, object]:
        """Serialize a command result for evidence output."""
        return asdict(self)


@dataclass(frozen=True)
class VerificationCheck:
    """Describe one verification command in an impact-based plan."""

    check_id: str
    argv: tuple[str, ...]
    blocking: bool = True
    timeout_seconds: int = 300


@dataclass(frozen=True)
class VerificationPlan:
    """Describe the ordered checks for one verification level."""

    level: str
    checks: tuple[VerificationCheck, ...]


@dataclass(frozen=True)
class VerificationReport:
    """Summarize all commands in one verification profile."""

    profile: str
    passed: bool
    commands: tuple[CommandResult, ...]

    def to_dict(self) -> dict[str, object]:
        """Serialize the complete verification report."""
        return asdict(self)


@dataclass(frozen=True)
class ProjectVerifier:
    """Run allowlisted, shell-free project verification commands."""

    root: Path
    profiles: dict[str, tuple[tuple[str, ...], ...]]

    @classmethod
    def load(cls, root: Path | None = None) -> ProjectVerifier:
        """Load fixed argv command lists from verification policy."""
        repo_root = (root or project_root()).resolve()
        payload = load_yaml_mapping(repo_root / ".agent-harness" / "policies" / "verification.yaml")
        rows = _mapping(payload.get("profiles"), "verification.profiles")
        profiles: dict[str, tuple[tuple[str, ...], ...]] = {}
        for name, raw in rows.items():
            row = _mapping(raw, f"verification.profiles.{name}")
            raw_commands = row.get("commands")
            if not isinstance(raw_commands, list):
                msg = f"Expected command list for verification profile {name}"
                raise TypeError(msg)
            commands: list[tuple[str, ...]] = []
            for raw_command in raw_commands:
                if not isinstance(raw_command, list) or not all(isinstance(item, str) for item in raw_command):
                    msg = f"Expected argv list for verification profile {name}"
                    raise ValueError(msg)
                command = tuple(raw_command)
                if command and command[0] in {"python", "python3"}:
                    command = (sys.executable, *command[1:])
                commands.append(command)
            profiles[name] = tuple(commands)
        return cls(root=repo_root, profiles=profiles)

    def commands_for(self, profile: str) -> tuple[tuple[str, ...], ...]:
        """Return immutable argv commands for a known profile."""
        if profile not in self.profiles:
            msg = f"Unknown verification profile: {profile}"
            raise ValueError(msg)
        return self.profiles[profile]

    def verify(
        self,
        profile: str,
        *,
        evidence: EvidenceStore | None = None,
        timeout_seconds: int = 900,
        skill_id: str = "harness",
        runner: CommandRunner | None = None,
    ) -> VerificationReport:
        """Run profile commands sequentially and stop after the first failure."""
        results: list[CommandResult] = []
        for argv in self.commands_for(profile):
            if runner is not None:
                result = runner.run(argv, skill_id=skill_id, timeout_seconds=timeout_seconds)
            else:
                start = time.perf_counter()
                completed = subprocess.run(  # noqa: S603
                    argv,
                    cwd=self.root,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                )
                result = CommandResult(
                    argv=argv,
                    exit_code=completed.returncode,
                    duration_ms=(time.perf_counter() - start) * 1000,
                    stdout=completed.stdout,
                    stderr=completed.stderr,
                )
            results.append(result)
            if evidence is not None:
                evidence.append_jsonl("commands.jsonl", result.to_dict())
            if result.exit_code != 0:
                break
        report = VerificationReport(
            profile=profile,
            passed=bool(results) and all(result.exit_code == 0 for result in results),
            commands=tuple(results),
        )
        if evidence is not None:
            evidence.write_json("verification.json", report.to_dict())
        return report

    def determine_pytest_targets(self, changed_files: list[str] | tuple[str, ...]) -> list[str]:
        """Return minimal pytest targets for the changed files."""
        from tools.agent_harness.project_adapter import KBOProjectAdapter

        return KBOProjectAdapter().pytest_targets(changed_files)

    def needs_crawler_gate(self, changed_files: list[str] | tuple[str, ...]) -> bool:
        """Return whether the crawler selector gate is required."""
        from tools.agent_harness.project_adapter import KBOProjectAdapter

        return KBOProjectAdapter().needs_crawler_gate(changed_files)

    def needs_certification(self, changed_files: list[str] | tuple[str, ...], profile: str = "") -> bool:
        """Return whether certification-gated review is required."""
        from tools.agent_harness.project_adapter import KBOProjectAdapter

        return KBOProjectAdapter().needs_certification(changed_files, profile)

    def build_plan(self, *, level: str, changed_files: list[str] | tuple[str, ...] = ()) -> VerificationPlan:
        """Build an impact-based verification plan for one level."""
        from tools.agent_harness.project_adapter import KBOProjectAdapter

        adapter = KBOProjectAdapter()
        python = sys.executable
        targets = adapter.pytest_targets(changed_files)
        if level == "none":
            return VerificationPlan(level=level, checks=())
        if level == "quick":
            return VerificationPlan(
                level=level,
                checks=(
                    VerificationCheck(
                        check_id="pytest-affected",
                        argv=(python, "-m", "pytest", *targets, "-q"),
                    ),
                    VerificationCheck(
                        check_id="ruff",
                        argv=("ruff", "check", "tools/agent_harness"),
                    ),
                ),
            )
        if level == "full":
            return VerificationPlan(
                level=level,
                checks=(
                    VerificationCheck(check_id="pytest-full", argv=(python, "-m", "pytest", "-q")),
                    VerificationCheck(
                        check_id="ruff-project",
                        argv=("ruff", "check", "src", "tests", "scripts", "tools"),
                    ),
                ),
            )
        checks: list[VerificationCheck] = [
            VerificationCheck(
                check_id="pytest-affected",
                argv=(python, "-m", "pytest", *targets, "-q"),
            ),
            VerificationCheck(
                check_id="ruff-project",
                argv=("ruff", "check", "src", "tests", "scripts", "tools"),
            ),
        ]
        if adapter.needs_crawler_gate(changed_files):
            checks.append(
                VerificationCheck(
                    check_id="crawler-gate",
                    argv=(
                        python,
                        "-m",
                        "src.cli.crawler_selector_gate",
                        "--config",
                        "Docs/references/crawler_selector_gate.json",
                        "--json",
                    ),
                )
            )
        return VerificationPlan(level="standard", checks=tuple(checks))


__all__ = ["CommandResult", "ProjectVerifier", "VerificationCheck", "VerificationPlan", "VerificationReport"]
