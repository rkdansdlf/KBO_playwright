"""Execute existing KBO verification commands and capture evidence."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION
from tools.agent_harness.exceptions import HarnessConfigError, PermissionDeniedError
from tools.agent_harness.registry import _mapping, load_yaml_mapping, project_root

if TYPE_CHECKING:
    from collections.abc import Iterator
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


def update_verification_report(
    evidence: EvidenceStore,
    *,
    status: str,
    profile: str | None = None,
) -> None:
    """Replace the report's complete verification profile and status."""
    if status not in {"pending", "passed", "failed"}:
        msg = f"Unknown verification report status: {status}"
        raise ValueError(msg)
    lines = evidence.read_text("report.md").splitlines()
    updated: list[str] = []
    found = False
    for line in lines:
        if line.startswith("- Verification:"):
            selected_profile = profile
            if selected_profile is None:
                selected_profile = line.partition("`")[2].partition("`")[0] or "unknown"
            updated.append(f"- Verification: `{selected_profile}` ({status})")
            found = True
        else:
            updated.append(line)
    if not found:
        msg = "Harness report is missing its verification line"
        raise ValueError(msg)
    evidence.write_text("report.md", "\n".join(updated) + "\n")


def begin_verification(evidence: EvidenceStore, profile: str) -> str:
    """Invalidate prior evidence and start one identified verification attempt."""
    verification_id = secrets.token_hex(8)
    evidence.write_json(
        "verification.json",
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "run_id": evidence.run_id,
            "verification_id": verification_id,
            "profile": profile,
            "status": "running",
            "commands": [],
        },
    )
    evidence.append_jsonl(
        "commands.jsonl",
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "run_id": evidence.run_id,
            "event": "verification_started",
            "verification_id": verification_id,
            "profile": profile,
        },
    )
    update_verification_report(evidence, status="pending", profile=profile)
    return verification_id


def finish_verification_failure(
    evidence: EvidenceStore,
    *,
    profile: str,
    verification_id: str,
    error: str,
    commands: tuple[CommandResult, ...] = (),
) -> None:
    """Persist a terminal failed attempt after denial, abort, or timeout."""
    evidence.write_json(
        "verification.json",
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "run_id": evidence.run_id,
            "verification_id": verification_id,
            "profile": profile,
            "passed": False,
            "error": error,
            "commands": [command.to_dict() for command in commands],
        },
    )
    update_verification_report(evidence, status="failed", profile=profile)


@contextmanager
def verification_run_lock(evidence: EvidenceStore) -> Iterator[None]:
    """Serialize verification attempts for one evidence run across processes."""
    lock_path = evidence.root / ".verify.lock"
    decision = evidence.permissions.check_write(lock_path, "harness")
    if not decision.allowed:
        msg = "Harness policy denied verification lock path"
        raise PermissionDeniedError(msg)
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    try:
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"{os.getpid()}\n".encode())
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(descriptor)
        msg = f"Harness verification run is already active: {exc}"
        raise PermissionDeniedError(msg) from exc
    try:
        yield
    finally:
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


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
        """Run profile commands sequentially through the required command boundary."""
        if runner is None:
            msg = "ProjectVerifier requires a CommandRunner"
            raise HarnessConfigError(msg)
        commands = self.commands_for(profile)
        verification_id = begin_verification(evidence, profile) if evidence is not None else secrets.token_hex(8)
        results: list[CommandResult] = []
        try:
            for argv in commands:
                result = runner.run(argv, skill_id=skill_id, timeout_seconds=timeout_seconds)
                results.append(result)
                if evidence is not None:
                    evidence.append_jsonl(
                        "commands.jsonl",
                        {
                            "schema_version": EVIDENCE_SCHEMA_VERSION,
                            "run_id": evidence.run_id,
                            "verification_id": verification_id,
                            **result.to_dict(),
                        },
                    )
                if result.exit_code != 0:
                    break
        except (OSError, PermissionDeniedError, subprocess.SubprocessError) as exc:
            if evidence is not None:
                finish_verification_failure(
                    evidence,
                    profile=profile,
                    verification_id=verification_id,
                    error=str(exc),
                    commands=tuple(results),
                )
            raise
        report = VerificationReport(
            profile=profile,
            passed=bool(results) and all(result.exit_code == 0 for result in results),
            commands=tuple(results),
        )
        if evidence is not None:
            evidence.write_json(
                "verification.json",
                {
                    "schema_version": EVIDENCE_SCHEMA_VERSION,
                    "run_id": evidence.run_id,
                    "verification_id": verification_id,
                    **report.to_dict(),
                },
            )
            status = "passed" if report.passed else "failed"
            update_verification_report(evidence, status=status, profile=profile)
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
                        argv=(python, "-m", "ruff", "check", "tools/agent_harness"),
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
                        argv=(python, "-m", "ruff", "check", "src", "tests", "scripts", "tools"),
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
                argv=(python, "-m", "ruff", "check", "src", "tests", "scripts", "tools"),
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


__all__ = [
    "CommandResult",
    "ProjectVerifier",
    "VerificationCheck",
    "VerificationPlan",
    "VerificationReport",
    "begin_verification",
    "finish_verification_failure",
    "update_verification_report",
    "verification_run_lock",
]
