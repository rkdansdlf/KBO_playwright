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
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from tools.agent_harness.command_runner import CommandRunner
    from tools.agent_harness.evidence import EvidenceStore


def _load_gate_policies(
    payload: dict[str, object],
    section: str,
    levels: dict[str, GatePolicy] | None = None,
) -> dict[str, GatePolicy]:
    """Load one gate-policy table, letting a profile inherit its level's gates."""
    rows = _mapping(payload.get(section), f"verification.{section}")
    policies: dict[str, GatePolicy] = {}
    for name, raw in rows.items():
        row = _mapping(raw, f"verification.{section}.{name}")
        label = f"{section}.{name}"
        raw_gates = row.get("gates")
        gates = () if raw_gates is None else tuple(str(gate) for gate in _string_list(raw_gates, f"{label}.gates"))
        level = row.get("level")
        inherited = levels.get(str(level)) if levels is not None and level is not None else None
        if level is not None and inherited is None:
            msg = f"verification.{section}.{name} references unknown level: {level}"
            raise ValueError(msg)
        conditional = inherited.conditional_gates if inherited is not None else ()
        if gates:
            declared = _mapping(row.get("conditional_gates", {}), f"verification.{label}.conditional_gates")
            conditional = tuple((str(gate), str(predicate)) for gate, predicate in declared.items())
        always_gates = tuple(str(gate) for gate in _string_list(row.get("always_gates", []), f"{label}.always_gates"))
        extra_targets = _string_list(row.get("extra_pytest_targets", []), f"{label}.extra_pytest_targets")
        policies[name] = GatePolicy(
            gates=gates or (inherited.gates if inherited is not None else ()),
            conditional_gates=conditional,
            always_gates=always_gates,
            extra_pytest_targets=tuple(str(target) for target in extra_targets),
            level=str(level) if level is not None else None,
            description=str(row.get("description", "")),
        )
    _validate_gate_ids(section, policies)
    return policies


def _string_list(value: object, label: str) -> list[str]:
    """Require a YAML list of strings."""
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        msg = f"Expected string list for {label}"
        raise ValueError(msg)
    return value


def _validate_gate_ids(section: str, policies: dict[str, GatePolicy]) -> None:
    """Fail fast when policy names a gate or predicate the catalog cannot build."""
    for name, policy in policies.items():
        for gate_id in (*policy.gates, *policy.always_gates, *(gate for gate, _ in policy.conditional_gates)):
            if gate_id not in GATE_CATALOG:
                msg = f"verification.{section}.{name} references unknown gate: {gate_id}"
                raise ValueError(msg)
        for _, predicate in policy.conditional_gates:
            if predicate not in CONDITION_PREDICATES:
                msg = f"verification.{section}.{name} references unknown predicate: {predicate}"
                raise ValueError(msg)


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
class GatePolicy:
    """Describe the gates one verification level or profile requires."""

    gates: tuple[str, ...]
    conditional_gates: tuple[tuple[str, str], ...] = ()
    always_gates: tuple[str, ...] = ()
    extra_pytest_targets: tuple[str, ...] = ()
    level: str | None = None
    description: str = ""


DEFAULT_TIMEOUT_SECONDS = 300
PYTEST_FULL_TIMEOUT_SECONDS = 900
MYPY_SCOPED_TIMEOUT_SECONDS = 600
CRAWLER_GATE_CONFIG = "Docs/references/crawler_selector_gate.json"
CRAWLER_GATE_TEST_TARGET = "tests/monitoring/test_crawler_selector_gate.py"


def _python_module(*tokens: str) -> tuple[str, ...]:
    """Return an argv that runs an allowlisted module under the Harness interpreter."""
    return (sys.executable, "-m", *tokens)


PYTHON_MODULE_FLAG_INDEX = 1
PYTHON_MODULE_NAME_INDEX = 2


def _gate_name(argv: tuple[str, ...]) -> str:
    """Return a readable gate label for a `-m <module>` argv, else the whole command."""
    if len(argv) > PYTHON_MODULE_NAME_INDEX and argv[PYTHON_MODULE_FLAG_INDEX] == "-m":
        return argv[PYTHON_MODULE_NAME_INDEX]
    return " ".join(argv)


def _needs_crawler_gate(changed_files: tuple[str, ...]) -> bool:
    """Return whether crawler selector contracts are affected by these files."""
    from tools.agent_harness.project_adapter import KBOProjectAdapter

    return KBOProjectAdapter().needs_crawler_gate(changed_files)


@dataclass(frozen=True)
class _GateContext:
    """Carry the inputs a gate builder needs to assemble argv."""

    changed_files: tuple[str, ...] = ()
    extra_pytest_targets: tuple[str, ...] = ()

    @property
    def pytest_targets(self) -> tuple[str, ...]:
        """Return affected pytest targets extended with any profile-specific targets."""
        from tools.agent_harness.project_adapter import KBOProjectAdapter

        targets = list(KBOProjectAdapter().pytest_targets(self.changed_files))
        targets.extend(target for target in self.extra_pytest_targets if target not in targets)
        return tuple(targets)


def _build_pytest_affected(ctx: _GateContext) -> VerificationCheck:
    """Run pytest against the affected test targets."""
    return VerificationCheck(
        check_id="pytest-affected",
        argv=_python_module("pytest", *ctx.pytest_targets, "-q"),
    )


def _build_pytest_full(_ctx: _GateContext) -> VerificationCheck:
    """Run the entire test suite with a timeout sized for the documented baseline."""
    return VerificationCheck(
        check_id="pytest-full",
        argv=_python_module("pytest", "-q"),
        timeout_seconds=PYTEST_FULL_TIMEOUT_SECONDS,
    )


def _build_pytest_crawler_gate(ctx: _GateContext) -> VerificationCheck:
    """Run the affected tests plus the crawler selector gate's own test module."""
    targets = (*ctx.pytest_targets, CRAWLER_GATE_TEST_TARGET)
    return VerificationCheck(check_id="pytest-crawler-gate", argv=_python_module("pytest", *targets, "-q"))


def _build_ruff_changed(_ctx: _GateContext) -> VerificationCheck:
    """Lint the Harness control plane only."""
    return VerificationCheck(check_id="ruff-changed", argv=_python_module("ruff", "check", "tools/agent_harness"))


def _build_ruff_project(_ctx: _GateContext) -> VerificationCheck:
    """Lint the whole repository scope used by CI."""
    return VerificationCheck(
        check_id="ruff-project",
        argv=_python_module("ruff", "check", "src", "tests", "scripts", "tools"),
    )


def _build_format_check(_ctx: _GateContext) -> VerificationCheck:
    """Verify formatter compliance across the repository scope used by CI."""
    return VerificationCheck(
        check_id="format-check",
        argv=_python_module("ruff", "format", "--check", "src", "tests", "scripts", "tools"),
    )


def _build_mypy_scoped(_ctx: _GateContext) -> VerificationCheck:
    """Run the certified-clean scoped mypy gate through its allowlisted module entrypoint."""
    return VerificationCheck(
        check_id="mypy-scoped",
        argv=_python_module("scripts.check_mypy_scoped"),
        timeout_seconds=MYPY_SCOPED_TIMEOUT_SECONDS,
    )


def _build_crawler_gate(_ctx: _GateContext) -> VerificationCheck:
    """Validate crawler selector contracts against fixtures."""
    return VerificationCheck(
        check_id="crawler-gate",
        argv=_python_module(
            "src.cli.crawler_selector_gate",
            "--config",
            CRAWLER_GATE_CONFIG,
            "--json",
        ),
    )


def _build_doctor(_ctx: _GateContext) -> VerificationCheck:
    """Validate the Harness manifest, policies, and golden datasets."""
    return VerificationCheck(check_id="doctor", argv=_python_module("tools.agent_harness", "doctor", "--json"))


GATE_CATALOG: dict[str, Callable[[_GateContext], VerificationCheck]] = {
    "pytest-affected": _build_pytest_affected,
    "pytest-crawler-gate": _build_pytest_crawler_gate,
    "pytest-full": _build_pytest_full,
    "ruff-changed": _build_ruff_changed,
    "ruff-project": _build_ruff_project,
    "format-check": _build_format_check,
    "mypy-scoped": _build_mypy_scoped,
    "crawler-gate": _build_crawler_gate,
    "doctor": _build_doctor,
}

CONDITION_PREDICATES: dict[str, Callable[[tuple[str, ...]], bool]] = {
    "crawler_changed": _needs_crawler_gate,
}


def _build_gate(gate_id: str, ctx: _GateContext) -> VerificationCheck:
    """Build one gate from the catalog, failing loudly on an unknown id."""
    builder = GATE_CATALOG.get(gate_id)
    if builder is None:
        msg = f"Unknown verification gate: {gate_id} (known: {', '.join(sorted(GATE_CATALOG))})"
        raise ValueError(msg)
    return builder(ctx)


def _resolve_gates(policy: GatePolicy, ctx: _GateContext) -> tuple[VerificationCheck, ...]:
    """Expand a gate policy into ordered checks, applying conditional predicates once.

    A profile may declare a gate unconditionally that its inherited level also declares
    conditionally, so gate ids are de-duplicated while preserving declaration order.
    """
    ordered: list[str] = []
    for gate_id in (*policy.gates, *policy.always_gates, *(gate for gate, _ in policy.conditional_gates)):
        predicate_name = next(
            (name for gate, name in policy.conditional_gates if gate == gate_id),
            None,
        )
        if predicate_name is not None:
            predicate = CONDITION_PREDICATES.get(predicate_name)
            if predicate is None:
                msg = f"Unknown conditional gate predicate: {predicate_name}"
                raise ValueError(msg)
            if not predicate(ctx.changed_files):
                continue
        if gate_id not in ordered:
            ordered.append(gate_id)
    return tuple(_build_gate(gate_id, ctx) for gate_id in ordered)


@dataclass(frozen=True)
class VerificationReport:
    """Summarize all commands in one verification profile."""

    profile: str
    passed: bool
    commands: tuple[CommandResult, ...]
    timed_out: bool = False
    timed_out_check: str = ""

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
    levels: dict[str, GatePolicy]
    profiles: dict[str, GatePolicy]

    @classmethod
    def load(cls, root: Path | None = None) -> ProjectVerifier:
        """Load gate policies for every verification level and profile."""
        repo_root = (root or project_root()).resolve()
        payload = load_yaml_mapping(repo_root / ".agent-harness" / "policies" / "verification.yaml")
        levels = _load_gate_policies(payload, "levels")
        profiles = _load_gate_policies(payload, "profiles", levels)
        if not levels:
            msg = "verification policy declares no levels"
            raise HarnessConfigError(msg)
        return cls(root=repo_root, levels=levels, profiles=profiles)

    def policy_for(self, name: str) -> GatePolicy:
        """Return the level or profile policy, reporting known names on failure."""
        for table in (self.levels, self.profiles):
            policy = table.get(name)
            if policy is not None:
                return policy
        msg = f"Unknown verification level or profile: {name}"
        raise ValueError(msg)

    def commands_for(self, profile: str) -> tuple[tuple[str, ...], ...]:
        """Return immutable argv commands for a known profile."""
        return self.commands_for_profile(profile)

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
        except subprocess.TimeoutExpired as exc:
            if evidence is not None:
                finish_verification_failure(
                    evidence,
                    profile=profile,
                    verification_id=verification_id,
                    error=f"gate timed out after {timeout_seconds}s: {exc}",
                    commands=tuple(results),
                )
            return VerificationReport(
                profile=profile,
                passed=False,
                commands=tuple(results),
                timed_out=True,
                timed_out_check=_gate_name(argv),
            )
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
        """Build an impact-based verification plan for one level declared in policy."""
        policy = self.levels.get(level)
        if policy is None:
            msg = f"Unknown verification level: {level} (known: {', '.join(sorted(self.levels))})"
            raise ValueError(msg)
        context = _GateContext(changed_files=tuple(changed_files))
        return VerificationPlan(level=level, checks=_resolve_gates(policy, context))

    def build_profile_plan(
        self,
        profile: str,
        changed_files: list[str] | tuple[str, ...] = (),
    ) -> VerificationPlan:
        """Build the gate plan for a named verification profile, inheriting its level."""
        policy = self.profiles.get(profile)
        if policy is None:
            msg = f"Unknown verification profile: {profile} (known: {', '.join(sorted(self.profiles))})"
            raise ValueError(msg)
        context = _GateContext(
            changed_files=tuple(changed_files),
            extra_pytest_targets=policy.extra_pytest_targets,
        )
        return VerificationPlan(level=profile, checks=_resolve_gates(policy, context))

    def commands_for_profile(
        self,
        profile: str,
        changed_files: list[str] | tuple[str, ...] = (),
    ) -> tuple[tuple[str, ...], ...]:
        """Return the argv a verification profile runs through the shared gate catalog."""
        return tuple(check.argv for check in self.build_profile_plan(profile, changed_files).checks)


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
