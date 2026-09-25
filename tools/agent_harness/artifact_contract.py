"""Validate Harness evidence completeness, schema, and cross-file consistency."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION, TaskRequest, gate_verdict
from tools.agent_harness.exceptions import ArtifactContractError

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from tools.agent_harness.registry import HarnessRegistry

REQUIRED_ARTIFACTS = (
    "task.json",
    "plan.json",
    "context.json",
    "skill-trace.jsonl",
    "commands.jsonl",
    "verification.json",
    "report.md",
)
SUPPORTED_SCHEMA_VERSIONS = {"1", EVIDENCE_SCHEMA_VERSION}


def _safe_file(path: Path) -> bool:
    """Return whether a path is a regular, single-link, non-symlink artifact file."""
    if not path.is_file() or path.is_symlink():
        return False
    try:
        return path.stat().st_nlink == 1
    except OSError:
        return False


@dataclass(frozen=True)
class ArtifactContractReport:
    """Summarize one evidence bundle contract check."""

    run_id: str
    valid: bool
    require_verified: bool
    required_files: tuple[str, ...]
    issues: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        """Serialize the artifact contract report."""
        return {
            "run_id": self.run_id,
            "valid": self.valid,
            "require_verified": self.require_verified,
            "required_files": list(self.required_files),
            "issues": list(self.issues),
        }


@dataclass(frozen=True)
class _ArtifactState:
    task: dict[str, object] | None
    plan: dict[str, object] | None
    context: dict[str, object] | None
    verification: dict[str, object] | None
    trace: tuple[dict[str, object], ...]
    commands: tuple[dict[str, object], ...]


def _load_json(path: Path, issues: list[str]) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        issues.append(f"{path.name}: invalid JSON: {exc}")
        return None
    if not isinstance(payload, dict):
        issues.append(f"{path.name}: expected JSON object")
        return None
    return payload


def _check_identity(
    payload: Mapping[str, object],
    label: str,
    run_id: str,
    issues: list[str],
    *,
    require_current: bool = False,
) -> None:
    schema_version = payload.get("schema_version")
    if schema_version is not None and schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        issues.append(f"{label}: unsupported schema_version {schema_version!r}")
    if require_current and schema_version != EVIDENCE_SCHEMA_VERSION:
        issues.append(f"{label}: current bundle requires schema_version {EVIDENCE_SCHEMA_VERSION}")
    if schema_version == EVIDENCE_SCHEMA_VERSION and payload.get("run_id") != run_id:
        issues.append(f"{label}: run_id does not match artifact directory")


def _load_jsonl(
    path: Path,
    run_id: str,
    issues: list[str],
    *,
    require_current: bool = False,
) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        issues.append(f"{path.name}: cannot read JSONL: {exc}")
        return ()
    for line_number, line in enumerate(lines, start=1):
        label = f"{path.name}:{line_number}"
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            issues.append(f"{label}: invalid JSON: {exc}")
            continue
        if not isinstance(payload, dict):
            issues.append(f"{label}: expected JSON object")
            continue
        _check_identity(payload, label, run_id, issues, require_current=require_current)
        records.append(payload)
    return tuple(records)


def _check_command_record(payload: Mapping[str, object], label: str, issues: list[str]) -> bool:
    argv = payload.get("argv")
    exit_code = payload.get("exit_code")
    duration_ms = payload.get("duration_ms")
    valid = True
    if not isinstance(argv, list) or not all(isinstance(token, str) for token in argv):
        issues.append(f"{label}: argv must be a string list")
        valid = False
    if not isinstance(exit_code, int) or isinstance(exit_code, bool):
        issues.append(f"{label}: exit_code must be an integer")
        valid = False
    if not isinstance(duration_ms, (int, float)) or isinstance(duration_ms, bool):
        issues.append(f"{label}: duration_ms must be numeric")
        valid = False
    if not isinstance(payload.get("stdout"), str) or not isinstance(payload.get("stderr"), str):
        issues.append(f"{label}: stdout and stderr must be strings")
        valid = False
    return valid


def _check_non_final_verification(
    payload: Mapping[str, object],
    label: str,
    issues: list[str],
    *,
    require_verified: bool,
) -> bool:
    status = payload.get("status")
    if status not in {"pending", "running"}:
        return False
    if payload.get("commands") != []:
        issues.append(f"{label}: non-final verification must have an empty command list")
    if require_verified:
        issues.append(f"{label}: verification is {status}")
    return True


def _expected_verification_result(verification: dict[str, object]) -> bool:
    """Recompute the verdict from the recorded gates using the shared rule.

    This previously matched on the literal string ``level:none`` to decide whether an
    empty command list was a pass, which coupled the contract to one caller's naming and
    disagreed with the profile-mode runner. The recorded ``gate_ids`` now carry the fact.
    """
    commands = verification.get("commands")
    if not isinstance(commands, list):
        return False
    exit_codes = [int(command.get("exit_code", 1)) for command in commands if isinstance(command, dict)]
    declared = verification.get("gate_ids")
    if not isinstance(declared, list):
        # A bundle written before gate ids were recorded cannot prove how many gates the
        # policy declared, so fall back to the weaker historical rule instead of
        # rejecting evidence that was legitimately produced. Such a bundle is not
        # evidence of a complete run, only of a passing one.
        return bool(exit_codes) and all(code == 0 for code in exit_codes)
    return gate_verdict(
        declared_gate_ids=(str(item) for item in declared),
        exit_codes=exit_codes,
    )


def _check_verification(
    payload: dict[str, object],
    label: str,
    issues: list[str],
    *,
    require_verified: bool,
) -> None:
    if _check_non_final_verification(payload, label, issues, require_verified=require_verified):
        return
    if "passed" not in payload:
        issues.append(f"{label}: expected non-final status or final passed result")
        return
    passed = payload.get("passed")
    profile = payload.get("profile")
    commands = payload.get("commands")
    commands_valid = isinstance(commands, list) and all(isinstance(item, dict) for item in commands)
    if not isinstance(passed, bool):
        issues.append(f"{label}: passed must be boolean")
    if not isinstance(profile, str) or not profile:
        issues.append(f"{label}: profile must be a non-empty string")
    if not commands_valid:
        issues.append(f"{label}: commands must be an object list")
        return
    for index, command in enumerate(commands):
        _check_command_record(command, f"{label}.commands.{index}", issues)
    if isinstance(passed, bool) and passed is not _expected_verification_result(payload):
        issues.append(f"{label}: passed flag does not match command exit codes")
    if require_verified and passed is not True:
        issues.append(f"{label}: verification passed flag is false")


def _check_required_files(artifact_dir: Path, issues: list[str]) -> None:
    if artifact_dir.is_symlink() or not artifact_dir.is_dir():
        issues.append("artifact directory is missing or is a symbolic link")
    issues.extend(
        f"required artifact is missing or unsafe: {name}"
        for name in REQUIRED_ARTIFACTS
        if not _safe_file(artifact_dir / name)
    )


def _load_state(
    artifact_dir: Path,
    run_id: str,
    issues: list[str],
    *,
    require_current: bool = False,
) -> _ArtifactState:
    def load_json(name: str) -> dict[str, object] | None:
        path = artifact_dir / name
        return _load_json(path, issues) if _safe_file(path) else None

    trace_path = artifact_dir / "skill-trace.jsonl"
    commands_path = artifact_dir / "commands.jsonl"
    return _ArtifactState(
        task=load_json("task.json"),
        plan=load_json("plan.json"),
        context=load_json("context.json"),
        verification=load_json("verification.json"),
        trace=(
            _load_jsonl(trace_path, run_id, issues, require_current=require_current) if _safe_file(trace_path) else ()
        ),
        commands=(
            _load_jsonl(commands_path, run_id, issues, require_current=require_current)
            if _safe_file(commands_path)
            else ()
        ),
    )


def _check_documents(
    state: _ArtifactState,
    run_id: str,
    issues: list[str],
    *,
    require_current: bool,
) -> None:
    for label, payload in (
        ("task.json", state.task),
        ("plan.json", state.plan),
        ("context.json", state.context),
    ):
        if payload is not None:
            _check_identity(payload, label, run_id, issues, require_current=require_current)
    context = state.context
    if context is not None:
        if context.get("external_execution") != "reference_only":
            issues.append("context.json: external_execution must remain reference_only")
        if context.get("secrets_included") is not False:
            issues.append("context.json: secrets_included must be false")
    if state.plan is not None and state.plan.get("external_execution") != "reference_only":
        issues.append("plan.json: external_execution must remain reference_only")


def _requires_current_schema(state: _ArtifactState) -> bool:
    """Return whether any structured record marks the bundle as current evidence."""
    payloads = (state.task, state.plan, state.context, state.verification, *state.trace, *state.commands)
    return any(payload is not None and payload.get("schema_version") == EVIDENCE_SCHEMA_VERSION for payload in payloads)


def _check_streams(state: _ArtifactState, issues: list[str], *, require_current: bool) -> None:
    if not state.trace:
        issues.append("skill-trace.jsonl: at least one trace record is required")
    for index, record in enumerate(state.commands):
        label = f"commands.jsonl:{index + 1}"
        if record.get("event") == "verification_started":
            if not record.get("verification_id") or not record.get("profile"):
                issues.append(f"{label}: verification start requires verification_id and profile")
            continue
        if record.get("event") is not None:
            issues.append(f"{label}: unknown commands.jsonl event")
            continue
        _check_command_record(record, label, issues)
        if require_current and not record.get("verification_id"):
            issues.append(f"{label}: current command requires verification_id")


def _command_evidence(record: Mapping[str, object]) -> tuple[object, ...]:
    return (
        record.get("argv"),
        record.get("exit_code"),
        record.get("duration_ms"),
        record.get("stdout"),
        record.get("stderr"),
    )


def _check_verification_attempt(state: _ArtifactState, issues: list[str], *, require_current: bool) -> None:
    verification = state.verification
    if verification is None or "passed" not in verification:
        return
    verification_id = verification.get("verification_id")
    if not isinstance(verification_id, str) or not verification_id:
        if require_current:
            issues.append("verification.json: current final verification requires verification_id")
        return
    commands = verification.get("commands")
    if not isinstance(commands, list):
        return
    started = [
        str(record.get("verification_id")) for record in state.commands if record.get("event") == "verification_started"
    ]
    if require_current and (not started or started[-1] != verification_id):
        issues.append("verification.json: final verification is not the latest started attempt")
    recorded = [
        record
        for record in state.commands
        if record.get("verification_id") == verification_id and record.get("event") is None
    ]
    expected = [_command_evidence(command) for command in commands if isinstance(command, dict)]
    actual = [_command_evidence(record) for record in recorded]
    if actual != expected:
        issues.append("commands.jsonl: records differ from final verification attempt")


def _check_trace_stages(plan: dict[str, object], trace: tuple[dict[str, object], ...], issues: list[str]) -> None:
    raw_stages = plan.get("stages", [])
    if not isinstance(raw_stages, list):
        issues.append("plan.json: stages must be a list")
        return
    planned = {str(stage.get("stage")) for stage in raw_stages if isinstance(stage, dict) and stage.get("stage")}
    traced = {str(record.get("stage")) for record in trace if record.get("stage") and "execution" in record}
    missing = planned - traced
    if missing:
        issues.append(f"skill-trace.jsonl: missing planned stages: {', '.join(sorted(missing))}")


def _check_consistency(state: _ArtifactState, issues: list[str]) -> None:
    if state.task is not None and state.plan is not None:
        if state.task.get("profile") != state.plan.get("profile"):
            issues.append("task.json and plan.json profile values differ")
        if state.task.get("task") != state.plan.get("task"):
            issues.append("task.json and plan.json task values differ")
    plan = state.plan
    verification = state.verification
    if plan is not None and verification is not None and "passed" in verification:
        actual = str(verification.get("profile", ""))
        planned = str(plan.get("verification", ""))
        if actual != planned and not actual.startswith("level:"):
            issues.append("plan.json and verification.json profile values differ")
    if plan is not None and state.trace:
        _check_trace_stages(plan, state.trace, issues)


def _check_report(
    artifact_dir: Path,
    state: _ArtifactState,
    run_id: str,
    issues: list[str],
    *,
    require_verified: bool,
) -> None:
    path = artifact_dir / "report.md"
    if not _safe_file(path):
        return
    try:
        report = path.read_text(encoding="utf-8")
    except OSError as exc:
        issues.append(f"report.md: cannot read report: {exc}")
        return
    if run_id not in report:
        issues.append("report.md: run ID is missing")
    profile = str((state.task or {}).get("profile", ""))
    if profile and f"- Profile: `{profile}`" not in report:
        issues.append("report.md: profile does not match task.json")
    verification = state.verification
    if verification is not None:
        profile = verification.get("profile")
        passed = verification.get("passed")
        if isinstance(profile, str) and isinstance(passed, bool):
            status = "passed" if passed else "failed"
        elif profile and verification.get("status") in {"pending", "running"}:
            status = "pending"
        else:
            status = ""
        if profile and status and f"- Verification: `{profile}` ({status})" not in report:
            issues.append("report.md: verification profile or status does not match verification.json")
    elif require_verified and "passed)" not in report:
        issues.append("report.md: successful verification status is missing")


def validate_artifact_bundle(artifact_dir: Path, *, require_verified: bool = False) -> ArtifactContractReport:
    """Validate required files, supported schemas, and cross-artifact consistency."""
    run_id = artifact_dir.name
    issues: list[str] = []
    _check_required_files(artifact_dir, issues)
    if artifact_dir.is_symlink() or not artifact_dir.is_dir():
        state = _ArtifactState(None, None, None, None, (), ())
    else:
        state = _load_state(artifact_dir, run_id, issues)
    require_current = _requires_current_schema(state)
    if require_verified and not require_current:
        issues.append("verified contract requires a current schema bundle")
    _check_documents(state, run_id, issues, require_current=require_current)
    for index, record in enumerate((*state.trace, *state.commands), start=1):
        _check_identity(
            record,
            f"jsonl:{index}",
            run_id,
            issues,
            require_current=require_current,
        )
    _check_streams(state, issues, require_current=require_current)
    if state.verification is not None:
        _check_identity(
            state.verification,
            "verification.json",
            run_id,
            issues,
            require_current=require_current,
        )
        _check_verification(
            state.verification,
            "verification.json",
            issues,
            require_verified=require_verified,
        )
    _check_consistency(state, issues)
    _check_verification_attempt(state, issues, require_current=require_current)
    _check_report(
        artifact_dir,
        state,
        run_id,
        issues,
        require_verified=require_verified,
    )
    return ArtifactContractReport(
        run_id=run_id,
        valid=not issues,
        require_verified=require_verified,
        required_files=REQUIRED_ARTIFACTS,
        issues=tuple(issues),
    )


def _task_request_from_artifact(task: dict[str, object], issues: list[str]) -> TaskRequest | None:
    prompt = task.get("task")
    changed_files = task.get("changed_files", [])
    explicit_profile = task.get("explicit_profile")
    if not isinstance(prompt, str):
        issues.append("task.json: task must be a string")
        return None
    if not isinstance(changed_files, list) or not all(isinstance(path, str) for path in changed_files):
        issues.append("task.json: changed_files must be a string list")
        return None
    if explicit_profile is not None and not isinstance(explicit_profile, str):
        issues.append("task.json: explicit_profile must be a string or null")
        return None
    return TaskRequest(
        prompt=prompt,
        changed_files=list(changed_files),
        explicit_profile=explicit_profile,
    )


def validate_execution_plan(artifact_dir: Path, registry: HarnessRegistry) -> tuple[str, ...]:
    """Return mismatches between persisted task inputs and the recomputed route plan."""
    from tools.agent_harness.planner import build_plan
    from tools.agent_harness.router import TaskRouter

    issues: list[str] = []
    if artifact_dir.is_symlink() or not artifact_dir.is_dir():
        issues.append("artifact directory is missing or is a symbolic link")
        return tuple(issues)
    state = _load_state(artifact_dir, artifact_dir.name, issues)
    if state.task is None or state.plan is None:
        issues.append("execution plan requires task.json and plan.json")
        return tuple(issues)
    request = _task_request_from_artifact(state.task, issues)
    if request is None:
        return tuple(issues)
    decision = TaskRouter(registry).route_request(request)
    expected = build_plan(request.prompt, decision)
    expected_values = {
        "profile": expected.profile,
        "verification": expected.verification,
        "output": expected.output,
        "external_execution": expected.external_execution,
        "reason": expected.reason,
        "stages": list(expected.stages),
    }
    for field, expected_value in expected_values.items():
        if state.plan.get(field) != expected_value:
            issues.append(f"plan.json: {field} differs from recomputed routing plan")
    if state.task.get("profile") != expected.profile:
        issues.append("task.json: profile differs from recomputed routing plan")
    return tuple(issues)


def assert_artifact_contract(artifact_dir: Path, *, require_verified: bool = False) -> ArtifactContractReport:
    """Return a valid report or raise a typed artifact contract error."""
    report = validate_artifact_bundle(artifact_dir, require_verified=require_verified)
    if not report.valid:
        msg = "Invalid Harness artifact contract: " + "; ".join(report.issues)
        raise ArtifactContractError(msg)
    return report


__all__ = [
    "REQUIRED_ARTIFACTS",
    "SUPPORTED_SCHEMA_VERSIONS",
    "ArtifactContractReport",
    "assert_artifact_contract",
    "validate_artifact_bundle",
    "validate_execution_plan",
]
