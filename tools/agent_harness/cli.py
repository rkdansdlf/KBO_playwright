"""Command-line interface for the repository-local agent Harness."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from subprocess import SubprocessError
from typing import TYPE_CHECKING

import yaml

from tools.agent_harness.artifact_contract import (
    ArtifactContractError,
    validate_artifact_bundle,
    validate_execution_plan,
)
from tools.agent_harness.context_builder import ContextBuilder
from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION
from tools.agent_harness.evidence import EvidenceStore
from tools.agent_harness.exceptions import HarnessConfigError, PermissionDeniedError
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.planner import build_plan
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.runner import HarnessRunner
from tools.agent_harness.verifier import CommandResult, ProjectVerifier

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from tools.agent_harness.command_runner import CommandRunner
    from tools.agent_harness.golden_tasks import GoldenTaskDataset, GoldenTaskReplay
    from tools.agent_harness.router import RouteDecision

HARNESS_LOAD_ERRORS = (HarnessConfigError, OSError, TypeError, ValueError, yaml.YAMLError)


@dataclass(frozen=True)
class DoctorReport:
    """Summarize local Harness configuration and adapter readiness."""

    status: str
    skill_count: int
    profile_count: int
    issues: tuple[str, ...]
    warnings: tuple[str, ...]
    skills: tuple[dict[str, object], ...]

    def to_dict(self) -> dict[str, object]:
        """Serialize the doctor report."""
        return asdict(self)


def build_parser() -> argparse.ArgumentParser:
    """Build the Harness command parser."""
    parser = argparse.ArgumentParser(description="KBO_playwright AI development Harness.")
    subparsers = parser.add_subparsers(dest="command")

    doctor = subparsers.add_parser("doctor", help="Validate skill locks, adapters, policy, and OpenCode wiring.")
    doctor.add_argument("--json", action="store_true", help="Render machine-readable JSON.")
    doctor.add_argument(
        "--strict",
        action="store_true",
        help="Treat license-gate warnings as failures.",
    )

    plan = subparsers.add_parser("plan", help="Route a task and render its Harness stages.")
    plan.add_argument("task", help="Development task to route.")
    plan.add_argument("--profile", help="Explicit Harness profile override.")
    plan.add_argument("--changed-files", nargs="*", default=[], help="Changed files for file-signal routing.")
    plan.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    route = subparsers.add_parser("route", help="Show task classification and skill selection only.")
    route.add_argument("task", help="Development task to classify.")
    route.add_argument("--profile", help="Explicit Harness profile override.")
    route.add_argument("--changed-files", nargs="*", default=[], help="Changed files for file-signal routing.")
    route.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    replay = subparsers.add_parser("replay", help="Replay operational tasks without running project commands.")
    replay.add_argument("--round", type=int, choices=[1, 2, 3], help="Replay one operational round.")
    replay.add_argument("--task-id", help="Replay one task identifier.")
    replay.add_argument("--metrics-out", help="Write secret-free replay metrics to this artifact path.")
    replay.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    context = subparsers.add_parser("context", help="Manage local Harness context metadata.")
    context.add_argument("action", choices=["refresh"])
    context.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    run = subparsers.add_parser("run", help="Create an auditable Harness task handoff.")
    run.add_argument("task", help="Development task to initialize.")
    run.add_argument("--profile", help="Explicit Harness profile override.")
    run.add_argument("--changed-files", nargs="*", default=[], help="Changed files for file-signal routing.")
    run.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    verify = subparsers.add_parser("verify", help="Run existing project gates for a Harness run.")
    verify.add_argument("run_id", help="Harness run identifier.")
    verify.add_argument(
        "--level",
        choices=["none", "quick", "standard", "full"],
        help="Impact-based verification level (default: run's fixed profile).",
    )
    verify.add_argument("--changed-files", nargs="*", default=[], help="Changed files for impact selection.")
    verify.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    validate = subparsers.add_parser("validate", help="Validate a Harness run artifact contract.")
    validate.add_argument("run_id", help="Harness run identifier.")
    validate.add_argument(
        "--require-verified",
        action="store_true",
        help="Require a final passing verification artifact.",
    )
    validate.add_argument("--json", action="store_true", help="Render machine-readable JSON.")

    report = subparsers.add_parser("report", help="Print a Harness run report.")
    report.add_argument("run_id", help="Harness run identifier.")
    return parser


def _doctor(registry: HarnessRegistry) -> DoctorReport:
    issues = registry.validate()
    warnings: list[str] = []
    skill_path = registry.root / ".agents" / "skills" / "kbo-agent-harness" / "SKILL.md"
    if not skill_path.is_file():
        issues.append("project Harness SKILL.md is missing")

    opencode_path = registry.root / "opencode.json"
    try:
        opencode = json.loads(opencode_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        issues.append(f"invalid opencode.json: {exc}")
    else:
        skills_config = opencode.get("skills", {})
        paths = skills_config.get("paths", []) if isinstance(skills_config, dict) else []
        if ".agents/skills" not in paths:
            issues.append("opencode.json does not register .agents/skills")

    rows: list[dict[str, object]] = []
    for name in registry.stack_skills:
        locked = registry.locked_skills[name]
        adapter = registry.adapters[name]
        if locked.redistribution != "allowed":
            warnings.append(
                f"{name}: {locked.redistribution}; execution remains disabled",
            )
        rows.append(
            {
                "name": name,
                "role": adapter.role,
                "revision": locked.revision,
                "license": locked.license,
                "mode": "reference_only",
                "network": name == "last30days",
            }
        )
    return DoctorReport(
        status="PASS" if not issues else "FAIL",
        skill_count=len(rows),
        profile_count=len(registry.profiles),
        issues=tuple(issues),
        warnings=tuple(warnings),
        skills=tuple(rows),
    )


def _render_plan(payload: dict[str, object]) -> str:
    lines = [
        f"Profile: {payload['profile']}",
        f"Task: {payload['task']}",
        f"External adapters: {payload['external_execution']}",
        "Stages:",
    ]
    stages = payload.get("stages", [])
    if isinstance(stages, list):
        for stage in stages:
            if isinstance(stage, dict):
                name = stage.get("stage", "unknown")
                selected = stage.get("skills", stage.get("profile", ""))
                lines.append(f"- {name}: {selected}")
    return "\n".join(lines) + "\n"


def _write_output(payload: object, *, as_json: bool) -> None:
    if as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    else:
        sys.stdout.write(str(payload))


def _refresh_context(registry: HarnessRegistry, permissions: PermissionPolicy) -> dict[str, object]:
    payload = ContextBuilder(registry, permissions).build()
    target = registry.root / "artifacts" / "agent-harness" / "shared" / "context.json"
    relative = target.relative_to(registry.root)
    decision = permissions.check_write(target, "harness")
    if not decision.allowed:
        msg = f"Harness policy denied context path: {relative}"
        raise PermissionDeniedError(msg)
    target.parent.mkdir(parents=True, exist_ok=True)
    EvidenceStore(target.parent, permissions).write_json(target.name, payload)
    return {**payload, "path": str(relative)}


def _verify_run(
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
    run_id: str,
    level: str | None = None,
    changed_files: list[str] | tuple[str, ...] = (),
) -> dict[str, object]:
    from tools.agent_harness.verifier import verification_run_lock

    artifacts_root = registry.root / "artifacts" / "agent-harness"
    evidence = EvidenceStore.open(artifacts_root, run_id, permissions)
    try:
        with verification_run_lock(evidence):
            return _verify_run_locked(registry, permissions, evidence, level, changed_files)
    except PermissionDeniedError as exc:
        return {"run_id": run_id, "passed": False, "error": str(exc)}


def _verify_run_locked(  # noqa: PLR0911
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
    evidence: EvidenceStore,
    level: str | None,
    changed_files: list[str] | tuple[str, ...],
) -> dict[str, object]:
    from tools.agent_harness.command_runner import CommandRunner

    run_id = evidence.run_id
    try:
        if evidence.needs_current_schema_upgrade():
            evidence.upgrade_current_schema()
    except (OSError, TypeError, ValueError):
        pass
    artifact_report = validate_artifact_bundle(evidence.root)
    if not artifact_report.valid:
        return {
            "run_id": run_id,
            "passed": False,
            "error": "artifact contract failed",
            "issues": list(artifact_report.issues),
        }
    plan_issues = validate_execution_plan(evidence.root, registry)
    if plan_issues:
        return {
            "run_id": run_id,
            "passed": False,
            "error": "execution plan contract failed",
            "issues": list(plan_issues),
        }
    plan = evidence.read_json("plan.json")
    verification_profile = str(plan["verification"])
    runner = CommandRunner(permissions=permissions, root=registry.root)
    try:
        verifier = ProjectVerifier.load(registry.root)
        if level is not None:
            return _verify_level(verifier, evidence, level, changed_files, runner)
        report = verifier.verify(verification_profile, evidence=evidence, skill_id="harness", runner=runner)
    except HARNESS_LOAD_ERRORS as exc:
        return {"run_id": run_id, "passed": False, "error": str(exc)}
    except (OSError, PermissionDeniedError, SubprocessError) as exc:
        return {"run_id": run_id, "passed": False, "error": str(exc)}
    contract = validate_artifact_bundle(evidence.root, require_verified=report.passed)
    if not contract.valid:
        return {
            "run_id": run_id,
            "passed": False,
            "error": "artifact contract failed",
            "issues": list(contract.issues),
        }
    return {"run_id": run_id, "artifact_contract": "passed", **report.to_dict()}


def _verify_level(
    verifier: ProjectVerifier,
    evidence: EvidenceStore,
    level: str,
    changed_files: list[str] | tuple[str, ...],
    runner: CommandRunner,
) -> dict[str, object]:
    from tools.agent_harness.verifier import (
        VerificationReport,
        begin_verification,
        finish_verification_failure,
        update_verification_report,
    )

    run_id = evidence.run_id
    profile = f"level:{level}"
    vplan = verifier.build_plan(level=level, changed_files=changed_files)
    verification_id = begin_verification(evidence, profile)
    results: list[CommandResult] = []
    for check in vplan.checks:
        try:
            result = runner.run(check.argv, skill_id="harness", timeout_seconds=check.timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            error = f"gate '{check.check_id}' timed out after {check.timeout_seconds}s: {exc}"
            finish_verification_failure(
                evidence,
                profile=profile,
                verification_id=verification_id,
                error=error,
                commands=tuple(results),
            )
            return {
                "run_id": run_id,
                "passed": False,
                "timed_out": True,
                "timed_out_check": check.check_id,
                "error": error,
            }
        except (OSError, PermissionDeniedError, SubprocessError) as exc:
            finish_verification_failure(
                evidence,
                profile=profile,
                verification_id=verification_id,
                error=str(exc),
                commands=tuple(results),
            )
            return {"run_id": run_id, "passed": False, "error": str(exc)}
        results.append(result)
        evidence.append_jsonl(
            "commands.jsonl",
            {
                "schema_version": EVIDENCE_SCHEMA_VERSION,
                "run_id": evidence.run_id,
                "verification_id": verification_id,
                **result.to_dict(),
            },
        )
        if result.exit_code != 0 and check.blocking:
            break
    report = VerificationReport(
        profile=profile,
        passed=all(result.exit_code == 0 for result in results),
        commands=tuple(results),
    )
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
    contract = validate_artifact_bundle(evidence.root, require_verified=report.passed)
    if not contract.valid:
        return {
            "run_id": run_id,
            "passed": False,
            "error": "artifact contract failed",
            "issues": list(contract.issues),
        }
    return {"run_id": run_id, "artifact_contract": "passed", **report.to_dict()}


def _handle_doctor(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    _permissions: PermissionPolicy,
) -> int:
    report = _doctor(registry)
    if args.json:
        payload = report.to_dict()
        if args.strict:
            payload = {**payload, "strict": True}
        _write_output(payload, as_json=True)
    else:
        _write_output(
            f"Harness doctor: {report.status} ({report.skill_count} skills, {report.profile_count} profiles)\n"
            + "".join(f"WARNING: {warning}\n" for warning in report.warnings)
            + "".join(f"ERROR: {issue}\n" for issue in report.issues),
            as_json=False,
        )
    if report.status != "PASS":
        return 1
    if args.strict and report.warnings:
        return 1
    return 0


def _render_route(payload: dict[str, object]) -> str:
    lines = [
        f"Profile: {payload['profile']} ({payload['reason']})",
        f"Task: {payload['task']}",
        f"Context: {payload['context']}",
        f"Workflow: {payload['workflow']}",
        f"Guards: {payload['guards']}",
        f"Verification: {payload['verification']}",
        f"Output: {payload['output']}",
    ]
    return "\n".join(lines) + "\n"


def _route_request(args: argparse.Namespace, registry: HarnessRegistry) -> RouteDecision:
    """Build a TaskRequest from CLI args with explicit, files, prompt priority."""
    from tools.agent_harness.dto import TaskRequest

    request = TaskRequest(
        prompt=args.task,
        changed_files=list(args.changed_files or []),
        explicit_profile=args.profile,
    )
    return TaskRouter(registry).route_request(request)


def _handle_route(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    _permissions: PermissionPolicy,
) -> int:
    decision = _route_request(args, registry)
    payload = {"task": args.task, **decision.to_dict()}
    _write_output(payload if args.json else _render_route(payload), as_json=args.json)
    return 0


def _replay_error(args: argparse.Namespace, error: str, prefix: str) -> int:
    if args.json:
        _write_output({"status": "error", "error": error}, as_json=True)
    else:
        sys.stderr.write(f"{prefix}: {error}\n")
    return 2


def _persist_replay_metrics(
    args: argparse.Namespace,
    dataset: GoldenTaskDataset,
    observations: tuple[GoldenTaskReplay, ...],
    permissions: PermissionPolicy,
) -> tuple[Path | None, str | None]:
    if not args.metrics_out:
        return None, None
    from tools.agent_harness.metrics import summarize_replay, write_metrics

    try:
        path = write_metrics(Path(args.metrics_out), summarize_replay(dataset, observations), permissions)
    except (OSError, PermissionDeniedError) as exc:
        return None, str(exc)
    return path, None


def _render_replay(payload: dict[str, object], *, as_json: bool) -> None:
    if as_json:
        _write_output(payload, as_json=True)
        return
    lines = [
        f"Golden task replay: {'PASS' if payload['suite_ok'] else 'FAIL'} ({payload['task_count']} tasks)",
        f"Declared deviations: {len(payload['declared_deviations'])}",
    ]
    lines.extend(
        f"- {row['task_id']}: {row['actual_profile']} "
        f"permission={row['permission_decision']}->{row['routed_permission_decision']}"
        f"({row['routed_permission_skill_id']}) checks={','.join(row['planned_checks']) or 'none'}"
        + (" [declared-deviation]" if row["known_deviation"] else "")
        for row in payload["observations"]
    )
    _write_output("\n".join(lines) + "\n", as_json=False)


def _handle_replay(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    from tools.agent_harness.golden_tasks import load_golden_tasks, replay_dataset

    try:
        dataset = load_golden_tasks(registry.golden_tasks_path)
        observations = replay_dataset(
            dataset,
            registry,
            permissions,
            ProjectVerifier.load(registry.root),
            round_number=args.round,
        )
    except HARNESS_LOAD_ERRORS as exc:
        return _replay_error(args, str(exc), "Golden task replay configuration error")
    if args.task_id:
        observations = tuple(row for row in observations if row.task_id == args.task_id)
    if not observations:
        error = f"No golden tasks matched round={args.round} task_id={args.task_id}"
        if args.json:
            _write_output({"status": "error", "error": error}, as_json=True)
        else:
            sys.stderr.write(f"{error}\n")
        return 1
    task_ids = {row.task_id for row in observations}
    payload = {
        "schema_version": dataset.schema_version,
        "execution": "read_only_replay",
        "round": args.round,
        "task_count": len(observations),
        "suite_ok": all(row.suite_ok for row in observations),
        "declared_deviations": [
            {"task_id": deviation.task_id, "reason": deviation.reason}
            for deviation in dataset.known_deviations
            if deviation.task_id in task_ids
        ],
        "observations": [row.to_dict() for row in observations],
    }
    metrics_path, metrics_error = _persist_replay_metrics(args, dataset, observations, permissions)
    if metrics_error is not None:
        return _replay_error(args, metrics_error, "Replay metrics write failed")
    if metrics_path is not None:
        payload["metrics_path"] = str(metrics_path)
    _render_replay(payload, as_json=args.json)
    return 0 if payload["suite_ok"] else 1


def _handle_plan(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    _permissions: PermissionPolicy,
) -> int:
    decision = _route_request(args, registry)
    payload = build_plan(args.task, decision).to_dict()
    _write_output(payload if args.json else _render_plan(payload), as_json=args.json)
    return 0


def _handle_context(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    try:
        payload = _refresh_context(registry, permissions)
    except PermissionDeniedError as exc:
        if args.json:
            _write_output({"status": "denied", "error": str(exc)}, as_json=True)
        else:
            sys.stderr.write(f"Context refresh denied: {exc}\n")
        return 2
    _write_output(payload if args.json else f"Context refreshed: {payload['path']}\n", as_json=args.json)
    return 0


def _handle_run(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    try:
        run = HarnessRunner(registry, permissions).run(
            args.task, args.profile, changed_files=list(args.changed_files or [])
        )
    except ArtifactContractError as exc:
        # Fail closed with a clean exit code instead of a traceback on a contract violation.
        _write_output(
            {"status": "error", "error": str(exc)} if args.json else f"Harness run failed: {exc}\n",
            as_json=args.json,
        )
        return 2
    payload = {"run_id": run.run_id, "profile": run.profile, "artifact_dir": str(run.artifact_dir)}
    _write_output(payload if args.json else f"Harness run initialized: {run.run_id}\n", as_json=args.json)
    return 0


def _handle_verify(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    payload = _verify_run(registry, permissions, args.run_id, args.level, list(args.changed_files or []))
    # Exit 3 keeps an unfinished gate distinguishable from a real gate failure, so a slow
    # machine is never reported as a test failure. Checked before `error` because profile
    # mode reports a timeout through `timed_out` while level mode also sets `error`.
    if payload.get("timed_out") is True:
        _write_output(
            payload if args.json else f"Verification timed out: {args.run_id}\n",
            as_json=args.json,
        )
        return 3
    if payload.get("error") is not None:
        _write_output(payload if args.json else f"Verification denied: {args.run_id}\n", as_json=args.json)
        return 2
    status = "passed" if payload["passed"] is True else "failed"
    _write_output(payload if args.json else f"Verification {status}: {args.run_id}\n", as_json=args.json)
    return 0 if payload["passed"] is True else 1


def _handle_validate(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    evidence = EvidenceStore.open(
        registry.root / "artifacts" / "agent-harness",
        args.run_id,
        permissions,
    )
    report = validate_artifact_bundle(evidence.root, require_verified=args.require_verified)
    plan_issues = validate_execution_plan(evidence.root, registry)
    issues = (*report.issues, *plan_issues)
    valid = report.valid and not plan_issues
    payload = report.to_dict()
    payload["valid"] = valid
    payload["issues"] = list(issues)
    if args.json:
        _write_output(payload, as_json=True)
    else:
        status = "PASS" if valid else "FAIL"
        _write_output(
            f"Artifact contract: {status}\n" + "".join(f"ERROR: {issue}\n" for issue in issues),
            as_json=False,
        )
    return 0 if valid else 1


def _handle_report(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    evidence = EvidenceStore.open(
        registry.root / "artifacts" / "agent-harness",
        args.run_id,
        permissions,
    )
    try:
        report = evidence.read_text("report.md")
    except (OSError, PermissionDeniedError) as exc:
        sys.stderr.write(f"Harness report unavailable: {exc}\n")
        return 2
    sys.stdout.write(report)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run a Harness CLI command and return its process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    try:
        registry = HarnessRegistry.load()
        permissions = PermissionPolicy.load(registry.root)
    except HARNESS_LOAD_ERRORS as exc:
        if getattr(args, "json", False):
            _write_output({"status": "error", "error": str(exc)}, as_json=True)
        else:
            sys.stderr.write(f"Harness configuration error: {exc}\n")
        return 2
    handlers: dict[str, Callable[[argparse.Namespace, HarnessRegistry, PermissionPolicy], int]] = {
        "doctor": _handle_doctor,
        "plan": _handle_plan,
        "route": _handle_route,
        "replay": _handle_replay,
        "context": _handle_context,
        "run": _handle_run,
        "verify": _handle_verify,
        "validate": _handle_validate,
        "report": _handle_report,
    }
    handler = handlers.get(args.command)
    if handler is None:
        parser.error(f"Unsupported command: {args.command}")
    return handler(args, registry, permissions)


__all__ = ["DoctorReport", "build_parser", "main"]
