"""Command-line interface for the repository-local agent Harness."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.context_builder import ContextBuilder
from tools.agent_harness.evidence import EvidenceStore
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.planner import build_plan
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.runner import HarnessRunner
from tools.agent_harness.verifier import ProjectVerifier

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from tools.agent_harness.command_runner import CommandRunner
    from tools.agent_harness.router import RouteDecision


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
    if not permissions.can_write(relative):
        msg = f"Harness policy denied context path: {relative}"
        raise PermissionError(msg)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".json.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(target)
    return {**payload, "path": str(relative)}


def _verify_run(
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
    run_id: str,
    level: str | None = None,
    changed_files: list[str] | tuple[str, ...] = (),
) -> dict[str, object]:
    from tools.agent_harness.command_runner import CommandRunner
    from tools.agent_harness.exceptions import PermissionDeniedError

    artifacts_root = registry.root / "artifacts" / "agent-harness"
    evidence = EvidenceStore.open(artifacts_root, run_id, permissions)
    plan = json.loads((evidence.root / "plan.json").read_text(encoding="utf-8"))
    verification_profile = str(plan["verification"])
    runner = CommandRunner(permissions=permissions, root=registry.root)
    if level is not None:
        verifier = ProjectVerifier.load(registry.root)
        return _verify_level(verifier, evidence, level, changed_files, runner)
    try:
        report = ProjectVerifier.load(registry.root).verify(
            verification_profile, evidence=evidence, skill_id="harness", runner=runner
        )
    except PermissionDeniedError as exc:
        return {"run_id": run_id, "passed": False, "error": str(exc)}
    status = "passed" if report.passed else "failed"
    report_path = evidence.root / "report.md"
    current = report_path.read_text(encoding="utf-8")
    updated = current.replace(f"`{verification_profile}` (pending)", f"`{verification_profile}` ({status})")
    evidence.write_text("report.md", updated)
    return {"run_id": run_id, **report.to_dict()}


def _verify_level(
    verifier: ProjectVerifier,
    evidence: EvidenceStore,
    level: str,
    changed_files: list[str] | tuple[str, ...],
    runner: CommandRunner,
) -> dict[str, object]:
    from tools.agent_harness.exceptions import PermissionDeniedError
    from tools.agent_harness.verifier import VerificationReport

    run_id = evidence.run_id
    vplan = verifier.build_plan(level=level, changed_files=changed_files)
    results = []
    for check in vplan.checks:
        try:
            result = runner.run(check.argv, skill_id="harness", timeout_seconds=check.timeout_seconds)
        except PermissionDeniedError as exc:
            return {"run_id": run_id, "passed": False, "error": str(exc)}
        results.append(result)
        evidence.append_jsonl("commands.jsonl", result.to_dict())
        if result.exit_code != 0 and check.blocking:
            break
    report = VerificationReport(
        profile=f"level:{level}",
        passed=all(result.exit_code == 0 for result in results),
        commands=tuple(results),
    )
    evidence.write_json("verification.json", report.to_dict())
    status = "passed" if report.passed else "failed"
    report_path = evidence.root / "report.md"
    current = report_path.read_text(encoding="utf-8")
    evidence.write_text("report.md", current.replace("(pending)", f"(level:{level} {status})"))
    return {"run_id": run_id, **report.to_dict()}


def _handle_doctor(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    _permissions: PermissionPolicy,
) -> int:
    report = _doctor(registry)
    if args.json:
        _write_output(report.to_dict(), as_json=True)
    else:
        _write_output(
            f"Harness doctor: {report.status} ({report.skill_count} skills, {report.profile_count} profiles)\n"
            + "".join(f"WARNING: {warning}\n" for warning in report.warnings)
            + "".join(f"ERROR: {issue}\n" for issue in report.issues),
            as_json=False,
        )
    return 0 if report.status == "PASS" else 1


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
    payload = _refresh_context(registry, permissions)
    _write_output(payload if args.json else f"Context refreshed: {payload['path']}\n", as_json=args.json)
    return 0


def _handle_run(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    run = HarnessRunner(registry, permissions).run(
        args.task, args.profile, changed_files=list(args.changed_files or [])
    )
    payload = {"run_id": run.run_id, "profile": run.profile, "artifact_dir": str(run.artifact_dir)}
    _write_output(payload if args.json else f"Harness run initialized: {run.run_id}\n", as_json=args.json)
    return 0


def _handle_verify(
    args: argparse.Namespace,
    registry: HarnessRegistry,
    permissions: PermissionPolicy,
) -> int:
    payload = _verify_run(registry, permissions, args.run_id, args.level, list(args.changed_files or []))
    if payload.get("error") is not None:
        _write_output(payload if args.json else f"Verification denied: {args.run_id}\n", as_json=args.json)
        return 2
    _write_output(payload if args.json else f"Verification passed: {args.run_id}\n", as_json=args.json)
    return 0 if payload["passed"] is True else 1


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
    sys.stdout.write((evidence.root / "report.md").read_text(encoding="utf-8"))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run a Harness CLI command and return its process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    registry = HarnessRegistry.load()
    permissions = PermissionPolicy.load(registry.root)
    handlers: dict[str, Callable[[argparse.Namespace, HarnessRegistry, PermissionPolicy], int]] = {
        "doctor": _handle_doctor,
        "plan": _handle_plan,
        "route": _handle_route,
        "context": _handle_context,
        "run": _handle_run,
        "verify": _handle_verify,
        "report": _handle_report,
    }
    handler = handlers.get(args.command)
    if handler is None:
        parser.error(f"Unsupported command: {args.command}")
    return handler(args, registry, permissions)


__all__ = ["DoctorReport", "build_parser", "main"]
