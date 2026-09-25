"""Artifact completeness and schema contract tests."""

from __future__ import annotations

import json
import shutil
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tools.agent_harness.artifact_contract import validate_artifact_bundle
from tools.agent_harness.cli import _handle_report, main
from tools.agent_harness.command_runner import CommandRunner
from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION
from tools.agent_harness.evidence import EvidenceStore
from tools.agent_harness.exceptions import PermissionDeniedError
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.runner import HarnessRunner
from tools.agent_harness.verifier import CommandResult, GatePolicy, ProjectVerifier, verification_run_lock

if TYPE_CHECKING:
    from tools.agent_harness.runner import HarnessRun

REQUIRED_ARTIFACTS = (
    "task.json",
    "plan.json",
    "context.json",
    "skill-trace.jsonl",
    "commands.jsonl",
    "verification.json",
    "report.md",
)


def _new_run(tmp_path: Path, task: str = "artifact contract probe") -> tuple[HarnessRun, PermissionPolicy]:
    source_registry = HarnessRegistry.load()
    root = tmp_path / "repo"
    root.mkdir(parents=True)
    registry = replace(source_registry, root=root)
    permissions = replace(PermissionPolicy.load(), root=root)
    return HarnessRunner(registry, permissions).run(task, profile="research"), permissions


class _FakeRunner(CommandRunner):
    def __init__(self) -> None:
        pass

    def run(self, argv: object, **kwargs: object) -> CommandResult:
        _ = kwargs
        assert isinstance(argv, (list, tuple))
        return CommandResult(
            argv=tuple(str(token) for token in argv),
            exit_code=0,
            duration_ms=0.0,
            stdout="",
            stderr="",
        )


def _verify(run: HarnessRun, permissions: PermissionPolicy, *, exit_code: int = 0) -> None:
    class _Runner(_FakeRunner):
        def run(self, argv: object, **kwargs: object) -> CommandResult:
            result = super().run(argv, **kwargs)
            return CommandResult(
                argv=result.argv,
                exit_code=exit_code,
                duration_ms=result.duration_ms,
                stdout=result.stdout,
                stderr=result.stderr,
            )

    evidence = EvidenceStore(run.artifact_dir, permissions)
    verifier = ProjectVerifier(
        root=run.artifact_dir,
        levels={},
        profiles={"research": GatePolicy(gates=("doctor",))},
    )
    verifier.verify("research", evidence=evidence, runner=_Runner())


def _downgrade_bundle_to_v1(run: HarnessRun) -> None:
    for name in ("task.json", "plan.json", "context.json", "verification.json"):
        path = run.artifact_dir / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.pop("schema_version", None)
        payload.pop("run_id", None)
        payload.pop("verification_id", None)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name in ("skill-trace.jsonl", "commands.jsonl"):
        path = run.artifact_dir / name
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
        for record in records:
            record.pop("schema_version", None)
            record.pop("run_id", None)
            record.pop("verification_id", None)
        path.write_text(
            "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
            encoding="utf-8",
        )


def test_verification_run_lock_is_exclusive(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    evidence = EvidenceStore(run.artifact_dir, permissions)

    with verification_run_lock(evidence):
        with pytest.raises(PermissionDeniedError):
            with verification_run_lock(evidence):
                pass


def test_fresh_run_satisfies_base_contract(tmp_path: Path) -> None:
    run, _ = _new_run(tmp_path)

    report = validate_artifact_bundle(run.artifact_dir)

    assert report.valid is True
    assert report.run_id == run.run_id
    assert report.required_files == REQUIRED_ARTIFACTS
    assert report.issues == ()


def test_verified_run_satisfies_verified_contract(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)

    _verify(run, permissions)

    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)
    assert report.valid is True
    verification = json.loads((run.artifact_dir / "verification.json").read_text(encoding="utf-8"))
    assert verification["passed"] is True


@pytest.mark.parametrize("name", REQUIRED_ARTIFACTS)
def test_missing_required_artifact_fails_contract(tmp_path: Path, name: str) -> None:
    run, _ = _new_run(tmp_path)
    (run.artifact_dir / name).unlink()

    report = validate_artifact_bundle(run.artifact_dir)

    assert report.valid is False
    assert any(name in issue for issue in report.issues)


def test_malformed_json_and_jsonl_fail_contract(tmp_path: Path) -> None:
    json_run, _ = _new_run(tmp_path, "malformed json")
    (json_run.artifact_dir / "task.json").write_text("{", encoding="utf-8")
    jsonl_run, _ = _new_run(tmp_path / "jsonl", "malformed jsonl")
    (jsonl_run.artifact_dir / "skill-trace.jsonl").write_text("{\n", encoding="utf-8")

    assert validate_artifact_bundle(json_run.artifact_dir).valid is False
    assert validate_artifact_bundle(jsonl_run.artifact_dir).valid is False


def test_pending_verification_fails_only_when_verified_is_required(tmp_path: Path) -> None:
    run, _ = _new_run(tmp_path)

    assert validate_artifact_bundle(run.artifact_dir).valid is True
    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert report.valid is False
    assert any("pending" in issue for issue in report.issues)


def test_cross_artifact_profile_mismatch_fails_contract(tmp_path: Path) -> None:
    run, _ = _new_run(tmp_path)
    task_path = run.artifact_dir / "task.json"
    task = json.loads(task_path.read_text(encoding="utf-8"))
    task["profile"] = "analytics"
    task_path.write_text(json.dumps(task, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = validate_artifact_bundle(run.artifact_dir)

    assert report.valid is False
    assert any("profile" in issue for issue in report.issues)


def test_failed_verification_does_not_satisfy_success_contract(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)

    _verify(run, permissions, exit_code=1)

    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)
    assert report.valid is False
    assert any("passed" in issue for issue in report.issues)


def test_new_json_and_jsonl_artifacts_have_v2_identity(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions)

    for name in ("task.json", "plan.json", "context.json", "verification.json"):
        payload = json.loads((run.artifact_dir / name).read_text(encoding="utf-8"))
        assert payload["schema_version"] == EVIDENCE_SCHEMA_VERSION
        assert payload["run_id"] == run.run_id
    for name in ("skill-trace.jsonl", "commands.jsonl"):
        lines = (run.artifact_dir / name).read_text(encoding="utf-8").splitlines()
        assert lines
        for line in lines:
            payload = json.loads(line)
            assert payload["schema_version"] == EVIDENCE_SCHEMA_VERSION
            assert payload["run_id"] == run.run_id


def test_selective_schema_downgrade_is_rejected(tmp_path: Path) -> None:
    run, _ = _new_run(tmp_path)
    for name in ("task.json", "plan.json"):
        path = run.artifact_dir / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.pop("schema_version", None)
        payload.pop("run_id", None)
        payload.pop("verification_id", None)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = validate_artifact_bundle(run.artifact_dir)

    assert report.valid is False
    assert any("current bundle requires" in issue for issue in report.issues)


def test_v1_bundle_remains_readable_but_cannot_claim_verified_success(tmp_path: Path) -> None:
    run, _ = _new_run(tmp_path)
    _downgrade_bundle_to_v1(run)

    report = validate_artifact_bundle(run.artifact_dir)
    verified = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert report.valid is True
    assert verified.valid is False
    assert any("current schema" in issue for issue in verified.issues)


def test_passed_flag_is_recomputed_from_command_exit_codes(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions)
    path = run.artifact_dir / "verification.json"
    verification = json.loads(path.read_text(encoding="utf-8"))
    verification["commands"][0]["exit_code"] = 1
    path.write_text(json.dumps(verification, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert report.valid is False
    assert any("exit codes" in issue or "differ" in issue for issue in report.issues)


def test_final_attempt_must_be_latest_started_attempt(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions)
    evidence = EvidenceStore(run.artifact_dir, permissions)
    evidence.append_jsonl(
        "commands.jsonl",
        {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "run_id": run.run_id,
            "event": "verification_started",
            "verification_id": "newer-attempt",
            "profile": "research",
        },
    )

    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert report.valid is False
    assert any("latest started attempt" in issue for issue in report.issues)


def test_final_attempt_must_match_command_stream(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions)
    path = run.artifact_dir / "commands.jsonl"
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    records[-1]["stdout"] = "tampered"
    path.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records), encoding="utf-8")

    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert report.valid is False
    assert any("differ from final verification" in issue for issue in report.issues)


def test_report_profile_must_match_final_verification(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions)
    report_path = run.artifact_dir / "report.md"
    report_path.write_text(
        report_path.read_text(encoding="utf-8").replace("`research` (passed)", "`level:none` (passed)"),
        encoding="utf-8",
    )

    validation = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert validation.valid is False
    assert any("profile or status" in issue for issue in validation.issues)


def test_reverification_replaces_stale_report_status(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions, exit_code=0)
    _verify(run, permissions, exit_code=1)

    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert "(failed)" in (run.artifact_dir / "report.md").read_text(encoding="utf-8")
    assert report.valid is False
    assert any("passed" in issue for issue in report.issues)


def test_schema_and_run_identity_tampering_are_rejected(tmp_path: Path) -> None:
    schema_run, _ = _new_run(tmp_path / "schema", "schema tampering")
    task_path = schema_run.artifact_dir / "task.json"
    task = json.loads(task_path.read_text(encoding="utf-8"))
    task["schema_version"] = "999"
    task_path.write_text(json.dumps(task, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    identity_run, _ = _new_run(tmp_path / "identity", "identity tampering")
    identity_path = identity_run.artifact_dir / "plan.json"
    plan = json.loads(identity_path.read_text(encoding="utf-8"))
    plan["run_id"] = "different-run"
    identity_path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    assert validate_artifact_bundle(schema_run.artifact_dir).valid is False
    assert validate_artifact_bundle(identity_run.artifact_dir).valid is False


def test_v1_historical_commands_upgrade_with_legacy_attempt_id(tmp_path: Path) -> None:
    run, permissions = _new_run(tmp_path)
    _verify(run, permissions)
    _downgrade_bundle_to_v1(run)
    evidence = EvidenceStore(run.artifact_dir, permissions)

    evidence.upgrade_current_schema()
    report = validate_artifact_bundle(run.artifact_dir, require_verified=True)

    assert evidence.needs_current_schema_upgrade() is False
    assert report.valid is True
    verification = evidence.read_json("verification.json")
    commands = [json.loads(line) for line in evidence.read_text("commands.jsonl").splitlines()]
    assert all(record["verification_id"] == verification["verification_id"] for record in commands)


def test_symlinked_artifact_directory_is_rejected(tmp_path: Path) -> None:
    run, _ = _new_run(tmp_path)
    linked_run = tmp_path / "linked-run"
    linked_run.symlink_to(run.artifact_dir, target_is_directory=True)

    report = validate_artifact_bundle(linked_run)

    assert report.valid is False
    assert any("symbolic link" in issue for issue in report.issues)


def test_report_command_refuses_symlink_without_leaking_content(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run, permissions = _new_run(tmp_path)
    outside = tmp_path / "outside-report.md"
    outside.write_text("secret-report-content", encoding="utf-8")
    report_path = run.artifact_dir / "report.md"
    report_path.unlink()
    report_path.symlink_to(outside)
    registry = replace(HarnessRegistry.load(), root=tmp_path / "repo")

    exit_code = _handle_report(Namespace(run_id=run.run_id), registry, permissions)
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "secret-report-content" not in captured.out
    assert "secret-report-content" not in captured.err


def test_cli_validate_gate(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "cli artifact gate", "--profile", "research", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = Path("artifacts") / "agent-harness" / run_id
    try:
        assert main(["validate", run_id, "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["valid"] is True
        assert main(["validate", run_id, "--require-verified", "--json"]) == 1
        assert json.loads(capsys.readouterr().out)["valid"] is False
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)
