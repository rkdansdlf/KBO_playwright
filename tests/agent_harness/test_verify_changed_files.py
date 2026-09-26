"""Regression tests for `verify <run-id>` passing the run's changed files to the gates.

`ProjectVerifier.verify()` had no `changed_files` parameter and called
`build_profile_plan(profile)`, so the default verification path always resolved the
affected-pytest gate to `pytest tests/agent_harness -q` no matter what the run touched.
`run()` had already recorded the change set in `task.json`, so the data was there and
unused, and no test covered it.

The gate now resolves the run's own suites. These tests pin that wiring at three levels:
the verifier, the CLI call site, and the argv that reaches the command runner.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from tools.agent_harness.cli import main
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.verifier import CommandResult, ProjectVerifier

ROOT = Path(__file__).resolve().parents[2]


class _RecordingRunner:
    """Capture the argv the gates would execute, without spawning anything."""

    def __init__(self) -> None:
        self.argv: list[tuple[str, ...]] = []

    def run(self, argv: object, **kwargs: object) -> CommandResult:
        _ = kwargs
        tokens = tuple(str(token) for token in argv)  # type: ignore[union-attr]
        self.argv.append(tokens)
        return CommandResult(argv=tokens, exit_code=0, duration_ms=0.0, stdout="", stderr="")


def _pytest_paths(argv: tuple[str, ...]) -> list[str]:
    if len(argv) < 4 or "pytest" not in argv:
        return []
    return [token for token in argv if token.startswith("tests/")]


def test_verify_without_changed_files_still_runs_only_the_harness_suite() -> None:
    """The base behaviour is unchanged for a run that recorded no changes."""
    runner = _RecordingRunner()
    ProjectVerifier.load().verify("project", runner=runner)  # type: ignore[arg-type]

    affected = [argv for argv in runner.argv if "pytest" in argv]

    assert affected, "expected an affected-pytest gate"
    assert _pytest_paths(affected[0]) == ["tests/agent_harness"]


def test_verify_uses_the_runs_changed_files() -> None:
    """The core regression: a crawler run must reach the crawler suite."""
    runner = _RecordingRunner()
    ProjectVerifier.load().verify(  # type: ignore[arg-type]
        "crawler",
        changed_files=["src/crawlers/game_boxscore_crawler.py"],
        runner=runner,
    )

    affected = next(argv for argv in runner.argv if "pytest" in argv)
    paths = _pytest_paths(affected)

    assert "tests/agent_harness" in paths
    assert "tests/crawlers" in paths


def test_verify_gate_ids_record_the_full_declared_set() -> None:
    """Gate ids come from policy, so a widened subsystem does not add phantom gates."""
    report = ProjectVerifier.load().verify(  # type: ignore[arg-type]
        "crawler",
        changed_files=["src/crawlers/game_boxscore_crawler.py"],
        runner=_RecordingRunner(),
    )

    assert report.gate_ids == ("pytest-affected", "ruff-project", "doctor", "crawler-gate", "pytest-crawler-gate")
    assert report.passed is True


def test_cli_verify_reads_changed_files_from_task_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end wiring: the run's recorded change set must reach the gate argv."""
    monkeypatch.chdir(ROOT)
    assert main(["run", "crawler selector drift", "--profile", "crawler-bug", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = ROOT / "artifacts" / "agent-harness" / run_id
    try:
        task = json.loads((artifact_dir / "task.json").read_text(encoding="utf-8"))
        assert task["changed_files"] == [], "fixture expects a run with no recorded changes"
        task["changed_files"] = ["src/crawlers/game_boxscore_crawler.py"]
        (artifact_dir / "task.json").write_text(json.dumps(task), encoding="utf-8")

        recorded: list[tuple[str, ...]] = []

        def _record(self: object, argv: object, **kwargs: object) -> CommandResult:
            _ = (self, kwargs)
            tokens = tuple(str(token) for token in argv)  # type: ignore[union-attr]
            recorded.append(tokens)
            return CommandResult(argv=tokens, exit_code=0, duration_ms=0.0, stdout="", stderr="")

        monkeypatch.setattr("tools.agent_harness.command_runner.CommandRunner.run", _record)
        assert main(["verify", run_id, "--json"]) == 0
        capsys.readouterr()

        affected = next(argv for argv in recorded if "pytest" in argv)
        assert "tests/crawlers" in _pytest_paths(affected)
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)


def test_contract_rejects_a_malformed_changed_files_field(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hand-edited change set is caught by the artifact contract before verify runs.

    `verify` also defends against it, but the contract is the first line of defence, so
    the observable behaviour is a clean contract failure rather than a TypeError.
    """
    monkeypatch.chdir(ROOT)
    assert main(["run", "malformed change set", "--profile", "feature", "--json"]) == 0
    run_id = str(json.loads(capsys.readouterr().out)["run_id"])
    artifact_dir = ROOT / "artifacts" / "agent-harness" / run_id
    try:
        task = json.loads((artifact_dir / "task.json").read_text(encoding="utf-8"))
        task["changed_files"] = "src/cli/kbo.py"
        (artifact_dir / "task.json").write_text(json.dumps(task), encoding="utf-8")

        assert main(["verify", run_id, "--json"]) == 2
        payload = json.loads(capsys.readouterr().out)

        # Either the bundle check or the plan re-derivation catches it first; both run
        # before any gate executes, and both must name the offending field.
        assert payload["error"].endswith("contract failed")
        assert any("changed_files" in issue for issue in payload.get("issues", []))
    finally:
        shutil.rmtree(artifact_dir, ignore_errors=True)


def test_project_verifier_still_requires_a_command_runner() -> None:
    verifier = replace(ProjectVerifier.load(), levels=ProjectVerifier.load().levels)

    with pytest.raises(Exception, match="requires a CommandRunner"):
        verifier.verify("project", changed_files=["src/cli/kbo.py"])


def test_permission_policy_allows_no_extra_executable_for_domain_suites() -> None:
    """Domain suites run as `python -m pytest <paths>`, which the policy already allows."""
    policy = PermissionPolicy.load()

    assert "pytest" in policy.python_modules
