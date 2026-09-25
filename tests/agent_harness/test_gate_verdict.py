"""Contract tests for the single shared verification verdict.

`gate_verdict` in `dto.py` is the one pass/fail rule shared by the profile-mode runner,
the level-mode runner, and this evidence contract checker. Before it existed the three
disagreed: a run with zero commands passed in level mode, failed in profile mode, and was
accepted here only when the profile string happened to be `level:none`.
"""

from __future__ import annotations

import pytest

from tools.agent_harness.artifact_contract import _check_verification
from tools.agent_harness.dto import gate_verdict

EXIT_ZERO = {"argv": ["gate"], "exit_code": 0, "duration_ms": 1.0, "stdout": "", "stderr": ""}
EXIT_ONE = {"argv": ["gate"], "exit_code": 1, "duration_ms": 1.0, "stdout": "", "stderr": ""}


def _verification(**body: object) -> dict[str, object]:
    return {
        "schema_version": "2",
        "run_id": "20260101T000000Z-abcdef01",
        "verification_id": "attempt01",
        "profile": "project",
        **body,
    }


def _issues(verification: dict[str, object]) -> list[str]:
    issues: list[str] = []
    _check_verification(verification, "verification.json", issues, require_verified=False)
    return issues


def test_gate_verdict_requires_every_declared_gate_to_have_run() -> None:
    assert gate_verdict(declared_gate_ids=("a", "b"), exit_codes=(0, 0)) is True
    assert gate_verdict(declared_gate_ids=("a", "b"), exit_codes=(0,)) is False
    assert gate_verdict(declared_gate_ids=("a",), exit_codes=()) is False
    assert gate_verdict(declared_gate_ids=("a",), exit_codes=(1,)) is False


def test_gate_verdict_accepts_a_deliberately_empty_policy() -> None:
    """A research route declares zero gates; that is success, not a silent skip."""
    assert gate_verdict(declared_gate_ids=(), exit_codes=()) is True


def test_declared_zero_gates_is_accepted_by_the_contract() -> None:
    assert _issues(_verification(passed=True, gate_ids=[], commands=[])) == []


def test_partial_gate_run_cannot_claim_success() -> None:
    issues = _issues(_verification(passed=True, gate_ids=["a", "b"], commands=[dict(EXIT_ZERO)]))

    assert any("does not match command exit codes" in issue for issue in issues)


def test_failing_gate_is_rejected() -> None:
    issues = _issues(_verification(passed=True, gate_ids=["a"], commands=[dict(EXIT_ONE)]))

    assert any("does not match command exit codes" in issue for issue in issues)


def test_legacy_bundle_without_gate_ids_still_validates() -> None:
    """Evidence written before gate ids existed must remain readable."""
    assert _issues(_verification(passed=True, commands=[dict(EXIT_ZERO)])) == []


def test_legacy_bundle_with_a_failing_gate_is_still_rejected() -> None:
    issues = _issues(_verification(passed=True, commands=[dict(EXIT_ONE)]))

    assert any("does not match command exit codes" in issue for issue in issues)


@pytest.mark.parametrize(
    ("declared", "observed", "expected"),
    [
        ((), (), True),
        (("a",), (0,), True),
        (("a",), (0, 0), False),
        (("a", "b"), (0, 0), True),
        (("a", "b"), (0, 1), False),
    ],
)
def test_verdict_truth_table(declared: tuple[str, ...], observed: tuple[int, ...], expected: bool) -> None:
    assert gate_verdict(declared_gate_ids=declared, exit_codes=observed) is expected
