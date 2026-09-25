"""Regression tests for the unreferenced-public-symbol lint gate.

`lint_unreachable_code.py` catches statements after a terminator. This gate covers the
other half: a public function, class, or method that exists but is never called.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.lint_unused_defs import ALLOWLIST, main as lint_main
from scripts.lint_unused_defs import scan_file

ROOT = Path(__file__).resolve().parents[2]
HARNESS_DIR = ROOT / "tools" / "agent_harness"


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "sample.py"
    path.write_text(body, encoding="utf-8")
    return path


def _counts() -> dict[str, int]:
    from scripts.lint_unused_defs import _reference_counts

    return _reference_counts()


def test_unreferenced_function_is_reported(tmp_path: Path) -> None:
    path = _write(tmp_path, "def orphan():\n    return 1\n")

    assert scan_file(path, {}) == [(1, "orphan")]


def test_referenced_function_is_not_reported(tmp_path: Path) -> None:
    path = _write(tmp_path, "def used():\n    return 1\n\n\ndef caller():\n    return used()\n")

    assert scan_file(path, {"used": 1, "caller": 0}) == [(5, "caller")]


def test_unreferenced_method_is_reported_with_its_class(tmp_path: Path) -> None:
    path = _write(tmp_path, "class Adapter:\n    def orphan_method(self):\n        return 1\n")

    assert scan_file(path, {}) == [(1, "Adapter"), (2, "Adapter.orphan_method")]


def test_private_definitions_are_ignored(tmp_path: Path) -> None:
    path = _write(tmp_path, "def _internal():\n    return 1\n")

    assert scan_file(path, {}) == []


def test_allowlisted_names_are_skipped(tmp_path: Path) -> None:
    path = _write(tmp_path, "def to_dict(self):\n    return {}\n")

    assert "to_dict" in ALLOWLIST
    assert scan_file(path, {}) == []


def test_assignment_targets_do_not_count_as_references(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A class attribute is bound at definition but consumed through serialization.

    Counting the binding site as a reference would make every dataclass field look alive,
    which is the false positive this gate has to avoid.
    """
    module = tmp_path / "fields.py"
    module.write_text("class Report:\n    timed_out: bool = False\n", encoding="utf-8")
    monkeypatch.setattr(
        "scripts.lint_unused_defs._reference_files",
        lambda: [module],
    )

    from scripts.lint_unused_defs import _reference_counts

    assert _reference_counts().get("timed_out", 0) == 0
    # The attribute is out of scope for the gate by design. The unreferenced class itself
    # is still reported, which proves the scan is live rather than silently empty.
    reported = [symbol for _, symbol in scan_file(module, _reference_counts())]
    assert "Report.timed_out" not in reported
    assert reported == ["Report"]


def test_harness_has_no_unreferenced_public_symbols() -> None:
    """Scope is the Harness control plane only.

    `src/` is a library surface where unreferenced public helpers are legitimate, so it is
    deliberately excluded; extending the gate there would make it noise.
    """
    counts = _counts()
    offenders: list[str] = []
    for path in sorted(HARNESS_DIR.rglob("*.py")):
        for line, symbol in scan_file(path, counts):
            offenders.append(f"{path.relative_to(ROOT)}:{line} {symbol}")

    assert offenders == []


def test_cli_reports_a_clean_tree(capsys: pytest.CaptureFixture[str]) -> None:
    assert lint_main([]) == 0
    assert "Total: 0 unreferenced public symbols" in capsys.readouterr().out


def test_removed_dead_api_stays_removed() -> None:
    """Guard the P20 cleanup so the deleted pass-throughs and duplicates cannot return."""
    verifier = (HARNESS_DIR / "verifier.py").read_text(encoding="utf-8")
    adapter = (HARNESS_DIR / "project_adapter.py").read_text(encoding="utf-8")
    exceptions = (HARNESS_DIR / "exceptions.py").read_text(encoding="utf-8")

    assert "def policy_for" not in verifier
    assert "def determine_pytest_targets" not in verifier
    assert "def needs_crawler_gate" not in verifier
    assert "def needs_certification" not in verifier
    assert "verification_profiles" not in adapter
    for name in ("SkillUnavailableError", "RouteValidationError", "VerificationFailedError"):
        assert name not in exceptions, name
