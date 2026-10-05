"""Every ``alert_warning`` name must have a matching ``alert_success``.

``alert_warning`` opens a durable incident at ``scheduler:<name>:warning``. The
runbook's only supported way to close an incident is for the check that opened it
to report recovery, and nothing else owns that key -- ``alert_failure`` writes
``:failed`` and the APScheduler lifecycle listener resolves ``<job_id>:failed``.
So a name that warns but never succeeds is a name whose incidents stay OPEN
forever, and ``SILENCED_BY`` will point an operator at a warning nobody can act
on.

That is easy to do by accident: a conditional warning ("only when the sweep left
work behind") reads as complete on its own, and the healthy branch that has to
clear it is the branch nobody is looking at. This test is the check that the
healthy branch exists, and it is written as a scan rather than a list so a new
job is covered the day it is added.

``alert_success`` also resolves ``:failed``, so this never asks a job to decide
between the two keys.
"""

from __future__ import annotations

import ast
import pathlib

#: Names allowed to warn without a matching success, with the reason.
#:
#: Keep this empty if you can. An entry is an admission that the warning is
#: permanent by design, which is rarely what the operator reading it assumes.
DELIBERATELY_UNRECOVERED: dict[str, str] = {}

JOB_MODULES = (
    "src/scheduler/jobs/maintenance.py",
    "src/scheduler/jobs/daily.py",
    "src/scheduler/jobs/sentinel.py",
    "src/scheduler/jobs/live.py",
    "src/scheduler/jobs/stadium.py",
    "src/scheduler/jobs/alerts.py",
)


def _alert_names(source: str) -> tuple[set[str], set[str]]:
    """Return the literal names passed to ``alert_warning`` and ``alert_success``.

    Only literal first arguments are collected. A name built at runtime cannot be
    matched against its counterpart here, and guessing would make the scan report
    pairs that do not exist; such a call is simply out of scope.
    """
    warned: set[str] = set()
    succeeded: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or not node.args:
            continue
        if node.func.id not in {"alert_warning", "alert_success"}:
            continue
        first = node.args[0]
        if not isinstance(first, ast.Constant) or not isinstance(first.value, str):
            continue
        (warned if node.func.id == "alert_warning" else succeeded).add(first.value)
    return warned, succeeded


def test_every_warning_name_can_report_recovery() -> None:
    unrecovered: dict[str, str] = {}
    for module_path in JOB_MODULES:
        path = pathlib.Path(module_path)
        if not path.exists():
            continue
        warned, succeeded = _alert_names(path.read_text(encoding="utf-8"))
        for name in sorted(warned - succeeded):
            unrecovered[name] = module_path

    assert set(unrecovered) == set(DELIBERATELY_UNRECOVERED), (
        "these jobs open a warning incident that nothing can close: "
        f"{sorted(unrecovered)}; call alert_success on the healthy branch, or state "
        "the reason in DELIBERATELY_UNRECOVERED"
    )


def test_every_stated_exception_is_actually_unrecovered() -> None:
    """A stale backlist entry hides the next real one, so fail on it too."""
    if not DELIBERATELY_UNRECOVERED:
        return

    all_pairs: list[tuple[str, set[str], set[str]]] = []
    for module_path in JOB_MODULES:
        path = pathlib.Path(module_path)
        if path.exists():
            warned, succeeded = _alert_names(path.read_text(encoding="utf-8"))
            all_pairs.append((module_path, warned, succeeded))

    for name in DELIBERATELY_UNRECOVERED:
        still_unrecovered = any(name in warned - succeeded for _path, warned, succeeded in all_pairs)
        assert still_unrecovered, (
            f"{name!r} is listed in DELIBERATELY_UNRECOVERED but now reports recovery; remove the entry"
        )


def test_the_scan_can_see_the_names_it_claims_to_check() -> None:
    """Guard against a silent no-op.

    If the extraction broke -- renamed helpers, a wrapper, a different call shape
    -- both sets would come back empty and the test above would pass while
    checking nothing. These two names are the ones this scan was written for.
    """
    _warned, succeeded = _alert_names(
        pathlib.Path("src/scheduler/jobs/sentinel.py").read_text(encoding="utf-8"),
    )

    assert "rag_audit_sentinel" in succeeded


def test_a_warning_without_a_recovery_is_detected() -> None:
    """Prove the scan bites, using a synthetic module rather than the repository."""
    warned, succeeded = _alert_names(
        "def job():\n    alert_warning('stuck_name', 'details')\n    alert_success('paired_name')\n",
    )

    assert warned == {"stuck_name"}
    assert succeeded == {"paired_name"}
    assert warned - succeeded == {"stuck_name"}
