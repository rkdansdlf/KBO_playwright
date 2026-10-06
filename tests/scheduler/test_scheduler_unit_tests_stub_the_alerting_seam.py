"""Guard the unit tests that exercise `crawl_daily_games` failure paths.

Regression for the 2026-10-07 incident: both that exercised the 2026-10-03 daily
sync path claimed stubbing, but one left ``_publish_quality_incident`` real and two
only stubbed the success alert, so a unit test run dispatched a real
notification and `DeliveryRecorder` committed a real ``notification_deliveries``
row. The write is harmless and fast until anything else holds the SQLite file, at
which point it waits out ``PRAGMA busy_timeout = 120000`` -- 125s, seen twice in a
full run.

The exact seams are stubbed by the tests' own patches; this contract pins that
the stubbing is present so the leak cannot quietly return when someone edits those
patch lists. A static source scan is the right granularity: the defects being
guarded against are precisely "a scheduler test forgot to mock one seam line."
"""

from __future__ import annotations

import pathlib

REPO = pathlib.Path(__file__).resolve().parents[2]


def _source(relative: str) -> str:
    return (REPO / relative).read_text(encoding="utf-8")


def test_daily_dag_failure_path_stubs_the_alerting_seam() -> None:
    source = _source("tests/scheduler/test_scheduler_daily_dag.py")

    assert "_publish_quality_incident" in source, (
        "the PARTIAL_FAILURE alert path publishes incidents; leaving it real "
        "commits a notification_deliveries row from a unit test"
    )
    assert "alert_warning" in source, (
        "the branch-under-test calls alert_warning; leaving it real dispatches "
        "for real and blocks on the SQLite busy_timeout"
    )


def test_scheduler_fix_stubs_alert_warning_and_quality_publish() -> None:
    source = _source("tests/test_scheduler_fix.py")

    assert "scripts.scheduler.alert_warning" in source, (
        "the PARTIAL_FAILURE branch warns; leaving alert_warning real commits a "
        "notification_deliveries row from a unit test"
    )
    assert "_publish_quality_incident" in source, (
        "the DAG path publishes a quality incident before branching on its status; "
        "leaving it real commits incident wiring from a unit test"
    )


def test_incident_wiring_tests_are_the_only_place_the_bridge_is_real() -> None:
    """Explains why this guard scans only the two files above.

    `tests/scheduler/test_incident_wiring.py` is the one test module that asserts
    real incident/ledger behaviour, so it must call the real bridge. Everywhere
    else that reaches the bridge should record a mock instead of a row. This test
    does not forbid that -- it just records why the contract is file-specific.
    """
    source = _source("tests/scheduler/test_incident_wiring.py")

    assert "apply_incidents(" in source
    assert "session_factory=" in source
