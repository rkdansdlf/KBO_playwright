"""`crawl_p1p2_data_job` reports what its children collected, not that they returned.

The three children returned `None` until 2026-10-19, so the job had nothing to
read and treated "the call came back" as "the data is in". That is what let a
sweep in which `seat` lost every team to `FETCH_HTTP_ERROR` and `food` parsed
nothing alert success -- and the alert is the only part an operator sees.

The tests drive the real job body with stubbed children and pin the three
readings that have to stay separate: whether the job executed (`JobStatus`),
what was collected (the marker's `collection`), and what an operator is told
(the incident). A raising child is pinned too: it must not be swallowed into a
success, it must not stop the children that have not run yet, and it must not
add a dead letter the child already wrote.
"""

from __future__ import annotations

import importlib.util
import inspect
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.crawlers.sweep_outcome import CrawlSweepOutcome
from src.scheduler import locks
from src.scheduler.jobs import daily
from src.scheduler.jobs.daily import (
    JobStatus,
    _JOB_REGISTRY,
    _register_job,
    _update_job_status,
    clear_job_registry,
    crawl_p1p2_data_job,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCK_HEALTH_SCRIPT = REPO_ROOT / "scripts" / "check_p1p2_lock_health.py"
KST = ZoneInfo("Asia/Seoul")

CHILD_MODULES = {
    "seat": "src.cli.collection.crawl_seat_sections.main",
    "parking": "src.cli.collection.crawl_parking.main",
    "food": "src.cli.collection.crawl_stadium_food.main",
}


@pytest.fixture(autouse=True)
def _isolated_job_registry():
    clear_job_registry()
    try:
        yield
    finally:
        clear_job_registry()


@pytest.fixture(autouse=True)
def _satisfied_dependency() -> None:
    """`crawl_daily_games` gates this job; an unsatisfied gate would skip the body.

    The skip path has its own test, which overrides this status on purpose.
    """
    _register_job("crawl_daily_games", dependencies=[])
    _update_job_status("crawl_daily_games", JobStatus.SUCCESS, "test setup")


@pytest.fixture(autouse=True)
def _isolated_scheduler(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep the database probe, the tier lock, the marker file and the alert fan-out local."""
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()
    monkeypatch.setattr(daily, "_scheduler_job_lock", MagicMock())
    monkeypatch.setattr(daily, "P1P2_RUN_MARKER", tmp_path / "p1p2_data.json")
    yield
    locks._reset_db_gate()


@pytest.fixture
def alerts(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    success = MagicMock()
    warning = MagicMock()
    monkeypatch.setattr(daily, "alert_success", success)
    monkeypatch.setattr(daily, "alert_warning", warning)
    return SimpleNamespace(success=success, warning=warning)


@pytest.fixture
def marker(tmp_path: Path) -> Path:
    return tmp_path / "p1p2_data.json"


@pytest.fixture
def enqueued(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Spy on the dead letter service, which the aggregation must not call itself."""
    spy = MagicMock()
    monkeypatch.setattr("src.services.crawl_dead_letter_service.enqueue_failure", spy)
    return spy


def _outcome(crawler: str, status: str, **overrides: object) -> CrawlSweepOutcome:
    values: dict[str, object] = {"crawler": crawler, "status": status}
    return CrawlSweepOutcome(**{**values, **overrides})


def _stub_children(
    monkeypatch: pytest.MonkeyPatch,
    results: dict[str, CrawlSweepOutcome | None | BaseException],
) -> dict[str, list[Any]]:
    """Install one recording stub per child and return its call log."""
    calls: dict[str, list[Any]] = {name: [] for name in CHILD_MODULES}

    def _make(name: str) -> Callable[[list[str] | None], CrawlSweepOutcome | None]:
        def _main(argv: list[str] | None = None) -> CrawlSweepOutcome | None:
            calls[name].append(argv)
            result = results[name]
            if isinstance(result, BaseException):
                raise result
            return result

        return _main

    for name, target in CHILD_MODULES.items():
        monkeypatch.setattr(target, _make(name))
    return calls


def _read_marker(marker: Path) -> dict[str, Any]:
    return json.loads(marker.read_text(encoding="utf-8"))


def _load_lock_health_check():
    spec = importlib.util.spec_from_file_location("check_p1p2_lock_health", LOCK_HEALTH_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_clean_sweep_records_success_and_what_it_collected(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
) -> None:
    """The happy path has to keep working, and now it also says what arrived."""
    calls = _stub_children(
        monkeypatch,
        {
            "seat": _outcome("seat", "success", written=30),
            "parking": _outcome("parking", "success", written=3),
            "food": _outcome("food", "success", written=21),
        },
    )

    crawl_p1p2_data_job()

    payload = _read_marker(marker)
    assert payload["status"] == "ok"
    assert payload["collection"]["status"] == "success"
    assert payload["collection"]["written"] == 54
    assert _JOB_REGISTRY["crawl_p1p2_data_job"].status is JobStatus.SUCCESS
    alerts.success.assert_called_once()
    alerts.warning.assert_not_called()
    assert calls == {"seat": [["--save"]], "parking": [["--save"]], "food": [["--save"]]}


def test_a_single_failed_child_is_a_warning_that_names_it(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
) -> None:
    """The 2026-10-19 shape: `seat` failed, the other two stored data.

    It is not a success -- the alert must not claim one -- and it is not a job
    failure either, because the job ran to completion and two thirds of the
    sweep landed. The warning carries the child's name so the report is
    actionable without opening the ledger.
    """
    _stub_children(
        monkeypatch,
        {
            "seat": _outcome("seat", "failed", failed=2, failed_units=("SK", "LG"), error_code="FETCH_HTTP_ERROR"),
            "parking": _outcome("parking", "success", written=3),
            "food": _outcome("food", "success", written=21),
        },
    )

    crawl_p1p2_data_job()

    payload = _read_marker(marker)
    assert payload["collection"]["status"] == "partial"
    assert payload["collection"]["written"] == 24
    assert _JOB_REGISTRY["crawl_p1p2_data_job"].status is JobStatus.WARNING
    alerts.success.assert_not_called()
    alerts.warning.assert_called_once()
    assert "seat=failed" in alerts.warning.call_args.args[1]
    assert "FETCH_HTTP_ERROR" in alerts.warning.call_args.args[1]


def test_every_child_failing_is_a_failure_rather_than_a_quiet_success(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
) -> None:
    """A sweep that stored nothing must be the loudest case, not the default one."""
    _stub_children(
        monkeypatch,
        {
            "seat": _outcome("seat", "failed", failed=2),
            "parking": _outcome("parking", "failed", failed=2),
            "food": _outcome("food", "failed", failed=2),
        },
    )

    crawl_p1p2_data_job()

    payload = _read_marker(marker)
    assert payload["collection"]["status"] == "failed"
    assert payload["collection"]["written"] == 0
    assert _JOB_REGISTRY["crawl_p1p2_data_job"].status is JobStatus.FAILURE
    alerts.success.assert_not_called()
    assert "every crawler failed" in alerts.warning.call_args.args[1]


def test_a_raising_child_is_recorded_and_the_rest_of_the_sweep_still_runs(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
    enqueued: MagicMock,
) -> None:
    """Isolation, and the reason the exception is not propagated.

    Letting it out would abandon the children that had not run yet -- losing
    their collection for the day -- while the failure itself is already written
    down: each child records its own ledger row and dead letter before raising.
    Re-running the child here to recover would re-crawl every team and add a
    second dead letter for a failure this run already recorded, so the child is
    called exactly once and the aggregation adds no letter of its own.
    """
    calls = _stub_children(
        monkeypatch,
        {
            "seat": RuntimeError("seat blew up"),
            "parking": _outcome("parking", "success", written=3),
            "food": _outcome("food", "success", written=21),
        },
    )

    crawl_p1p2_data_job()

    payload = _read_marker(marker)
    seat = next(child for child in payload["collection"]["children"] if child["crawler"] == "seat")
    assert seat["status"] == "failed"
    assert seat["detail"] == "RuntimeError: seat blew up"
    assert payload["collection"]["status"] == "partial"
    assert _JOB_REGISTRY["crawl_p1p2_data_job"].status is JobStatus.WARNING
    alerts.success.assert_not_called()
    alerts.warning.assert_called_once()
    assert calls == {"seat": [["--save"]], "parking": [["--save"]], "food": [["--save"]]}
    enqueued.assert_not_called()
    assert "enqueue_failure" not in inspect.getsource(crawl_p1p2_data_job)


def test_a_child_that_returns_no_outcome_is_reported_as_failed(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
) -> None:
    """A silent child cannot reinstate the inference that produced the defect.

    The old job read "the call returned" as "the data is in". A child that
    returns nothing is exactly the shape that inference was built on, so it is
    reported as a failure with its reason written down rather than assumed
    healthy.
    """
    _stub_children(
        monkeypatch,
        {
            "seat": None,
            "parking": _outcome("parking", "success", written=3),
            "food": _outcome("food", "success", written=21),
        },
    )

    crawl_p1p2_data_job()

    payload = _read_marker(marker)
    seat = next(child for child in payload["collection"]["children"] if child["crawler"] == "seat")
    assert seat["status"] == "failed"
    assert seat["detail"] == "returned no outcome"
    assert payload["collection"]["status"] == "partial"
    alerts.success.assert_not_called()


def test_the_sweep_is_skipped_without_its_dependency(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
) -> None:
    """A skip is a fourth outcome, and it must not be reported as a collection.

    Nothing ran, so there is nothing to collect; the marker says so and the
    alert stays silent because this run learned nothing about the data either
    way.
    """
    calls = _stub_children(monkeypatch, {"seat": None, "parking": None, "food": None})
    _update_job_status("crawl_daily_games", JobStatus.FAILURE, "test setup")

    crawl_p1p2_data_job()

    payload = _read_marker(marker)
    assert payload["status"] == "skipped"
    assert "collection" not in payload
    assert _JOB_REGISTRY["crawl_p1p2_data_job"].status is JobStatus.SKIPPED
    alerts.success.assert_not_called()
    alerts.warning.assert_not_called()
    assert calls == {"seat": [], "parking": [], "food": []}


def test_a_failed_collection_still_reads_as_a_completed_run_to_the_lock_check(
    monkeypatch: pytest.MonkeyPatch,
    alerts: SimpleNamespace,
    marker: Path,
) -> None:
    """The two readings stay separate, which is what makes both of them usable.

    The lock check answers whether the job ran and left no lock error behind. If
    a data failure also failed that check, every quiet crawler day would open a
    lock investigation and the real lock signature would be lost among them --
    so `status` stays the execution state and `collection` carries the data one.
    A job-level error still fails the check; that is the direction the script
    owns and it is pinned here against the marker the job actually writes.
    """
    _stub_children(
        monkeypatch,
        {
            "seat": _outcome("seat", "failed", failed=2),
            "parking": _outcome("parking", "failed", failed=2),
            "food": _outcome("food", "failed", failed=2),
        },
    )
    crawl_p1p2_data_job()
    assert _read_marker(marker)["collection"]["status"] == "failed"

    module = _load_lock_health_check()
    module.LAST_RUN_MARKER = marker
    ran_ok, detail = module._p1p2_run_status()
    assert ran_ok is True, detail

    marker.write_text(
        json.dumps({"ts": datetime.now(KST).isoformat(), "status": "error"}, ensure_ascii=False),
        encoding="utf-8",
    )
    ran_ok, detail = module._p1p2_run_status()
    assert ran_ok is False
    assert "error" in detail
