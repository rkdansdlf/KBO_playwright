"""A failed DAG stage must be findable in one line, not buried under "executed".

``run_daily_update`` runs its steps through a DAG whose stages mostly carry
``allow_failure=True`` — the pipeline continues when one fails, on purpose. But its
own summary then said ``[DAG] All 15 tasks executed`` **either way**, and the
per-task failure sat in a ``logger.exception`` line deep in the log. Measured
2026-11-02: step_1 failed on a stale module cache, the November schedule was never
crawled, and the pipeline's own summary reported every task executed. The failure
sat between two green-looking lines for days.

The summary now names every stage that did not succeed — failed **and** skipped
(the downstream stages that lost their dependency) — with the error, at ERROR level,
in one grep-able line. It does not raise and does not alert: an alert needs a
decision about who owns a stage failure, and the per-task traceback already exists.
"""

from __future__ import annotations

import logging

import pytest

from src.cli.pipelines.run_daily_update import _log_dag_stage_failures
from src.pipeline.dag import TaskExecutionResult, TaskState


def _result(task_id: str, state: TaskState, error: str | None = None) -> TaskExecutionResult:
    res = TaskExecutionResult(task_id=task_id, state=state)
    res.error = error
    return res


def _capture(caplog: pytest.LogCaptureFixture, results: dict[str, object]) -> str:
    with caplog.at_level(logging.ERROR):
        _log_dag_stage_failures(results)
    return caplog.text




class TestAFailedStageIsNamed:
    def test_the_summary_names_the_stage_and_its_error(self, caplog: pytest.LogCaptureFixture) -> None:
        results = {
            "step_0_auto_healer": _result("step_0_auto_healer", TaskState.SUCCESS),
            "step_1_schedule": _result("step_1_schedule", TaskState.FAILED, "NameError: stale import"),
        }

        text = _capture(caplog, results)

        assert "step_1_schedule(failed): NameError" in text
        assert "1/2" in text

    def test_it_reads_at_error_level(self, caplog: pytest.LogCaptureFixture) -> None:
        """The per-task traceback was already there; the summary has to be louder."""
        results = {"step_1_schedule": _result("step_1_schedule", TaskState.FAILED, "boom")}

        with caplog.at_level(logging.INFO):
            _log_dag_stage_failures(results)

        assert any(r.levelno == logging.ERROR for r in caplog.records)


class TestASkippedStageIsAlsoNamed:
    def test_a_stage_that_lost_its_dependency_is_summarized(self, caplog: pytest.LogCaptureFixture) -> None:
        """A skipped stage did no work either; it belongs in the same line."""
        results = {
            "step_1_schedule": _result("step_1_schedule", TaskState.FAILED, "upstream died"),
            "step_2_detail_crawl": _result("step_2_detail_crawl", TaskState.SKIPPED, "Upstream dependency failed: step_1_schedule"),
        }

        text = _capture(caplog, results)

        assert "step_1_schedule(failed): upstream died" in text
        assert "step_2_detail_crawl(skipped)" in text
        assert "2/2" in text


class TestTheCleanCaseStaysClean:
    def test_all_success_reports_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        results = {
            "step_1_schedule": _result("step_1_schedule", TaskState.SUCCESS),
            "step_2_detail_crawl": _result("step_2_detail_crawl", TaskState.SUCCESS),
        }

        assert _capture(caplog, results) == ""

    def test_an_empty_result_map_reports_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        assert _capture(caplog, {}) == ""

    def test_nothing_is_raised(self, caplog: pytest.LogCaptureFixture) -> None:
        """The summary is a log line; the pipeline has already continued."""
        results = {"step_1_schedule": _result("step_1_schedule", TaskState.FAILED, "boom")}

        _capture(caplog, results)

        assert True
