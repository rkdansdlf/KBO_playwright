"""Team defense aggregation job tests.

Regression: the job imported ``src.aggregators.team_defense_aggregator``, a module that
has never existed in this repository, so it raised ``ModuleNotFoundError`` every night
at 03:45 instead of aggregating team defense. The failure escaped the job's own handler
because ``ModuleNotFoundError`` is not part of ``SCHEDULER_JOB_EXCEPTIONS``, and no test
covered the job.
"""

from __future__ import annotations

import ast
import inspect
from unittest.mock import AsyncMock, patch

from src.scheduler.jobs import maintenance

STEP_TARGET = "src.cli.pipelines.advanced_daily_steps.aggregate_team_defense_step"


def test_job_runs_the_shared_aggregate_step() -> None:
    """The job delegates to the same step the daily pipeline runs."""
    step = AsyncMock()

    with (
        patch(STEP_TARGET, step),
        patch.object(maintenance, "_scheduler_job_lock"),
        patch.object(maintenance, "datetime") as mock_dt,
    ):
        mock_dt.now.return_value.year = 2026

        maintenance.aggregate_team_defense_job()

    step.assert_awaited_once_with(2026)


def test_job_reports_a_resolution_failure_instead_of_escaping() -> None:
    """A module that cannot be imported must be reported, not raise out of the job."""
    with (
        patch.dict("sys.modules", {"src.cli.pipelines.advanced_daily_steps": None}),
        patch.object(maintenance, "_scheduler_job_lock"),
    ):
        maintenance.aggregate_team_defense_job()  # must not raise


def test_job_no_longer_imports_the_phantom_module() -> None:
    """The never-existing import must not come back."""
    tree = ast.parse(inspect.getsource(maintenance.aggregate_team_defense_job))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}

    assert "src.aggregators.team_defense_aggregator" not in imported
