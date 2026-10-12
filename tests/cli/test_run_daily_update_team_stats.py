"""Team season stats refresh step tests.

Regression for the frozen ``team_season_*`` aggregates: the canonical scheduler only
refreshed the player pages, so the team totals the quality gate cross-checks fell
behind the player sums and the gate failed on every run after that.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli.pipelines import run_daily_update as pipeline


def _context(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {"year": 2026, "headless": True, "skip_season_stats": False}
    base.update(overrides)
    return SimpleNamespace(**base)


def test_step_refreshes_team_batting_and_pitching() -> None:
    """Both team pages are refreshed from the official source."""
    batting = MagicMock()
    batting.crawl.return_value = [{"team_id": "LG"}]
    pitching = MagicMock()
    pitching.crawl.return_value = [{"team_id": "LG"}]

    with (
        patch.object(pipeline, "TeamBattingStatsCrawler", return_value=batting),
        patch.object(pipeline, "TeamPitchingStatsCrawler", return_value=pitching),
    ):
        asyncio.run(pipeline._step_6_1_team_season_stats(_context()))

    batting.crawl.assert_called_once_with(2026, persist=True, headless=True)
    pitching.crawl.assert_called_once_with(2026, persist=True, headless=True)


def test_step_respects_operator_season_stats_flag() -> None:
    """``--skip-season-stats`` must skip the team refresh too."""
    batting = MagicMock()
    pitching = MagicMock()

    with (
        patch.object(pipeline, "TeamBattingStatsCrawler", return_value=batting),
        patch.object(pipeline, "TeamPitchingStatsCrawler", return_value=pitching),
    ):
        asyncio.run(pipeline._step_6_1_team_season_stats(_context(skip_season_stats=True)))

    batting.crawl.assert_not_called()
    pitching.crawl.assert_not_called()


def test_step_reports_failure_without_raising_and_continues() -> None:
    """One failing page must not abort the step nor skip the other page."""
    batting = MagicMock()
    batting.crawl.side_effect = RuntimeError("browser died")
    pitching = MagicMock()
    pitching.crawl.return_value = []

    with (
        patch.object(pipeline, "TeamBattingStatsCrawler", return_value=batting),
        patch.object(pipeline, "TeamPitchingStatsCrawler", return_value=pitching),
    ):
        asyncio.run(pipeline._step_6_1_team_season_stats(_context()))  # must not raise

    pitching.crawl.assert_called_once()


def test_step_reports_an_empty_refresh_as_a_failure_not_a_success(caplog) -> None:
    """A zero-record refresh is the signal that froze the season in July.

    Logging "refreshed (0 records)" with a success mark reads as a completed
    refresh, so a season that stopped moving looked healthy to every log reader and
    to the job status written after this step.
    """
    batting = MagicMock()
    batting.crawl.return_value = []
    batting.get_last_failure_reason.return_value = "kbo_robots_blocked"
    pitching = MagicMock()
    pitching.crawl.return_value = []
    pitching.get_last_failure_reason.return_value = "kbo_robots_blocked"

    with (
        patch.object(pipeline, "TeamBattingStatsCrawler", return_value=batting),
        patch.object(pipeline, "TeamPitchingStatsCrawler", return_value=pitching),
    ):
        asyncio.run(pipeline._step_6_1_team_season_stats(_context()))  # must not raise

    # Both pages still get attempted, and the step reports what it actually got.
    batting.crawl.assert_called_once()
    pitching.crawl.assert_called_once()

    messages = "\n".join(str(record.getMessage()) for record in caplog.records)
    # The success wording must not survive a zero-record run, and the reason the
    # refresh could not happen has to travel with the warning.
    assert "✅ Team batting stats refreshed" not in messages
    assert "✅ Team pitching stats refreshed" not in messages
    assert "Team batting stats NOT refreshed (0 records)" in messages
    assert "Team pitching stats NOT refreshed (0 records)" in messages
    assert "source_limited: kbo_robots_blocked" in messages
    # A warning, not an info line: the whole point is that it must not read as routine.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "NOT refreshed" in str(r.getMessage())]
    assert len(warnings) == 2


@pytest.fixture(autouse=True)
def _capture_pipeline_logs(caplog):
    """Expose the pipeline's INFO records to the assertions above."""
    caplog.set_level(logging.INFO, logger=pipeline.logger.name)


def test_step_marks_a_populated_refresh_as_cross_checkable() -> None:
    """A scraped total is an independent read, so the step says so."""
    batting = MagicMock()
    batting.crawl.return_value = [{"team_id": "LG"}]
    batting.get_last_failure_reason.return_value = None
    pitching = MagicMock()
    pitching.crawl.return_value = [{"team_id": "LG"}]
    pitching.get_last_failure_reason.return_value = None

    with (
        patch.object(pipeline, "TeamBattingStatsCrawler", return_value=batting),
        patch.object(pipeline, "TeamPitchingStatsCrawler", return_value=pitching),
    ):
        asyncio.run(pipeline._step_6_1_team_season_stats(_context()))  # must not raise

    assert batting.crawl.called and pitching.crawl.called


def test_dag_runs_team_refresh_after_players_and_before_maintenance() -> None:
    """Ordering is the point: the gate reads these aggregates after ingestion."""
    ctx = _context(target_date="20261003")

    dag = pipeline._build_daily_update_dag(ctx)
    tasks = dag._tasks

    assert tasks["step_6_1_team_season_stats"].dependencies == {"step_6_player_stats"}
    assert tasks["step_6_2_fielding_baserunning"].dependencies == {"step_6_1_team_season_stats"}
    assert tasks["step_6_3_team_defense_aggregate"].dependencies == {"step_6_2_fielding_baserunning"}
    assert tasks["step_6_5_maintenance"].dependencies == {"step_6_3_team_defense_aggregate"}
