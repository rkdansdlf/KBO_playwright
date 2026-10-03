"""Fielding/baserunning refresh and team defense aggregation step tests.

Second half of the same gap as the team season stats: the player fielding and
baserunning lines that feed team defense were only crawled by GitHub Actions'
advanced daily, so the canonical scheduler aggregated stale inputs.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.cli.pipelines import run_daily_update as pipeline


def _context(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {"year": 2026, "headless": True, "skip_season_stats": False}
    base.update(overrides)
    return SimpleNamespace(**base)


def test_fielding_and_baserunning_are_both_refreshed() -> None:
    """Both player pages are crawled, in order."""
    fielding = AsyncMock()
    baserunning = AsyncMock()

    with (
        patch.object(pipeline, "crawl_fielding_step", fielding),
        patch.object(pipeline, "crawl_baserunning_step", baserunning),
    ):
        asyncio.run(pipeline._step_6_2_fielding_baserunning(_context()))

    fielding.assert_awaited_once_with(2026)
    baserunning.assert_awaited_once_with(2026)


def test_baserunning_still_runs_when_fielding_fails() -> None:
    """One failing page must not abort the step nor skip the other page."""
    fielding = AsyncMock(side_effect=RuntimeError("browser died"))
    baserunning = AsyncMock()

    with (
        patch.object(pipeline, "crawl_fielding_step", fielding),
        patch.object(pipeline, "crawl_baserunning_step", baserunning),
    ):
        asyncio.run(pipeline._step_6_2_fielding_baserunning(_context()))  # must not raise

    baserunning.assert_awaited_once_with(2026)


def test_fielding_step_respects_operator_season_stats_flag() -> None:
    """``--skip-season-stats`` must skip both player pages."""
    fielding = AsyncMock()
    baserunning = AsyncMock()

    with (
        patch.object(pipeline, "crawl_fielding_step", fielding),
        patch.object(pipeline, "crawl_baserunning_step", baserunning),
    ):
        asyncio.run(pipeline._step_6_2_fielding_baserunning(_context(skip_season_stats=True)))

    fielding.assert_not_awaited()
    baserunning.assert_not_awaited()


def test_team_defense_is_aggregated_for_the_season() -> None:
    """The aggregate runs after the player lines it derives from."""
    aggregate = AsyncMock()

    with patch.object(pipeline, "aggregate_team_defense_step", aggregate):
        asyncio.run(pipeline._step_6_3_team_defense_aggregate(_context()))

    aggregate.assert_awaited_once_with(2026)


def test_team_defense_failure_is_reported_not_raised() -> None:
    """A failure becomes a tracked gate warning, not a broken pipeline."""
    aggregate = AsyncMock(side_effect=RuntimeError("aggregate failed"))

    with patch.object(pipeline, "aggregate_team_defense_step", aggregate):
        asyncio.run(pipeline._step_6_3_team_defense_aggregate(_context()))  # must not raise


def test_team_defense_respects_operator_season_stats_flag() -> None:
    """``--skip-season-stats`` must skip the aggregate too."""
    aggregate = AsyncMock()

    with patch.object(pipeline, "aggregate_team_defense_step", aggregate):
        asyncio.run(pipeline._step_6_3_team_defense_aggregate(_context(skip_season_stats=True)))

    aggregate.assert_not_awaited()
