"""Pure/mock tests for weekly maintenance orchestrator."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli import run_weekly_maintenance as weekly


def test_profile_delay_valid_and_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILE_BACKFILL_DELAY", "2.5")
    assert weekly._profile_delay() == 2.5

    monkeypatch.setenv("PROFILE_BACKFILL_DELAY", "bad")
    assert weekly._profile_delay() == 1.5


def test_run_weekly_step_success_and_exception() -> None:
    action = AsyncMock()
    asyncio.run(weekly._run_weekly_step("step", "error", action))
    action.assert_awaited_once()

    failing_action = AsyncMock(side_effect=RuntimeError("boom"))
    asyncio.run(weekly._run_weekly_step("step", "error", failing_action))
    failing_action.assert_awaited_once()


def test_profile_enrichment_step_invokes_backfill_and_collect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILE_BACKFILL_DELAY", "0.1")
    backfill = AsyncMock()
    collect = AsyncMock()

    with (
        patch.dict("sys.modules", {"scripts.backfill_player_profiles": MagicMock(backfill=backfill)}),
        patch("src.cli.run_weekly_maintenance.collect_profiles", collect),
    ):
        asyncio.run(weekly._profile_enrichment_step(7))

    backfill.assert_awaited_once_with(limit=7, delay=0.1)
    collect.assert_awaited_once_with(limit=7)


def test_healthcheck_step_invokes_cli() -> None:
    with patch("src.cli.run_weekly_maintenance.healthcheck_main") as healthcheck:
        asyncio.run(weekly._healthcheck_step())
    healthcheck.assert_called_once_with([])


def test_team_events_and_fan_culture_steps() -> None:
    team_crawler = MagicMock()
    team_crawler.run = AsyncMock()
    fan_crawler = MagicMock()
    fan_crawler.run = AsyncMock()

    with (
        patch.dict(
            "sys.modules",
            {"src.crawlers.team_event_crawler": MagicMock(TeamEventCrawler=MagicMock(return_value=team_crawler))},
        ),
        patch.dict(
            "sys.modules",
            {"src.crawlers.fan_culture_crawler": MagicMock(FanCultureCrawler=MagicMock(return_value=fan_crawler))},
        ),
    ):
        asyncio.run(weekly._team_events_step())
        asyncio.run(weekly._fan_culture_step())

    team_crawler.run.assert_awaited_once_with(save=True)
    fan_crawler.run.assert_awaited_once_with(save=True)


def test_resolve_null_player_ids_step_invokes_resolver() -> None:
    mock_resolve = MagicMock(return_value={"resolved_groups": 1, "updated_rows": 2, "duplicate_null_rows": 0})
    with patch(
        "scripts.maintenance.resolve_null_player_ids_conservative.resolve_null_player_ids",
        mock_resolve,
    ):
        asyncio.run(weekly._resolve_null_player_ids_step())
    mock_resolve.assert_called_once()


def test_run_weekly_maintenance_routes_all_steps() -> None:
    with (
        patch("src.cli.run_weekly_maintenance._run_weekly_step", new=AsyncMock()) as run_step,
    ):
        asyncio.run(weekly.run_weekly_maintenance(profile_limit=3))

    assert run_step.await_count == 7


def test_main_parses_args_and_runs() -> None:
    with (
        patch("sys.argv", ["run_weekly_maintenance", "--profile-limit", "12"]),
        patch("src.cli.run_weekly_maintenance.asyncio.run") as run,
        patch("src.cli.run_weekly_maintenance.run_weekly_maintenance", new=MagicMock(return_value="weekly")),
    ):
        assert weekly.main() == 0
    assert run.call_count == 1
