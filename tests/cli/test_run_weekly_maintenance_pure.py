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


def test_cleanup_oci_duplicates_skips_success_and_exception() -> None:
    weekly._cleanup_oci_duplicates(None)

    with patch("src.cli.run_weekly_maintenance.cleanup_oci_duplicates", return_value={"games": 2}) as cleanup:
        weekly._cleanup_oci_duplicates("oci")
    cleanup.assert_called_once_with(database_url="oci", apply=True)

    with patch("src.cli.run_weekly_maintenance.cleanup_oci_duplicates", side_effect=RuntimeError("boom")):
        weekly._cleanup_oci_duplicates("oci")


def test_sync_weekly_to_oci_skips_success_and_exception() -> None:
    weekly._sync_weekly_to_oci(None)

    session = MagicMock()
    session_cm = MagicMock()
    session_cm.__enter__.return_value = session
    syncer = MagicMock()
    with (
        patch("src.cli.run_weekly_maintenance.SessionLocal", return_value=session_cm),
        patch("src.cli.run_weekly_maintenance.OCISync", return_value=syncer),
    ):
        weekly._sync_weekly_to_oci("oci")

    syncer.sync_kbo_seasons.assert_called_once()
    syncer.sync_cheer_chants.assert_called_once()
    syncer.close.assert_called_once()

    failing_syncer = MagicMock()
    failing_syncer.sync_player_basic.side_effect = RuntimeError("boom")
    with (
        patch("src.cli.run_weekly_maintenance.SessionLocal", return_value=session_cm),
        patch("src.cli.run_weekly_maintenance.OCISync", return_value=failing_syncer),
    ):
        weekly._sync_weekly_to_oci("oci")
    failing_syncer.close.assert_called_once()


def test_run_weekly_maintenance_routes_all_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OCI_DB_URL", "oci")
    with (
        patch("src.cli.run_weekly_maintenance._run_weekly_step", new=AsyncMock()) as run_step,
        patch("src.cli.run_weekly_maintenance._cleanup_oci_duplicates") as cleanup,
        patch("src.cli.run_weekly_maintenance._sync_weekly_to_oci") as sync,
    ):
        asyncio.run(weekly.run_weekly_maintenance(profile_limit=3, sync=True))

    assert run_step.await_count == 4
    cleanup.assert_called_once_with("oci")
    sync.assert_called_once_with("oci")


def test_main_parses_args_and_runs() -> None:
    with (
        patch("sys.argv", ["run_weekly_maintenance", "--profile-limit", "12", "--sync"]),
        patch("src.cli.run_weekly_maintenance.asyncio.run") as run,
        patch("src.cli.run_weekly_maintenance.run_weekly_maintenance", new=MagicMock(return_value="weekly")),
    ):
        assert weekly.main() == 0
    assert run.call_count == 1
