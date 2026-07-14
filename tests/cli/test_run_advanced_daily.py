from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.cli.run_advanced_daily as run_advanced_daily
from src.cli.run_advanced_daily import main


class TestRunAdvancedDailyCLI:
    def test_main_default_year(self):
        with (
            patch("sys.argv", ["run_advanced_daily"]),
            patch("src.cli.run_advanced_daily.crawl_all_fielding_stats") as mock_f,
            patch("src.cli.run_advanced_daily.crawl_baserunning_stats") as mock_b,
            patch("src.cli.run_advanced_daily.TeamBattingStatsCrawler") as MockTB,
            patch("src.cli.run_advanced_daily.TeamPitchingStatsCrawler") as MockTP,
            patch("src.cli.run_advanced_daily._aggregate_team_defense_step", new_callable=AsyncMock),
            patch("src.cli.run_advanced_daily._rebuild_rankings_step", new_callable=AsyncMock),
            patch("src.cli.run_advanced_daily.datetime") as mock_dt,
        ):
            mock_dt.now.return_value.year = 2025
            mock_f.return_value = []
            mock_b.return_value = []
            mock_tb = MagicMock()
            mock_tb.crawl = MagicMock(return_value=[])
            MockTB.return_value = mock_tb
            mock_tp = MagicMock()
            mock_tp.crawl = MagicMock(return_value=[])
            MockTP.return_value = mock_tp
            main()
            mock_f.assert_called_once_with(2025)

    def test_main_with_year(self):
        with (
            patch("sys.argv", ["run_advanced_daily", "--year", "2024"]),
            patch("src.cli.run_advanced_daily.crawl_all_fielding_stats") as mock_f,
            patch("src.cli.run_advanced_daily.crawl_baserunning_stats") as mock_b,
            patch("src.cli.run_advanced_daily.TeamBattingStatsCrawler") as MockTB,
            patch("src.cli.run_advanced_daily.TeamPitchingStatsCrawler") as MockTP,
            patch("src.cli.run_advanced_daily.SessionLocal") as mock_sesh,
            patch("src.cli.run_advanced_daily.PlayerSeasonFieldingRepository"),
            patch("src.cli.run_advanced_daily.PlayerSeasonBaserunningRepository"),
            patch("src.cli.run_advanced_daily._aggregate_team_defense_step", new_callable=AsyncMock),
            patch("src.cli.run_advanced_daily._rebuild_rankings_step", new_callable=AsyncMock),
        ):
            mock_f.return_value = []
            mock_b.return_value = []
            mock_tb = MagicMock()
            mock_tb.crawl = MagicMock(return_value=[])
            MockTB.return_value = mock_tb
            mock_tp = MagicMock()
            mock_tp.crawl = MagicMock(return_value=[])
            MockTP.return_value = mock_tp
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            main()
            mock_f.assert_called_once_with(2024)

    def test_main_passes_sync_and_visible_browser_options(self):
        with (
            patch("sys.argv", ["run_advanced_daily", "--year", "2024", "--sync", "--no-headless"]),
            patch("src.cli.run_advanced_daily.run_advanced_update", new_callable=AsyncMock) as update,
        ):
            assert main() == 0

        update.assert_awaited_once_with(2024, sync=True, headless=False)


class TestAdvancedDailySteps:
    def test_filter_player_rows_removes_unknown_columns_and_missing_players(self):
        records = [
            {"player_id": 1, "season": 2025, "hits": 10, "unknown": "ignore"},
            {"season": 2025, "hits": 5},
        ]

        assert run_advanced_daily._filter_player_rows(records, {"player_id", "season", "hits"}) == [
            {"player_id": 1, "season": 2025, "hits": 10},
        ]

    def test_run_step_reports_known_error(self):
        async def failing_action():
            raise RuntimeError("crawler failed")

        assert asyncio.run(run_advanced_daily._run_step("Step", "failure", failing_action)) is True

    def test_crawl_fielding_step_filters_and_saves_records(self):
        model = SimpleNamespace(
            __table__=SimpleNamespace(
                columns=[
                    SimpleNamespace(key="player_id"),
                    SimpleNamespace(key="season"),
                    SimpleNamespace(key="assists"),
                ],
            ),
        )
        records = [{"player_id": 1, "season": 2025, "assists": 12, "discard": "value"}]
        with (
            patch("src.models.player.PlayerSeasonFielding", model),
            patch("src.cli.run_advanced_daily.crawl_all_fielding_stats", return_value=records),
            patch("src.cli.run_advanced_daily.PlayerSeasonFieldingRepository") as repository,
        ):
            repository.return_value.upsert_many.return_value = 1

            asyncio.run(run_advanced_daily._crawl_fielding_step(2025))

        repository.return_value.upsert_many.assert_called_once_with(
            [{"player_id": 1, "season": 2025, "assists": 12}],
        )

    def test_crawl_baserunning_step_filters_and_saves_records(self):
        model = SimpleNamespace(
            __table__=SimpleNamespace(
                columns=[
                    SimpleNamespace(key="player_id"),
                    SimpleNamespace(key="season"),
                    SimpleNamespace(key="stolen_bases"),
                ],
            ),
        )
        records = [{"player_id": 1, "season": 2025, "stolen_bases": 7, "discard": "value"}]
        with (
            patch("src.models.player.PlayerSeasonBaserunning", model),
            patch("src.cli.run_advanced_daily.crawl_baserunning_stats", return_value=records),
            patch("src.cli.run_advanced_daily.PlayerSeasonBaserunningRepository") as repository,
        ):
            repository.return_value.upsert_many.return_value = 1

            asyncio.run(run_advanced_daily._crawl_baserunning_step(2025))

        repository.return_value.upsert_many.assert_called_once_with(
            [{"player_id": 1, "season": 2025, "stolen_bases": 7}],
        )

    def test_aggregate_team_defense_uses_active_teams(self):
        team = MagicMock()
        team.team_id = "team_id"
        team.is_active = "is_active"
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            SimpleNamespace(team_id=1),
            SimpleNamespace(team_id=2),
        ]
        with (
            patch("src.models.team.Team", team),
            patch("src.cli.run_advanced_daily.SessionLocal") as session_local,
            patch("src.aggregators.team_fielding_aggregator.TeamFieldingAggregator") as aggregator,
        ):
            session_local.return_value.__enter__.return_value = session

            asyncio.run(run_advanced_daily._aggregate_team_defense_step(2025))

        aggregator.return_value.run_all.assert_called_once_with(2025, [1, 2])

    def test_rebuild_rankings_step_runs_in_thread(self):
        with patch("src.cli.calculate_rankings.rebuild_rankings", return_value=12) as rebuild:
            asyncio.run(run_advanced_daily._rebuild_rankings_step(2025))

        rebuild.assert_called_once_with(2025)

    def test_sync_advanced_to_oci_skips_without_url(self):
        with patch.dict("os.environ", {"OCI_DB_URL": ""}, clear=False):
            assert run_advanced_daily._sync_advanced_to_oci(2025) is False

    def test_sync_advanced_to_oci_runs_all_syncs_and_closes_connection(self):
        syncer = MagicMock()
        with (
            patch.dict("os.environ", {"OCI_DB_URL": "oci-url"}, clear=False),
            patch("src.cli.run_advanced_daily.SessionLocal") as session_local,
            patch("src.cli.run_advanced_daily.OCISync", return_value=syncer),
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            assert run_advanced_daily._sync_advanced_to_oci(2025) is False

        syncer.sync_fielding_stats.assert_called_once_with(2025)
        syncer.sync_baserunning_stats.assert_called_once_with(2025)
        syncer.sync_team_season_batting.assert_called_once_with(2025)
        syncer.sync_team_season_pitching.assert_called_once_with(2025)
        syncer.sync_team_season_fielding.assert_called_once_with(2025)
        syncer.sync_team_season_baserunning.assert_called_once_with(2025)
        syncer.sync_stat_rankings.assert_called_once_with(2025)
        syncer.close.assert_called_once()

    def test_sync_advanced_to_oci_returns_error_and_closes_connection(self):
        syncer = MagicMock()
        syncer.sync_fielding_stats.side_effect = RuntimeError("target unavailable")
        with (
            patch.dict("os.environ", {"OCI_DB_URL": "oci-url"}, clear=False),
            patch("src.cli.run_advanced_daily.SessionLocal") as session_local,
            patch("src.cli.run_advanced_daily.OCISync", return_value=syncer),
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            assert run_advanced_daily._sync_advanced_to_oci(2025) is True

        syncer.close.assert_called_once()

    def test_run_advanced_update_raises_after_any_step_error(self):
        with (
            patch("src.cli.run_advanced_daily._run_step", new_callable=AsyncMock, return_value=False) as run_step,
            patch("src.cli.run_advanced_daily._sync_advanced_to_oci", return_value=True) as sync,
        ):
            with pytest.raises(RuntimeError, match="finished with errors"):
                asyncio.run(run_advanced_daily.run_advanced_update(2025, sync=True))

        assert run_step.await_count == 6
        sync.assert_called_once_with(2025)
