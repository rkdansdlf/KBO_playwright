from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import src.cli.daily_preview_batch as daily_preview_batch
from src.cli.daily_preview_batch import main


class TestDailyPreviewBatchCLI:
    def test_main_no_previews(self):
        with (
            patch("src.cli.daily_preview_batch.PreviewCrawler") as MockCrawler,
            patch("src.cli.daily_preview_batch.write_refresh_manifest") as mock_manifest,
            patch("src.cli.daily_preview_batch.datetime") as mock_dt,
        ):
            mock_dt.now.return_value.strftime.return_value = "20251015"
            mock_instance = MagicMock()
            mock_instance.crawl_preview_for_date = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_manifest.return_value = "/tmp/manifest.json"

            result = main(["--date", "20251015", "--no-sync"])
            assert result == 0
            mock_instance.crawl_preview_for_date.assert_called_once_with("20251015")

    def test_main_with_previews(self):
        with (
            patch("src.cli.daily_preview_batch.PreviewCrawler") as MockCrawler,
            patch("src.cli.daily_preview_batch.SessionLocal"),
            patch("src.cli.daily_preview_batch.ContextAggregator"),
            patch("src.cli.daily_preview_batch.save_pregame_lineups") as mock_save,
            patch("src.cli.daily_preview_batch.write_refresh_manifest"),
            patch("src.cli.daily_preview_batch.datetime") as mock_dt,
        ):
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_dt.strptime.return_value.date.return_value.year = 2025
            mock_dt.strptime.return_value.replace.return_value.date.return_value = MagicMock()
            mock_dt.strptime.return_value.replace.return_value.date.return_value.year = 2025
            mock_instance = MagicMock()
            mock_instance.crawl_preview_for_date = AsyncMock(
                return_value=[{"game_id": "20251015LGHH0", "away_team_name": "LG", "home_team_name": "SS"}],
            )
            MockCrawler.return_value = mock_instance
            mock_save.return_value = True

            result = main(["--date", "20251015", "--no-sync"])
            assert result == 0


class TestPreviewContext:
    def test_adds_team_context_and_optional_series_context(self):
        preview: dict[str, object] = {
            "game_id": "20251015LGSS0",
            "away_team_name": "LG",
            "home_team_name": "SSG",
        }
        aggregator = MagicMock()
        aggregator.get_head_to_head_summary.return_value = {"wins": 3}
        aggregator.get_team_l10_summary.side_effect = [{"wins": 7}, {"wins": 5}]
        aggregator.get_team_recent_metrics.side_effect = [{"era": 2.5}, {"era": 3.5}]
        aggregator.get_recent_player_movements.side_effect = [["away move"], ["home move"]]
        aggregator.get_daily_roster_changes.side_effect = [["away roster"], ["home roster"]]
        aggregator.get_postseason_series_summary.return_value = {"round": "playoff"}

        with patch("src.cli.daily_preview_batch.resolve_team_code", side_effect=["LG", "SS"]):
            daily_preview_batch._add_team_context(preview, aggregator, 2025, date(2025, 10, 15))

        assert preview["matchup_h2h"] == {"wins": 3}
        assert preview["away_recent_l10"] == {"wins": 7}
        assert preview["home_metrics"] == {"era": 3.5}
        assert preview["away_movements"] == ["away move"]
        assert preview["home_roster_changes"] == ["home roster"]
        assert preview["series_context"] == {"round": "playoff"}

    def test_skips_team_context_when_team_code_cannot_be_resolved(self):
        preview: dict[str, object] = {"away_team_name": "LG", "home_team_name": "Unknown"}
        aggregator = MagicMock()

        with patch("src.cli.daily_preview_batch.resolve_team_code", side_effect=["LG", None]):
            daily_preview_batch._add_team_context(preview, aggregator, 2025, date(2025, 10, 15))

        aggregator.get_head_to_head_summary.assert_not_called()

    def test_swallows_team_context_errors(self):
        preview: dict[str, object] = {"game_id": "G1", "away_team_name": "LG", "home_team_name": "SSG"}
        aggregator = MagicMock()
        aggregator.get_head_to_head_summary.side_effect = RuntimeError("database unavailable")

        with patch("src.cli.daily_preview_batch.resolve_team_code", side_effect=["LG", "SS"]):
            daily_preview_batch._add_team_context(preview, aggregator, 2025, date(2025, 10, 15))

        assert "matchup_h2h" not in preview

    def test_adds_pitcher_context_for_available_starters(self):
        preview: dict[str, object] = {"away_starter_id": 101, "home_starter_id": 202}
        aggregator = MagicMock()
        aggregator.get_pitcher_season_stats.side_effect = [{"era": 2.1}, {"era": 3.1}]

        daily_preview_batch._add_pitcher_context(preview, aggregator, 2025)

        assert preview["away_starter_stats"] == {"era": 2.1}
        assert preview["home_starter_stats"] == {"era": 3.1}
        assert aggregator.get_pitcher_season_stats.call_args_list[0].args == (101, 2025)

    def test_swallows_pitcher_context_errors(self):
        preview: dict[str, object] = {"game_id": "G1", "away_starter_id": 101}
        aggregator = MagicMock()
        aggregator.get_pitcher_season_stats.side_effect = RuntimeError("database unavailable")

        daily_preview_batch._add_pitcher_context(preview, aggregator, 2025)

        assert "away_starter_stats" not in preview


class TestPreviewPersistence:
    def test_saves_contexts_only_for_previews_with_successful_lineups(self):
        session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = session
        previews: list[dict[str, object]] = [{"game_id": "G1"}, {}, {"game_id": "G2"}]

        with (
            patch("src.cli.daily_preview_batch.SessionLocal", session_factory),
            patch("src.cli.daily_preview_batch.ContextAggregator"),
            patch("src.cli.daily_preview_batch._add_team_context") as add_team_context,
            patch("src.cli.daily_preview_batch._add_pitcher_context") as add_pitcher_context,
            patch("src.cli.daily_preview_batch.save_pregame_lineups", side_effect=[True, False]),
        ):
            saved_ids = daily_preview_batch._save_preview_contexts(previews, "20251015")

        assert saved_ids == ["G1"]
        assert add_team_context.call_count == 2
        assert add_pitcher_context.call_count == 2

    def test_sync_skips_when_oci_is_not_configured(self, monkeypatch):
        monkeypatch.delenv("OCI_DB_URL", raising=False)

        with patch("src.cli.daily_preview_batch.SessionLocal") as session_factory:
            daily_preview_batch._sync_saved_pregame_games(["G1"])

        session_factory.assert_not_called()

    def test_syncs_unique_game_ids_and_closes_syncer(self, monkeypatch):
        monkeypatch.setenv("OCI_DB_URL", "postgresql://oci")
        session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = session
        syncer = MagicMock()

        with (
            patch("src.cli.daily_preview_batch.SessionLocal", session_factory),
            patch("src.cli.daily_preview_batch.OCISync", return_value=syncer) as sync_class,
        ):
            daily_preview_batch._sync_saved_pregame_games(["G2", "G1", "G2"])

        sync_class.assert_called_once_with("postgresql://oci", session)
        assert syncer.sync_pregame_game.call_args_list[0].args == ("G1",)
        assert syncer.sync_pregame_game.call_args_list[1].args == ("G2",)
        syncer.close.assert_called_once()


class TestRunPreviewBatch:
    def test_saves_syncs_and_writes_manifest_for_previews(self):
        crawler = MagicMock()
        crawler.crawl_preview_for_date = AsyncMock(return_value=[{"game_id": "G1"}])

        with (
            patch("src.cli.daily_preview_batch.PreviewCrawler", return_value=crawler),
            patch("src.cli.daily_preview_batch._save_preview_contexts", return_value=["G2", "G1"]) as save_contexts,
            patch("src.cli.daily_preview_batch._sync_saved_pregame_games") as sync_games,
            patch("src.cli.daily_preview_batch._write_pregame_manifest", return_value="manifest.json") as manifest,
        ):
            saved_ids = asyncio.run(daily_preview_batch.run_preview_batch("20251015", sync_to_oci=True))

        assert saved_ids == ["G2", "G1"]
        save_contexts.assert_called_once_with([{"game_id": "G1"}], "20251015")
        sync_games.assert_called_once_with(["G2", "G1"])
        manifest.assert_called_once_with("20251015", ["G2", "G1"])

    def test_does_not_sync_when_no_games_were_saved(self):
        crawler = MagicMock()
        crawler.crawl_preview_for_date = AsyncMock(return_value=[{"game_id": "G1"}])

        with (
            patch("src.cli.daily_preview_batch.PreviewCrawler", return_value=crawler),
            patch("src.cli.daily_preview_batch._save_preview_contexts", return_value=[]),
            patch("src.cli.daily_preview_batch._sync_saved_pregame_games") as sync_games,
            patch("src.cli.daily_preview_batch._write_pregame_manifest", return_value="manifest.json") as manifest,
        ):
            saved_ids = asyncio.run(daily_preview_batch.run_preview_batch("20251015", sync_to_oci=True))

        assert saved_ids == []
        sync_games.assert_not_called()
        manifest.assert_called_once_with("20251015", [])
