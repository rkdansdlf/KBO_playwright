from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import src.cli.daily_highlight_batch as daily_highlight_batch
from src.cli.daily_highlight_batch import main


class TestDailyHighlightBatchCLI:
    def test_main_no_games(self):
        with (
            patch("src.cli.daily_highlight_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_highlight_batch.HighlightAggregator"),
            patch("src.cli.daily_highlight_batch.datetime") as mock_dt,
        ):
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015"])
            assert result == 0

    def test_main_dry_run(self):
        with (
            patch("src.cli.daily_highlight_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_highlight_batch.HighlightAggregator"),
            patch("src.cli.daily_highlight_batch.datetime") as mock_dt,
        ):
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = [mock_game]
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015", "--dry-run"])
            assert result == 0

    def test_main_force(self):
        with (
            patch("src.cli.daily_highlight_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_highlight_batch.HighlightAggregator") as MockAgg,
            patch("src.cli.daily_highlight_batch.datetime") as mock_dt,
        ):
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = [mock_game]
            mock_sesh.return_value.__enter__.return_value = mock_session
            mock_agg = MagicMock()
            mock_agg.aggregate_game_highlights.return_value = [MagicMock()]
            MockAgg.return_value = mock_agg

            result = main(["--date", "20251015", "--force", "--no-sync", "--no-notify"])
            assert result == 0
            mock_agg.aggregate_game_highlights.assert_called_once_with("20251015LGHH0")

    def test_main_uses_today_and_passes_all_options(self):
        batch = AsyncMock(return_value=[])

        with (
            patch("src.cli.daily_highlight_batch.datetime") as mock_datetime,
            patch("src.cli.daily_highlight_batch.run_highlight_batch", batch),
        ):
            mock_datetime.now.return_value.strftime.return_value = "20251016"

            assert main(["--force", "--dry-run", "--no-sync", "--no-notify"]) == 0

        batch.assert_awaited_once_with(
            "20251016",
            force=True,
            dry_run=True,
            sync_to_oci=False,
            notify=False,
        )


class TestHighlightProcessing:
    def test_process_skips_existing_ignores_empty_and_saves_new_highlights(self):
        session = MagicMock()
        existing = SimpleNamespace(game_id="G1")
        generated = SimpleNamespace(game_id="G3")
        session.query.return_value.filter.return_value.all.side_effect = [[existing], [], []]
        aggregator = MagicMock()
        aggregator.aggregate_game_highlights.side_effect = [[], [generated]]
        games = [
            SimpleNamespace(game_id="G1", away_team="LG", home_team="SSG"),
            SimpleNamespace(game_id="G2", away_team="KT", home_team="NC"),
            SimpleNamespace(game_id="G3", away_team="KIA", home_team="DOOSAN"),
        ]

        with patch("src.cli.daily_highlight_batch.HighlightAggregator", return_value=aggregator):
            processed_ids, highlights_by_game, game_map = daily_highlight_batch._process_highlight_games(
                session,
                games,
                force=False,
                dry_run=False,
            )

        assert processed_ids == ["G1", "G3"]
        assert highlights_by_game == {"G1": [existing], "G3": [generated]}
        assert game_map["G2"] is games[1]
        aggregator.aggregate_game_highlights.assert_has_calls([call("G2"), call("G3")])
        aggregator.save_highlights.assert_called_once_with("G3", [generated])

    def test_process_dry_run_does_not_save(self):
        session = MagicMock()
        aggregator = MagicMock()
        highlight = SimpleNamespace(game_id="G1")
        aggregator.aggregate_game_highlights.return_value = [highlight]
        game = SimpleNamespace(game_id="G1", away_team="LG", home_team="SSG")

        with patch("src.cli.daily_highlight_batch.HighlightAggregator", return_value=aggregator):
            processed_ids, highlights_by_game, _ = daily_highlight_batch._process_highlight_games(
                session,
                [game],
                force=False,
                dry_run=True,
            )

        assert processed_ids == ["G1"]
        assert highlights_by_game == {"G1": [highlight]}
        aggregator.save_highlights.assert_not_called()

    def test_load_completed_games_returns_filtered_query_results(self):
        session = MagicMock()
        games = [MagicMock()]
        session.query.return_value.filter.return_value.all.return_value = games

        result = daily_highlight_batch._load_completed_games(session, date(2025, 10, 15))

        assert result == games
        session.query.assert_called_once()


class TestHighlightNotificationsAndSync:
    def test_syncs_unique_game_ids_and_closes_syncer(self, monkeypatch):
        monkeypatch.setenv("OCI_DB_URL", "postgresql://oci")
        sync_session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = sync_session
        syncer = MagicMock()

        with (
            patch("src.cli.daily_highlight_batch.SessionLocal", session_factory),
            patch("src.cli.daily_highlight_batch.OCISync", return_value=syncer) as sync_class,
        ):
            daily_highlight_batch._sync_highlights_to_oci(
                ["G2", "G1", "G2"],
                dry_run=False,
                sync_to_oci=True,
            )

        sync_class.assert_called_once_with("postgresql://oci", sync_session)
        assert syncer.sync_specific_game.call_args_list[0].args == ("G1",)
        assert syncer.sync_specific_game.call_args_list[1].args == ("G2",)
        syncer.close.assert_called_once()

    def test_sync_failure_is_logged_and_syncer_is_closed(self, monkeypatch):
        monkeypatch.setenv("OCI_DB_URL", "postgresql://oci")
        syncer = MagicMock()
        syncer.sync_specific_game.side_effect = RuntimeError("sync failed")
        session_factory = MagicMock()

        with (
            patch("src.cli.daily_highlight_batch.SessionLocal", session_factory),
            patch("src.cli.daily_highlight_batch.OCISync", return_value=syncer),
        ):
            daily_highlight_batch._sync_highlights_to_oci(["G1"], dry_run=False, sync_to_oci=True)

        syncer.close.assert_called_once()

    def test_sync_skips_without_oci_configuration(self, monkeypatch):
        monkeypatch.delenv("OCI_DB_URL", raising=False)

        with patch("src.cli.daily_highlight_batch.SessionLocal") as session_factory:
            daily_highlight_batch._sync_highlights_to_oci(["G1"], dry_run=False, sync_to_oci=None)

        session_factory.assert_not_called()

    def test_formats_special_matchups_and_top_plays(self):
        game_one = SimpleNamespace(away_team="LG", home_team="SSG", away_score=3, home_score=2)
        game_two = SimpleNamespace(away_team="KIA", home_team="DOOSAN", away_score=4, home_score=5)
        lead_change = SimpleNamespace(
            game_id="G1",
            highlight_type="LEAD_CHANGE",
            tags=[],
            wpa=0.250,
            inning=8,
            inning_half="top",
            description="go-ahead double",
        )
        walk_off = SimpleNamespace(
            game_id="G2",
            highlight_type="WALK_OFF",
            tags=[],
            wpa=-0.500,
            inning=9,
            inning_half="bottom",
            description="walk-off single",
        )

        message = daily_highlight_batch._highlight_notification_message(
            "20251015",
            ["G1", "G2"],
            {"G1": [lead_change], "G2": [walk_off]},
            {"G1": game_one, "G2": game_two},
        )

        assert "2" in message
        assert "LG vs SSG" in message
        assert "KIA vs DOOSAN" in message
        assert message.index("walk-off single") < message.index("go-ahead double")

    def test_formats_empty_highlights_and_sends_notification_outcomes(self):
        assert daily_highlight_batch._format_top_highlight_plays([], {}).endswith("\n")

        with patch("src.cli.daily_highlight_batch.SlackWebhookClient.send_alert", side_effect=[True, False]) as send:
            daily_highlight_batch._send_highlight_notification("summary", dry_run=False)
            daily_highlight_batch._send_highlight_notification("summary", dry_run=False)
            daily_highlight_batch._send_highlight_notification("dry run", dry_run=True)

        assert send.call_count == 2


class TestRunHighlightBatch:
    def test_returns_empty_list_for_invalid_date(self):
        with patch("src.cli.daily_highlight_batch.parse_date_str", side_effect=ValueError("bad date")):
            result = asyncio.run(daily_highlight_batch.run_highlight_batch("not-a-date"))

        assert result == []

    def test_orchestrates_processing_sync_and_notification(self):
        session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = session
        game = SimpleNamespace(game_id="G1")
        processed = (["G1"], {"G1": []}, {"G1": game})

        with (
            patch("src.cli.daily_highlight_batch.parse_date_str", return_value=date(2025, 10, 15)),
            patch("src.cli.daily_highlight_batch.SessionLocal", session_factory),
            patch("src.cli.daily_highlight_batch._load_completed_games", return_value=[game]) as load_games,
            patch("src.cli.daily_highlight_batch._process_highlight_games", return_value=processed) as process,
            patch("src.cli.daily_highlight_batch._sync_highlights_to_oci") as sync,
            patch("src.cli.daily_highlight_batch._highlight_notification_message", return_value="summary"),
            patch("src.cli.daily_highlight_batch._send_highlight_notification") as notify,
        ):
            result = asyncio.run(
                daily_highlight_batch.run_highlight_batch(
                    "20251015",
                    force=True,
                    dry_run=False,
                    sync_to_oci=True,
                    notify=True,
                ),
            )

        assert result == ["G1"]
        load_games.assert_called_once_with(session, date(2025, 10, 15))
        process.assert_called_once_with(session, [game], force=True, dry_run=False)
        sync.assert_called_once_with(["G1"], dry_run=False, sync_to_oci=True)
        notify.assert_called_once_with("summary", dry_run=False)
