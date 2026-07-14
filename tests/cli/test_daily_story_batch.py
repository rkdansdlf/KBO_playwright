from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.cli.daily_story_batch as daily_story_batch
from src.cli.daily_story_batch import main


class TestDailyStoryBatchCLI:
    def test_main_no_games(self):
        with (
            patch("src.cli.daily_story_batch.refresh_game_status_for_date") as mock_status,
            patch("src.cli.daily_story_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_story_batch.write_refresh_manifest") as mock_manifest,
        ):
            mock_status.return_value = {"updated": 0}
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015"])
            assert result == 0
            mock_manifest.assert_called_once()

    def test_main_with_games(self):
        with (
            patch("src.cli.daily_story_batch.refresh_game_status_for_date") as mock_status,
            patch("src.cli.daily_story_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_story_batch.GameStoryBuilder") as MockBuilder,
            patch("src.cli.daily_story_batch.write_refresh_manifest"),
        ):
            mock_status.return_value = {"updated": 0}
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_game]
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session
            mock_builder = MagicMock()
            mock_builder.build.return_value = {"timeline": [{"event": "HR"}], "source": {}}
            MockBuilder.return_value = mock_builder

            result = main(["--date", "20251015"])
            assert result == 0

    def test_main_uses_today_and_passes_no_sync_option(self):
        batch = AsyncMock(return_value=[])

        with (
            patch("src.cli.daily_story_batch.datetime") as mock_datetime,
            patch("src.cli.daily_story_batch.run_story_batch", batch),
        ):
            mock_datetime.now.return_value.strftime.return_value = "20251016"

            assert main(["--no-sync"]) == 0

        batch.assert_awaited_once_with("20251016", sync_to_oci=False)


class TestStoryHelpers:
    def test_dump_story_json_serializes_payload(self):
        assert daily_story_batch.dump_story_json({"summary": "KBO"}) == '{"summary": "KBO"}'

    def test_upsert_creates_new_summary(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        with patch("src.cli.daily_story_batch.GameSummary") as summary_class:
            daily_story_batch._upsert_story_summary(session, "G1", '{"timeline": []}')

        summary_class.assert_called_once_with(
            game_id="G1",
            summary_type=daily_story_batch.STORY_SUMMARY_TYPE,
            detail_text='{"timeline": []}',
        )
        session.add.assert_called_once()

    def test_upsert_updates_all_existing_summaries(self):
        session = MagicMock()
        first = MagicMock()
        second = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [first, second]

        daily_story_batch._upsert_story_summary(session, "G1", '{"timeline": [1]}')

        assert first.detail_text == '{"timeline": [1]}'
        assert second.detail_text == '{"timeline": [1]}'
        session.add.assert_not_called()

    def test_build_story_data_fetches_ordered_game_events(self):
        session = MagicMock()
        events = [MagicMock()]
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = events
        builder = MagicMock()
        builder.build.return_value = {"timeline": []}
        game = SimpleNamespace(game_id="G1")

        result = daily_story_batch._build_story_data(builder, session, game)

        assert result == {"timeline": []}
        builder.build.assert_called_once_with(game, events)

    def test_trusted_relay_ids_handles_empty_status_and_wpa_fallback(self):
        session = MagicMock()
        query = session.query.return_value
        query.filter.return_value.all.return_value = []
        query.filter.return_value.distinct.return_value.all.return_value = [("G2",)]

        assert daily_story_batch._trusted_relay_game_ids(session, []) == set()
        assert daily_story_batch._trusted_relay_game_ids(session, ["G2"]) == {"G2"}

    def test_trusted_relay_ids_accepts_verified_status(self):
        session = MagicMock()
        verified = SimpleNamespace(game_id="G1", validation_status="verified")
        session.query.return_value.filter.return_value.all.return_value = [verified]

        assert daily_story_batch._trusted_relay_game_ids(session, ["G1"]) == {"G1"}


class TestStorySync:
    def test_sync_skips_without_oci_url(self, monkeypatch):
        monkeypatch.delenv("OCI_DB_URL", raising=False)

        with patch("src.cli.daily_story_batch.SessionLocal") as session_factory:
            daily_story_batch._sync_story_summaries(["G1"])

        session_factory.assert_not_called()

    def test_syncs_unique_game_ids_and_closes_syncer(self, monkeypatch):
        monkeypatch.setenv("OCI_DB_URL", "postgresql://oci")
        sync_session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = sync_session
        syncer = MagicMock()

        with (
            patch("src.cli.daily_story_batch.SessionLocal", session_factory),
            patch("src.cli.daily_story_batch.OCISync", return_value=syncer) as sync_class,
        ):
            daily_story_batch._sync_story_summaries(["G2", "G1", "G2"])

        sync_class.assert_called_once_with("postgresql://oci", sync_session)
        syncer.sync_review_summaries_for_games.assert_called_once_with(
            ["G1", "G2"],
            summary_type=daily_story_batch.STORY_SUMMARY_TYPE,
        )
        syncer.close.assert_called_once()


class TestRunStoryBatch:
    def test_saves_trusted_games_syncs_and_writes_manifest(self):
        session = MagicMock()
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = session
        trusted_game = SimpleNamespace(game_id="G1")
        untrusted_game = SimpleNamespace(game_id="G2")
        story_data = {"timeline": [], "source": {"warnings": ["missing relay"]}}

        with (
            patch("src.cli.daily_story_batch.parse_date_str", return_value=date(2025, 10, 15)),
            patch("src.cli.daily_story_batch.refresh_game_status_for_date", return_value={"updated": 1}),
            patch("src.cli.daily_story_batch.SessionLocal", session_factory),
            patch("src.cli.daily_story_batch.GameStoryBuilder"),
            patch(
                "src.cli.daily_story_batch._trusted_relay_game_ids",
                return_value={"G1"},
            ),
            patch("src.cli.daily_story_batch._build_story_data", return_value=story_data) as build_story,
            patch("src.cli.daily_story_batch._upsert_story_summary") as upsert,
            patch("src.cli.daily_story_batch._sync_story_summaries") as sync,
            patch("src.cli.daily_story_batch.write_refresh_manifest", return_value="manifest.json") as manifest,
        ):
            session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
                trusted_game,
                untrusted_game,
            ]

            result = asyncio.run(daily_story_batch.run_story_batch("20251015", sync_to_oci=True))

        assert result == ["G1"]
        build_story.assert_called_once()
        upsert.assert_called_once_with(session, "G1", '{"timeline": [], "source": {"warnings": ["missing relay"]}}')
        session.commit.assert_called_once()
        sync.assert_called_once_with(["G1"])
        manifest.assert_called_once_with(
            phase="postgame_story",
            target_date="20251015",
            game_ids=["G1"],
            datasets=["game", "game_events", "game_summary"],
        )

    def test_rolls_back_and_reraises_database_write_errors(self):
        session = MagicMock()
        session.commit.side_effect = RuntimeError("database unavailable")
        session_factory = MagicMock()
        session_factory.return_value.__enter__.return_value = session
        game = SimpleNamespace(game_id="G1")

        with (
            patch("src.cli.daily_story_batch.parse_date_str", return_value=date(2025, 10, 15)),
            patch("src.cli.daily_story_batch.refresh_game_status_for_date", return_value={"updated": 0}),
            patch("src.cli.daily_story_batch.SessionLocal", session_factory),
            patch("src.cli.daily_story_batch.GameStoryBuilder"),
            patch("src.cli.daily_story_batch._trusted_relay_game_ids", return_value={"G1"}),
            patch(
                "src.cli.daily_story_batch._build_story_data",
                return_value={"timeline": [{"event": "hit"}], "source": {}},
            ),
            patch("src.cli.daily_story_batch._upsert_story_summary"),
        ):
            session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [game]

            with pytest.raises(RuntimeError, match="database unavailable"):
                asyncio.run(daily_story_batch.run_story_batch("20251015", sync_to_oci=False))

        session.rollback.assert_called_once()
