from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

import src.cli.live_crawler as live_crawler
from src.cli.live_crawler import LiveGameInput, LiveSaveOptions, RelaySaveInput


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class TestFetchNaverLiveStatuses:
    def test_returns_statuses_from_successful_response(self, monkeypatch):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "result": {
                "games": [
                    {"awayTeamCode": "OB", "homeTeamCode": "SS", "status": "RUNNING"},
                    {"awayTeamCode": "HH", "homeTeamCode": "LT"},
                ],
            },
        }
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=response)
        relay = MagicMock()
        relay._schedule_query_context.return_value = {"date": "20260101"}
        relay.schedule_api_base_url = "https://example.test/schedule"
        relay.headers = {"User-Agent": "test"}
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", lambda: client)

        statuses = asyncio.run(live_crawler._fetch_naver_live_statuses(relay))

        assert statuses == {("OB", "SS"): "RUNNING"}
        client.get.assert_awaited_once()

    def test_returns_empty_statuses_when_request_fails(self, monkeypatch):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(side_effect=live_crawler.httpx.HTTPError("offline"))
        relay = MagicMock()
        relay._schedule_query_context.return_value = {}
        relay.schedule_api_base_url = "https://example.test/schedule"
        relay.headers = {}
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", lambda: client)

        assert asyncio.run(live_crawler._fetch_naver_live_statuses(relay)) == {}


class TestEvaluateGameLifecycles:
    def test_skips_cancelled_and_before_games_while_retaining_running_game(self):
        relay = MagicMock()
        relay._naver_team_code.side_effect = lambda code: code
        games = [
            {"game_id": "cancelled", "away_team_code": "A", "home_team_code": "B"},
            {"game_id": "before", "away_team_code": "C", "home_team_code": "D"},
            {"game_id": "running", "away_team_code": "E", "home_team_code": "F"},
        ]

        candidates, all_finished = live_crawler._evaluate_game_lifecycles(
            games,
            relay,
            {("A", "B"): "CANCELLED", ("C", "D"): "BEFORE", ("E", "F"): "RUNNING"},
        )

        assert [candidate[0]["game_id"] for candidate in candidates] == ["running"]
        assert candidates[0][1] == "running"
        assert all_finished is False


class TestRelaySaving:
    def test_empty_relay_payload_is_not_saved(self):
        result = asyncio.run(
            live_crawler._save_live_relay_and_snapshot(
                RelaySaveInput("game", "20260101", [], [], None, "running"),
                save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
            ),
        )

        assert result is False

    def test_saves_relay_and_inline_detail_snapshot(self, monkeypatch):
        detail_crawler = MagicMock()
        detail_crawler.crawl_game = AsyncMock(return_value={"game_id": "game"})
        save_relay = MagicMock(return_value=2)
        save_snapshot = MagicMock(return_value=True)
        trigger_healing = MagicMock()
        monkeypatch.setattr(live_crawler, "save_relay_data", save_relay)
        monkeypatch.setattr(live_crawler, "save_game_snapshot", save_snapshot)
        monkeypatch.setattr(live_crawler, "_trigger_fallback_healing_if_unverified", trigger_healing)

        result = asyncio.run(
            live_crawler._save_live_relay_and_snapshot(
                RelaySaveInput(
                    "game",
                    "20260101",
                    [{"description": "hit"}],
                    [],
                    {"parser_version": "v1"},
                    "result_pending_stabilization",
                ),
                save_options=LiveSaveOptions(detail_crawler=detail_crawler, detail_snapshot_background=False),
            ),
        )

        assert result is True
        save_relay.assert_called_once()
        detail_crawler.crawl_game.assert_awaited_once_with("game", "20260101", lightweight=True)
        save_snapshot.assert_called_once()
        trigger_healing.assert_called_once_with("game")

    def test_background_detail_snapshot_is_queued(self, monkeypatch):
        queue_snapshot = MagicMock(return_value=True)
        monkeypatch.setattr(live_crawler, "save_relay_data", MagicMock(return_value=1))
        monkeypatch.setattr(live_crawler, "_submit_live_detail_snapshot_background", queue_snapshot)

        result = asyncio.run(
            live_crawler._save_live_relay_and_snapshot(
                RelaySaveInput("game", "20260101", [{"description": "hit"}], [], None, "running"),
                save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=True),
            ),
        )

        assert result is True
        queue_snapshot.assert_called_once_with("game", "20260101")


class TestLiveSync:
    def test_does_not_create_sync_client_when_sync_is_disabled(self, monkeypatch):
        session_local = MagicMock()
        monkeypatch.setattr(live_crawler, "SessionLocal", session_local)

        failures = live_crawler._sync_live_touched_games(sync_to_oci=False, touched_game_ids={"game"})

        assert failures == []
        session_local.assert_not_called()

    def test_collects_per_game_sync_failure_and_closes_client(self, monkeypatch):
        synced = []
        sync_client = MagicMock()

        def sync_game(game_id):
            synced.append(game_id)
            if game_id == "bad":
                raise RuntimeError("unavailable")

        sync_client.sync_specific_game.side_effect = sync_game
        session = _FakeSession()
        monkeypatch.setenv("OCI_DB_URL", "postgresql://example.test/kbo")
        monkeypatch.setattr(live_crawler, "SessionLocal", lambda: session)
        monkeypatch.setattr(live_crawler, "OCISync", MagicMock(return_value=sync_client))

        failures = live_crawler._sync_live_touched_games(sync_to_oci=True, touched_game_ids={"good", "bad"})

        assert synced == ["bad", "good"]
        assert failures == [{"game_id": "bad", "phase": "sync_specific_game", "error": "unavailable"}]
        sync_client.close.assert_called_once()


class TestProcessAndCycle:
    def test_processes_one_game_and_returns_resolved_lifecycle(self, monkeypatch):
        relay = MagicMock()
        relay.crawl_game_events = AsyncMock(return_value={"events": [{"description": "우천 중단"}], "raw_pbp_rows": []})
        save = AsyncMock(return_value=True)
        monkeypatch.setattr(live_crawler, "_save_live_relay_and_snapshot", save)

        result = asyncio.run(
            live_crawler._process_single_live_game(
                LiveGameInput(
                    game={"game_id": "game"},
                    lifecycle_state="running",
                    nav_status_raw="RUNNING",
                    relay_crawler=relay,
                    today_str="20260101",
                ),
                save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
            ),
        )

        assert result == ("game", "suspended")
        save.assert_awaited_once()

    def test_cycle_returns_finished_state_when_schedule_has_no_today_games(self, monkeypatch):
        class FixedDateTime:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 1, 1, 18, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        schedule = MagicMock()
        schedule.crawl_schedule = AsyncMock(return_value=[])
        monkeypatch.setattr(live_crawler, "datetime", FixedDateTime)
        monkeypatch.setattr(live_crawler, "ScheduleCrawler", MagicMock(return_value=schedule))

        result = asyncio.run(live_crawler.run_live_crawler_cycle(sync_to_oci=False))

        assert result == {
            "active": False,
            "active_playing": False,
            "active_suspended": False,
            "all_finished": True,
            "game_ids_playing": [],
        }

    def test_cycle_aggregates_processed_games_and_sync_failures(self, monkeypatch):
        class FixedDateTime:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 1, 1, 18, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        schedule = MagicMock()
        schedule.crawl_schedule = AsyncMock(
            return_value=[
                {"game_id": "running", "game_date": "20260101"},
                {"game_id": "suspended", "game_date": "20260101"},
            ],
        )
        process = AsyncMock(side_effect=[("running", "running"), ("suspended", "suspended")])
        manifest = MagicMock(return_value="manifest.json")
        monkeypatch.setattr(live_crawler, "datetime", FixedDateTime)
        monkeypatch.setattr(live_crawler, "ScheduleCrawler", MagicMock(return_value=schedule))
        monkeypatch.setattr(live_crawler, "NaverRelayCrawler", MagicMock())
        monkeypatch.setattr(live_crawler, "GameDetailCrawler", MagicMock())
        monkeypatch.setattr(live_crawler, "_fetch_naver_live_statuses", AsyncMock(return_value={}))
        monkeypatch.setattr(
            live_crawler,
            "_evaluate_game_lifecycles",
            lambda games, relay, statuses: ([(game, "running", "RUNNING") for game in games], False),
        )
        monkeypatch.setattr(live_crawler, "_apply_dynamic_delay_scaling", MagicMock())
        monkeypatch.setattr(live_crawler, "_process_single_live_game", process)
        monkeypatch.setattr(live_crawler, "write_refresh_manifest", manifest)
        monkeypatch.setattr(
            live_crawler,
            "_sync_live_touched_games",
            MagicMock(return_value=[{"game_id": "suspended", "phase": "sync_specific_game", "error": "timeout"}]),
        )

        result = asyncio.run(live_crawler.run_live_crawler_cycle(sync_to_oci=True))

        assert result["active"] is True
        assert result["active_playing"] is True
        assert result["active_suspended"] is True
        assert set(result["game_ids_playing"]) == {"running", "suspended"}
        assert result["oci_sync_failed_game_ids"] == ["suspended"]
        assert manifest.call_args.kwargs["game_ids"] == {"running", "suspended"}


class TestMainLoop:
    def test_fixed_mode_sleeps_at_configured_interval_for_active_game(self, monkeypatch):
        cycle = AsyncMock(
            return_value={
                "active": True,
                "active_playing": True,
                "active_suspended": False,
                "game_ids_playing": ["game"],
            },
        )
        sleep = AsyncMock(side_effect=asyncio.CancelledError)
        monkeypatch.setattr(live_crawler, "run_live_crawler_cycle", cycle)
        monkeypatch.setattr(live_crawler.asyncio, "sleep", sleep)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(live_crawler.main_loop(2, sync_to_oci=False))

        cycle.assert_awaited_once_with(sync_to_oci=False)
        sleep.assert_awaited_once_with(120)

    def test_dynamic_mode_uses_enriched_interval_for_active_game(self, monkeypatch):
        cycle = AsyncMock(
            return_value={
                "active": True,
                "active_playing": False,
                "active_suspended": False,
                "game_ids_playing": ["game"],
            },
        )
        sleep = AsyncMock(side_effect=asyncio.CancelledError)
        enriched_state = MagicMock(return_value={"game": {"event_count": 2, "max_inning": 8}})
        monkeypatch.setattr(live_crawler, "run_live_crawler_cycle", cycle)
        monkeypatch.setattr(live_crawler, "_query_enriched_game_state", enriched_state)
        monkeypatch.setattr(live_crawler.asyncio, "sleep", sleep)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(live_crawler.main_loop(2, sync_to_oci=False, dynamic=True))

        enriched_state.assert_called_once_with(["game"])
        sleep.assert_awaited_once_with(21)
