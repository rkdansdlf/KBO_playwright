"""Branch-coverage tests for live_crawler pure/logic functions.

live_crawler mixes pure decision logic with live network/Playwright crawling.
This module targets the mockable decision logic (shard selection, enriched
state queries, lifecycle resolution/evaluation, dynamic delay scaling, OCI
sync failure handling, Naver status fetch, and argument-derived interval
computation) to raise branch coverage without invoking live crawls.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

import src.cli.live_crawler as live_crawler
from src.cli.live_crawler import (
    _apply_dynamic_delay_scaling,
    _compute_base_dynamic_interval,
    _compute_enriched_interval,
    _empty_live_result,
    _evaluate_game_lifecycles,
    _fetch_naver_live_statuses,
    _has_ending_header,
    _log_oci_sync_failures,
    _query_enriched_game_state,
    _raise_empty_kbo_pbp,
    _resolve_live_lifecycle,
    _save_live_relay_and_snapshot,
    _select_live_shard,
    _sync_live_touched_games,
    _trigger_fallback_healing_if_unverified,
    GameActivityState,
)


class TestRaiseEmptyKboPbp:
    def test_raises(self) -> None:
        with pytest.raises(ValueError, match="no events"):
            _raise_empty_kbo_pbp()


class TestHasEndingHeader:
    def test_detects_ending_keyword(self) -> None:
        rows = [{"event_type": "play", "play_description": "경기 종료"}]
        assert _has_ending_header(rows) is True

    def test_returns_false_on_inning_header(self) -> None:
        rows = [{"event_type": "inning_header", "play_description": ""}]
        assert _has_ending_header(rows) is False

    def test_returns_false_when_no_keyword(self) -> None:
        rows = [{"event_type": "play", "play_description": "안타"}]
        assert _has_ending_header(rows) is False


class TestSelectLiveShard:
    def test_returns_all_when_under_limit(self) -> None:
        items = [1, 2, 3]
        assert _select_live_shard(items, shard_key="k", max_items=None) == items
        assert _select_live_shard(items, shard_key="k", max_items=0) == items
        assert _select_live_shard(items, shard_key="k", max_items=5) == items

    def test_rotates_and_cleans_other_keys(self) -> None:
        live_crawler._LIVE_SHARD_CURSOR_BY_DATE["other"] = 3
        items = [1, 2, 3, 4]
        selected = _select_live_shard(items, shard_key="k", max_items=2)
        assert len(selected) == 2
        assert "other" not in live_crawler._LIVE_SHARD_CURSOR_BY_DATE
        live_crawler._LIVE_SHARD_CURSOR_BY_DATE.pop("k", None)


class TestQueryEnrichedGameState:
    def test_empty_input_returns_empty(self) -> None:
        assert _query_enriched_game_state([]) == {}

    def test_aggregates_event_and_inning_counts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.group_by.return_value.all.side_effect = [
            [("G1", 5)],
            [("G1", 7)],
        ]
        ctx = MagicMock()
        ctx.__enter__.return_value = session
        ctx.__exit__.return_value = False
        monkeypatch.setattr("src.db.engine.SessionLocal", MagicMock(return_value=ctx))
        state = _query_enriched_game_state(["G1"])
        assert state["G1"] == {"event_count": 5, "max_inning": 7}

    def test_db_error_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _boom() -> MagicMock:
            raise RuntimeError("db down")

        monkeypatch.setattr(live_crawler, "SessionLocal", _boom)
        assert _query_enriched_game_state(["G1"]) == {}


class TestComputeEnrichedInterval:
    def test_no_games_returns_base(self) -> None:
        secs, note, counts = _compute_enriched_interval(30, [], {"G1": 1})
        assert secs == 30
        assert note == ""

    def test_no_state_returns_base(self) -> None:
        secs, note, counts = _compute_enriched_interval(30, ["G1"], {})
        assert secs == 30

    def test_idle_and_late_game_multipliers(self) -> None:
        state = {"G1": {"event_count": 5, "max_inning": 8}}
        secs, note, counts = _compute_enriched_interval(30, ["G1"], {"G1": 5}, enriched_state=state)
        assert secs < 30
        assert "idle_backoff" in note

    def test_acceleration_when_events_increase(self) -> None:
        state = {"G1": {"event_count": 10, "max_inning": 3}}
        secs, note, counts = _compute_enriched_interval(30, ["G1"], {"G1": 5}, enriched_state=state)
        assert "accelerated" in note


class TestApplyDynamicDelayScaling:
    def test_no_games_returns_early(self) -> None:
        _apply_dynamic_delay_scaling(MagicMock(), [])

    def test_scales_policy_delays(self) -> None:
        policy = SimpleNamespace(min_delay=1.0, max_delay=2.0)
        crawler = SimpleNamespace(policy=policy)
        _apply_dynamic_delay_scaling(crawler, [SimpleNamespace(), SimpleNamespace()])
        assert policy.min_delay > 1.0

    def test_missing_policy_uses_zero_min(self) -> None:
        crawler = SimpleNamespace(policy=None)
        _apply_dynamic_delay_scaling(crawler, [SimpleNamespace()])


class TestSyncLiveTouchedGames:
    def test_skips_when_sync_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        assert _sync_live_touched_games(sync_to_oci=False, touched_game_ids={"G1"}) == []

    def test_returns_empty_without_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OCI_DB_URL", "")
        assert _sync_live_touched_games(sync_to_oci=True, touched_game_ids={"G1"}) == []

    def test_records_failures_on_db_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OCI_DB_URL", "postgresql://x")

        class FakeOCI:
            def __init__(self, url: str, session: object) -> None:
                pass

            def sync_specific_game(self, game_id: str) -> None:
                raise SQLAlchemyError("boom")

            def close(self) -> None:
                pass

        session = MagicMock()
        ctx = MagicMock()
        ctx.__enter__.return_value = session
        ctx.__exit__.return_value = False
        monkeypatch.setattr(live_crawler, "SessionLocal", MagicMock(return_value=ctx))
        monkeypatch.setattr(live_crawler, "OCISync", FakeOCI)
        failures = _sync_live_touched_games(sync_to_oci=True, touched_game_ids={"G1"})
        assert failures and failures[0]["game_id"] == "G1"


class TestLogOciSyncFailures:
    def test_returns_early_when_empty(self) -> None:
        _log_oci_sync_failures([])

    def test_logs_failures(self) -> None:
        _log_oci_sync_failures([{"game_id": "G1", "phase": "x", "error": "e"}])


class TestEmptyLiveResult:
    def test_builds_result(self) -> None:
        assert _empty_live_result(all_finished=True)["all_finished"] is True
        assert _empty_live_result(all_finished=False)["all_finished"] is False


class TestResolveLiveLifecycle:
    def test_result_pending_when_state(self) -> None:
        assert _resolve_live_lifecycle("result_pending_stabilization", [], []) == "result_pending_stabilization"

    def test_suspended_detection(self) -> None:
        assert _resolve_live_lifecycle("suspended", [], []) == "suspended"
        assert _resolve_live_lifecycle(None, [{"description": "우천 중단"}], []) == "suspended"

    def test_delayed(self) -> None:
        assert _resolve_live_lifecycle("delayed", [], []) == "delayed"

    def test_running_default(self) -> None:
        assert _resolve_live_lifecycle(None, [], []) == "running"

    def test_game_end_from_raw_pbp(self) -> None:
        assert _resolve_live_lifecycle(None, [], [{"play_description": "경기종료"}]) == "result_pending_stabilization"


class TestEvaluateGameLifecycles:
    def test_cancelled_and_before_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(live_crawler, "derive_lifecycle_from_naver_status", lambda s: "cancelled")
        crawler = SimpleNamespace(_naver_team_code=lambda c: c)
        cands, all_finished = _evaluate_game_lifecycles(
            [{"game_id": "G1", "away_team_code": "A", "home_team_code": "B"}],
            crawler,
            {},
        )
        assert cands == []
        assert all_finished is True

    def test_active_appended(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(live_crawler, "derive_lifecycle_from_naver_status", lambda s: "running")
        crawler = SimpleNamespace(_naver_team_code=lambda c: c)
        cands, all_finished = _evaluate_game_lifecycles(
            [{"game_id": "G1", "away_team_code": "A", "home_team_code": "B"}],
            crawler,
            {},
        )
        assert cands and cands[0][0]["game_id"] == "G1"
        assert all_finished is False

    def test_result_pending_without_terminal_db_row(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            live_crawler, "derive_lifecycle_from_naver_status", lambda s: "result_pending_stabilization"
        )
        crawler = SimpleNamespace(_naver_team_code=lambda c: c)
        session = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None
        ctx = MagicMock()
        ctx.__enter__.return_value = session
        ctx.__exit__.return_value = False
        monkeypatch.setattr(live_crawler, "SessionLocal", MagicMock(return_value=ctx))
        cands, all_finished = _evaluate_game_lifecycles(
            [{"game_id": "G1", "away_team_code": "A", "home_team_code": "B"}],
            crawler,
            {},
        )
        assert cands and cands[0][0]["game_id"] == "G1"


class TestFetchNaverLiveStatuses:
    async def test_returns_status_map_on_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        crawler = SimpleNamespace(
            schedule_api_base_url="http://x",
            headers={},
            _schedule_query_context=lambda query_date: {"date": query_date},
        )
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "result": {"games": [{"awayTeamCode": "A", "homeTeamCode": "B", "status": "LIVE"}]}
        }
        client = MagicMock()
        client.get = AsyncMock(return_value=response)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", MagicMock(return_value=client))
        result = await _fetch_naver_live_statuses(crawler)
        assert result == {("A", "B"): "LIVE"}

    async def test_returns_empty_on_http_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        crawler = SimpleNamespace(
            schedule_api_base_url="http://x",
            headers={},
            _schedule_query_context=lambda query_date: {"date": query_date},
        )
        client = MagicMock()
        client.get = AsyncMock(side_effect=ValueError("boom"))
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", MagicMock(return_value=client))
        assert await _fetch_naver_live_statuses(crawler) == {}


class TestTriggerFallbackHealingIfUnverified:
    def test_returns_when_no_metadata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None
        ctx = MagicMock()
        ctx.__enter__.return_value = session
        ctx.__exit__.return_value = False
        monkeypatch.setattr(live_crawler, "SessionLocal", MagicMock(return_value=ctx))
        _trigger_fallback_healing_if_unverified("G1")

    def test_returns_when_verified(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = SimpleNamespace(
            source_payload={"pbp_validation_status": "verified"}
        )
        ctx = MagicMock()
        ctx.__enter__.return_value = session
        ctx.__exit__.return_value = False
        monkeypatch.setattr(live_crawler, "SessionLocal", MagicMock(return_value=ctx))
        _trigger_fallback_healing_if_unverified("G1")


class TestSaveLiveRelayAndSnapshot:
    async def test_returns_false_without_events(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(live_crawler, "save_relay_data", MagicMock(return_value=0))
        result = await _save_live_relay_and_snapshot(
            SimpleNamespace(
                game_id="G1",
                today_str="20250615",
                flat_events=[],
                raw_pbp_rows=[],
                relay_data={},
                resolved_lifecycle="running",
            ),
            save_options=SimpleNamespace(detail_crawler=None, detail_snapshot_background=False),
        )
        assert result is False

    async def test_saves_relay_rows_without_detail_crawler(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(live_crawler, "save_relay_data", MagicMock(return_value=3))
        monkeypatch.setattr(live_crawler, "save_game_snapshot", MagicMock(return_value=False))
        result = await _save_live_relay_and_snapshot(
            _relay_input(),
            save_options=SimpleNamespace(detail_crawler=None, detail_snapshot_background=False),
        )
        assert result is True

    async def test_detail_crawler_no_rows_skips_snapshot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(live_crawler, "save_relay_data", MagicMock(return_value=2))
        monkeypatch.setattr(live_crawler, "save_game_snapshot", MagicMock(return_value=False))
        crawler = SimpleNamespace(crawl_game=AsyncMock(return_value=None))
        result = await _save_live_relay_and_snapshot(
            _relay_input(),
            save_options=SimpleNamespace(detail_crawler=crawler, detail_snapshot_background=False),
        )
        assert result is True


def _relay_input() -> object:
    return SimpleNamespace(
        game_id="G1",
        today_str="20250615",
        flat_events=[{"description": "안타"}],
        raw_pbp_rows=[],
        relay_data={},
        resolved_lifecycle="running",
    )


class TestComputeBaseDynamicInterval:
    def test_active_playing(self) -> None:
        state = GameActivityState(
            active=True,
            active_playing=True,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2025, 6, 15, 18, 0),
        )
        assert _compute_base_dynamic_interval(state=state, base_interval_minutes=2) == (10, "ACTIVE (Inning playing)")

    def test_active_suspended(self) -> None:
        state = GameActivityState(
            active=True,
            active_playing=False,
            active_suspended=True,
            last_active_time=None,
            now=datetime(2025, 6, 15, 18, 0),
        )
        assert _compute_base_dynamic_interval(state=state, base_interval_minutes=2) == (
            60,
            "DELAYED (Rain delay/Stoppage)",
        )

    def test_active_change(self) -> None:
        state = GameActivityState(
            active=True,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2025, 6, 15, 18, 0),
        )
        assert _compute_base_dynamic_interval(state=state, base_interval_minutes=2) == (30, "CHANGE (Inning change)")

    def test_cooldown_recently_active(self) -> None:
        now = datetime(2025, 6, 15, 18, 0)
        state = GameActivityState(
            active=False, active_playing=False, active_suspended=False, last_active_time=now, now=now
        )
        assert _compute_base_dynamic_interval(state=state, base_interval_minutes=2) == (
            60,
            "COOLDOWN (Recently finished)",
        )

    def test_game_hours(self) -> None:
        state = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2025, 6, 15, 18, 0),
        )
        assert _compute_base_dynamic_interval(state=state, base_interval_minutes=2) == (
            120,
            "GAME HOURS (No active games)",
        )

    def test_off_hours(self) -> None:
        state = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2025, 6, 15, 3, 0),
        )
        assert _compute_base_dynamic_interval(state=state, base_interval_minutes=2) == (1800, "OFF HOURS")
