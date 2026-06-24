from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import src.cli.live_crawler as live_crawler
from src.cli.live_crawler import (
    GameActivityState,
    _LIVE_SHARD_CURSOR_BY_DATE,
    _compute_base_dynamic_interval,
    _compute_enriched_interval,
    _query_enriched_game_state,
    _select_live_shard,
)

# ===================================================================
# _compute_base_dynamic_interval tests (refactored helper)
# ===================================================================


class TestBaseDynamicInterval:
    def make_now(self, hour: int) -> datetime:
        return datetime(2025, 4, 1, hour, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    def _state(
        self, *, active=False, active_playing=False, active_suspended=False, last_active_time=None, now=None
    ) -> GameActivityState:
        return GameActivityState(
            active=active,
            active_playing=active_playing,
            active_suspended=active_suspended,
            last_active_time=last_active_time,
            now=now or self.make_now(14),
        )

    def test_active_playing_returns_10s(self):
        secs, label = _compute_base_dynamic_interval(
            state=self._state(active=True, active_playing=True),
            base_interval_minutes=2,
        )
        assert secs == 10
        assert "ACTIVE" in label

    def test_active_suspended_returns_60s(self):
        secs, label = _compute_base_dynamic_interval(
            state=self._state(active=True, active_suspended=True),
            base_interval_minutes=2,
        )
        assert secs == 60
        assert "DELAYED" in label

    def test_active_other_returns_30s(self):
        secs, label = _compute_base_dynamic_interval(
            state=self._state(active=True),
            base_interval_minutes=2,
        )
        assert secs == 30
        assert "CHANGE" in label

    def test_cooldown_returns_60s(self):
        last_active = datetime(2025, 4, 1, 13, 55, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        secs, label = _compute_base_dynamic_interval(
            state=self._state(last_active_time=last_active),
            base_interval_minutes=2,
        )
        assert secs == 60
        assert "COOLDOWN" in label

    def test_game_hours_returns_120s(self):
        secs, label = _compute_base_dynamic_interval(
            state=self._state(),
            base_interval_minutes=2,
        )
        assert secs == 120
        assert "GAME HOURS" in label

    def test_off_hours_returns_1800s(self):
        secs, label = _compute_base_dynamic_interval(
            state=self._state(now=self.make_now(3)),
            base_interval_minutes=2,
        )
        assert secs == 1800
        assert "OFF HOURS" in label


# ===================================================================
# _compute_enriched_interval tests
# ===================================================================


class TestComputeEnrichedInterval:
    def test_no_game_ids_returns_base(self):
        secs, note, counts = _compute_enriched_interval(
            10,
            [],
            {},
            None,
        )
        assert secs == 10
        assert note == ""
        assert counts == {}

    def test_no_enriched_state_returns_base(self):
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {},
            None,
        )
        assert secs == 10
        assert note == ""
        assert counts == {}

    def test_idle_detection_backs_off(self):
        """No new events since last cycle → 1.8x multiplier."""
        state = {"g1": {"event_count": 3, "max_inning": 3}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {"g1": 3},
            state,
        )
        assert secs >= 18  # 10 * 1.8 = 18
        assert counts["g1"] == 3

    def test_new_events_accelerate(self):
        """New events since last cycle → 0.6x multiplier."""
        state = {"g1": {"event_count": 5, "max_inning": 3}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {"g1": 3},
            state,
        )
        assert secs <= 6  # 10 * 0.6 = 6
        assert counts["g1"] == 5

    def test_late_game_accelerates(self):
        """Inning >= 7 → 0.7x multiplier."""
        state = {"g1": {"event_count": 10, "max_inning": 8}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {"g1": 10},
            state,
        )
        assert secs <= 7  # 10 * 0.7 = 7
        assert counts["g1"] == 10

    def test_clamped_min_5s(self):
        """Interval cannot go below 5 seconds."""
        state = {"g1": {"event_count": 5, "max_inning": 8}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {"g1": 3},
            state,
        )
        # 0.6x (new events) * 0.7x (late game) = 0.42x → 10*0.42 = 4.2 → clamp to 5
        assert secs >= 5

    def test_clamped_max_120s(self):
        """Interval cannot exceed 120 seconds."""
        state = {"g1": {"event_count": 3, "max_inning": 3}}
        secs, note, counts = _compute_enriched_interval(
            120,
            ["g1"],
            {"g1": 3},
            state,
        )
        # Idle: 120*1.8 = 216 → clamp to 120
        assert secs <= 120

    def test_multiple_games_most_aggressive_wins(self):
        """With multiple games, the lowest multiplier (most aggressive) is used."""
        state = {
            "g1": {"event_count": 3, "max_inning": 8},  # idle + late game
            "g2": {"event_count": 10, "max_inning": 3},  # stable
        }
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1", "g2"],
            {"g1": 3, "g2": 10},
            state,
        )
        # g1: idle(1.8) + late(0.7) → min=0.7
        # g2: no change → no multiplier
        # min = 0.7 → 7s
        assert secs == 7

    def test_first_cycle_no_last_counts_uses_base(self):
        """No previous event counts → no idle/new-event detection, just inning."""
        state = {"g1": {"event_count": 5, "max_inning": 3}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {},
            state,
        )
        assert secs == 10  # inning 3 < 7, no prior counts
        assert counts["g1"] == 5

    def test_first_cycle_with_late_inning(self):
        """No previous counts but inning >= 7 still triggers acceleration."""
        state = {"g1": {"event_count": 5, "max_inning": 8}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {},
            state,
        )
        assert secs == 7  # 10 * 0.7 = 7 (late inning only)
        assert counts["g1"] == 5

    def test_mixed_idle_and_new_events_different_games(self):
        """One game idle, one game has new events → min multiplier wins (acceleration)."""
        state = {
            "g1": {"event_count": 3, "max_inning": 4},  # idle (1.8x)
            "g2": {"event_count": 8, "max_inning": 4},  # new events (0.6x)
        }
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1", "g2"],
            {"g1": 3, "g2": 5},
            state,
        )
        assert secs == 6  # min(1.8, 0.6) = 0.6 → 10*0.6 = 6

    def test_extra_note_includes_reasons(self):
        """The extra_note string contains enriched reason labels."""
        state = {"g1": {"event_count": 3, "max_inning": 8}}
        secs, note, counts = _compute_enriched_interval(
            10,
            ["g1"],
            {"g1": 3},
            state,
        )
        assert "enriched" in note
        assert "accelerated" in note
        assert "idle_backoff" in note


# ===================================================================
# _select_live_shard tests
# ===================================================================


class TestSelectLiveShard:
    def setup_method(self):
        _LIVE_SHARD_CURSOR_BY_DATE.clear()

    def test_no_limit_returns_all_without_advancing_cursor(self):
        items = ["g1", "g2", "g3"]

        assert _select_live_shard(items, shard_key="20260531", max_items=None) == items
        assert _LIVE_SHARD_CURSOR_BY_DATE == {}

    def test_round_robin_shards_active_games_across_cycles(self):
        items = ["g1", "g2", "g3"]

        assert _select_live_shard(items, shard_key="20260531", max_items=1) == ["g1"]
        assert _select_live_shard(items, shard_key="20260531", max_items=1) == ["g2"]
        assert _select_live_shard(items, shard_key="20260531", max_items=1) == ["g3"]
        assert _select_live_shard(items, shard_key="20260531", max_items=1) == ["g1"]

    def test_new_day_resets_old_cursor(self):
        _select_live_shard(["g1", "g2"], shard_key="20260531", max_items=1)

        assert _select_live_shard(["n1", "n2"], shard_key="20260601", max_items=1) == ["n1"]
        assert _LIVE_SHARD_CURSOR_BY_DATE == {"20260601": 1}


class TestRunLiveCrawlerCycleSharding:
    def test_background_detail_snapshot_does_not_call_inline_detail_crawler(self, monkeypatch):
        crawled_game_ids = []
        queued_detail_snapshots = []
        manifest_calls = []

        class _FrozenDateTime:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 5, 31, 20, 46, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        class _FakeScheduleCrawler:
            async def crawl_schedule(self, year: int, month: int):
                return [
                    {
                        "game_id": "20260531AABB0",
                        "game_date": "20260531",
                        "away_team_code": "AA",
                        "home_team_code": "BB",
                    },
                ]

        class _FakeStatusResponse:
            status_code = 200

            def json(self):
                return {
                    "result": {
                        "games": [
                            {"awayTeamCode": "AA", "homeTeamCode": "BB", "status": "RUNNING"},
                        ]
                    }
                }

        class _FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, *args, **kwargs):
                return _FakeStatusResponse()

        class _FakeRelayCrawler:
            schedule_api_base_url = "https://example.test/status"
            headers = {}

            def _schedule_query_context(self, *, query_date: str | None = None):
                return {"date": query_date}

            def _naver_team_code(self, code: str):
                return code

            async def crawl_game_events(self, game_id: str):
                crawled_game_ids.append(game_id)
                return {
                    "events": [{"description": "play", "inning": 1, "inning_half": "top"}],
                    "raw_pbp_rows": [{"play_description": "play", "inning": 1, "inning_half": "top"}],
                }

        class _InlineDetailCrawler:
            def __init__(self, request_delay: float):
                raise AssertionError("detail crawler should not be constructed inline in background mode")

        def _fake_submit_detail(game_id: str, today_str: str):
            queued_detail_snapshots.append((game_id, today_str))
            return True

        def _fake_manifest(**kwargs):
            manifest_calls.append(kwargs)
            return "manifest.json"

        _LIVE_SHARD_CURSOR_BY_DATE.clear()
        monkeypatch.setattr(live_crawler, "datetime", _FrozenDateTime)
        monkeypatch.setattr(live_crawler, "ScheduleCrawler", _FakeScheduleCrawler)
        monkeypatch.setattr(live_crawler, "NaverRelayCrawler", _FakeRelayCrawler)
        monkeypatch.setattr(live_crawler, "GameDetailCrawler", _InlineDetailCrawler)
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", _FakeAsyncClient)
        monkeypatch.setattr(live_crawler, "save_relay_data", lambda *args, **kwargs: 1)
        monkeypatch.setattr(live_crawler, "save_game_snapshot", lambda *args, **kwargs: True)
        monkeypatch.setattr(live_crawler, "_submit_live_detail_snapshot_background", _fake_submit_detail)
        monkeypatch.setattr(live_crawler, "write_refresh_manifest", _fake_manifest)

        result = asyncio.run(
            live_crawler.run_live_crawler_cycle(
                sync_to_oci=False,
                max_active_games=1,
                detail_snapshot_background=True,
            )
        )

        assert crawled_game_ids == ["20260531AABB0"]
        assert queued_detail_snapshots == [("20260531AABB0", "20260531")]
        assert manifest_calls[0]["datasets"] == ["game_events", "game_play_by_play"]
        assert result["game_ids_playing"] == ["20260531AABB0"]

    def test_consecutive_bounded_cycles_rotate_active_games(self, monkeypatch):
        crawled_game_ids = []

        class _FrozenDateTime:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 5, 31, 20, 46, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        class _FakeScheduleCrawler:
            async def crawl_schedule(self, year: int, month: int):
                assert (year, month) == (2026, 5)
                return [
                    {
                        "game_id": "20260531AABB0",
                        "game_date": "20260531",
                        "away_team_code": "AA",
                        "home_team_code": "BB",
                    },
                    {
                        "game_id": "20260531CCDD0",
                        "game_date": "20260531",
                        "away_team_code": "CC",
                        "home_team_code": "DD",
                    },
                ]

        class _FakeStatusResponse:
            status_code = 200

            def json(self):
                return {
                    "result": {
                        "games": [
                            {"awayTeamCode": "AA", "homeTeamCode": "BB", "status": "RUNNING"},
                            {"awayTeamCode": "CC", "homeTeamCode": "DD", "status": "RUNNING"},
                        ]
                    }
                }

        class _FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, *args, **kwargs):
                return _FakeStatusResponse()

        class _FakeRelayCrawler:
            schedule_api_base_url = "https://example.test/status"
            headers = {}

            def _schedule_query_context(self, *, query_date: str | None = None):
                return {"date": query_date}

            def _naver_team_code(self, code: str):
                return code

            async def crawl_game_events(self, game_id: str):
                crawled_game_ids.append(game_id)
                return {
                    "events": [{"description": "play", "inning": 1, "inning_half": "top"}],
                    "raw_pbp_rows": [{"play_description": "play", "inning": 1, "inning_half": "top"}],
                }

        class _FakeDetailCrawler:
            def __init__(self, request_delay: float):
                self.request_delay = request_delay

            async def crawl_game(self, game_id: str, game_date: str, lightweight: bool = False):
                return {"game_id": game_id, "game_date": game_date}

        _LIVE_SHARD_CURSOR_BY_DATE.clear()
        monkeypatch.setattr(live_crawler, "datetime", _FrozenDateTime)
        monkeypatch.setattr(live_crawler, "ScheduleCrawler", _FakeScheduleCrawler)
        monkeypatch.setattr(live_crawler, "NaverRelayCrawler", _FakeRelayCrawler)
        monkeypatch.setattr(live_crawler, "GameDetailCrawler", _FakeDetailCrawler)
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", _FakeAsyncClient)
        monkeypatch.setattr(live_crawler, "save_relay_data", lambda *args, **kwargs: 1)
        monkeypatch.setattr(live_crawler, "save_game_snapshot", lambda *args, **kwargs: True)
        monkeypatch.setattr(live_crawler, "write_refresh_manifest", lambda **kwargs: "manifest.json")

        asyncio.run(live_crawler.run_live_crawler_cycle(sync_to_oci=False, max_active_games=1))
        asyncio.run(live_crawler.run_live_crawler_cycle(sync_to_oci=False, max_active_games=1))

        assert crawled_game_ids == ["20260531AABB0", "20260531CCDD0"]

    def test_oci_sync_failure_isolated_per_game(self, monkeypatch):
        class _FrozenDateTime:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 5, 31, 20, 46, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        class _FakeScheduleCrawler:
            async def crawl_schedule(self, year: int, month: int):
                return [
                    {
                        "game_id": "20260531AABB0",
                        "game_date": "20260531",
                        "away_team_code": "AA",
                        "home_team_code": "BB",
                    },
                    {
                        "game_id": "20260531CCDD0",
                        "game_date": "20260531",
                        "away_team_code": "CC",
                        "home_team_code": "DD",
                    },
                ]

        class _FakeStatusResponse:
            status_code = 200

            def json(self):
                return {
                    "result": {
                        "games": [
                            {"awayTeamCode": "AA", "homeTeamCode": "BB", "status": "RUNNING"},
                            {"awayTeamCode": "CC", "homeTeamCode": "DD", "status": "RUNNING"},
                        ]
                    }
                }

        class _FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, *args, **kwargs):
                return _FakeStatusResponse()

        class _FakeRelayCrawler:
            schedule_api_base_url = "https://example.test/status"
            headers = {}

            def _schedule_query_context(self, *, query_date: str | None = None):
                return {"date": query_date}

            def _naver_team_code(self, code: str):
                return code

            async def crawl_game_events(self, game_id: str):
                return {
                    "events": [{"description": "play", "inning": 1, "inning_half": "top"}],
                    "raw_pbp_rows": [{"play_description": "play", "inning": 1, "inning_half": "top"}],
                }

        class _FakeDetailCrawler:
            def __init__(self, request_delay: float):
                self.request_delay = request_delay

            async def crawl_game(self, game_id: str, game_date: str, lightweight: bool = False):
                return {"game_id": game_id, "game_date": game_date}

        class _FakeSessionContext:
            def __enter__(self):
                return object()

            def __exit__(self, exc_type, exc, tb):
                return False

        sync_calls = []
        closed = []

        class _FakeOCISync:
            def __init__(self, oci_url, session):
                self.oci_url = oci_url
                self.session = session

            def sync_specific_game(self, game_id: str):
                sync_calls.append(game_id)
                if game_id == "20260531AABB0":
                    raise RuntimeError("could not receive data from server: Operation timed out")
                return {"game": 1}

            def close(self):
                closed.append(True)

        _LIVE_SHARD_CURSOR_BY_DATE.clear()
        monkeypatch.setenv("OCI_DB_URL", "postgresql://example")
        monkeypatch.setattr(live_crawler, "datetime", _FrozenDateTime)
        monkeypatch.setattr(live_crawler, "ScheduleCrawler", _FakeScheduleCrawler)
        monkeypatch.setattr(live_crawler, "NaverRelayCrawler", _FakeRelayCrawler)
        monkeypatch.setattr(live_crawler, "GameDetailCrawler", _FakeDetailCrawler)
        monkeypatch.setattr(live_crawler.httpx, "AsyncClient", _FakeAsyncClient)
        monkeypatch.setattr(live_crawler, "save_relay_data", lambda *args, **kwargs: 1)
        monkeypatch.setattr(live_crawler, "save_game_snapshot", lambda *args, **kwargs: True)
        monkeypatch.setattr(live_crawler, "write_refresh_manifest", lambda **kwargs: "manifest.json")
        monkeypatch.setattr(live_crawler, "SessionLocal", lambda: _FakeSessionContext())
        monkeypatch.setattr(live_crawler, "OCISync", _FakeOCISync)

        result = asyncio.run(live_crawler.run_live_crawler_cycle(sync_to_oci=True, max_active_games=None))

        assert sync_calls == ["20260531AABB0", "20260531CCDD0"]
        assert closed == [True]
        assert result["oci_sync_failure_count"] == 1
        assert result["oci_sync_failed_game_ids"] == ["20260531AABB0"]


# ===================================================================
# _query_enriched_game_state smoke test
# ===================================================================


class TestQueryEnrichedGameState:
    def test_empty_list_returns_empty(self):
        assert _query_enriched_game_state([]) == {}

    def test_unknown_game_ids_returns_zero_counts(self):
        """DB query with non-existent game IDs should return 0 counts gracefully."""
        result = _query_enriched_game_state(["nonexistent_game_id"])
        assert isinstance(result, dict)
        assert "nonexistent_game_id" in result
        assert result["nonexistent_game_id"]["event_count"] == 0
        assert result["nonexistent_game_id"]["max_inning"] == 0
