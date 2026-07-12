from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.cli.live_crawler import (
    GameActivityState,
    _apply_dynamic_delay_scaling,
    _compute_base_dynamic_interval,
    _compute_enriched_interval,
    _empty_live_result,
    _has_ending_header,
    _resolve_live_lifecycle,
    _select_live_shard,
)

KST = datetime


class TestHasEndingHeader:
    def test_empty_list(self) -> None:
        assert _has_ending_header([]) is False

    def test_no_ending_term(self) -> None:
        rows = [
            {"event_type": "inning_header", "play_description": "1회말"},
            {"event_type": "play", "play_description": "스트라이크"},
        ]
        assert _has_ending_header(rows) is False

    def test_inning_header_no_ending(self) -> None:
        rows = [
            {"event_type": "inning_header", "play_description": "9회말"},
            {"event_type": "play", "play_description": "안타"},
        ]
        assert _has_ending_header(rows) is False

    def test_ending_term_found(self) -> None:
        rows = [
            {"event_type": "play", "play_description": "경기 종료"},
        ]
        assert _has_ending_header(rows) is True

    def test_game_ending_term(self) -> None:
        rows = [
            {"event_type": "play", "play_description": "게임 종료"},
        ]
        assert _has_ending_header(rows) is True

    def test_ending_term_ignores_inning_header(self) -> None:
        rows = [
            {"event_type": "inning_header", "play_description": "경기 종료"},
            {"event_type": "play", "play_description": "after"},
        ]
        assert _has_ending_header(rows) is False


class TestSelectLiveShard:
    def test_none_max_items(self) -> None:
        result = _select_live_shard([1, 2, 3], shard_key="test", max_items=None)
        assert result == [1, 2, 3]

    def test_zero_max_items(self) -> None:
        result = _select_live_shard([1, 2, 3], shard_key="test", max_items=0)
        assert result == [1, 2, 3]

    def test_less_than_max(self) -> None:
        result = _select_live_shard([1, 2], shard_key="test", max_items=5)
        assert result == [1, 2]

    def test_exact_max(self) -> None:
        result = _select_live_shard([1, 2, 3], shard_key="test", max_items=3)
        assert result == [1, 2, 3]

    def test_shard_selects_subset(self) -> None:
        items = list(range(20))
        result = _select_live_shard(items, shard_key="k1", max_items=5)
        assert len(result) == 5


class TestComputeEnrichedInterval:
    def test_no_game_ids(self) -> None:
        result = _compute_enriched_interval(30, [], {}, None)
        assert result[0] == 30  # base interval preserved

    def test_no_enriched_state(self) -> None:
        result = _compute_enriched_interval(30, ["g1"], {}, None)
        assert result[0] == 30

    def test_empty_enriched_state(self) -> None:
        result = _compute_enriched_interval(30, ["g1"], {}, {})
        assert result[0] == 30

    def test_new_game_starts_minimum(self) -> None:
        enriched = {"g1": {"event_count": 1, "max_inning": 1}}
        result = _compute_enriched_interval(30, ["g1"], {"g1": 0}, enriched)
        # No prev match for idle detection (prev=0, ec=1, ec > prev)
        # Combined with inning >= 7 not met
        assert result[0] > 0

    def test_idle_detection(self) -> None:
        enriched = {"g1": {"event_count": 10, "max_inning": 5}}
        last = {"g1": 10}
        result = _compute_enriched_interval(30, ["g1"], last, enriched)
        # idle: prev=10, ec=10, ec>0 → 1.8x multiplier
        assert result[0] >= 30 * 1.4  # at least ~1.8x

    def test_accelerated(self) -> None:
        enriched = {"g1": {"event_count": 15, "max_inning": 7}}
        last = {"g1": 10}
        result = _compute_enriched_interval(30, ["g1"], last, enriched)
        # ec 15 > 10 → 0.6x, inning >= 7 → 0.7x
        # Combined = min(0.6, 0.7) = 0.6
        # 30 * 0.6 = 18
        assert result[0] <= 25  # accelerated
        assert "accelerated" in result[1]

    def test_multiple_games_with_different_state(self) -> None:
        enriched = {
            "g1": {"event_count": 10, "max_inning": 5},
            "g2": {"event_count": 20, "max_inning": 7},
        }
        last = {"g1": 5, "g2": 15}
        result = _compute_enriched_interval(30, ["g1", "g2"], last, enriched)
        # g1: idle (10 > 5) → 0.6x
        # g2: fast (20 > 15) → 0.6x, inning >= 7 → 0.7x
        # combined = min(0.6, 0.6, 0.7) = 0.6
        # 30 * 0.6 = 18
        assert result[0] < 30
        assert "accelerated" in result[1] or "idle" in result[1]

    def test_both_idle_and_accelerated(self) -> None:
        enriched = {
            "g1": {"event_count": 5, "max_inning": 3},
            "g2": {"event_count": 5, "max_inning": 3},
        }
        last = {"g1": 5, "g2": 3}
        result = _compute_enriched_interval(30, ["g1", "g2"], last, enriched)
        # g1: idle (prev=5, ec=5, ec>0) → 1.8x
        # g2: fast (prev=3, ec=5, ec>prev) → 0.6x
        # combined = min(1.8, 0.6) = 0.6
        assert result[0] < 30

    def test_min_polling_floor(self) -> None:
        enriched = {"g1": {"event_count": 50, "max_inning": 7}}
        last = {"g1": 0}
        result = _compute_enriched_interval(10, ["g1"], last, enriched)
        # prev=0, ec=50, ec>prev → 0.6x; inning >=7 → 0.7x
        # combined=0.6, 10*0.6=6, max(5,6)=6
        assert result[0] >= 5

    def test_max_cap(self) -> None:
        enriched = {"g1": {"event_count": 5, "max_inning": 3}}
        last = {"g1": 5}
        result = _compute_enriched_interval(120, ["g1"], last, enriched)
        # idle: 1.8x → 120*1.8=216 → min(120, 216)=120 → max(5,120)=120
        assert result[0] <= 120

    def test_return_value_shape(self) -> None:
        enriched = {"g1": {"event_count": 5, "max_inning": 3}}
        last = {"g1": 3}
        result = _compute_enriched_interval(30, ["g1"], last, enriched)
        assert len(result) == 3
        assert isinstance(result[0], int)
        assert isinstance(result[1], str)
        assert isinstance(result[2], dict)


class TestEmptyLiveResult:
    def test_all_finished_true(self) -> None:
        result = _empty_live_result(all_finished=True)
        assert result["active"] is False
        assert result["all_finished"] is True
        assert result["game_ids_playing"] == []

    def test_all_finished_false(self) -> None:
        result = _empty_live_result(all_finished=False)
        assert result["all_finished"] is False


class TestApplyDynamicDelayScaling:
    def test_no_candidates_leaves_policy_unchanged(self) -> None:
        relay_crawler = MagicMock()
        relay_crawler.policy.min_delay = 1.0
        relay_crawler.policy.max_delay = 2.0

        _apply_dynamic_delay_scaling(relay_crawler, [])

        assert relay_crawler.policy.min_delay == 1.0
        assert relay_crawler.policy.max_delay == 2.0

    def test_scales_policy_by_active_game_count(self) -> None:
        relay_crawler = MagicMock()
        relay_crawler.policy.min_delay = 1.0
        relay_crawler.policy.max_delay = 2.0

        _apply_dynamic_delay_scaling(relay_crawler, [({"game_id": "G1"}, None, None), ({"game_id": "G2"}, None, None)])

        assert relay_crawler.policy.min_delay == 1.5
        assert relay_crawler.policy.max_delay == 3.0

    def test_missing_policy_is_allowed(self) -> None:
        relay_crawler = MagicMock()
        relay_crawler.policy = None

        _apply_dynamic_delay_scaling(relay_crawler, [({"game_id": "G1"}, None, None)])


class TestResolveLiveLifecycle:
    def test_running_with_events(self) -> None:
        result = _resolve_live_lifecycle(None, [{"description": "안타"}], [])
        assert result == "running"

    def test_delayed(self) -> None:
        result = _resolve_live_lifecycle("delayed", [], [])
        assert result == "delayed"

    def test_suspended_from_lifecycle(self) -> None:
        result = _resolve_live_lifecycle("suspended", [], [])
        assert result == "suspended"

    def test_suspended_from_event_description(self) -> None:
        result = _resolve_live_lifecycle(None, [{"description": "우천 중단"}], [])
        assert result == "suspended"

    def test_game_end_from_event(self) -> None:
        result = _resolve_live_lifecycle(None, [{"description": "경기 종료"}], [])
        assert result == "result_pending_stabilization"

    def test_game_end_from_pbp_rows(self) -> None:
        result = _resolve_live_lifecycle(None, [], [{"play_description": "경기 종료"}])
        assert result == "result_pending_stabilization"

    def test_result_pending_stabilization_preserved(self) -> None:
        result = _resolve_live_lifecycle("result_pending_stabilization", [{"description": "안타"}], [])
        assert result == "result_pending_stabilization"

    def test_game_end_from_has_ending_header(self) -> None:
        result = _resolve_live_lifecycle(None, [], [{"event_type": "play", "play_description": "경기 종료"}])
        assert result == "result_pending_stabilization"

    def test_suspension_from_raw_pbp_rows(self) -> None:
        result = _resolve_live_lifecycle(None, [], [{"play_description": "서스펜디드"}])
        assert result == "suspended"

    def test_suspension_from_flat_events_with_지연(self) -> None:
        result = _resolve_live_lifecycle(None, [{"description": "지연 발생"}], [])
        assert result == "suspended"

    def test_no_events_no_lifecycle_running(self) -> None:
        result = _resolve_live_lifecycle(None, [], [])
        assert result == "running"


class TestComputeBaseDynamicInterval:
    def test_active_playing(self) -> None:
        gs = GameActivityState(
            active=True,
            active_playing=True,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2026, 6, 15, 18, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 10
        assert "ACTIVE" in label

    def test_active_suspended(self) -> None:
        gs = GameActivityState(
            active=True,
            active_playing=False,
            active_suspended=True,
            last_active_time=None,
            now=datetime(2026, 6, 15, 18, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 60
        assert "DELAYED" in label

    def test_active_inning_change(self) -> None:
        gs = GameActivityState(
            active=True,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2026, 6, 15, 18, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 30
        assert "CHANGE" in label

    def test_recently_finished(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=datetime(2026, 6, 15, 18, 0),
            now=datetime(2026, 6, 15, 18, 5),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 60
        assert "COOLDOWN" in label

    def test_game_hours_no_active(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2026, 6, 15, 14, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 120
        assert "GAME HOURS" in label

    def test_game_hours_edge(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2026, 6, 15, 12, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 120
        assert "GAME HOURS" in label

    def test_off_hours(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2026, 6, 15, 23, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 1800
        assert "OFF HOURS" in label

    def test_off_hours_morning(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=None,
            now=datetime(2026, 6, 15, 6, 0),
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 1800
        assert "OFF HOURS" in label

    def test_recently_active_elapsed_600s(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=datetime(2026, 6, 15, 17, 50),
            now=datetime(2026, 6, 15, 17, 55),  # 300s < 600s
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 60
        assert "COOLDOWN" in label

    def test_recently_active_elapsed_over_600s(self) -> None:
        gs = GameActivityState(
            active=False,
            active_playing=False,
            active_suspended=False,
            last_active_time=datetime(2026, 6, 15, 17, 30),
            now=datetime(2026, 6, 15, 18, 0),  # 1800s > 600s, but hour 18 is game hours
        )
        interval, label = _compute_base_dynamic_interval(state=gs, base_interval_minutes=30)
        assert interval == 120  # game hours, not cooldown
        assert "GAME HOURS" in label
