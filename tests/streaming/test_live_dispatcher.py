"""Unit tests for LiveStreamBroadcaster and live streaming dispatcher."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.api.live_stream_dto import CircuitState, StreamEventType
from src.api.routers.live_stream import live_relay_breaker
from src.api.websocket_manager import ws_manager
from src.streaming.live_dispatcher import LiveStreamBroadcaster, _parse_runner_bits
from src.streaming.pbp_stream import LivePbpEventStream


def test_parse_runner_bits() -> None:
    """Test parsing runner bitmask from different string formats."""
    assert _parse_runner_bits("---") == 0
    assert _parse_runner_bits("000") == 0
    assert _parse_runner_bits("") == 0
    assert _parse_runner_bits("1--") == 1
    assert _parse_runner_bits("-2-") == 2
    assert _parse_runner_bits("--3") == 4
    assert _parse_runner_bits("12-") == 3
    assert _parse_runner_bits("1-3") == 5
    assert _parse_runner_bits("123") == 7
    assert _parse_runner_bits("100") == 1
    assert _parse_runner_bits("110") == 3
    assert _parse_runner_bits("111") == 7


@pytest.mark.asyncio
async def test_live_dispatcher_delta_tracking_and_broadcast() -> None:
    """Test extracting only delta events across consecutive polling cycles."""
    await live_relay_breaker.reset()
    broadcaster = LiveStreamBroadcaster.get_instance()
    game_id = "20260401LGKIA0"
    broadcaster.reset_game(game_id)

    # Cycle 1: First 2 events
    cycle_1_events = [
        {
            "event_seq": 1,
            "inning": 1,
            "half": "TOP",
            "batter": "홍창기",
            "pitcher": "양현종",
            "description": "중전 안타",
            "score_home": 0,
            "score_away": 0,
            "outs": 0,
            "base_state": "1--",
        },
        {
            "event_seq": 2,
            "inning": 1,
            "half": "TOP",
            "batter": "신민재",
            "pitcher": "양현종",
            "description": "희생번트 아웃",
            "score_home": 0,
            "score_away": 0,
            "outs": 1,
            "base_state": "-2-",
        },
    ]

    with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
        count = await broadcaster.dispatch_crawled_events(
            game_id=game_id,
            flat_events=cycle_1_events,
            resolved_lifecycle="running",
        )
        assert count == 2
        assert mock_broadcast.await_count == 2
        first_call = mock_broadcast.await_args_list[0].args
        assert first_call[0] == game_id
        assert first_call[1]["type"] == StreamEventType.PLAY_EVENT.value
        assert first_call[1]["event"]["event_seq"] == 1
        assert first_call[1]["event"]["batter"] == "홍창기"

    # Verify event stream history
    history = LivePbpEventStream.get_instance().get_history(game_id)
    assert len(history) == 2
    assert history[0].batter_name == "홍창기"

    # Cycle 2: Same 2 events + 1 new scoring event
    cycle_2_events = [
        *cycle_1_events,
        {
            "event_seq": 3,
            "inning": 1,
            "half": "TOP",
            "batter": "오스틴",
            "pitcher": "양현종",
            "description": "좌전 적시타 (1타점)",
            "score_home": 0,
            "score_away": 1,
            "outs": 1,
            "base_state": "1--",
            "wpa": 0.12,
        },
    ]

    with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
        count2 = await broadcaster.dispatch_crawled_events(
            game_id=game_id,
            flat_events=cycle_2_events,
            resolved_lifecycle="running",
        )
        assert count2 == 1  # Only the new event (seq 3) is processed
        # 1 PLAY_EVENT + 1 SCORE_UPDATE (because score changed from 0-0 to 0-1)
        assert mock_broadcast.await_count == 2
        types = [call.args[1]["type"] for call in mock_broadcast.await_args_list]
        assert StreamEventType.PLAY_EVENT.value in types
        assert StreamEventType.SCORE_UPDATE.value in types


@pytest.mark.asyncio
async def test_live_dispatcher_lifecycle_transitions() -> None:
    """Test broadcasting STATUS_CHANGE and GAME_FINISHED on lifecycle state changes."""
    await live_relay_breaker.reset()
    broadcaster = LiveStreamBroadcaster.get_instance()
    game_id = "20260401KTDOS0"
    broadcaster.reset_game(game_id)

    # Initial state: running
    await broadcaster.dispatch_crawled_events(
        game_id=game_id,
        flat_events=[],
        resolved_lifecycle="running",
    )

    # Transition 1: Rain delay
    with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
        await broadcaster.dispatch_crawled_events(
            game_id=game_id,
            flat_events=[],
            resolved_lifecycle="delayed",
        )
        assert mock_broadcast.await_count == 1
        args = mock_broadcast.await_args_list[0].args
        assert args[1]["type"] == StreamEventType.STATUS_CHANGE.value
        assert args[1]["previous_status"] == "running"
        assert args[1]["new_status"] == "delayed"

    # Transition 2: Game finished
    with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
        await broadcaster.dispatch_crawled_events(
            game_id=game_id,
            flat_events=[],
            resolved_lifecycle="result_pending_stabilization",
            home_team="두산",
            away_team="kt",
        )
        assert mock_broadcast.await_count == 1
        args = mock_broadcast.await_args_list[0].args
        assert args[1]["type"] == StreamEventType.GAME_FINISHED.value
        assert args[1]["summary"]["game_id"] == game_id
        assert args[1]["summary"]["home_team"] == "두산"


@pytest.mark.asyncio
async def test_live_dispatcher_circuit_breaker_isolation() -> None:
    """Test that when circuit breaker is tripped, dispatcher gracefully suppresses broadcast."""
    await live_relay_breaker.trip("Test trip")
    broadcaster = LiveStreamBroadcaster.get_instance()
    game_id = "20260401SSGNC0"
    broadcaster.reset_game(game_id)

    events = [
        {
            "event_seq": 1,
            "inning": 1,
            "half": "TOP",
            "batter": "최정",
            "pitcher": "신민혁",
            "description": "솔로 홈런",
            "score_home": 0,
            "score_away": 1,
            "outs": 0,
            "base_state": "---",
        }
    ]

    with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
        count = await broadcaster.dispatch_crawled_events(
            game_id=game_id,
            flat_events=events,
            resolved_lifecycle="running",
        )
        assert count == 0
        mock_broadcast.assert_not_called()

    # Reset breaker for subsequent tests
    await live_relay_breaker.reset()


@pytest.mark.asyncio
async def test_live_dispatcher_cleanup_expired_games() -> None:
    """Test TTL cache eviction of finished games."""
    broadcaster = LiveStreamBroadcaster.get_instance()
    game_old = "20260401OLD0"
    game_fresh = "20260401FRESH0"
    game_running = "20260401RUNNING0"

    broadcaster.reset_game(game_old)
    broadcaster.reset_game(game_fresh)
    broadcaster.reset_game(game_running)

    # 1. Dispatch games with different lifecycles
    await broadcaster.dispatch_crawled_events(game_old, flat_events=[], resolved_lifecycle="final")
    await broadcaster.dispatch_crawled_events(game_fresh, flat_events=[], resolved_lifecycle="final")
    await broadcaster.dispatch_crawled_events(game_running, flat_events=[], resolved_lifecycle="running")

    # Artificially age the old game
    now = time.time()
    broadcaster._lifecycle_timestamps[game_old] = now - 10000.0  # > 2 hours ago
    broadcaster._lifecycle_timestamps[game_fresh] = now - 60.0  # 1 minute ago

    # Run cleanup with 7200s (2h) max age
    evicted = broadcaster.cleanup_expired_games(max_age_seconds=7200.0)

    assert game_old in evicted
    assert game_fresh not in evicted
    assert game_running not in evicted
    assert game_old not in broadcaster._last_lifecycle
    assert game_fresh in broadcaster._last_lifecycle
    assert game_running in broadcaster._last_lifecycle


@pytest.mark.asyncio
async def test_live_dispatcher_coalesced_score_updates() -> None:
    """Test score update coalescing when coalesce_score_updates=True."""
    broadcaster = LiveStreamBroadcaster.get_instance()
    game_id = "20260401COALESCE0"
    broadcaster.reset_game(game_id)

    # Pre-populate score 0-0
    broadcaster._last_score[game_id] = (0, 0)

    # Batch with 2 consecutive run-scoring plays in the same cycle:
    # Play 1: score becomes 0-1
    # Play 2: score becomes 0-3
    events = [
        {
            "event_seq": 1,
            "inning": 2,
            "half": "TOP",
            "batter": "타자1",
            "pitcher": "투수1",
            "description": "적시타 (1점)",
            "score_home": 0,
            "score_away": 1,
            "outs": 1,
            "base_state": "1--",
        },
        {
            "event_seq": 2,
            "inning": 2,
            "half": "TOP",
            "batter": "타자2",
            "pitcher": "투수1",
            "description": "2점 홈런 (2점)",
            "score_home": 0,
            "score_away": 3,
            "outs": 1,
            "base_state": "---",
        },
    ]

    with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
        count = await broadcaster.dispatch_crawled_events(
            game_id=game_id,
            flat_events=events,
            resolved_lifecycle="running",
            coalesce_score_updates=True,
        )
        assert count == 2
        # 2 PLAY_EVENTs + exactly 1 coalesced SCORE_UPDATE = 3 calls
        assert mock_broadcast.await_count == 3
        types = [call.args[1]["type"] for call in mock_broadcast.await_args_list]
        assert types.count(StreamEventType.PLAY_EVENT.value) == 2
        assert types.count(StreamEventType.SCORE_UPDATE.value) == 1

        # Verify final coalesced score update has the latest score (0-3)
        score_call = [
            c for c in mock_broadcast.await_args_list if c.args[1]["type"] == StreamEventType.SCORE_UPDATE.value
        ][0]
        assert score_call.args[1]["score_away"] == 3
        assert score_call.args[1]["score_home"] == 0
