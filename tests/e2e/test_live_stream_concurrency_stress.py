"""End-to-End Stress, High-Concurrency, and Resilience Test Suite for KBO Live Streaming.

Verifies:
1. 100 concurrent WebSocket connections across 5 channels with broadcast latency measurement.
2. Sudden client disconnection (dead connection) detection and leak-free resource recovery.
3. Handshake replay buffer slicing (15-event cap) alongside full REST API pagination.
4. Finished game memory TTL cache eviction.
5. Burst event score update coalescing (debouncing).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.api.live_stream_dto import StreamEventType
from src.api.routers.live_stream import MAX_HANDSHAKE_REPLAY_EVENTS, live_relay_breaker
from src.api.websocket_manager import ConnectionManager, ws_manager
from src.streaming.live_dispatcher import LiveStreamBroadcaster
from src.streaming.pbp_stream import LivePbpEvent, LivePbpEventStream


@pytest.fixture(autouse=True)
def _reset_stress_env() -> None:
    """Reset circuit breaker and in-memory caches before each stress test."""
    asyncio.run(live_relay_breaker.reset())
    broadcaster = LiveStreamBroadcaster.get_instance()
    broadcaster._last_seen_seq.clear()
    broadcaster._last_score.clear()
    broadcaster._last_lifecycle.clear()
    broadcaster._lifecycle_timestamps.clear()
    yield
    asyncio.run(live_relay_breaker.reset())


class TestLiveStreamConcurrencyStress:
    """High-concurrency and resilience test cases."""

    def test_stress_100_concurrent_connections_and_latency(self) -> None:
        """Test managing 100 concurrent client WebSockets distributed across 5 game channels."""
        client = TestClient(app)
        num_channels = 5
        clients_per_channel = 20
        total_clients = num_channels * clients_per_channel

        game_ids = [f"20260401GAME{i}0" for i in range(1, num_channels + 1)]
        open_sockets = []

        try:
            # Connect all 100 clients
            for gid in game_ids:
                for _ in range(clients_per_channel):
                    ws_ctx = client.websocket_connect(f"/ws/live/{gid}")
                    ws = ws_ctx.__enter__()
                    open_sockets.append((ws_ctx, ws, gid))
                    init_data = ws.receive_json()
                    assert init_data["type"] == StreamEventType.CONNECTION_ESTABLISHED.value

            # Verify active viewer distribution
            for gid in game_ids:
                assert ws_manager.get_active_count(gid) == clients_per_channel
            assert ws_manager.get_active_count() == total_clients

            # Measure broadcast latency for 1 game
            target_gid = game_ids[0]
            start_t = time.perf_counter()

            payload = {
                "type": StreamEventType.PLAY_EVENT.value,
                "game_id": target_gid,
                "event": {
                    "event_seq": 99,
                    "batter": "테스트타자",
                    "description": "적시타",
                    "score_home": 1,
                    "score_away": 0,
                },
            }
            delivered = asyncio.run(ws_manager.broadcast_to_game(target_gid, payload))
            elapsed_ms = (time.perf_counter() - start_t) * 1000.0

            assert delivered == clients_per_channel
            # Broadcast to 20 sockets must be very fast (< 50ms in-process)
            assert elapsed_ms < 50.0

            # Verify that each connected client on target_gid received the message
            for _, ws, gid in open_sockets:
                if gid == target_gid:
                    msg = ws.receive_json()
                    assert msg["type"] == StreamEventType.PLAY_EVENT.value
                    assert msg["event"]["event_seq"] == 99

        finally:
            for ws_ctx, _, _ in open_sockets:
                with contextlib.suppress(Exception):
                    ws_ctx.__exit__(None, None, None)

    def test_dead_connection_mass_cleanup(self) -> None:
        """Test mass client drop / sudden disconnect detection and resource pruning."""
        cm = ConnectionManager()
        game_id = "20260401DEADCONN0"

        class _MockDeadSocket:
            def __init__(self, should_fail: bool = False) -> None:
                self.should_fail = should_fail
                self.closed = False

            async def send_text(self, _data: str) -> None:
                if self.should_fail:
                    raise OSError("Broken pipe / client dropped")

        # Register 50 sockets: 30 dead, 20 alive
        dead_count = 30
        alive_count = 20
        all_sockets = []

        for i in range(dead_count + alive_count):
            sock = _MockDeadSocket(should_fail=(i < dead_count))
            all_sockets.append(sock)
            cm._global_connections.add(sock)
            cm._game_connections[game_id].add(sock)
            cm._client_subscriptions[sock].add(game_id)

        assert cm.get_active_count(game_id) == dead_count + alive_count

        # Broadcast payload
        delivered = asyncio.run(
            cm.broadcast_to_game(game_id, {"type": "TEST", "content": "ping"}),
        )

        # Delivered only to the 20 alive sockets
        assert delivered == alive_count

        # All 30 dead sockets must have been pruned from all sets
        assert cm.get_active_count(game_id) == alive_count
        assert len(cm._global_connections) == alive_count
        for dead_sock in all_sockets[:dead_count]:
            assert dead_sock not in cm._global_connections
            assert dead_sock not in cm._game_connections[game_id]
            assert dead_sock not in cm._client_subscriptions

    def test_handshake_replay_slicing_and_rest_pagination(self) -> None:
        """Test that WebSocket handshake returns at most 15 events while REST returns full history."""
        game_id = "20260401SLICETEST0"
        stream = LivePbpEventStream.get_instance()
        stream.clear_game(game_id)

        # Seed 30 sequential events
        total_events = 30
        for i in range(1, total_events + 1):
            stream.publish(
                LivePbpEvent(
                    game_id=game_id,
                    event_seq=i,
                    inning=(i // 4) + 1,
                    half="TOP",
                    batter_name=f"타자_{i:02d}",
                    pitcher_name="에이스투수",
                    description=f"타석 결과 {i}",
                    score_home=0,
                    score_away=i // 10,
                    outs=(i % 3),
                    base_state="---",
                ),
            )

        client = TestClient(app)

        # 1. Connect WebSocket and check handshake replay slicing
        with client.websocket_connect(f"/ws/live/{game_id}") as ws:
            handshake = ws.receive_json()
            assert handshake["type"] == StreamEventType.CONNECTION_ESTABLISHED.value
            replay = handshake.get("replay_history", [])

            # Must be capped at MAX_HANDSHAKE_REPLAY_EVENTS (15)
            assert len(replay) == MAX_HANDSHAKE_REPLAY_EVENTS
            # Must be the latest 15 events (from event_seq 16 to 30)
            assert replay[0]["event_seq"] == total_events - MAX_HANDSHAKE_REPLAY_EVENTS + 1
            assert replay[-1]["event_seq"] == total_events
            assert replay[0]["batter"] == f"타자_{total_events - MAX_HANDSHAKE_REPLAY_EVENTS + 1:02d}"

        # 2. Check REST endpoint returns all 30 events
        resp_full = client.get(f"/api/v1/live/{game_id}/pbp?limit=100")
        assert resp_full.status_code == 200
        full_data = resp_full.json()
        assert full_data["total_events"] == total_events
        assert full_data["events"][0]["event_seq"] == 1
        assert full_data["events"][-1]["event_seq"] == total_events

        # 3. Check REST endpoint pagination limit
        resp_limit = client.get(f"/api/v1/live/{game_id}/pbp?limit=5")
        assert resp_limit.status_code == 200
        limit_data = resp_limit.json()
        assert limit_data["total_events"] == 5
        assert limit_data["events"][0]["event_seq"] == 26
        assert limit_data["events"][-1]["event_seq"] == 30

    def test_ttl_cache_eviction_lifecycle(self) -> None:
        """Test automatic memory purging for games finished beyond TTL."""
        broadcaster = LiveStreamBroadcaster.get_instance()
        game_expired = "20260401EXPIRED0"
        game_fresh = "20260401FRESH0"
        game_active = "20260401ACTIVE0"

        broadcaster.reset_game(game_expired)
        broadcaster.reset_game(game_fresh)
        broadcaster.reset_game(game_active)

        # Simulate 3 games in different states
        asyncio.run(broadcaster.dispatch_crawled_events(game_expired, flat_events=[], resolved_lifecycle="final"))
        asyncio.run(
            broadcaster.dispatch_crawled_events(
                game_fresh, flat_events=[], resolved_lifecycle="result_pending_stabilization"
            )
        )
        asyncio.run(broadcaster.dispatch_crawled_events(game_active, flat_events=[], resolved_lifecycle="running"))

        now = time.time()
        # Expired: finished 3 hours ago (10,800s)
        broadcaster._lifecycle_timestamps[game_expired] = now - 10800.0
        # Fresh: finished 10 minutes ago (600s)
        broadcaster._lifecycle_timestamps[game_fresh] = now - 600.0

        # Run eviction with default TTL (7200s = 2h)
        evicted = broadcaster.cleanup_expired_games(max_age_seconds=7200.0)

        assert game_expired in evicted
        assert game_fresh not in evicted
        assert game_active not in evicted

        # Assert memory structures freed for expired game
        assert game_expired not in broadcaster._last_lifecycle
        assert game_expired not in broadcaster._last_score
        assert game_expired not in broadcaster._lifecycle_timestamps

        # Assert fresh and active remain in memory
        assert game_fresh in broadcaster._last_lifecycle
        assert game_active in broadcaster._last_lifecycle

    def test_burst_event_score_update_coalescing(self) -> None:
        """Test coalescing rapid multi-run events into a single score update."""
        broadcaster = LiveStreamBroadcaster.get_instance()
        game_id = "20260401BURST0"
        broadcaster.reset_game(game_id)
        broadcaster._last_score[game_id] = (0, 0)

        # 5 events in one polling cycle with 2 separate run-scoring plays:
        # Event 1: Out (0-0)
        # Event 2: Walk (0-0)
        # Event 3: Single (1-0)
        # Event 4: 2-run Double (3-0)
        # Event 5: Groundout (3-0)
        burst_events = [
            {
                "event_seq": 1,
                "inning": 3,
                "half": "BOT",
                "batter": "타자1",
                "pitcher": "투수1",
                "description": "삼진 아웃",
                "score_home": 0,
                "score_away": 0,
                "outs": 1,
                "base_state": "---",
            },
            {
                "event_seq": 2,
                "inning": 3,
                "half": "BOT",
                "batter": "타자2",
                "pitcher": "투수1",
                "description": "볼넷",
                "score_home": 0,
                "score_away": 0,
                "outs": 1,
                "base_state": "1--",
            },
            {
                "event_seq": 3,
                "inning": 3,
                "half": "BOT",
                "batter": "타자3",
                "pitcher": "투수1",
                "description": "적시타 (1득점)",
                "score_home": 1,
                "score_away": 0,
                "outs": 1,
                "base_state": "1--",
            },
            {
                "event_seq": 4,
                "inning": 3,
                "half": "BOT",
                "batter": "타자4",
                "pitcher": "투수1",
                "description": "2루타 (2득점)",
                "score_home": 3,
                "score_away": 0,
                "outs": 1,
                "base_state": "-2-",
            },
            {
                "event_seq": 5,
                "inning": 3,
                "half": "BOT",
                "batter": "타자5",
                "pitcher": "투수1",
                "description": "땅볼 아웃",
                "score_home": 3,
                "score_away": 0,
                "outs": 2,
                "base_state": "-2-",
            },
        ]

        with patch.object(ws_manager, "broadcast_to_game", new_callable=AsyncMock) as mock_broadcast:
            count = asyncio.run(
                broadcaster.dispatch_crawled_events(
                    game_id=game_id,
                    flat_events=burst_events,
                    resolved_lifecycle="running",
                    coalesce_score_updates=True,
                ),
            )
            assert count == 5
            # 5 PLAY_EVENTs + 1 coalesced SCORE_UPDATE = 6 total broadcasts
            assert mock_broadcast.await_count == 6

            msg_types = [call.args[1]["type"] for call in mock_broadcast.await_args_list]
            assert msg_types.count(StreamEventType.PLAY_EVENT.value) == 5
            assert msg_types.count(StreamEventType.SCORE_UPDATE.value) == 1

            # Assert score update is emitted at the end with latest score 3-0
            last_msg = mock_broadcast.await_args_list[-1].args[1]
            assert last_msg["type"] == StreamEventType.SCORE_UPDATE.value
            assert last_msg["score_home"] == 3
            assert last_msg["score_away"] == 0
