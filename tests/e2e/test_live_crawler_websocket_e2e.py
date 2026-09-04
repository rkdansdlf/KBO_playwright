"""2026 KBO Regular Season End-to-End (E2E) Live Crawler & WebSocket Integration Test Suite.

Verifies the complete real-time pipeline:
Schedule & Status Ingestion -> Live Crawler Polling -> Delta Detection & WPA Enrichment
-> WebSocket Dispatcher -> Connected Single/Multi-Game Clients -> Lifecycle Finalization.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.api.live_stream_dto import CircuitState, StreamEventType
from src.api.routers.live_stream import live_relay_breaker
from src.api.websocket_manager import ws_manager
from src.cli.live.live_crawler import (
    LiveGameInput,
    LiveSaveOptions,
    _process_single_live_game,
)
from src.streaming.live_dispatcher import LiveStreamBroadcaster


@pytest.fixture(autouse=True)
def _reset_test_environment() -> None:
    """Reset circuit breaker, broadcasters, and active connections before and after each test."""
    asyncio.run(live_relay_breaker.reset())
    LiveStreamBroadcaster.get_instance()._last_seen_seq.clear()
    LiveStreamBroadcaster.get_instance()._last_score.clear()
    LiveStreamBroadcaster.get_instance()._last_lifecycle.clear()
    yield
    asyncio.run(live_relay_breaker.reset())


class Test2026LiveCrawlerWebSocketE2E:
    """Comprehensive E2E test suite for 2026 regular season scenarios."""

    def test_scenario_1_single_game_full_lifecycle_e2e(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scenario 1: 2026 Opening Day (KIA vs LG) full lifecycle E2E.

        Covers: Before -> Inning 1 (Scoring) -> Inning 7 (High Leverage / WPA) -> Inning 9 (Walk-off & Final).
        """
        game_id = "20260401LGKIA0"
        broadcaster = LiveStreamBroadcaster.get_instance()
        broadcaster.reset_game(game_id)

        # Mock DB saving to succeed instantly without touching disk
        monkeypatch.setattr("src.cli.live.live_crawler._save_live_relay_and_snapshot", AsyncMock(return_value=True))

        client = TestClient(app)
        with client.websocket_connect(f"/ws/live/{game_id}") as ws:
            # 1. Handshake verification
            handshake = ws.receive_json()
            assert handshake["type"] == StreamEventType.CONNECTION_ESTABLISHED.value
            assert handshake["game_id"] == game_id
            assert handshake["circuit_state"] == CircuitState.CLOSED.value

            # 2. Cycle 1: 1st Inning plays (Single -> Sac Bunt -> RBI Single)
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
                    "description": "1루수 희생번트 아웃",
                    "score_home": 0,
                    "score_away": 0,
                    "outs": 1,
                    "base_state": "-2-",
                },
                {
                    "event_seq": 3,
                    "inning": 1,
                    "half": "TOP",
                    "batter": "오스틴",
                    "pitcher": "양현종",
                    "description": "좌중간 1타점 적시타",
                    "score_home": 0,
                    "score_away": 1,
                    "outs": 1,
                    "base_state": "1--",
                    "wpa": 0.125,
                },
            ]

            mock_relay_1 = MagicMock()
            mock_relay_1.crawl_game_events = AsyncMock(
                return_value={"events": cycle_1_events, "raw_pbp_rows": []},
            )

            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "KIA", "away_team_code": "LG"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay_1,
                        today_str="20260401",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                ),
            )

            # Assert WebSocket client receives Cycle 1 events in order
            ev1 = ws.receive_json()
            assert ev1["type"] == StreamEventType.PLAY_EVENT.value
            assert ev1["event"]["event_seq"] == 1
            assert ev1["event"]["batter"] == "홍창기"

            ev2 = ws.receive_json()
            assert ev2["type"] == StreamEventType.PLAY_EVENT.value
            assert ev2["event"]["event_seq"] == 2
            assert ev2["event"]["description"] == "1루수 희생번트 아웃"

            ev3 = ws.receive_json()
            assert ev3["type"] == StreamEventType.PLAY_EVENT.value
            assert ev3["event"]["event_seq"] == 3
            assert ev3["event"]["score_away"] == 1

            # Score update event is emitted on run score
            score_ev = ws.receive_json()
            assert score_ev["type"] == StreamEventType.SCORE_UPDATE.value
            assert score_ev["score_away"] == 1
            assert score_ev["score_home"] == 0

            # 3. Cycle 2: Inning 7 High Leverage Situation (Tie score, bases loaded)
            cycle_2_events = [
                *cycle_1_events,
                {
                    "event_seq": 4,
                    "inning": 7,
                    "half": "BOT",
                    "batter": "김도영",
                    "pitcher": "유영찬",
                    "description": "우중간 동점 2루타",
                    "score_home": 1,
                    "score_away": 1,
                    "outs": 1,
                    "base_state": "-2-",
                    "wpa": 0.18,
                },
                {
                    "event_seq": 5,
                    "inning": 7,
                    "half": "BOT",
                    "batter": "최형우",
                    "pitcher": "유영찬",
                    "description": "만루 홈런 (4타점) 대역전",
                    "score_home": 5,
                    "score_away": 1,
                    "outs": 2,
                    "base_state": "---",
                    "wpa": 0.42,
                },
            ]

            mock_relay_2 = MagicMock()
            mock_relay_2.crawl_game_events = AsyncMock(
                return_value={"events": cycle_2_events, "raw_pbp_rows": []},
            )

            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "KIA", "away_team_code": "LG"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay_2,
                        today_str="20260401",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                ),
            )

            ev4 = ws.receive_json()
            assert ev4["event"]["event_seq"] == 4
            assert ev4["event"]["batter"] == "김도영"

            score_ev2 = ws.receive_json()
            assert score_ev2["type"] == StreamEventType.SCORE_UPDATE.value

            ev5 = ws.receive_json()
            assert ev5["event"]["event_seq"] == 5
            assert ev5["event"]["batter"] == "최형우"
            assert ev5["event"]["is_hot_moment"] is True  # Grand slam is a hot moment
            assert ev5["event"]["wpa"] >= 0.15

            score_ev3 = ws.receive_json()
            assert score_ev3["type"] == StreamEventType.SCORE_UPDATE.value
            assert score_ev3["score_home"] == 5

            # 4. Cycle 3: 9th Inning Final Out and Game Finished
            cycle_3_events = [
                *cycle_2_events,
                {
                    "event_seq": 6,
                    "inning": 9,
                    "half": "TOP",
                    "batter": "박동원",
                    "pitcher": "정해영",
                    "description": "헛스윙 삼진 아웃. 경기종료",
                    "score_home": 5,
                    "score_away": 1,
                    "outs": 3,
                    "base_state": "---",
                },
            ]

            mock_relay_3 = MagicMock()
            mock_relay_3.crawl_game_events = AsyncMock(
                return_value={"events": cycle_3_events, "raw_pbp_rows": []},
            )

            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "KIA", "away_team_code": "LG"},
                        lifecycle_state="result_pending_stabilization",
                        nav_status_raw="RESULT",
                        relay_crawler=mock_relay_3,
                        today_str="20260401",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                ),
            )

            ev6 = ws.receive_json()
            assert ev6["event"]["event_seq"] == 6

            final_msg = ws.receive_json()
            assert final_msg["type"] == StreamEventType.GAME_FINISHED.value
            assert final_msg["summary"]["game_id"] == game_id
            assert final_msg["summary"]["score_home"] == 5
            assert final_msg["summary"]["score_away"] == 1
            assert final_msg["summary"]["status"] == "result_pending_stabilization"

    def test_scenario_2_multi_game_saturday_concurrent_subscription(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Scenario 2: 5 Saturday regular season games running concurrently.

        Verifies that /ws/live/multi subscribers receive events strictly for subscribed games.
        """
        monkeypatch.setattr("src.cli.live.live_crawler._save_live_relay_and_snapshot", AsyncMock(return_value=True))

        game_sub1 = "20260516LGKIA0"
        game_sub2 = "20260516SSGNC0"
        game_unsub = "20260516LOTWO0"

        client = TestClient(app)
        with client.websocket_connect("/ws/live/multi") as ws:
            init = ws.receive_json()
            assert init["type"] == StreamEventType.CONNECTION_ESTABLISHED.value

            # Subscribe to games 1 and 2
            ws.send_json({"action": "subscribe", "games": [game_sub1, game_sub2]})
            sub_ack = ws.receive_json()
            assert sub_ack["type"] == StreamEventType.CHANNEL_SUBSCRIBED.value
            assert game_sub1 in sub_ack["subscribed_games"]
            assert game_sub2 in sub_ack["subscribed_games"]

            # Crawl event for game 1 (Subscribed)
            mock_relay_1 = MagicMock()
            mock_relay_1.crawl_game_events = AsyncMock(
                return_value={
                    "events": [
                        {
                            "event_seq": 1,
                            "inning": 1,
                            "half": "TOP",
                            "batter": "오지환",
                            "pitcher": "네일",
                            "description": "우전 안타",
                            "score_home": 0,
                            "score_away": 0,
                            "outs": 0,
                            "base_state": "1--",
                        }
                    ],
                    "raw_pbp_rows": [],
                }
            )
            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_sub1, "home_team_code": "KIA", "away_team_code": "LG"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay_1,
                        today_str="20260516",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )

            # Crawl event for unsubscribed game (LOTWO)
            mock_relay_unsub = MagicMock()
            mock_relay_unsub.crawl_game_events = AsyncMock(
                return_value={
                    "events": [
                        {
                            "event_seq": 1,
                            "inning": 1,
                            "half": "TOP",
                            "batter": "이정후",
                            "pitcher": "박세웅",
                            "description": "솔로 홈런",
                            "score_home": 0,
                            "score_away": 1,
                            "outs": 0,
                            "base_state": "---",
                        }
                    ],
                    "raw_pbp_rows": [],
                }
            )
            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_unsub, "home_team_code": "WO", "away_team_code": "LOT"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay_unsub,
                        today_str="20260516",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )

            # Crawl event for game 2 (Subscribed)
            mock_relay_2 = MagicMock()
            mock_relay_2.crawl_game_events = AsyncMock(
                return_value={
                    "events": [
                        {
                            "event_seq": 1,
                            "inning": 1,
                            "half": "BOT",
                            "batter": "박건우",
                            "pitcher": "김광현",
                            "description": "중월 2루타",
                            "score_home": 0,
                            "score_away": 0,
                            "outs": 0,
                            "base_state": "-2-",
                        }
                    ],
                    "raw_pbp_rows": [],
                }
            )
            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_sub2, "home_team_code": "NC", "away_team_code": "SSG"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay_2,
                        today_str="20260516",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )

            # Verify that client only received events for game_sub1 and game_sub2
            msg1 = ws.receive_json()
            assert msg1["game_id"] == game_sub1
            assert msg1["event"]["batter"] == "오지환"

            msg2 = ws.receive_json()
            assert msg2["game_id"] == game_sub2
            assert msg2["event"]["batter"] == "박건우"

            # Check dynamic unsubscription
            ws.send_json({"action": "unsubscribe", "games": [game_sub1]})
            unsub_ack = ws.receive_json()
            assert unsub_ack["type"] == StreamEventType.CHANNEL_UNSUBSCRIBED.value
            assert game_sub1 in unsub_ack["unsubscribed_games"]

    def test_scenario_3_rain_delay_and_resumption_e2e(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scenario 3: Rain delay interruption and game resumption E2E."""
        monkeypatch.setattr("src.cli.live.live_crawler._save_live_relay_and_snapshot", AsyncMock(return_value=True))
        game_id = "20260610HHLT0"
        broadcaster = LiveStreamBroadcaster.get_instance()
        broadcaster.reset_game(game_id)

        client = TestClient(app)
        with client.websocket_connect(f"/ws/live/{game_id}") as ws:
            ws.receive_json()  # handshake

            # 1. Game running normally
            mock_relay = MagicMock()
            mock_relay.crawl_game_events = AsyncMock(
                return_value={
                    "events": [
                        {
                            "event_seq": 1,
                            "inning": 3,
                            "half": "TOP",
                            "batter": "문현빈",
                            "pitcher": "반즈",
                            "description": "볼넷",
                            "score_home": 0,
                            "score_away": 0,
                            "outs": 1,
                            "base_state": "1--",
                        }
                    ],
                    "raw_pbp_rows": [],
                }
            )

            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "LT", "away_team_code": "HH"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay,
                        today_str="20260610",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )
            ev1 = ws.receive_json()
            assert ev1["type"] == StreamEventType.PLAY_EVENT.value

            # 2. Rain stoppage occurs
            mock_relay_delayed = MagicMock()
            mock_relay_delayed.crawl_game_events = AsyncMock(
                return_value={
                    "events": [
                        {
                            "event_seq": 1,
                            "inning": 3,
                            "half": "TOP",
                            "batter": "문현빈",
                            "pitcher": "반즈",
                            "description": "볼넷 (폭우로 경기 일시 중단)",
                            "score_home": 0,
                            "score_away": 0,
                            "outs": 1,
                            "base_state": "1--",
                        }
                    ],
                    "raw_pbp_rows": [],
                }
            )

            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "LT", "away_team_code": "HH"},
                        lifecycle_state="suspended",
                        nav_status_raw="SUSPENDED",
                        relay_crawler=mock_relay_delayed,
                        today_str="20260610",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )

            # Client receives STATUS_CHANGE notification
            status_msg = ws.receive_json()
            assert status_msg["type"] == StreamEventType.STATUS_CHANGE.value
            assert status_msg["previous_status"] == "running"
            assert status_msg["new_status"] == "suspended"

            # 3. Game resumes after rain clears
            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "LT", "away_team_code": "HH"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay,
                        today_str="20260610",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )
            resume_msg = ws.receive_json()
            assert resume_msg["type"] == StreamEventType.STATUS_CHANGE.value
            assert resume_msg["new_status"] == "running"

    def test_scenario_4_reconnection_and_replay_history_e2e(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scenario 4: Client reconnecting mid-game and receiving replay history buffer."""
        monkeypatch.setattr("src.cli.live.live_crawler._save_live_relay_and_snapshot", AsyncMock(return_value=True))
        game_id = "20260705WOSAM0"
        broadcaster = LiveStreamBroadcaster.get_instance()
        broadcaster.reset_game(game_id)

        # 3 events occur while client 1 is connected
        events = [
            {
                "event_seq": 1,
                "inning": 1,
                "half": "TOP",
                "batter": "김혜성",
                "pitcher": "원태인",
                "description": "2루타",
                "score_home": 0,
                "score_away": 0,
                "outs": 0,
                "base_state": "-2-",
            },
            {
                "event_seq": 2,
                "inning": 1,
                "half": "TOP",
                "batter": "도슨",
                "pitcher": "원태인",
                "description": "적시타",
                "score_home": 0,
                "score_away": 1,
                "outs": 0,
                "base_state": "1--",
            },
        ]
        mock_relay = MagicMock()
        mock_relay.crawl_game_events = AsyncMock(return_value={"events": events, "raw_pbp_rows": []})

        client = TestClient(app)
        with client.websocket_connect(f"/ws/live/{game_id}") as ws1:
            ws1.receive_json()  # handshake
            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "SAM", "away_team_code": "WO"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay,
                        today_str="20260705",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )
            # ws1 consumes the events
            ws1.receive_json()  # ev 1
            ws1.receive_json()  # ev 2
            ws1.receive_json()  # score update

        # Now Client 2 connects fresh to the ongoing game
        with client.websocket_connect(f"/ws/live/{game_id}") as ws2:
            handshake2 = ws2.receive_json()
            assert handshake2["type"] == StreamEventType.CONNECTION_ESTABLISHED.value
            assert "replay_history" in handshake2
            assert len(handshake2["replay_history"]) == 2
            assert handshake2["replay_history"][0]["batter"] == "김혜성"
            assert handshake2["replay_history"][1]["batter"] == "도슨"

            # Event 3 occurs in real-time
            events_3 = [
                *events,
                {
                    "event_seq": 3,
                    "inning": 1,
                    "half": "TOP",
                    "batter": "송성문",
                    "pitcher": "원태인",
                    "description": "삼진 아웃",
                    "score_home": 0,
                    "score_away": 1,
                    "outs": 1,
                    "base_state": "1--",
                },
            ]
            mock_relay_3 = MagicMock()
            mock_relay_3.crawl_game_events = AsyncMock(return_value={"events": events_3, "raw_pbp_rows": []})

            asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "SAM", "away_team_code": "WO"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay_3,
                        today_str="20260705",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )

            ev3 = ws2.receive_json()
            assert ev3["event"]["event_seq"] == 3
            assert ev3["event"]["batter"] == "송성문"

    def test_scenario_5_circuit_breaker_fault_isolation_e2e(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scenario 5: Circuit breaker tripping, degraded mode alert, and recovery E2E."""
        monkeypatch.setattr("src.cli.live.live_crawler._save_live_relay_and_snapshot", AsyncMock(return_value=True))
        game_id = "20260815KTSSG0"
        broadcaster = LiveStreamBroadcaster.get_instance()
        broadcaster.reset_game(game_id)

        client = TestClient(app)
        with client.websocket_connect(f"/ws/live/{game_id}") as ws:
            ws.receive_json()  # initial handshake

            # 1. Trip circuit breaker manually
            asyncio.run(live_relay_breaker.trip("Simulated upstream crawl failure"))

            # Client receives global circuit change alert
            cb_msg = ws.receive_json()
            assert cb_msg["type"] == StreamEventType.CIRCUIT_STATE_CHANGED.value
            assert cb_msg["new_state"] == CircuitState.OPEN.value
            assert cb_msg["is_degraded"] is True

            # 2. Live crawler cycle executes while breaker is OPEN
            mock_relay = MagicMock()
            mock_relay.crawl_game_events = AsyncMock(
                return_value={
                    "events": [
                        {
                            "event_seq": 1,
                            "inning": 1,
                            "half": "TOP",
                            "batter": "강백호",
                            "pitcher": "엔스",
                            "description": "안타",
                            "score_home": 0,
                            "score_away": 0,
                            "outs": 0,
                            "base_state": "1--",
                        }
                    ],
                    "raw_pbp_rows": [],
                }
            )

            # This must complete cleanly without throwing exceptions or crashing the crawler
            game_touched, lifecycle = asyncio.run(
                _process_single_live_game(
                    LiveGameInput(
                        game={"game_id": game_id, "home_team_code": "SSG", "away_team_code": "KT"},
                        lifecycle_state="running",
                        nav_status_raw="RUNNING",
                        relay_crawler=mock_relay,
                        today_str="20260815",
                    ),
                    save_options=LiveSaveOptions(detail_crawler=None, detail_snapshot_background=False),
                )
            )
            assert lifecycle == "running"

            # 3. Reset circuit breaker
            asyncio.run(live_relay_breaker.reset())
            reset_msg = ws.receive_json()
            assert reset_msg["type"] == StreamEventType.CIRCUIT_STATE_CHANGED.value
            assert reset_msg["new_state"] == CircuitState.CLOSED.value
            assert reset_msg["is_degraded"] is False
