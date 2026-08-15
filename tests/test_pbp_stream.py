"""Unit and integration tests for LivePbpEventStream."""

from __future__ import annotations

from src.crawlers.pbp_crawler import PBPCrawler
from src.streaming.pbp_stream import LivePbpEvent, LivePbpEventStream


def test_publish_and_subscribe_all() -> None:
    """Subscriber for all games should receive any published event."""
    stream = LivePbpEventStream()
    received = []

    stream.subscribe(received.append)

    event = LivePbpEvent(
        game_id="20260815LGKIA0",
        event_seq=1,
        inning=1,
        half="TOP",
        batter_name="박찬호",
        pitcher_name="켈리",
        description="중전 안타",
        score_home=0,
        score_away=0,
        outs=0,
        base_state="100",
        wpa=0.035,
        win_expectancy=0.535,
    )
    notified = stream.publish(event)
    assert notified == 1
    assert len(received) == 1
    assert received[0].description == "중전 안타"


def test_publish_and_subscribe_filtered_by_game() -> None:
    """Subscriber for game A should not receive events for game B."""
    stream = LivePbpEventStream()
    game_a_events = []
    game_b_events = []

    stream.subscribe(game_a_events.append, game_id="GAME_A")
    stream.subscribe(game_b_events.append, game_id="GAME_B")

    event_a = LivePbpEvent(
        game_id="GAME_A",
        event_seq=1,
        inning=1,
        half="TOP",
        batter_name="김도영",
        pitcher_name="원태인",
        description="좌월 1점 홈런",
        score_home=0,
        score_away=1,
        outs=0,
        base_state="000",
        wpa=0.120,
    )
    stream.publish(event_a)

    assert len(game_a_events) == 1
    assert len(game_b_events) == 0


def test_history_and_clear_buffer() -> None:
    """History should record stream and clear after game end."""
    stream = LivePbpEventStream(max_buffer_per_game=2)

    for seq in (1, 2, 3):
        stream.publish(
            LivePbpEvent(
                game_id="GAME_C",
                event_seq=seq,
                inning=seq,
                half="TOP",
                batter_name="홍길동",
                pitcher_name="이순신",
                description=f"이벤트 {seq}",
                score_home=0,
                score_away=0,
                outs=0,
                base_state="000",
            ),
        )

    history = stream.get_history("GAME_C")
    assert len(history) == 2  # max_buffer=2
    assert history[0].event_seq == 2
    assert history[1].event_seq == 3

    stream.clear_game("GAME_C")
    assert len(stream.get_history("GAME_C")) == 0


def test_pbp_crawler_stream_integration() -> None:
    """PBPCrawler._publish_to_stream should successfully deliver events to singleton stream."""
    stream = LivePbpEventStream.get_instance()
    received = []
    stream.subscribe(received.append, game_id="20260815LGKIA0")

    raw_event = {
        "event_seq": 10,
        "inning": 5,
        "inning_half": "bottom",
        "description": "5번 타자 문보경: 우중간 2루타",
        "batter": "문보경",
        "home_score": 3,
        "away_score": 2,
        "outs": 1,
        "bases_before": "1--",
        "wpa": 0.085,
        "win_expectancy_after": 0.650,
    }

    PBPCrawler._publish_to_stream("20260815LGKIA0", raw_event)

    assert len(received) == 1
    assert received[0].game_id == "20260815LGKIA0"
    assert received[0].batter_name == "문보경"
    assert received[0].wpa == 0.085
