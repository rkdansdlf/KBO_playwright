from __future__ import annotations

from datetime import date

from src.models.game import Game, GameEvent
from src.services.game_story_builder import GameStoryBuilder, STORY_SCHEMA_VERSION


def _story_payload():
    game = Game(
        game_id="20250405HHSS0",
        game_date=date(2025, 4, 5),
        away_team="HH",
        home_team="SS",
        away_score=7,
        home_score=6,
        game_status="COMPLETED",
    )
    events = [
        GameEvent(
            game_id=game.game_id,
            event_seq=1,
            inning=1,
            inning_half="BOTTOM",
            description="=====================================",
            event_type="unknown",
            wpa=0.99,
            away_score=0,
            home_score=0,
        ),
        GameEvent(
            game_id=game.game_id,
            event_seq=2,
            inning=1,
            inning_half="BOTTOM",
            description="좌익수 뒤 홈런 (홈런거리:115M)",
            event_type="HIT",
            result_code="HR",
            rbi=1,
            batter_name="이재현",
            pitcher_name="류현진",
            wpa=-0.285,
            away_score=0,
            home_score=1,
        ),
        GameEvent(
            game_id=game.game_id,
            event_seq=3,
            inning=8,
            inning_half="top",
            description="2루수 앞 땅볼로 출루 / 3루주자 노시환 : 실책으로 홈인",
            event_type="batting",
            result_code="ROE",
            rbi=0,
            batter_name="문현빈",
            pitcher_name="김재윤",
            wpa=0.22,
            away_score=4,
            home_score=6,
        ),
        GameEvent(
            game_id=game.game_id,
            event_seq=4,
            inning=9,
            inning_half="TOP",
            description="좌익수 앞 1루타 / 1루주자 임종찬 : 2루까지 진루",
            event_type="HIT",
            result_code="H1",
            rbi=0,
            batter_name="이원석",
            pitcher_name="김재윤",
            wpa=0.01,
            away_score=4,
            home_score=6,
        ),
        GameEvent(
            game_id=game.game_id,
            event_seq=5,
            inning=9,
            inning_half="TOP",
            description="우익수 뒤 홈런 (홈런거리:120M) / 1루주자 이원석 : 홈인; 2루주자 임종찬 : 홈인",
            event_type="HIT",
            result_code="HR",
            rbi=3,
            batter_name="문현빈",
            pitcher_name="김재윤",
            wpa=0.855,
            away_score=7,
            home_score=6,
        ),
    ]
    return GameStoryBuilder().build(game, events)


def test_story_builder_selects_go_ahead_home_run_and_normalizes_half():
    payload = _story_payload()

    assert payload["schema_version"] == STORY_SCHEMA_VERSION
    descriptions = [item["description"] for item in payload["timeline"]]
    assert "=====================================" not in descriptions

    homer = next(item for item in payload["timeline"] if item["event_seq"] == 5)
    assert homer["inning_label"] == "9회초"
    assert homer["batting_team"] == "HH"
    assert homer["score_before"] == {"away": 4, "home": 6}
    assert homer["score_after"] == {"away": 7, "home": 6}
    assert homer["runs_scored"] == 3
    assert {
        "home_run",
        "lead_change",
        "decisive_score",
        "late_high_wpa",
    }.issubset(set(homer["tags"]))


def test_story_builder_marks_critical_error_and_outputs_timeline_in_game_order():
    payload = _story_payload()

    error_event = next(item for item in payload["timeline"] if item["event_seq"] == 3)
    assert "critical_error" in error_event["tags"]
    assert payload["story_flags"]["home_runs"] >= 1
    assert payload["story_flags"]["lead_changes"] >= 1
    assert payload["story_flags"]["critical_errors"] >= 1

    event_sequence = [item["event_seq"] for item in payload["timeline"]]
    assert event_sequence == sorted(event_sequence)
