"""Unit tests for GameStreamGenerator."""

from __future__ import annotations

from src.simulation.stream_generator import GameStreamGenerator


def test_generate_game_stream_basics() -> None:
    """Test generating a full 9-inning game stream."""
    generator = GameStreamGenerator(home_team="KIA", away_team="LG", seed=42)
    events = generator.generate_game_stream(game_id="20260401LGHT0", max_innings=9)

    assert len(events) >= 50
    first_event = events[0]
    assert first_event.inning == 1
    assert first_event.is_bottom is False
    assert first_event.batter_name in generator.away_lineup

    last_event = events[-1]
    assert last_event.inning >= 9
    assert last_event.home_score >= 0
    assert last_event.away_score >= 0


def test_generate_game_stream_seed_determinism() -> None:
    """Test that same seed generates identical event streams."""
    gen1 = GameStreamGenerator(seed=123)
    events1 = gen1.generate_game_stream()

    gen2 = GameStreamGenerator(seed=123)
    events2 = gen2.generate_game_stream()

    assert len(events1) == len(events2)
    for e1, e2 in zip(events1, events2, strict=True):
        assert e1.result_type == e2.result_type
        assert e1.batter_name == e2.batter_name
        assert e1.home_score == e2.home_score
        assert e1.away_score == e2.away_score


def test_advance_runners_homerun() -> None:
    """Test 3-run home run advancement with runners on 1st and 2nd."""
    runners_before = 3  # 1st and 2nd
    outs_before = 1
    runners_after, outs_after, runs, desc = GameStreamGenerator._advance_runners(
        runners_before, outs_before, "HOMERUN", "김도영"
    )

    assert runs == 3
    assert runners_after == 0
    assert outs_after == 1
    assert "홈런" in desc


def test_advance_runners_double_play() -> None:
    """Test 6-4-3 double play transition."""
    runners_before = 1  # 1st base
    outs_before = 0
    runners_after, outs_after, runs, desc = GameStreamGenerator._advance_runners(
        runners_before, outs_before, "DOUBLE_PLAY", "박찬호"
    )

    assert runs == 0
    assert outs_after == 2
    assert runners_after == 0
    assert "병살타" in desc
