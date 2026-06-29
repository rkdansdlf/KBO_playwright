from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from src.models.game import Game, GameInningScore
from src.repositories.game_helpers import GAME_STATUS_COMPLETED
from src.utils.relay_validation import (
    cross_validate_with_box_score,
    validate_pbp_payload,
)


def test_validate_pbp_payload_empty():
    session = MagicMock()
    is_valid, reason = validate_pbp_payload(session, "20260401SKLG0", [], [])
    assert not is_valid
    assert reason == "empty_payload"


def test_validate_pbp_payload_no_innings():
    session = MagicMock()
    is_valid, reason = validate_pbp_payload(
        session,
        "20260401SKLG0",
        [{"description": "No inning"}],
        [{"play_description": "No inning"}],
    )
    assert not is_valid
    assert reason == "no_innings_found"


def test_validate_pbp_payload_starts_not_at_one():
    session = MagicMock()
    events = [
        {"inning": 2, "description": "some play"},
    ]
    raw_pbp = [
        {"inning": 2, "play_description": "some play"},
    ]
    is_valid, reason = validate_pbp_payload(session, "20260401SKLG0", events, raw_pbp)
    assert not is_valid
    assert "starts_at_inning" in reason


def test_validate_pbp_payload_missing_inning():
    session = MagicMock()
    events = [
        {"inning": 1, "description": "inning 1 play"},
        {"inning": 2, "description": "inning 2 play"},
        {"inning": 4, "description": "inning 4 play"},
    ]
    raw_pbp = [
        {"inning": 1, "play_description": "inning 1 play"},
        {"inning": 2, "play_description": "inning 2 play"},
        {"inning": 4, "play_description": "inning 4 play"},
    ]
    is_valid, reason = validate_pbp_payload(session, "20260401SKLG0", events, raw_pbp)
    assert not is_valid
    assert "missing_innings_[3]" in reason


def test_validate_pbp_payload_score_mismatch():
    session = MagicMock()
    mock_game = MagicMock(spec=Game)
    mock_game.game_status = GAME_STATUS_COMPLETED
    mock_game.home_score = 5
    mock_game.away_score = 3
    session.query().filter().first.return_value = mock_game

    events = [{"inning": i, "home_score": 0, "away_score": 0} for i in range(1, 9)] + [
        {"inning": 9, "home_score": 4, "away_score": 3},
    ]  # Last score is 4-3, mismatching 5-3
    raw_pbp = [{"inning": i, "play_description": "play"} for i in range(1, 10)]
    is_valid, reason = validate_pbp_payload(session, "20260401SKLG0", events, raw_pbp)
    assert not is_valid
    assert "score_mismatch" in reason


def test_validate_pbp_payload_success():
    session = MagicMock()
    mock_game = MagicMock(spec=Game)
    mock_game.game_status = GAME_STATUS_COMPLETED
    mock_game.home_score = 5
    mock_game.away_score = 3
    session.query().filter().first.return_value = mock_game

    events = [{"inning": i, "home_score": 0, "away_score": 0} for i in range(1, 9)] + [
        {"inning": 9, "home_score": 5, "away_score": 3},
    ]  # Match 5-3
    raw_pbp = [{"inning": i, "play_description": "play"} for i in range(1, 10)]
    is_valid, reason = validate_pbp_payload(session, "20260401SKLG0", events, raw_pbp)
    assert is_valid
    assert reason is None


# ===================================================================
# cross_validate_with_box_score tests
# ===================================================================


def test_cross_validate_no_inning_scores_returns_true_with_warning():
    """When inning_scores table has no rows, validation passes but warns."""
    session = MagicMock()
    session.query(GameInningScore).filter().order_by().all.return_value = []
    with patch.object(logging.getLogger("src.utils.relay_validation"), "warning") as mock_warn:
        is_valid, reason = cross_validate_with_box_score(session, "g1", [])
        assert is_valid
        assert reason is None
        mock_warn.assert_called_once()


def test_cross_validate_returns_true_when_scores_match():
    session = MagicMock()
    score_rows = [
        MagicMock(spec=GameInningScore, team_side="away", inning=1, runs=0),
        MagicMock(spec=GameInningScore, team_side="away", inning=2, runs=1),
        MagicMock(spec=GameInningScore, team_side="home", inning=1, runs=0),
        MagicMock(spec=GameInningScore, team_side="home", inning=2, runs=2),
    ]
    session.query(GameInningScore).filter().order_by().all.return_value = score_rows

    events = [
        {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0},
        {"inning": 1, "inning_half": "bottom", "home_score": 0, "away_score": 0},
        {"inning": 2, "inning_half": "top", "home_score": 0, "away_score": 1},
        {"inning": 2, "inning_half": "bottom", "home_score": 2, "away_score": 1},
    ]
    is_valid, reason = cross_validate_with_box_score(session, "g1", events)
    assert is_valid
    assert reason is None


def test_cross_validate_returns_false_when_scores_mismatch():
    session = MagicMock()
    score_rows = [
        MagicMock(spec=GameInningScore, team_side="away", inning=1, runs=0),
        MagicMock(spec=GameInningScore, team_side="home", inning=1, runs=3),
    ]
    session.query(GameInningScore).filter().order_by().all.return_value = score_rows

    events = [
        {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0},
        {"inning": 1, "inning_half": "bottom", "home_score": 1, "away_score": 0},
    ]
    is_valid, reason = cross_validate_with_box_score(session, "g1", events)
    assert not is_valid
    assert "inning_score_mismatch" in reason


def test_cross_validate_handles_none_scores_gracefully():
    """Events with None scores should not crash — they inherit previous values."""
    session = MagicMock()
    score_rows = [
        MagicMock(spec=GameInningScore, team_side="away", inning=1, runs=0),
        MagicMock(spec=GameInningScore, team_side="home", inning=1, runs=0),
    ]
    session.query(GameInningScore).filter().order_by().all.return_value = score_rows

    events = [
        {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0},
        {"inning": 1, "inning_half": "top", "home_score": None, "away_score": None, "event_seq": 2},
        {"inning": 1, "inning_half": "bottom", "home_score": 0, "away_score": 0},
    ]
    # Should not crash — None-score events inherit prev values
    is_valid, reason = cross_validate_with_box_score(session, "g1", events)
    assert is_valid
    assert reason is None


# ===================================================================
# validate_pbp_payload edge cases
# ===================================================================


def test_validate_pbp_payload_with_none_scores_does_not_crash():
    """Final score extraction should handle None scores gracefully."""
    session = MagicMock()
    mock_game = MagicMock(spec=Game)
    mock_game.game_status = GAME_STATUS_COMPLETED
    mock_game.home_score = 3
    mock_game.away_score = 1
    session.query().filter().first.return_value = mock_game

    events = [{"inning": i, "home_score": 0, "away_score": 0} for i in range(1, 9)] + [
        {"inning": 9, "home_score": 3, "away_score": 1},
    ]
    raw_pbp = [{"inning": i, "play_description": "play"} for i in range(1, 10)]
    is_valid, reason = validate_pbp_payload(session, "g1", events, raw_pbp)
    assert is_valid
    assert reason is None
