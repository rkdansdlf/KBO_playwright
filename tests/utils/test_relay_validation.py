from unittest.mock import MagicMock

from src.models.game import Game, GameInningScore
from src.repositories.game_helpers import GAME_STATUS_COMPLETED
from src.utils.relay_validation import (
    ALL_VALIDATION_STATES,
    TERMINAL_VALIDATION_STATES,
    VALIDATION_VERIFIED,
    cross_validate_with_box_score,
    validate_live_events,
    validate_pbp_payload,
)


class TestValidateLiveEvents:
    def test_empty_events_no_warnings(self):
        assert validate_live_events([]) == []

    def test_score_regression_detected(self):
        events = [
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 0},
            {"inning": 1, "inning_half": "bottom", "home_score": 2, "away_score": 0, "outs": 0},
            {"inning": 2, "inning_half": "top", "home_score": 1, "away_score": 0, "outs": 0},
        ]
        warnings = validate_live_events(events)
        assert any("home_score decreased" in w for w in warnings)

    def test_inning_regression_detected(self):
        events = [
            {"inning": 2, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 0},
            {"inning": 1, "inning_half": "bottom", "home_score": 0, "away_score": 0, "outs": 0},
        ]
        warnings = validate_live_events(events)
        assert any("inning regressed" in w for w in warnings)

    def test_out_count_out_of_range(self):
        events = [
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 5},
        ]
        warnings = validate_live_events(events)
        assert any("out count out of range" in w for w in warnings)

    def test_event_seq_reversal_detected(self):
        events = [
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 0, "event_seq": 2},
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 1, "event_seq": 1},
        ]
        warnings = validate_live_events(events)
        assert any("event_seq reversed" in w for w in warnings)

    def test_half_regression_detected(self):
        events = [
            {"inning": 1, "inning_half": "bottom", "home_score": 0, "away_score": 0, "outs": 0},
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 1},
        ]
        warnings = validate_live_events(events)
        assert any("half regressed" in w for w in warnings)

    def test_clean_sequence_no_warnings(self):
        events = [
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0, "outs": 0, "event_seq": 1},
            {"inning": 1, "inning_half": "bottom", "home_score": 0, "away_score": 0, "outs": 1, "event_seq": 2},
            {"inning": 2, "inning_half": "top", "home_score": 1, "away_score": 0, "outs": 0, "event_seq": 3},
        ]
        assert validate_live_events(events) == []


class TestValidatePbpPayload:
    def test_empty_payload(self):
        assert validate_pbp_payload(MagicMock(), "g1", [], []) == (False, "empty_payload")

    def test_no_innings_found(self):
        session = MagicMock()
        assert validate_pbp_payload(session, "g1", [{"desc": "no inning"}], [{"desc": "no inning"}]) == (False, "no_innings_found")

    def test_starts_at_inning_2(self):
        session = MagicMock()
        events = [{"inning": 2}]
        raw = [{"inning": 2}]
        assert validate_pbp_payload(session, "g1", events, raw)[0] is False

    def test_missing_innings(self):
        session = MagicMock()
        events = [{"inning": 1}, {"inning": 3}]
        raw = [{"inning": 1}, {"inning": 3}]
        ok, reason = validate_pbp_payload(session, "g1", events, raw)
        assert ok is False
        assert "missing_innings" in reason

    def test_score_mismatch(self):
        session = MagicMock()
        mock_game = MagicMock(spec=Game)
        mock_game.game_status = GAME_STATUS_COMPLETED
        mock_game.home_score = 5
        mock_game.away_score = 3
        session.query().filter().first.return_value = mock_game
        events = [{"inning": i, "home_score": 0, "away_score": 0} for i in range(1, 10)]
        events[-1]["home_score"] = 4
        events[-1]["away_score"] = 3
        raw = [{"inning": i} for i in range(1, 10)]
        ok, reason = validate_pbp_payload(session, "g1", events, raw)
        assert ok is False
        assert "score_mismatch" in reason

    def test_valid_payload(self):
        session = MagicMock()
        mock_game = MagicMock(spec=Game)
        mock_game.game_status = GAME_STATUS_COMPLETED
        mock_game.home_score = 5
        mock_game.away_score = 3
        session.query().filter().first.return_value = mock_game
        events = [{"inning": i, "home_score": 5, "away_score": 3} for i in range(1, 10)]
        raw = [{"inning": i} for i in range(1, 10)]
        ok, reason = validate_pbp_payload(session, "g1", events, raw)
        assert ok is True
        assert reason is None


class TestCrossValidateWithBoxScore:
    def test_no_inning_scores_returns_true(self):
        session = MagicMock()
        session.query(GameInningScore).filter().order_by().all.return_value = []
        assert cross_validate_with_box_score(session, "g1", []) == (True, None)

    def test_match_returns_true(self):
        session = MagicMock()
        score_rows = [
            MagicMock(spec=GameInningScore, team_side="away", inning=1, runs=0),
            MagicMock(spec=GameInningScore, team_side="home", inning=1, runs=2),
        ]
        session.query(GameInningScore).filter().order_by().all.return_value = score_rows
        events = [
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "bottom", "home_score": 2, "away_score": 0},
        ]
        assert cross_validate_with_box_score(session, "g1", events) == (True, None)

    def test_mismatch_returns_false(self):
        session = MagicMock()
        score_rows = [
            MagicMock(spec=GameInningScore, team_side="away", inning=1, runs=1),
            MagicMock(spec=GameInningScore, team_side="home", inning=1, runs=0),
        ]
        session.query(GameInningScore).filter().order_by().all.return_value = score_rows
        events = [
            {"inning": 1, "inning_half": "top", "home_score": 0, "away_score": 0},
        ]
        ok, reason = cross_validate_with_box_score(session, "g1", events)
        assert ok is False
        assert "inning_score_mismatch" in reason


class TestValidationStates:
    def test_all_validation_states_defined(self):
        assert VALIDATION_VERIFIED in ALL_VALIDATION_STATES
        assert len(ALL_VALIDATION_STATES) == 7

    def test_terminal_validation_states(self):
        assert VALIDATION_VERIFIED in TERMINAL_VALIDATION_STATES
        assert "pending_live" not in TERMINAL_VALIDATION_STATES
