"""Tests for CrawlGate — data freshness/quality gate for pipeline control."""

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game
from src.monitoring.crawl_gate import CrawlGate
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_DRAW


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    return sessionmaker(bind=engine)()


class TestCheckFreshness:
    def test_returns_true_when_no_issues(self):
        session = _make_session()
        gate = CrawlGate(session)
        with patch("src.cli.freshness_gate.collect_freshness_issues", return_value={}):
            assert gate.check_freshness("20251001") is True
        assert gate.issues == []

    def test_returns_false_and_populates_issues(self):
        session = _make_session()
        gate = CrawlGate(session)
        fake_issues = {"G1": ["missing metadata"], "G2": ["no lineup"]}
        with patch("src.cli.freshness_gate.collect_freshness_issues", return_value=fake_issues):
            assert gate.check_freshness("20251001") is False
        assert len(gate.issues) == 2
        assert "[G1] missing metadata" in gate.issues
        assert "[G2] no lineup" in gate.issues


class TestCheckGameCompletionRate:
    def test_returns_true_when_no_games(self):
        session = _make_session()
        gate = CrawlGate(session)
        assert gate.check_game_completion_rate("2025-10-01") is True

    def test_returns_true_when_all_completed(self):
        session = _make_session()
        session.add(Game(game_id="G1", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.add(Game(game_id="G2", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.commit()
        gate = CrawlGate(session)
        assert gate.check_game_completion_rate("2025-10-01") is True
        assert gate.issues == []

    def test_returns_true_when_rate_meets_threshold(self):
        session = _make_session()
        session.add(Game(game_id="G1", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.add(Game(game_id="G2", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.add(Game(game_id="G3", game_date=date(2025, 10, 1), game_status=GAME_STATUS_DRAW))
        session.add(Game(game_id="G4", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.add(Game(game_id="G5", game_date=date(2025, 10, 1), game_status="SCHEDULED"))
        session.commit()
        gate = CrawlGate(session)
        assert gate.check_game_completion_rate("2025-10-01") is True

    def test_returns_false_when_rate_below_threshold(self):
        session = _make_session()
        session.add(Game(game_id="G1", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.add(Game(game_id="G2", game_date=date(2025, 10, 1), game_status="SCHEDULED"))
        session.add(Game(game_id="G3", game_date=date(2025, 10, 1), game_status="SCHEDULED"))
        session.add(Game(game_id="G4", game_date=date(2025, 10, 1), game_status="SCHEDULED"))
        session.add(Game(game_id="G5", game_date=date(2025, 10, 1), game_status="SCHEDULED"))
        session.add(Game(game_id="G6", game_date=date(2025, 10, 1), game_status="SCHEDULED"))
        session.commit()
        gate = CrawlGate(session)
        assert gate.check_game_completion_rate("2025-10-01") is False
        assert len(gate.issues) == 1
        assert "Completion rate" in gate.issues[0]

    def test_only_considers_target_date(self):
        session = _make_session()
        session.add(Game(game_id="G1", game_date=date(2025, 10, 1), game_status=GAME_STATUS_COMPLETED))
        session.add(Game(game_id="G2", game_date=date(2025, 10, 2), game_status="SCHEDULED"))
        session.commit()
        gate = CrawlGate(session)
        assert gate.check_game_completion_rate("2025-10-01") is True


class TestCheckStandingsIntegrity:
    def test_returns_true_when_valid(self):
        session = _make_session()
        gate = CrawlGate(session)
        with patch(
            "src.validators.standings_integrity.validate_standings_integrity",
            return_value={"ok": True, "mismatches": [], "missing_score_games": []},
        ):
            assert gate.check_standings_integrity("20251001") is True
        assert gate.issues == []

    def test_returns_false_on_mismatches(self):
        session = _make_session()
        gate = CrawlGate(session)
        with patch(
            "src.validators.standings_integrity.validate_standings_integrity",
            return_value={
                "ok": False,
                "mismatches": [{"team_code": "LG", "issue": "value_mismatch"}],
                "missing_score_games": [],
            },
        ):
            assert gate.check_standings_integrity("20251001") is False
        assert len(gate.issues) == 1
        assert "mismatches" in gate.issues[0]

    def test_returns_false_on_missing_scores(self):
        session = _make_session()
        gate = CrawlGate(session)
        with patch(
            "src.validators.standings_integrity.validate_standings_integrity",
            return_value={
                "ok": False,
                "mismatches": [],
                "missing_score_games": ["20251001LGSS0"],
            },
        ):
            assert gate.check_standings_integrity("20251001") is False
        assert len(gate.issues) == 1
        assert "missing scores" in gate.issues[0]

    def test_returns_false_on_both_mismatches_and_missing_scores(self):
        session = _make_session()
        gate = CrawlGate(session)
        with patch(
            "src.validators.standings_integrity.validate_standings_integrity",
            return_value={
                "ok": False,
                "mismatches": [{"team_code": "SS", "issue": "value_mismatch"}],
                "missing_score_games": ["20251001SSG0"],
            },
        ):
            assert gate.check_standings_integrity("20251001") is False
        assert len(gate.issues) == 2


class TestRunAllChecks:
    def test_returns_true_when_all_pass(self):
        session = _make_session()
        gate = CrawlGate(session)
        with (
            patch.object(gate, "check_freshness", return_value=True),
            patch.object(gate, "check_game_completion_rate", return_value=True),
            patch.object(gate, "check_standings_integrity", return_value=True),
        ):
            assert gate.run_all_checks("20251001") is True

    def test_returns_false_when_any_fails(self):
        session = _make_session()
        gate = CrawlGate(session)
        with (
            patch.object(gate, "check_freshness", return_value=True),
            patch.object(gate, "check_game_completion_rate", return_value=False),
            patch.object(gate, "check_standings_integrity", return_value=True),
        ):
            assert gate.run_all_checks("20251001") is False

    def test_exits_when_enforce_mode_and_failure(self):
        session = _make_session()
        gate = CrawlGate(session, enforce=True)
        with (
            patch.object(gate, "check_freshness", return_value=True),
            patch.object(gate, "check_game_completion_rate", return_value=False),
            patch.object(gate, "check_standings_integrity", return_value=True),
        ):
            with pytest.raises(SystemExit) as exc_info:
                gate.run_all_checks("20251001")
            assert exc_info.value.code == 1

    def test_does_not_exit_when_enforce_mode_and_all_pass(self):
        session = _make_session()
        gate = CrawlGate(session, enforce=True)
        with (
            patch.object(gate, "check_freshness", return_value=True),
            patch.object(gate, "check_game_completion_rate", return_value=True),
            patch.object(gate, "check_standings_integrity", return_value=True),
        ):
            assert gate.run_all_checks("20251001") is True

    def test_calls_all_three_checks(self):
        session = _make_session()
        gate = CrawlGate(session)
        with (
            patch.object(gate, "check_freshness", return_value=True) as mock_fresh,
            patch.object(gate, "check_game_completion_rate", return_value=True) as mock_rate,
            patch.object(gate, "check_standings_integrity", return_value=True) as mock_stand,
        ):
            gate.run_all_checks("20251001")
        mock_fresh.assert_called_once_with("20251001")
        mock_rate.assert_called_once_with("20251001")
        mock_stand.assert_called_once_with("20251001")
