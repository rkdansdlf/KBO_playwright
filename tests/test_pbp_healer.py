"""
Unit tests for PBP auto-healing (run_pbp_healer_async, _find_unverified_pbp_games).

Async tests use asyncio.run() to avoid requiring pytest-asyncio plugin.
In-memory SQLite uses per-table creation (not Base.metadata.create_all)
to avoid FK resolution errors from unrelated models.
"""

from __future__ import annotations

import asyncio
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game, GameIdAlias, GameMetadata

# ---------------------------------------------------------------------------
# In-memory DB helpers
# ---------------------------------------------------------------------------


def _build_engine():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameIdAlias.__table__.create(bind=engine)
    GameMetadata.__table__.create(bind=engine)
    return engine


def _make_game(game_id: str, status: str, game_date=None) -> Game:
    return Game(
        game_id=game_id,
        game_status=status,
        game_date=game_date or (datetime.date.today() - datetime.timedelta(days=1)),
    )


def _make_metadata(game_id: str, pbp_status: str, error: str = "none") -> GameMetadata:
    # Pass dict directly — SQLAlchemy JSON column auto-serializes
    return GameMetadata(
        game_id=game_id,
        source_payload={"pbp_validation_status": pbp_status, "pbp_validation_error": error},
    )


def _new_session():
    engine = _build_engine()
    Session = sessionmaker(bind=engine)
    return Session()


# ---------------------------------------------------------------------------
# _find_unverified_pbp_games
# ---------------------------------------------------------------------------


class TestFindUnverifiedPBPGames:
    """Tests for the DB query that scans for unverified PBP games."""

    def test_returns_empty_when_no_unverified_games(self):
        """All games verified → scan returns nothing."""
        session = _new_session()
        session.add(_make_game("2025053001", "COMPLETED"))
        session.add(_make_metadata("2025053001", "verified"))
        session.commit()

        with patch("src.cli.auto_healer.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__ = lambda s: session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            from src.cli.auto_healer import _find_unverified_pbp_games

            result = _find_unverified_pbp_games(lookback_days=3)
        assert result == []

    def test_returns_unverified_completed_games(self):
        """COMPLETED games with unverified PBP should be returned."""
        session = _new_session()
        session.add(_make_game("2025053001", "COMPLETED"))
        session.add(_make_metadata("2025053001", "unverified", "missing_innings_[4]"))
        session.commit()

        with patch("src.cli.auto_healer.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__ = lambda s: session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            from src.cli.auto_healer import _find_unverified_pbp_games

            result = _find_unverified_pbp_games(lookback_days=3)

        assert len(result) == 1
        assert result[0]["game_id"] == "2025053001"
        assert result[0]["error_reason"] == "missing_innings_[4]"

    def test_excludes_scheduled_games(self):
        """SCHEDULED games should NOT be healed (not yet finished)."""
        session = _new_session()
        session.add(_make_game("2025053002", "SCHEDULED"))
        session.add(_make_metadata("2025053002", "unverified", "empty_payload"))
        session.commit()

        with patch("src.cli.auto_healer.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__ = lambda s: session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            from src.cli.auto_healer import _find_unverified_pbp_games

            result = _find_unverified_pbp_games(lookback_days=3)
        assert result == []

    def test_includes_draw_games(self):
        """DRAW (무승부) games should also be included."""
        session = _new_session()
        session.add(_make_game("2025053003", "DRAW"))
        session.add(_make_metadata("2025053003", "unverified", "score_mismatch"))
        session.commit()

        with patch("src.cli.auto_healer.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__ = lambda s: session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            from src.cli.auto_healer import _find_unverified_pbp_games

            result = _find_unverified_pbp_games(lookback_days=3)
        assert len(result) == 1
        assert result[0]["game_id"] == "2025053003"

    def test_lookback_days_filters_old_games(self):
        """Games older than lookback_days should be excluded."""
        old_date = datetime.date.today() - datetime.timedelta(days=10)
        session = _new_session()
        session.add(_make_game("2025052001", "COMPLETED", game_date=old_date))
        session.add(_make_metadata("2025052001", "unverified", "missing_innings_[2]"))
        session.commit()

        with patch("src.cli.auto_healer.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__ = lambda s: session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            from src.cli.auto_healer import _find_unverified_pbp_games

            result = _find_unverified_pbp_games(lookback_days=3)
        assert result == []

    def test_multiple_unverified_games_all_returned(self):
        """Multiple unverified games within lookback window are all returned."""
        session = _new_session()
        for i, gid in enumerate(["2025053001", "2025053002", "2025053003"]):
            session.add(_make_game(gid, "COMPLETED"))
            session.add(_make_metadata(gid, "unverified", f"error_{i}"))
        session.commit()

        with patch("src.cli.auto_healer.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__ = lambda s: session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            from src.cli.auto_healer import _find_unverified_pbp_games

            result = _find_unverified_pbp_games(lookback_days=3)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# run_pbp_healer_async
# ---------------------------------------------------------------------------
_RECOVER_PATH = "src.services.relay_recovery_service.recover_relay_data"


class TestRunPBPHealerAsync:
    """Tests for the async PBP healer orchestrator."""

    def test_dry_run_does_not_send_telegram(self):
        """In dry-run mode, no re-crawl and no Telegram messages."""
        fake_games = [
            {
                "game_id": "2025053001",
                "game_date": "2025-05-30",
                "away_team": "NC",
                "home_team": "SSG",
                "error_reason": "missing_innings_[4]",
            }
        ]

        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=fake_games),
            patch("src.cli.auto_healer.TelegramBotClient.send_message") as mock_tg,
        ):
            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=True, lookback_days=3))

        assert result["found"] == 1
        assert result["recovered"] == 0
        assert result["skipped"] == 1
        mock_tg.assert_not_called()

    def test_no_unverified_games_returns_zero(self):
        """When no unverified games, function returns early without Telegram."""
        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=[]),
            patch("src.cli.auto_healer.TelegramBotClient.send_message") as mock_tg,
        ):
            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=False, lookback_days=3))

        assert result == {"found": 0, "recovered": 0, "failed": 0, "skipped": 0}
        mock_tg.assert_not_called()

    def test_successful_heal_sends_two_telegram_messages(self):
        """Discovery alert + success result alert both sent."""
        fake_games = [
            {
                "game_id": "2025053001",
                "game_date": "2025-05-30",
                "away_team": "KT",
                "home_team": "LG",
                "error_reason": "score_mismatch",
            }
        ]
        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=fake_games),
            patch("src.cli.auto_healer.TelegramBotClient.send_message") as mock_tg,
            patch(
                _RECOVER_PATH,
                AsyncMock(
                    return_value=SimpleNamespace(
                        saved_games=1,
                        report_rows=[{"game_id": "2025053001", "status": "saved"}],
                    )
                ),
            ),
        ):
            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=False, lookback_days=3))

        assert result["found"] == 1
        assert result["recovered"] == 1
        assert result["failed"] == 0
        # discovery + result
        assert mock_tg.call_count == 2
        result_msg = mock_tg.call_args_list[1][0][0]
        assert "복구" in result_msg

    def test_kbo_returns_no_data_marks_failed(self):
        """When KBO returns None, game is counted as failed."""
        fake_games = [
            {
                "game_id": "2025053001",
                "game_date": "2025-05-30",
                "away_team": "두산",
                "home_team": "삼성",
                "error_reason": "empty_payload",
            }
        ]

        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=fake_games),
            patch("src.cli.auto_healer.TelegramBotClient.send_message") as mock_tg,
            patch(_RECOVER_PATH, AsyncMock(return_value=SimpleNamespace(saved_games=0, report_rows=[]))),
        ):
            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=False, lookback_days=3))

        assert result["found"] == 1
        assert result["recovered"] == 0
        assert result["failed"] == 1
        assert mock_tg.call_count == 2
        failure_msg = mock_tg.call_args_list[1][0][0]
        assert "실패" in failure_msg

    def test_revalidation_failure_counts_as_failed(self):
        """Re-crawl succeeds but re-validation still fails → counted as failed."""
        fake_games = [
            {
                "game_id": "2025053001",
                "game_date": "2025-05-30",
                "away_team": "한화",
                "home_team": "롯데",
                "error_reason": "score_mismatch",
            }
        ]
        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=fake_games),
            patch("src.cli.auto_healer.TelegramBotClient.send_message"),
            patch(_RECOVER_PATH, AsyncMock(return_value=SimpleNamespace(saved_games=0, report_rows=[]))),
        ):
            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=False, lookback_days=3))

        assert result["recovered"] == 0
        assert result["failed"] == 1

    def test_targeted_mode_skips_db_scan(self):
        """When target_game_ids given, _find_unverified_pbp_games is NOT called."""
        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games") as mock_scan,
            patch("src.cli.auto_healer.TelegramBotClient.send_message"),
            patch("src.cli.auto_healer.SessionLocal") as mock_sl,
        ):
            mock_session = MagicMock()
            mock_session.execute.return_value.fetchall.return_value = []
            mock_sl.return_value.__enter__ = lambda s: mock_session
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)

            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=True, target_game_ids=["2025053001"]))

        mock_scan.assert_not_called()
        assert result["found"] == 0

    def test_crawler_exception_counts_as_failed(self):
        """If PBPCrawler raises an exception, the game is counted as failed."""
        fake_games = [
            {
                "game_id": "2025053001",
                "game_date": "2025-05-30",
                "away_team": "키움",
                "home_team": "NC",
                "error_reason": "empty_payload",
            }
        ]

        with (
            patch("src.cli.auto_healer._find_unverified_pbp_games", return_value=fake_games),
            patch("src.cli.auto_healer.TelegramBotClient.send_message"),
            patch(_RECOVER_PATH, AsyncMock(return_value=SimpleNamespace(saved_games=0, report_rows=[]))),
        ):
            from src.cli.auto_healer import run_pbp_healer_async

            result = asyncio.run(run_pbp_healer_async(dry_run=False, lookback_days=3))

        assert result["failed"] == 1
        assert result["recovered"] == 0


# ---------------------------------------------------------------------------
# run_pbp_healer (CLI wrapper)
# ---------------------------------------------------------------------------


class TestRunPBPHealerCLI:
    """Tests for the CLI sync wrapper."""

    def test_exit_code_zero_on_all_recovered(self):
        """Exit code 0 when no failures."""
        from src.cli.auto_healer import run_pbp_healer

        with patch(
            "asyncio.run",
            return_value={"found": 2, "recovered": 2, "failed": 0, "skipped": 0},
        ):
            code = run_pbp_healer(["--lookback-days", "3"])
        assert code == 0

    def test_exit_code_one_on_partial_failure(self):
        """Exit code 1 when some games could not be recovered."""
        from src.cli.auto_healer import run_pbp_healer

        with patch(
            "asyncio.run",
            return_value={"found": 2, "recovered": 1, "failed": 1, "skipped": 0},
        ):
            code = run_pbp_healer(["--lookback-days", "3"])
        assert code == 1

    def test_dry_run_flag_forwarded(self):
        """--dry-run flag is forwarded correctly."""
        from src.cli.auto_healer import run_pbp_healer

        captured = {}

        async def fake_healer(**kwargs):
            captured.update(kwargs)
            return {"found": 0, "recovered": 0, "failed": 0, "skipped": 0}

        with patch("src.cli.auto_healer.run_pbp_healer_async", side_effect=fake_healer):
            run_pbp_healer(["--dry-run", "--lookback-days", "5"])

        assert captured.get("dry_run") is True
        assert captured.get("lookback_days") == 5
