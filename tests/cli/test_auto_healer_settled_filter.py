"""The healer's automatic sets must not re-crawl games a settled letter answered.

Measured 2026-11-01..05: the nightly Step 0 recovery re-crawled the same ~40 past
season games (2011/2015 and the 2024 Premier12) every night. Each attempt failed
against a source that does not serve their records, opened a fresh dead letter (a
new ``original_run_id`` never collides), spent five retries, and returned the same
game the next night -- about 40 letters a night whose only new information was that
Naver still had nothing. The collection loader, the detail-recovery queue and the
boxscore repair already refuse that tuple; the healer's own target list did not.

The edges are what these tests pin:

* ``exhausted`` and ``ignored`` block; ``pending`` is what ``requeue`` produces and
  has to let the game back in;
* a finished relay keeps the game in front of the healer -- its data is still
  wanted even though an earlier attempt gave up (``20261026HTSS0``);
* stuck games are deliberately exempt: the healer owns a terminal write for them
  (UNRESOLVED_MISSING), so a settled letter does not trap them in a loop, and the
  recent-retry window (issue #55) must keep working;
* a targeted ``--game-id`` run is an operator naming games and is never filtered;
* a read that fails blocks nothing -- the filter must not drop the work itself.
"""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

import src.cli.backfill.auto_healer as auto_healer_module
from src.constants import FINAL_PBP_MARKER
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.game import Game, GamePlayByPlay
from src.services.recovery_manager import RecoveryManager

SETTLED_GAME = "20241124USVE0"
HEALABLE_GAME = "20241123TWJP0"
STUCK_GAME = "20261105KTSS0"


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (CrawlDeadLetter.__table__, GamePlayByPlay.__table__, Game.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)


@pytest.fixture(autouse=True)
def _use_the_test_session(session_factory, monkeypatch):
    monkeypatch.setattr(auto_healer_module, "SessionLocal", session_factory)
    # The healer's season gate is not what these tests are about; keep it open so
    # the stuck-set fixture does not depend on the real clock.
    monkeypatch.setattr(auto_healer_module, "automatic_write_allowed", MagicMock(return_value=True))
    monkeypatch.setattr(auto_healer_module, "current_season_year", MagicMock(return_value=2026))


def _game(game_id: str, *, game_date: date = date(2024, 11, 24)) -> MagicMock:
    return MagicMock(game_id=game_id, game_date=game_date)


def _letter(session_factory, *, game_id: str, status: str = DlqStatus.EXHAUSTED.value) -> None:
    with session_factory() as session:
        session.add(
            CrawlDeadLetter(
                dlq_id=f"dlq-{game_id}-{status}",
                original_run_id="run-1",
                crawler="game_detail",
                target_type="game",
                target_id=game_id,
                game_id=game_id,
                failure_stage="fetch",
                error_code="FETCH_HTTP_ERROR",
                status=status,
            ),
        )
        session.commit()


def _finished_relay(session_factory, *, game_id: str) -> None:
    with session_factory() as session:
        session.add(GamePlayByPlay(game_id=game_id, inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()


def _game_row(session_factory, game_id: str) -> None:
    with session_factory() as session:
        session.add(Game(game_id=game_id, game_date=date(2024, 11, 24), game_status="COMPLETED", is_primary=True))
        session.commit()


def _find(monkeypatch, *, stuck=None, inconsistent=None, pa=None):
    monkeypatch.setattr(auto_healer_module, "_find_stuck_games", MagicMock(return_value=stuck or []))
    monkeypatch.setattr(auto_healer_module, "_find_inconsistent_games", MagicMock(return_value=inconsistent or []))
    monkeypatch.setattr(auto_healer_module, "_find_pa_formula_inconsistent_games", MagicMock(return_value=pa or []))
    return auto_healer_module._find_recovery_targets(None)


class TestTheAutomaticSetsDropSettledLetters:
    def test_an_inconsistent_game_with_an_exhausted_letter_is_left_out(self, session_factory, monkeypatch) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)

        all_found, _stuck, inconsistent, _pa = _find(monkeypatch, inconsistent=[_game(SETTLED_GAME)])

        assert all_found == []
        assert inconsistent == []

    def test_an_ignored_letter_is_also_settled(self, session_factory, monkeypatch) -> None:
        _letter(session_factory, game_id=SETTLED_GAME, status=DlqStatus.IGNORED.value)

        all_found, *_ = _find(monkeypatch, inconsistent=[_game(SETTLED_GAME)])

        assert all_found == []

    def test_a_pending_letter_keeps_the_game_in(self, session_factory, monkeypatch) -> None:
        """``requeue`` returns a letter to ``pending``; that has to be the way back in."""
        _letter(session_factory, game_id=SETTLED_GAME, status=DlqStatus.PENDING.value)

        all_found, _stuck, inconsistent, _pa = _find(monkeypatch, inconsistent=[_game(SETTLED_GAME)])

        assert [game.game_id for game in all_found] == [SETTLED_GAME]
        assert [game.game_id for game in inconsistent] == [SETTLED_GAME]

    def test_a_finished_relay_keeps_the_game_in(self, session_factory, monkeypatch) -> None:
        """Its data is still wanted; the relay outranks the giving-up."""
        _letter(session_factory, game_id=SETTLED_GAME)
        _finished_relay(session_factory, game_id=SETTLED_GAME)

        all_found, *_ = _find(monkeypatch, inconsistent=[_game(SETTLED_GAME)])

        assert [game.game_id for game in all_found] == [SETTLED_GAME]

    def test_a_pa_formula_game_is_checked_too(self, session_factory, monkeypatch) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)

        all_found, _stuck, _inconsistent, pa = _find(monkeypatch, pa=[_game(SETTLED_GAME)])

        assert all_found == []
        assert pa == []

    def test_an_unlettered_game_is_untouched(self, session_factory, monkeypatch) -> None:
        """The filter has to leave the ordinary case alone."""
        all_found, *_ = _find(monkeypatch, inconsistent=[_game(HEALABLE_GAME)])

        assert [game.game_id for game in all_found] == [HEALABLE_GAME]


class TestStuckGamesAreDeliberatelyExempt:
    def test_a_settled_letter_does_not_drop_a_stuck_game(self, session_factory, monkeypatch) -> None:
        """The healer owns a terminal write for stuck games, and #55 retries them."""
        _letter(session_factory, game_id=STUCK_GAME)

        all_found, stuck, _inconsistent, _pa = _find(
            monkeypatch, stuck=[_game(STUCK_GAME, game_date=date(2026, 11, 5))]
        )

        assert [game.game_id for game in stuck] == [STUCK_GAME]
        assert [game.game_id for game in all_found] == [STUCK_GAME]


class TestATargetedRunIsNeverFiltered:
    def test_a_named_game_is_returned_even_with_a_settled_letter(self, session_factory) -> None:
        """``--game-id`` names games explicitly; the checkpoint bypass is the contract."""
        _game_row(session_factory, SETTLED_GAME)
        _letter(session_factory, game_id=SETTLED_GAME)

        all_found, _stuck, inconsistent, pa = auto_healer_module._find_recovery_targets([SETTLED_GAME])

        assert [game.game_id for game in all_found] == [SETTLED_GAME]
        assert inconsistent == []
        assert pa == []


class TestAReadThatFailsBlocksNothing:
    def test_the_targets_are_kept_when_the_letter_check_cannot_run(self, session_factory, monkeypatch) -> None:
        def _unreachable_session():
            raise SQLAlchemyError("database is down")

        monkeypatch.setattr(auto_healer_module, "SessionLocal", _unreachable_session)

        all_found, *_ = _find(monkeypatch, inconsistent=[_game(SETTLED_GAME)])

        assert [game.game_id for game in all_found] == [SETTLED_GAME]


class TestTheExcludedGameNeverReachesRecovery:
    def test_run_healer_crawls_only_the_unsettled_game(self, session_factory, monkeypatch, tmp_path) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)
        monkeypatch.setattr(
            auto_healer_module,
            "_find_stuck_games",
            MagicMock(return_value=[]),
        )
        monkeypatch.setattr(
            auto_healer_module,
            "_find_inconsistent_games",
            MagicMock(return_value=[_game(SETTLED_GAME), _game(HEALABLE_GAME)]),
        )
        monkeypatch.setattr(auto_healer_module, "_find_pa_formula_inconsistent_games", MagicMock(return_value=[]))
        recovery = AsyncMock(return_value={"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 1})
        monkeypatch.setattr(auto_healer_module, "_run_recovery", recovery)
        monkeypatch.setattr(
            auto_healer_module,
            "RecoveryManager",
            lambda **kwargs: RecoveryManager(str(tmp_path / "healer_checkpoint.json"), **kwargs),
        )
        monkeypatch.setattr(auto_healer_module, "_log_anomaly_summary", MagicMock())
        monkeypatch.setattr(auto_healer_module, "_log_healer_summary", MagicMock())
        monkeypatch.setattr(auto_healer_module, "_send_healer_start_alert", MagicMock())

        asyncio.run(auto_healer_module.run_healer_async(dry_run=False))

        candidates = [game.game_id for game in recovery.await_args.args[0]]
        assert candidates == [HEALABLE_GAME]
