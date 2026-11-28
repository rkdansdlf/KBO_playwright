"""The post-crawl audit heals only games that can still be healed.

The audit finds logic violations and re-crawls the games behind them. For a game
whose ``game_detail`` dead letter already settled, that re-crawl cannot succeed --
the source has answered -- and its only products are another five attempts and one
more letter per night. The collection loader and the healer's own target list
already refuse the tuple; the audit's heal call named games directly and skipped
the rule with them.

The edges are the same ones the other paths pin: ``pending`` (what ``requeue``
produces) stays healable, a finished relay keeps its game even when an attempt gave
up, and a failed read keeps every id -- a filter that cannot run must not silently
drop the work.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

import src.cli.pipelines.run_daily_update as run_daily_update_module
from src.constants import FINAL_PBP_MARKER
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.game import GamePlayByPlay

SETTLED_GAME = "20261027HTSS0"
HEALABLE_GAME = "20261028HTSS0"


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (CrawlDeadLetter.__table__, GamePlayByPlay.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)


@pytest.fixture(autouse=True)
def _use_the_test_session(session_factory, monkeypatch):
    monkeypatch.setattr(run_daily_update_module, "SessionLocal", session_factory)


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


class TestTheAuditHealDropsSettledLetters:
    def test_settled_without_a_relay_is_dropped(self, session_factory) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)

        assert run_daily_update_module._without_settled_unless_relay_finished([SETTLED_GAME]) == []

    def test_an_ignored_letter_is_also_settled(self, session_factory) -> None:
        _letter(session_factory, game_id=SETTLED_GAME, status=DlqStatus.IGNORED.value)

        assert run_daily_update_module._without_settled_unless_relay_finished([SETTLED_GAME]) == []

    def test_a_pending_letter_keeps_the_id(self, session_factory) -> None:
        _letter(session_factory, game_id=SETTLED_GAME, status=DlqStatus.PENDING.value)

        assert run_daily_update_module._without_settled_unless_relay_finished([SETTLED_GAME]) == [SETTLED_GAME]

    def test_a_finished_relay_keeps_the_id(self, session_factory) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)
        _finished_relay(session_factory, game_id=SETTLED_GAME)

        assert run_daily_update_module._without_settled_unless_relay_finished([SETTLED_GAME]) == [SETTLED_GAME]

    def test_an_unlettered_id_is_untouched(self, session_factory) -> None:
        assert run_daily_update_module._without_settled_unless_relay_finished([HEALABLE_GAME]) == [HEALABLE_GAME]

    def test_an_empty_input_asks_nothing(self) -> None:
        assert run_daily_update_module._without_settled_unless_relay_finished([]) == []

    def test_a_read_failure_keeps_every_id(self, monkeypatch) -> None:
        def _unreachable_session():
            raise SQLAlchemyError("database is down")

        monkeypatch.setattr(run_daily_update_module, "SessionLocal", _unreachable_session)

        assert run_daily_update_module._without_settled_unless_relay_finished([SETTLED_GAME, HEALABLE_GAME]) == [
            SETTLED_GAME,
            HEALABLE_GAME,
        ]


class TestTheAuditStepUsesTheFilter:
    def _violations(self, monkeypatch, ids: list[str]) -> None:
        monkeypatch.setattr(
            "scripts.verification.audit_game_logic.audit_game_logic",
            MagicMock(return_value=[{"game_id": game_id} for game_id in ids]),
        )

    def test_the_heal_call_skips_settled_games(self, session_factory, monkeypatch) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)
        heal = AsyncMock(return_value=0)
        monkeypatch.setattr(run_daily_update_module, "run_healer_async", heal)
        self._violations(monkeypatch, [SETTLED_GAME, HEALABLE_GAME])

        asyncio.run(run_daily_update_module._step_10_7_enrichment(MagicMock(year=2026, runner=MagicMock())))

        heal.assert_awaited_once()
        assert heal.await_args.kwargs["target_game_ids"] == [HEALABLE_GAME]

    def test_with_no_healable_target_the_call_is_skipped(self, session_factory, monkeypatch) -> None:
        _letter(session_factory, game_id=SETTLED_GAME)
        heal = AsyncMock(return_value=0)
        monkeypatch.setattr(run_daily_update_module, "run_healer_async", heal)
        self._violations(monkeypatch, [SETTLED_GAME])

        asyncio.run(run_daily_update_module._step_10_7_enrichment(MagicMock(year=2026, runner=MagicMock())))

        heal.assert_not_awaited()

    def test_every_healable_target_is_still_healed(self, session_factory, monkeypatch) -> None:
        """The filter has to leave the ordinary path alone."""
        heal = AsyncMock(return_value=0)
        monkeypatch.setattr(run_daily_update_module, "run_healer_async", heal)
        self._violations(monkeypatch, [HEALABLE_GAME])

        asyncio.run(run_daily_update_module._step_10_7_enrichment(MagicMock(year=2026, runner=MagicMock())))

        heal.assert_awaited_once()
        assert heal.await_args.kwargs["target_game_ids"] == [HEALABLE_GAME]
