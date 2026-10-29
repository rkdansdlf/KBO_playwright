"""A settled dead letter keeps the game out of the collection targets.

The repair path learned this first (2026-10-29): a failure log has no memory, so a
game that failed five times stayed in the log and every later run re-selected it.
The repair stopped re-selecting settled letters, but the collection path -- which is
what actually opens a ``game_detail`` run -- did not, so on 2026-10-30 the four 2024
Premier12 games settled the day before were back in ``exhausted`` with five more
attempts each. Naver has no record for that competition at all: the retries could
not succeed, and their only product was a refilled queue.

The filter has to cut both ways, and the edges are where it is easy to get wrong:

* the letter is matched through either ``game_id`` or ``target_id``, because both
  are written and only one of them is always set;
* only ``game_detail`` letters count -- a ``relay`` letter saying the same thing
  would otherwise veto a collection that could still work;
* only ``exhausted`` and ``ignored`` are settled. ``pending`` is what ``requeue``
  produces, and it has to let the game back in or the operator's decision to retry
  would be the one thing the filter ignores;
* a game whose relay ran to its closing marker is collected regardless. Its
  boxscore is still wanted even though an earlier attempt gave up, which is the
  same reason the relay outranks the status column.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.constants import FINAL_PBP_MARKER
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.game import Game, GamePlayByPlay
from src.services.game_collection_service import load_game_targets_from_db

GAME_ID = "20241124USVE0"
GAME_DATE = date(2024, 11, 24)


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GamePlayByPlay.__table__, CrawlDeadLetter.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)


@pytest.fixture(autouse=True)
def _use_the_test_session(session_factory, monkeypatch):
    monkeypatch.setattr("src.services.game_collection_service.SessionLocal", session_factory)
    return session_factory


def _game(session_factory, *, game_id: str = GAME_ID, status: str = "COMPLETED", finished_relay: bool = False) -> None:
    with session_factory() as session:
        session.add(Game(game_id=game_id, game_date=GAME_DATE, game_status=status, is_primary=True))
        if finished_relay:
            session.add(GamePlayByPlay(game_id=game_id, inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()


def _letter(
    session_factory,
    *,
    status: str,
    crawler: str = "game_detail",
    target_type: str = "game",
    game_id: str | None = GAME_ID,
    target_id: str | None = GAME_ID,
) -> None:
    with session_factory() as session:
        session.add(
            CrawlDeadLetter(
                dlq_id=f"dlq-{status}-{crawler}-{game_id}-{target_id}",
                original_run_id="run-1",
                crawler=crawler,
                target_type=target_type,
                target_id=target_id,
                game_id=game_id,
                failure_stage="fetch",
                error_code="FETCH_HTTP_ERROR",
                status=status,
            ),
        )
        session.commit()


def _targets() -> list[str]:
    return [target.game_id for target in load_game_targets_from_db(2024, 11)]


class TestASettledLetterExcludesTheGame:
    @pytest.mark.parametrize("status", [DlqStatus.EXHAUSTED.value, DlqStatus.IGNORED.value])
    def test_a_settled_game_is_not_selected(self, session_factory, status) -> None:
        _game(session_factory)
        _letter(session_factory, status=status)

        assert _targets() == []

    def test_a_game_with_no_letter_is_selected(self, session_factory) -> None:
        """The filter has to leave the ordinary case alone."""
        _game(session_factory)

        assert _targets() == [GAME_ID]

    def test_target_id_alone_is_enough_to_match(self, session_factory) -> None:
        """``game_id`` is nullable and not every writer fills it."""
        _game(session_factory)
        _letter(session_factory, status=DlqStatus.EXHAUSTED.value, game_id=None)

        assert _targets() == []

    def test_game_id_alone_is_enough_to_match(self, session_factory) -> None:
        _game(session_factory)
        _letter(session_factory, status=DlqStatus.EXHAUSTED.value, target_id=None)

        assert _targets() == []


class TestTheWayBackIn:
    @pytest.mark.parametrize(
        "status",
        [DlqStatus.PENDING.value, DlqStatus.RETRYING.value, DlqStatus.RESOLVED.value],
    )
    def test_a_letter_that_is_not_settled_does_not_exclude(self, session_factory, status) -> None:
        """``requeue`` returns a letter to ``pending``; that has to be the way back in."""
        _game(session_factory)
        _letter(session_factory, status=status)

        assert _targets() == [GAME_ID]


class TestWhatDoesNotCount:
    def test_another_crawlers_letter_does_not_exclude(self, session_factory) -> None:
        """A relay letter giving up says nothing about the boxscore collection."""
        _game(session_factory)
        _letter(session_factory, status=DlqStatus.EXHAUSTED.value, crawler="relay")

        assert _targets() == [GAME_ID]

    def test_another_target_types_letter_does_not_exclude(self, session_factory) -> None:
        _game(session_factory)
        _letter(session_factory, status=DlqStatus.EXHAUSTED.value, target_type="season")

        assert _targets() == [GAME_ID]

    def test_another_games_letter_does_not_exclude(self, session_factory) -> None:
        """A settled letter is a statement about one game, not about the season."""
        _game(session_factory)
        _letter(
            session_factory,
            status=DlqStatus.EXHAUSTED.value,
            game_id="20241123TWJP0",
            target_id="20241123TWJP0",
        )

        assert _targets() == [GAME_ID]


class TestTheRelayOutranksTheLetter:
    def test_a_finished_relay_is_collected_despite_the_letter(self, session_factory) -> None:
        """Its boxscore is still wanted: the feed saw the game happen."""
        _game(session_factory, finished_relay=True)
        _letter(session_factory, status=DlqStatus.EXHAUSTED.value)

        assert _targets() == [GAME_ID]

    def test_without_the_relay_the_same_game_is_excluded(self, session_factory) -> None:
        """The exception is the relay, not the letter being ignored."""
        _game(session_factory)
        _letter(session_factory, status=DlqStatus.EXHAUSTED.value)

        assert _targets() == []
