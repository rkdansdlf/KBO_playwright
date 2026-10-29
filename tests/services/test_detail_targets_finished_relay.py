"""A cancelled game must stay collectable when the relay says it was played.

The exclusion added on 2026-10-19 is right in general: a game that was never
played has no boxscore to fetch, and chasing it only fills the dead-letter queue
with failures nobody can resolve. But it reads the status column alone, and the
status column is the one that was wrong for ``20261026HTSS0``. The game was
cancelled *because* its detail never arrived, and this exclusion then guaranteed
it never would: the cancellation removed the game from the very targets whose
collection would have corrected it.

So the relay gets a vote. A feed that reached its closing marker saw the game
end, whatever the status column says.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.constants import FINAL_PBP_MARKER
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.game import Game, GamePlayByPlay
from src.services import game_collection_service
from src.services.game_collection_service import load_game_targets_from_db
from src.utils.game_status import GAME_STATUS_CANCELLED


@pytest.fixture
def session_factory(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    # ``crawl_dead_letters`` is read by the target query since 2026-10-30 (a settled
    # letter keeps a game out of collection), so the schema has to be here even
    # though these tests write no letters: the query names the table either way.
    for table in (Game.__table__, GamePlayByPlay.__table__, CrawlDeadLetter.__table__):
        table.create(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(game_collection_service, "SessionLocal", factory)
    return factory


def _game(session_factory, game_id: str, *, status: str) -> None:
    with session_factory() as session:
        session.add(Game(game_id=game_id, game_date=date(2026, 10, 26), game_status=status))
        session.commit()


def _relay(session_factory, game_id: str, *, finished: bool) -> None:
    with session_factory() as session:
        session.add(
            GamePlayByPlay(
                game_id=game_id,
                inning=9 if finished else 3,
                play_description=FINAL_PBP_MARKER if finished else "박정우 : 삼진 아웃",
            ),
        )
        session.commit()


def _targets() -> set[str]:
    return {target.game_id for target in load_game_targets_from_db(2026, 10)}


class TestWhichGamesTheDetailCrawlerChases:
    def test_a_cancelled_game_is_not_a_target(self, session_factory) -> None:
        """The rule this test exists to keep, for the games it is actually about."""
        _game(session_factory, "20261026SSHT0", status=GAME_STATUS_CANCELLED)

        assert _targets() == set()

    def test_a_cancelled_game_whose_relay_finished_is_a_target(self, session_factory) -> None:
        """10-26's shape: the status was written before the game was played."""
        _game(session_factory, "20261026SSHT0", status=GAME_STATUS_CANCELLED)
        _relay(session_factory, "20261026SSHT0", finished=True)

        assert _targets() == {"20261026SSHT0"}

    def test_play_by_play_without_the_marker_is_not_enough(self, session_factory) -> None:
        """A relay that stopped mid-game says nothing about the game having ended."""
        _game(session_factory, "20261026SSHT0", status=GAME_STATUS_CANCELLED)
        _relay(session_factory, "20261026SSHT0", finished=False)

        assert _targets() == set()

    def test_a_completed_game_is_still_a_target(self, session_factory) -> None:
        """The exclusion was never about these, and the relay vote does not disturb them."""
        _game(session_factory, "20261020HTLG0", status="COMPLETED")

        assert _targets() == {"20261020HTLG0"}
