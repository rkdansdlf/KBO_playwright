"""A status move leaves a record of who moved it and why.

``update_game_status`` is the only guarded way to move one game's status, and until
this change it moved it silently. Measured 2026-11-01: ``20261027HTSS0`` reads
``COMPLETED`` while its last audit entry is ``SCHEDULED -> CANCELLED``, and three of
the function's six call sites wrote no entry at all. ``auto_healer.py`` (the CLI) is
the one that did, which is why its 8,558 rows dominate the table -- the reader could
see a status change and nothing about where it came from.

The edges are what this file is for:

* a move that does not move writes nothing -- ``updated_at`` would not change either,
  and an entry for a no-op hides the entries that matter;
* a refusal writes nothing -- the guard returned before anything happened;
* the reason travels, and the function's own name is the fallback;
* the row lands in the caller's transaction, so a rollback takes both.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game
from src.repositories.game_status import STATUS_AUDIT_SOURCE, update_game_status
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED

#: The season ``automatic_write_allowed`` compares against. A game dated today keeps
#: the guard out of the way so the tests are about the audit and not about it.
TODAY = date.today()
GAME_ID = "20261102LGLT0"


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, CorrectionAuditTrail.__table__):
        table.create(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    session.add(Game(game_id=GAME_ID, game_date=TODAY, game_status=GAME_STATUS_SCHEDULED, is_primary=True))
    session.commit()
    return session


def _audits(session) -> list[CorrectionAuditTrail]:
    return session.query(CorrectionAuditTrail).filter(CorrectionAuditTrail.game_id == GAME_ID).all()


class TestAMoveIsRecorded:
    def test_it_writes_one_row(self, session) -> None:
        assert update_game_status(GAME_ID, GAME_STATUS_COMPLETED, session=session, reason="test: finished") is True

        rows = _audits(session)
        assert len(rows) == 1
        assert rows[0].field_name == "game_status"
        assert rows[0].raw_value == GAME_STATUS_SCHEDULED
        assert rows[0].corrected_value == GAME_STATUS_COMPLETED

    def test_the_reason_is_the_source_and_the_reason(self, session) -> None:
        update_game_status(GAME_ID, GAME_STATUS_COMPLETED, session=session, reason="auto_healer_service: 점수 없음")

        row = _audits(session)[0]
        assert row.corrected_source == "auto_healer_service: 점수 없음"
        assert row.correction_reason == "auto_healer_service: 점수 없음"

    def test_without_a_reason_it_names_the_function(self, session) -> None:
        """A reader has to be able to tell where an unexplained move came from."""
        update_game_status(GAME_ID, GAME_STATUS_COMPLETED, session=session)

        row = _audits(session)[0]
        assert row.corrected_source == STATUS_AUDIT_SOURCE
        assert row.raw_source == STATUS_AUDIT_SOURCE

    def test_the_pre_image_is_the_stored_status(self, session) -> None:
        """The row says what the status was, not what a document claimed it was."""
        update_game_status(GAME_ID, GAME_STATUS_CANCELLED, session=session)

        assert _audits(session)[0].raw_value == GAME_STATUS_SCHEDULED


class TestTheEdges:
    def test_a_move_that_does_not_move_writes_nothing(self, session) -> None:
        """Otherwise every no-op call adds a row that hides the real ones."""
        update_game_status(GAME_ID, GAME_STATUS_SCHEDULED, session=session, reason="no-op")

        assert _audits(session) == []

    def test_a_refused_write_writes_nothing(self, session) -> None:
        """The guard returns before the row or the audit exists."""
        old_game = session.query(Game).filter(Game.game_id == GAME_ID).one()
        old_game.game_date = date(2015, 10, 16)  # outside the current season
        session.commit()

        assert update_game_status(GAME_ID, GAME_STATUS_COMPLETED, session=session) is False
        assert _audits(session) == []

    def test_a_missing_game_writes_nothing(self, session) -> None:
        assert update_game_status("20990101XXXX0", GAME_STATUS_COMPLETED, session=session) is False
        assert _audits(session) == []

    def test_an_unknown_status_is_not_treated_as_no_change(self, session) -> None:
        """Anything that is not the stored value is a move and is recorded."""
        update_game_status(GAME_ID, "SOME_NEW_STATE", session=session)

        assert len(_audits(session)) == 1


class TestItIsTheCallersTransaction:
    def test_a_rollback_takes_the_audit_with_it(self, session) -> None:
        """An audit row that outlived a rolled-back status write would be a lie."""
        update_game_status(GAME_ID, GAME_STATUS_COMPLETED, session=session, reason="test")
        session.rollback()

        assert _audits(session) == []
        assert session.query(Game).filter(Game.game_id == GAME_ID).one().game_status == GAME_STATUS_SCHEDULED

    def test_a_commit_keeps_both(self, session) -> None:
        update_game_status(GAME_ID, GAME_STATUS_COMPLETED, session=session, reason="test")
        session.commit()
        session.expire_all()

        assert len(_audits(session)) == 1
        assert session.query(Game).filter(Game.game_id == GAME_ID).one().game_status == GAME_STATUS_COMPLETED
