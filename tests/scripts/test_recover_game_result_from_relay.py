"""Recovering a finished game's result from the relay that recorded it.

The relay is the one source that kept working for ``20261026HTSS0``: it carried
the game to its closing marker and its at-bats carry the running score, ending at
KIA 5 : 삼성 1. Every other collection failed -- eight ``game_detail`` attempts
between 18:00 and 20:00, none after the game ended -- so nothing else on disk
states the result.

The script is contracted here on the two things that make it safe to run against
production: it reads the result from a row the feed wrote rather than inferring
one, and it writes nothing that is not both justified and recoverable.
"""

from __future__ import annotations

import csv
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.constants import FINAL_PBP_MARKER
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.utils.game_state import RELAY_ACTIVE_STATES
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_COMPLETED

TODAY = date(2026, 10, 29)
GAME_ID = "20261026HTSS0"
GAME_DATE = date(2026, 10, 26)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GameEvent.__table__, GamePlayByPlay.__table__, CorrectionAuditTrail.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)()


@pytest.fixture(autouse=True)
def _backup_in_a_temp_dir(tmp_path, monkeypatch):
    """Keep the pre-image backup out of the repository's data directory."""
    from scripts.maintenance import recover_game_result_from_relay as module

    monkeypatch.setattr(module, "_backup_path", lambda today: str(tmp_path / "backup.csv"))
    return tmp_path / "backup.csv"


def _game(
    session,
    *,
    game_id: str = GAME_ID,
    status: str = GAME_STATUS_CANCELLED,
    home: int | None = None,
    away: int | None = None,
    lifecycle: str | None = "result_pending_stabilization",
    primary: bool = True,
    game_date: date = GAME_DATE,
) -> Game:
    game = Game(
        game_id=game_id,
        game_date=game_date,
        game_status=status,
        home_score=home,
        away_score=away,
        game_lifecycle_state=lifecycle,
        is_primary=primary,
    )
    session.add(game)
    session.commit()
    return game


def _relay(session, *, game_id: str = GAME_ID, finished: bool = True, scores: list[tuple[int, int, int]] | None = None):
    """Write the relay rows: play-by-play, and at-bats carrying the running score."""
    session.add(
        GamePlayByPlay(
            game_id=game_id,
            inning=9 if finished else 3,
            play_description=FINAL_PBP_MARKER if finished else "박정우 : 삼진 아웃",
        ),
    )
    for seq, away, home in scores if scores is not None else [(1, 0, 0), (77, 5, 1)]:
        session.add(
            GameEvent(
                game_id=game_id,
                event_seq=seq,
                inning=1,
                inning_half="top",
                description="박정우 : 삼진 아웃",
                away_score=away,
                home_score=home,
            ),
        )
    session.commit()


def _recover(session, *, apply: bool):
    from scripts.maintenance.recover_game_result_from_relay import recover

    return recover(apply=apply, session=session, today=TODAY)


def _audit(session, game_id: str = GAME_ID) -> dict[str, CorrectionAuditTrail]:
    rows = session.query(CorrectionAuditTrail).filter(CorrectionAuditTrail.game_id == game_id).all()
    return {row.field_name: row for row in rows}


class TestReadingTheResultFromTheRelay:
    def test_the_last_at_bat_states_the_final_score(self, session) -> None:
        """The score columns are the running score, so the last row is the result."""
        _game(session)
        _relay(session)

        assert _recover(session, apply=False) == [GAME_ID]

    def test_a_relay_without_the_marker_is_not_a_finished_game(self, session) -> None:
        _game(session)
        _relay(session, finished=False)

        assert _recover(session, apply=False) == []

    def test_a_relay_without_any_score_row_still_recovers_the_status(self, session) -> None:
        """The closing marker and the score are separate claims; only one is missing."""
        _game(session)
        _relay(session, scores=[])

        assert _recover(session, apply=False) == [GAME_ID]

        game = session.query(Game).filter(Game.game_id == GAME_ID).one()
        assert game.away_score is None
        assert game.home_score is None

    def test_a_score_that_never_moved_is_not_written(self, session) -> None:
        """``20151016NXOB0`` repeats 0-0 across 145 at-bats and still has a winning pitcher.

        Writing that pair would swap one false statement (a cancelled game) for a
        worse one (a 0-0 draw the same feed contradicts), so the status is recovered
        and the score is left alone.
        """
        _game(session)
        _relay(session, scores=[(1, 0, 0), (77, 0, 0)])

        assert _recover(session, apply=True) == [GAME_ID]

        game = session.query(Game).filter(Game.game_id == GAME_ID).one()
        assert game.away_score is None
        assert game.home_score is None
        assert game.game_status == GAME_STATUS_COMPLETED

    def test_the_audit_says_the_score_was_unread_rather_than_read(self, session) -> None:
        """A reason that claims a score would be the wrong record of the same run."""
        from scripts.maintenance.recover_game_result_from_relay import REASON_STATUS_WITHOUT_SCORE

        _game(session)
        _relay(session, scores=[(1, 0, 0), (77, 0, 0)])

        _recover(session, apply=True)

        audit = _audit(session)
        assert set(audit) == {"game_status", "game_lifecycle_state"}
        assert audit["game_status"].correction_reason == REASON_STATUS_WITHOUT_SCORE

    def test_a_retired_duplicate_is_not_touched(self, session) -> None:
        _game(session, primary=False)
        _relay(session)

        assert _recover(session, apply=False) == []


class TestWritingTheRecovery:
    def test_a_dry_run_writes_nothing(self, session) -> None:
        _game(session)
        _relay(session)

        _recover(session, apply=False)

        game = session.query(Game).filter(Game.game_id == GAME_ID).one()
        assert game.game_status == GAME_STATUS_CANCELLED
        assert game.home_score is None
        assert game.away_score is None
        assert _audit(session) == {}

    def test_applying_stores_the_score_the_status_and_the_lifecycle(self, session) -> None:
        _game(session)
        _relay(session)

        assert _recover(session, apply=True) == [GAME_ID]

        game = session.query(Game).filter(Game.game_id == GAME_ID).one()
        assert (game.away_score, game.home_score) == (5, 1)
        assert game.game_status == GAME_STATUS_COMPLETED
        assert game.game_lifecycle_state == "final"

    def test_every_changed_field_is_audited(self, session) -> None:
        _game(session)
        _relay(session)

        _recover(session, apply=True)

        audit = _audit(session)
        assert set(audit) == {"away_score", "home_score", "game_status", "game_lifecycle_state"}
        assert audit["game_status"].raw_value == GAME_STATUS_CANCELLED
        assert audit["game_status"].corrected_value == GAME_STATUS_COMPLETED
        assert audit["away_score"].raw_source == "relay_at_bat_running_score"

    def test_the_backup_holds_the_pre_image_and_not_what_is_written(self, session, _backup_in_a_temp_dir) -> None:
        """A backup of the new value restores nothing, which is why it is written first."""
        _game(session)
        _relay(session)

        _recover(session, apply=True)

        rows = list(csv.DictReader(_backup_in_a_temp_dir.read_text(encoding="utf-8").splitlines()))
        assert len(rows) == 1
        row = rows[0]
        assert row["game_id"] == GAME_ID
        assert row["game_status"] == GAME_STATUS_CANCELLED
        assert row["game_lifecycle_state"] == "result_pending_stabilization"
        # The pre-image, which is empty here -- and the recovered values beside it.
        assert row["away_score"] == ""
        assert row["home_score"] == ""
        assert row["recovered_away_score"] == "5"
        assert row["recovered_home_score"] == "1"

    def test_a_stored_score_is_never_overwritten(self, session) -> None:
        """A game that already has a result is not a candidate at all."""
        _game(session, status=GAME_STATUS_COMPLETED, home=1, away=5)
        _relay(session)

        assert _recover(session, apply=False) == []

    def test_one_stored_half_is_left_for_review(self, session) -> None:
        """Filling the other half would paper over a disagreement between sources."""
        _game(session, home=1)
        _relay(session)

        assert _recover(session, apply=False) == []


class TestTheWritableWindow:
    def test_a_season_outside_the_window_is_reported_and_left_alone(self, session) -> None:
        """The same policy the status refresh obeys, obeyed here too.

        The date is one the committed policy does not list. An approved date would
        pass, which is why the refusal case must not reuse one -- adding an approval
        to ``historical_backfill_dates.yaml`` is what made this test fail once, and
        the failure was correct.
        """
        _game(session, game_id="20150601XXXX0", game_date=date(2015, 6, 1))
        _relay(session, game_id="20150601XXXX0")

        assert _recover(session, apply=False) == []

        game = session.query(Game).filter(Game.game_id == "20150601XXXX0").one()
        assert game.game_status == GAME_STATUS_CANCELLED

    def test_an_approved_date_is_writable(self, session) -> None:
        """The approval is data, so the script has to read the file the reviewer edited."""
        _game(session, game_id="20151016NXOB0", game_date=date(2015, 10, 16))
        _relay(session, game_id="20151016NXOB0")

        assert _recover(session, apply=True) == ["20151016NXOB0"]


class TestTheLifecyclePath:
    @pytest.mark.parametrize("current", sorted(RELAY_ACTIVE_STATES))
    def test_an_active_lifecycle_is_settled(self, current: str) -> None:
        from scripts.maintenance.recover_game_result_from_relay import _lifecycle_path

        assert _lifecycle_path(current)[-1:] == ("final",)

    def test_running_goes_through_the_state_the_contract_allows(self) -> None:
        """``validate_transition`` refuses running -> final; the path respects that."""
        from scripts.maintenance.recover_game_result_from_relay import _lifecycle_path

        assert _lifecycle_path("running") == ("result_pending_stabilization", "final")

    def test_a_null_lifecycle_is_not_given_one(self) -> None:
        """NULL claims nothing, so there is nothing to correct."""
        from scripts.maintenance.recover_game_result_from_relay import _lifecycle_path

        assert _lifecycle_path(None) == ()

    def test_a_finished_lifecycle_is_left_alone(self) -> None:
        from scripts.maintenance.recover_game_result_from_relay import _lifecycle_path

        assert _lifecycle_path("final") == ()

    def test_an_unknown_value_is_not_written_past(self) -> None:
        from scripts.maintenance.recover_game_result_from_relay import _lifecycle_path

        assert _lifecycle_path("cancelled") == ()
