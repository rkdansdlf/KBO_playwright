"""The status derivation has to read the relay's evidence, or it re-cancels a played game.

``20261026HTSS0`` was played to the end -- the feed wrote its closing marker
twice, 582 play-by-play rows, and the at-bats carry the final score -- and yet
every re-derivation agreed with the stored CANCELLED. The reason was in the
derivation itself: ``has_any_detail`` was built from inning scores, lineups,
batting and pitching, so a game whose only surviving evidence was the relay had
"no detail". The stored value was then preserved by one branch and re-derived by
the next, because a past game with metadata and no detail is called CANCELLED.

Two things are contracted here. A finished relay is evidence a game was played,
so it belongs in that set; and it is evidence of a *result* only when a score
exists to be final about, so a relay-only game is never given a score it does not
have.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.constants import FINAL_PBP_MARKER
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
)
from src.repositories.game_helpers import DerivedGameStatusInput, _derive_game_status
from src.repositories.game_status import (
    StatusRefreshOptions,
    _has_finished_relay,
    refresh_game_status_for_date,
)
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_COMPLETED, GAME_STATUS_UNRESOLVED

TODAY = date(2026, 10, 29)
GAME_DATE = date(2026, 10, 26)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    for table in (
        Game.__table__,
        GameMetadata.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GamePlayByPlay.__table__,
        CorrectionAuditTrail.__table__,
    ):
        table.create(engine)
    return sessionmaker(bind=engine)()


def _input(**overrides: object) -> DerivedGameStatusInput:
    """Build a past-game input, with the relay flag off unless a test sets it."""
    fields: dict[str, object] = {
        "game_date": GAME_DATE,
        "home_score": None,
        "away_score": None,
        "current_status": GAME_STATUS_CANCELLED,
        "has_metadata": True,
        "has_inning_scores": False,
        "has_lineups": False,
        "has_batting": False,
        "has_pitching": False,
        "today": TODAY,
    }
    fields.update(overrides)
    return DerivedGameStatusInput(**fields)  # type: ignore[arg-type]


class TestTheRelayMarkerIsReadFromTheStoredRows:
    def test_a_closing_marker_is_found(self, session) -> None:
        session.add(GamePlayByPlay(game_id="20261026HTSS0", inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()

        assert _has_finished_relay(session, "20261026HTSS0") is True

    def test_play_by_play_without_the_marker_is_not_a_finished_game(self, session) -> None:
        """A live game's relay reaches the inning it is playing; that is not an ending."""
        session.add(GamePlayByPlay(game_id="20261026HTSS0", inning=5, play_description="박정우 : 삼진 아웃"))
        session.commit()

        assert _has_finished_relay(session, "20261026HTSS0") is False

    def test_another_game_s_marker_does_not_count(self, session) -> None:
        session.add(GamePlayByPlay(game_id="20261027HTSS0", inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()

        assert _has_finished_relay(session, "20261026HTSS0") is False


class TestDerivingAStatusFromAFinishedRelay:
    def test_a_finished_relay_with_a_score_is_a_completed_game(self) -> None:
        """No boxscore row ever landed, so this is the only evidence there is."""
        assert _derive_game_status(_input(home_score=1, away_score=5, has_finished_relay=True)) == (
            GAME_STATUS_COMPLETED
        )

    def test_a_finished_relay_without_a_score_is_not_called_cancelled(self) -> None:
        """The false claim is what gets corrected; a missing score is not invented here."""
        derived = _derive_game_status(_input(has_finished_relay=True))

        assert derived != GAME_STATUS_CANCELLED
        assert derived == GAME_STATUS_UNRESOLVED

    def test_a_game_with_nothing_but_metadata_is_still_cancelled(self) -> None:
        """The heuristic the fix narrows, left exactly as it was for everything else."""
        assert _derive_game_status(_input()) == GAME_STATUS_CANCELLED

    def test_the_flag_defaults_to_off(self) -> None:
        """Every caller that predates this evidence keeps the answer it had."""
        assert _input().has_finished_relay is False
        assert _derive_game_status(_input(home_score=1, away_score=5)) == GAME_STATUS_CANCELLED


class TestTheRefreshKeepsARecoveredGameRecovered:
    def test_a_cancelled_game_with_a_finished_relay_is_not_left_cancelled(self, session) -> None:
        """The step that stops the recovery being undone by the next nightly refresh."""
        session.add(Game(game_id="20261026HTSS0", game_date=GAME_DATE, game_status=GAME_STATUS_CANCELLED))
        session.add(GamePlayByPlay(game_id="20261026HTSS0", inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()

        refresh_game_status_for_date(
            "20261026",
            TODAY,
            session=session,
            options=StatusRefreshOptions(dry_run=False, caller="test"),
        )
        session.commit()

        game = session.query(Game).filter(Game.game_id == "20261026HTSS0").one()
        # No score was stored, so the derivation says "unknown" rather than claiming
        # a result -- but it no longer repeats a cancellation it has evidence against.
        assert game.game_status == GAME_STATUS_UNRESOLVED

    def test_a_recovered_game_with_a_score_stays_completed(self, session) -> None:
        session.add(
            Game(
                game_id="20261026HTSS0",
                game_date=GAME_DATE,
                game_status=GAME_STATUS_COMPLETED,
                home_score=1,
                away_score=5,
            ),
        )
        session.add(GamePlayByPlay(game_id="20261026HTSS0", inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()

        refresh_game_status_for_date(
            "20261026",
            TODAY,
            session=session,
            options=StatusRefreshOptions(dry_run=False, caller="test"),
        )
        session.commit()

        game = session.query(Game).filter(Game.game_id == "20261026HTSS0").one()
        assert game.game_status == GAME_STATUS_COMPLETED

    def test_the_evidence_travels_with_the_change(self, session) -> None:
        """A reader has to be able to disagree with the change, so it names what it read."""
        session.add(Game(game_id="20261026HTSS0", game_date=GAME_DATE, game_status=GAME_STATUS_CANCELLED))
        session.add(GamePlayByPlay(game_id="20261026HTSS0", inning=9, play_description=FINAL_PBP_MARKER))
        session.commit()

        refresh_game_status_for_date(
            "20261026",
            TODAY,
            session=session,
            options=StatusRefreshOptions(dry_run=False, caller="test"),
        )
        session.commit()

        audit = (
            session.query(CorrectionAuditTrail)
            .filter(
                CorrectionAuditTrail.game_id == "20261026HTSS0",
                CorrectionAuditTrail.field_name == "game_status",
            )
            .one()
        )
        assert audit.raw_value == GAME_STATUS_CANCELLED
        assert audit.corrected_value == GAME_STATUS_UNRESOLVED
        assert audit.extra_metadata["has_finished_relay"] is True
