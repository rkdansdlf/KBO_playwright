"""The healer must report the status the database holds, not the one it hoped for.

``_apply_heal_outcome`` used to announce ``COMPLETED`` for every saved detail and
to record every transition as ``SCHEDULED -> COMPLETED``. Both claims were wrong
whenever the row disagreed:

* ``save_game_detail`` derives the status from the scores and derives ``DRAW`` for
  a tie, so "detail saved" and "COMPLETED" are different statements.
* the stuck finder returns ``UNRESOLVED_MISSING`` games too, so ``SCHEDULED`` was
  not always the value the row held before the attempt.

These tests drive the real save path rather than mocking ``detail_saved``: a
mocked flag says nothing about what the row contains, which is exactly how the
original misreading survived.
"""

from __future__ import annotations

import contextlib
from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.cli.backfill import auto_healer
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import (
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.quarantine import QuarantinedRecord
from src.models.season import KboSeason
from src.models.team import Team
from src.repositories.game_save import save_game_detail
from src.utils.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)

#: ``date + away + home + dh``, matching the ``teams`` codes below. The save rebuilds
#: the id from the team codes (``game_helpers._canonicalize_game_id_for_payload``), so a
#: payload whose id disagrees with its own teams is written under a different id than
#: the one it names -- and the assertions would then read the wrong row.
GAME_ID = "20241015SSLG0"

#: Nine innings per side, because the save path only sums a line that looks like a
#: whole game (``game_save._innings_are_complete``). The scores are the sums of these
#: lines, so the payload cannot disagree with itself either: the same save derives the
#: status from the scores, and a score that contradicts the line score only produces a
#: consistency warning plus a status derived from numbers nobody meant.
WIN_HOME_LINE = [1, 1, 1, 1, 1, 1, 0, 0, 0]  # 6
WIN_AWAY_LINE = [2, 2, 2, 1, 1, 1, 1, 0, 0]  # 10
TIE_HOME_LINE = [1, 1, 1, 1, 1, 1, 0, 0, 0]  # 6
TIE_AWAY_LINE = [2, 2, 1, 1, 0, 0, 0, 0, 0]  # 6


@pytest.fixture
def healer_db():
    """Real tables on one shared in-memory database, with ``SessionLocal`` pointed at it.

    The audit is written through a second session, so a single-session fixture
    would report a row that the healer could never have seen.
    """
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    for table in (
        Game,
        GameIdAlias,
        GameInningScore,
        GameLineup,
        GameMetadata,
        GameSummary,
        GameBattingStat,
        GamePitchingStat,
        PlayerBasic,
        PlayerSeasonBatting,
        KboSeason,
        Team,
        QuarantinedRecord,
        CorrectionAuditTrail,
    ):
        table.__table__.create(engine)

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as seed:
        seed.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        seed.commit()

    @contextlib.contextmanager
    def _scoped():
        session = factory()
        try:
            yield session
            session.commit()
        finally:
            session.close()

    with (
        # ``_read_persisted_status`` and ``_reconcile_and_audit`` both open their own
        # session from the module-level factory.
        patch.object(auto_healer, "SessionLocal", factory),
        # ``update_game_status`` uses a different factory; route it to the same rows.
        patch("src.repositories.game_status.get_db_session", _scoped),
    ):
        yield factory


def _payload(*, home_line: list[int], away_line: list[int], game_id: str = GAME_ID) -> dict:
    return {
        "game_id": game_id,
        # An explicit "completed" on purpose: the tie test proves the scores decide for
        # a past game (``derive_stable_game_status`` -> ``_resolve_scored_status``).
        "game_status": "completed",
        "game_date": "2024-10-15",
        "teams": {
            "away": {"code": "SS", "score": sum(away_line), "line_score": away_line},
            "home": {"code": "LG", "score": sum(home_line), "line_score": home_line},
        },
        "metadata": {"stadium": "Jamsil"},
        "hitters": {"away": [], "home": []},
        "pitchers": {"away": [], "home": []},
    }


def _seed_game(factory, status: str, game_id: str = GAME_ID) -> None:
    with factory() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2024, 10, 15),
                away_team="SS",
                home_team="LG",
                game_status=status,
            )
        )
        session.commit()


def _save(
    factory,
    *,
    home_line: list[int],
    away_line: list[int],
    game_id: str = GAME_ID,
) -> None:
    payload = _payload(home_line=home_line, away_line=away_line, game_id=game_id)
    with factory() as session:
        assert save_game_detail(payload, session=session)
        session.commit()


def _status(factory, game_id: str = GAME_ID) -> str | None:
    with factory() as session:
        return session.execute(select(Game.game_status).where(Game.game_id == game_id)).scalar()


def _audits(factory) -> list[CorrectionAuditTrail]:
    with factory() as session:
        return list(session.execute(select(CorrectionAuditTrail).order_by(CorrectionAuditTrail.id)).scalars())


def _saved_item(game_id: str = GAME_ID):
    from src.services.game_collection_service import GameCollectionItemResult

    item = GameCollectionItemResult(game_id=game_id, game_date="20241015")
    item.detail_saved = True
    item.detail_status = "saved"
    return item


class TestTheSavePathOwnsTheStatus:
    def test_the_save_records_a_tie_as_draw(self, healer_db) -> None:
        """Proof that the healer must not assume COMPLETED: the save says DRAW."""
        _seed_game(healer_db, GAME_STATUS_SCHEDULED)
        _save(healer_db, home_line=TIE_HOME_LINE, away_line=TIE_AWAY_LINE)

        assert _status(healer_db) == GAME_STATUS_DRAW

    def test_the_save_records_a_win_as_completed(self, healer_db) -> None:
        _seed_game(healer_db, GAME_STATUS_SCHEDULED)
        _save(healer_db, home_line=WIN_HOME_LINE, away_line=WIN_AWAY_LINE)

        assert _status(healer_db) == GAME_STATUS_COMPLETED


class TestTheHealerReportsWhatWasPersisted:
    def test_a_tie_is_audited_as_draw_and_never_rewritten_to_a_win(self, healer_db) -> None:
        _seed_game(healer_db, GAME_STATUS_SCHEDULED)
        _save(healer_db, home_line=TIE_HOME_LINE, away_line=TIE_AWAY_LINE)

        outcome = auto_healer._apply_heal_outcome(
            GAME_ID,
            _saved_item(),
            previous_status=GAME_STATUS_SCHEDULED,
        )

        assert outcome == "completed"
        assert _status(healer_db) == GAME_STATUS_DRAW, "the healer overwrote a correct tie"
        audits = _audits(healer_db)
        assert len(audits) == 1
        assert audits[0].raw_value == GAME_STATUS_SCHEDULED
        assert audits[0].corrected_value == GAME_STATUS_DRAW

    def test_an_unresolved_game_is_audited_from_unresolved(self, healer_db) -> None:
        """The stuck finder returns UNRESOLVED games, so SCHEDULED would be a fiction."""
        _seed_game(healer_db, GAME_STATUS_UNRESOLVED)
        _save(healer_db, home_line=WIN_HOME_LINE, away_line=WIN_AWAY_LINE)

        outcome = auto_healer._apply_heal_outcome(
            GAME_ID,
            _saved_item(),
            previous_status=GAME_STATUS_UNRESOLVED,
        )

        assert outcome == "completed"
        audits = _audits(healer_db)
        assert len(audits) == 1
        assert audits[0].raw_value == GAME_STATUS_UNRESOLVED
        assert audits[0].corrected_value == GAME_STATUS_COMPLETED

    def test_the_status_is_read_through_the_alias_the_save_recorded(self, healer_db) -> None:
        """A payload id that disagrees with its teams is written under the rebuilt id.

        The save records the disagreement as a ``GameIdAlias`` row, so the healer's read
        has to canonicalize or it reports the stale original as "not healed".
        """
        original = "20241015LGSS0"
        _seed_game(healer_db, GAME_STATUS_SCHEDULED, original)
        _save(healer_db, home_line=WIN_HOME_LINE, away_line=WIN_AWAY_LINE, game_id=original)

        outcome = auto_healer._apply_heal_outcome(
            original,
            _saved_item(original),
            previous_status=GAME_STATUS_SCHEDULED,
        )

        assert outcome == "completed"
        assert _status(healer_db, GAME_ID) == GAME_STATUS_COMPLETED
        assert _audits(healer_db)[0].corrected_value == GAME_STATUS_COMPLETED

    def test_a_saved_detail_that_left_the_row_scheduled_is_not_a_heal(self, healer_db) -> None:
        """`detail_saved` alone is not evidence; the row is read back before claiming one."""
        _seed_game(healer_db, GAME_STATUS_SCHEDULED)

        outcome = auto_healer._apply_heal_outcome(
            GAME_ID,
            _saved_item(),
            previous_status=GAME_STATUS_SCHEDULED,
        )

        assert outcome == "unresolved"
        assert _status(healer_db) == GAME_STATUS_SCHEDULED
        assert _audits(healer_db) == []

    def test_the_audit_records_an_unknown_previous_status_as_unknown(self, healer_db) -> None:
        """No fabricated SCHEDULED when the caller could not supply the old value."""
        _seed_game(healer_db, GAME_STATUS_SCHEDULED)
        _save(healer_db, home_line=WIN_HOME_LINE, away_line=WIN_AWAY_LINE)

        auto_healer._apply_heal_outcome(GAME_ID, _saved_item())

        audits = _audits(healer_db)
        assert len(audits) == 1
        assert audits[0].raw_value is None
        assert audits[0].corrected_value == GAME_STATUS_COMPLETED


class TestAnUnreadableRowIsNotEvidence:
    def test_the_read_reports_none_when_the_database_cannot_be_read(self) -> None:
        """``None`` is "unknown", and the caller turns that into "not a heal".

        The alternative -- treating an unreadable row as healed because the save
        claimed success -- is how a green log line hides a row that never moved.
        """
        boom = SQLAlchemyError("connection reset by peer")

        with patch.object(auto_healer, "SessionLocal", side_effect=boom):
            assert auto_healer._read_persisted_status(GAME_ID) is None
