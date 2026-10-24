"""The repair moves only games that are provably over, and says why.

Every case here is a guard. A game without a score has no result to be final
about, a game without a box score is one whose detail never landed, and a game
dated in the future is still being played. Writing any of them to ``final`` would
state an outcome the rows do not support -- which is the same mistake that put
``running`` on 87 finished games in the first place.

``result_pending_stabilization`` is pinned separately. That value claims the
result is still settling, and this repair holds no evidence against the claim,
so leaving it alone is part of the contract rather than an omission.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.repair_running_lifecycle_on_finished_games import (
    INTERMEDIATE_LIFECYCLE,
    STALE_LIFECYCLE,
    TARGET_LIFECYCLE,
    repair,
)
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game, GameBattingStat

TODAY = date(2026, 10, 24)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GameBattingStat.__table__, CorrectionAuditTrail.__table__):
        table.create(engine)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()


def _game(
    session,
    game_id: str = "20261020HTLG0",
    *,
    status: str = "COMPLETED",
    lifecycle: str | None = STALE_LIFECYCLE,
    game_date: date = date(2026, 10, 20),
    home: int | None = 1,
    away: int | None = 13,
    batting: bool = True,
) -> Game:
    game = Game(
        game_id=game_id,
        game_date=game_date,
        game_status=status,
        game_lifecycle_state=lifecycle,
        home_score=home,
        away_score=away,
        home_team="LG",
        away_team="HT",
    )
    session.add(game)
    if batting:
        session.add(
            GameBattingStat(
                game_id=game_id,
                player_id=1,
                appearance_seq=1,
                team_side="home",
                team_code="LG",
                player_name="홍창기",
            ),
        )
    session.commit()
    return game


def _lifecycle(session, game_id: str) -> str | None:
    return session.execute(select(Game.game_lifecycle_state).where(Game.game_id == game_id)).scalar_one()


def _audits(session) -> list[CorrectionAuditTrail]:
    return list(session.execute(select(CorrectionAuditTrail)).scalars().all())


class TestWhatIsSelected:
    def test_a_finished_game_marked_running_is_repaired(self, session) -> None:
        _game(session)

        repaired = repair(apply=True, session=session, today=TODAY)

        assert repaired == ["20261020HTLG0"]
        assert _lifecycle(session, "20261020HTLG0") == TARGET_LIFECYCLE

    def test_a_game_without_scores_is_left_alone(self, session) -> None:
        """No score means no result, so nothing here can be final about it."""
        _game(session, home=None, away=None)

        assert repair(apply=True, session=session, today=TODAY) == []
        assert _lifecycle(session, "20261020HTLG0") == STALE_LIFECYCLE

    def test_a_game_without_a_box_score_is_left_alone(self, session) -> None:
        """The detail the recrawl was fetching never landed; the claim is unverified."""
        _game(session, batting=False)

        assert repair(apply=True, session=session, today=TODAY) == []

    def test_a_future_game_is_left_alone(self, session) -> None:
        _game(session, game_date=date(2026, 11, 1))

        assert repair(apply=True, session=session, today=TODAY) == []

    def test_a_game_that_already_ended_its_lifecycle_is_not_a_target(self, session) -> None:
        _game(session, lifecycle=TARGET_LIFECYCLE)

        assert repair(apply=True, session=session, today=TODAY) == []

    def test_a_settling_result_is_not_overruled(self, session) -> None:
        """The intermediate value is a claim this repair has no evidence against."""
        _game(session, lifecycle=INTERMEDIATE_LIFECYCLE)

        assert repair(apply=True, session=session, today=TODAY) == []
        assert _lifecycle(session, "20261020HTLG0") == INTERMEDIATE_LIFECYCLE

    def test_an_unfinished_game_is_not_a_target(self, session) -> None:
        _game(session, status="LIVE")

        assert repair(apply=True, session=session, today=TODAY) == []

    def test_a_draw_is_repaired_too(self, session) -> None:
        """A draw is a finished game with a result."""
        _game(session, status="DRAW", home=5, away=5)

        assert repair(apply=True, session=session, today=TODAY) == ["20261020HTLG0"]


class TestWhatIsWritten:
    def test_a_dry_run_writes_nothing(self, session) -> None:
        _game(session)

        repaired = repair(apply=False, session=session, today=TODAY)

        assert repaired == ["20261020HTLG0"]
        assert _lifecycle(session, "20261020HTLG0") == STALE_LIFECYCLE
        assert _audits(session) == []

    def test_an_applied_repair_is_committed_not_only_flushed(self, session) -> None:
        """A repair that only calls ``add`` reports success and writes nothing.

        The first version of this script did exactly that: it logged 87 repaired
        games and left 87 rows untouched, because a session context manager
        closes without committing. Reading through a second session is what makes
        the difference visible -- the writing session would have shown its own
        pending state either way.
        """
        _game(session)

        repair(apply=True, session=session, today=TODAY)

        fresh = sessionmaker(bind=session.get_bind())()
        try:
            assert (
                fresh.execute(
                    select(Game.game_lifecycle_state).where(Game.game_id == "20261020HTLG0"),
                ).scalar_one()
                == TARGET_LIFECYCLE
            )
            assert len(list(fresh.execute(select(CorrectionAuditTrail)).scalars().all())) == 1
        finally:
            fresh.close()

    def test_an_applied_repair_documents_the_transition_it_used(self, session) -> None:
        _game(session)

        repair(apply=True, session=session, today=TODAY)

        audits = _audits(session)
        assert len(audits) == 1
        audit = audits[0]
        assert audit.entity_id == "20261020HTLG0"
        assert audit.field_name == "game_lifecycle_state"
        assert audit.raw_value == STALE_LIFECYCLE
        assert audit.corrected_value == TARGET_LIFECYCLE
        assert audit.corrected_source == "repair_running_lifecycle_on_finished_games"
        assert (
            audit.extra_metadata["transition"] == f"{STALE_LIFECYCLE} -> {INTERMEDIATE_LIFECYCLE} -> {TARGET_LIFECYCLE}"
        )

    def test_the_evidence_travels_with_the_row(self, session) -> None:
        """The next reader finds the justification on the row, not only in a docstring."""
        _game(session, home=1, away=13)

        repair(apply=True, session=session, today=TODAY)

        metadata = _audits(session)[0].extra_metadata
        assert metadata["game_status"] == "COMPLETED"
        assert metadata["home_score"] == 1
        assert metadata["away_score"] == 13
        assert metadata["game_date"] == "2026-10-20"

    def test_every_selected_game_gets_its_own_row(self, session) -> None:
        _game(session, "20261020HTLG0")
        _game(session, "20261021HTLG0", game_date=date(2026, 10, 21))

        repaired = repair(apply=True, session=session, today=TODAY)

        assert sorted(repaired) == ["20261020HTLG0", "20261021HTLG0"]
        assert len(_audits(session)) == 2


class TestTheContractIsCheckedBeforeWriting:
    def test_a_contract_that_no_longer_allows_the_path_refuses_to_write(self, session, monkeypatch) -> None:
        """Writing past the state machine would be a guess, so the repair stops."""
        import scripts.maintenance.repair_running_lifecycle_on_finished_games as module

        _game(session)
        monkeypatch.setattr(module, "validate_transition", lambda current, target: (False, "not allowed"))

        assert repair(apply=True, session=session, today=TODAY) == []
        assert _lifecycle(session, "20261020HTLG0") == STALE_LIFECYCLE
        assert _audits(session) == []
