"""The repair voids a result only where its own record says the result is void.

Everything pinned here is a guard. A wrong threshold voids a real called game,
a missing evidence requirement lets a truncated crawl read as a no-game, and a
write that does not commit or a dry run that does commit are both silent
failures. The one production row in this state is ``20260830LGLT0``, so the
blast radius of a wrong rule is one row today -- but the rule is written for
every future no-game, which is why the boundaries are tested rather than
assumed.

The reversal is tested too. A status refresh of the date puts the result back,
and the only reason anyone finds out is that ``game_final_with_too_few_innings``
reports it. A test that the warning travels with the audit rows is what makes
that discoverable from the row itself and not only from this script's docstring.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.repair_no_game_declared import (
    MIN_RESULT_INNINGS,
    main,
    repair_no_games,
)
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import Game, GameInningScore, GamePlayByPlay

#: The declaration as the source actually stores it -- the last event of the
#: game's own play-by-play, crawled text rather than a value this script wrote.
DECLARATION = "3회말 1번타순 3구 후 우천으로 인한 노게임 선언"


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GameInningScore.__table__, GamePlayByPlay.__table__, CorrectionAuditTrail.__table__):
        table.create(engine)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()


def _game(
    session,
    game_id: str = "20260830LGLT0",
    *,
    home: int | None = 0,
    away: int | None = 2,
    status: str = "COMPLETED",
) -> None:
    session.add(
        Game(
            game_id=game_id,
            game_date=date(2026, 8, 30),
            home_team="LT",
            away_team="LG",
            home_score=home,
            away_score=away,
            winning_team="LG",
            winning_score=2,
            game_status=status,
            is_primary=True,
        ),
    )
    session.commit()


def _innings(session, game_id: str, *, away: int, home: int) -> None:
    for side, count in (("away", away), ("home", home)):
        for number in range(1, count + 1):
            session.add(GameInningScore(game_id=game_id, team_side=side, inning=number, runs=0))
    session.commit()


def _pbp(session, game_id: str, *descriptions: str) -> None:
    for description in descriptions:
        session.add(GamePlayByPlay(game_id=game_id, play_description=description))
    session.commit()


def _row(session, game_id: str) -> Game:
    return session.execute(select(Game).where(Game.game_id == game_id)).scalar_one()


def _audits(session, game_id: str) -> list[CorrectionAuditTrail]:
    return list(
        session.execute(
            select(CorrectionAuditTrail)
            .where(CorrectionAuditTrail.game_id == game_id)
            .order_by(CorrectionAuditTrail.field_name),
        ).scalars()
    )


def _innings_kept(session, game_id: str) -> int:
    return session.execute(
        select(func.count()).select_from(GameInningScore).where(GameInningScore.game_id == game_id),
    ).scalar()


class TestGuardedByTheDeclarationsOwnRecord:
    def test_a_dry_run_changes_nothing_at_all(self, session) -> None:
        _game(session)
        _innings(session, "20260830LGLT0", away=3, home=3)
        _pbp(session, "20260830LGLT0", DECLARATION)

        report = repair_no_games(apply=False, session=session)

        game = _row(session, "20260830LGLT0")
        assert report.repaired == ["20260830LGLT0"]
        assert "would repair" in report.render()
        assert game.game_status == "COMPLETED"
        assert (game.home_score, game.away_score, game.winning_team, game.winning_score) == (0, 2, "LG", 2)
        assert _audits(session, "20260830LGLT0") == []

    def test_apply_voids_the_result_and_keeps_every_played_inning(self, session) -> None:
        _game(session)
        _innings(session, "20260830LGLT0", away=3, home=3)
        _pbp(session, "20260830LGLT0", DECLARATION)

        repair_no_games(apply=True, session=session)

        game = _row(session, "20260830LGLT0")
        assert game.game_status == "CANCELLED"
        assert (game.home_score, game.away_score, game.winning_team, game.winning_score) == (None, None, None, None)
        # The three innings that were genuinely played are the evidence for this
        # change; deleting them would remove the basis for the change that
        # removed them.
        assert _innings_kept(session, "20260830LGLT0") == 6

    def test_a_short_game_whose_play_by_play_never_says_no_game_is_left_alone(self, session) -> None:
        """A lost crawl and a called game are identical from the innings.

        The line score cannot separate them, so a rule that voided on length
        alone would silently void every partial crawl. The declaration is the
        only thing that can settle it, and a game without one stays as stored.
        """
        _game(session, "20260830OTHER")
        _innings(session, "20260830OTHER", away=3, home=3)
        _pbp(session, "20260830OTHER", "3회말 1번타순 뜬공 아웃")

        report = repair_no_games(apply=True, session=session)

        game = _row(session, "20260830OTHER")
        assert report.skipped_no_declaration == ["20260830OTHER"]
        assert report.repaired == []
        assert game.game_status == "COMPLETED"
        assert (game.home_score, game.away_score) == (0, 2)
        assert _audits(session, "20260830OTHER") == []


class TestTheFloorIsFiveAndMeansFive:
    @pytest.mark.parametrize(
        ("away", "home", "expected"),
        [
            (1, 1, True),
            (4, 4, True),
            (MIN_RESULT_INNINGS, MIN_RESULT_INNINGS, False),
            (9, 9, False),
        ],
        ids=["one", "four", "floor", "nine"],
    )
    def test_only_a_line_shorter_than_the_minimum_is_a_candidate(self, session, away, home, expected) -> None:
        """Five is the league's minimum, so only below it is a result impossible.

        The floor cases matter more than the short ones: a threshold of nine
        would void the 23 stored games legitimately called between five and
        eight innings, and a threshold of four would miss a four-inning
        no-game. Pinning both edges is what makes the number an argument
        instead of a guess.
        """
        _game(session, "20260830EDGE")
        _innings(session, "20260830EDGE", away=away, home=home)
        _pbp(session, "20260830EDGE", DECLARATION)

        report = repair_no_games(apply=True, session=session)

        assert (report.repaired == ["20260830EDGE"]) is expected

    def test_a_line_missing_one_side_is_not_a_short_game(self, session) -> None:
        """Both sides must exist; one side's absence is a partial crawl."""
        _game(session, "20260830ONE")
        _innings(session, "20260830ONE", away=3, home=0)
        _pbp(session, "20260830ONE", DECLARATION)

        report = repair_no_games(apply=True, session=session)

        assert report.scanned == 0
        assert report.repaired == []
        assert _row(session, "20260830ONE").game_status == "COMPLETED"


class TestItLeavesAnAuditThatExplainsItself:
    def test_every_cleared_column_gets_its_own_row(self, session) -> None:
        _game(session)
        _innings(session, "20260830LGLT0", away=3, home=3)
        _pbp(session, "20260830LGLT0", DECLARATION)

        repair_no_games(apply=True, session=session)

        audits = _audits(session, "20260830LGLT0")
        assert sorted(a.field_name for a in audits) == [
            "away_score",
            "game_status",
            "home_score",
            "winning_score",
            "winning_team",
        ]
        status = next(a for a in audits if a.field_name == "game_status")
        assert (status.raw_value, status.corrected_value) == ("COMPLETED", "CANCELLED")
        assert status.raw_source == "stored_game_status"
        assert status.corrected_source == "repair_no_game_declared"

    def test_the_row_carries_both_the_declaration_and_the_reversal(self, session) -> None:
        """The warning must survive this script being gone.

        A reader of the row months from now has the docstring only if they
        thought to open the script, which they will not. What they have is the
        audit row, so the evidence and the reason the correction can be undone
        both live in ``correction_reason`` rather than only here.
        """
        _game(session)
        _innings(session, "20260830LGLT0", away=3, home=3)
        _pbp(session, "20260830LGLT0", DECLARATION)

        repair_no_games(apply=True, session=session)

        for audit in _audits(session, "20260830LGLT0"):
            assert DECLARATION in audit.correction_reason
            assert "game_final_with_too_few_innings" in audit.correction_reason
            assert "refresh_game_status_for_date" in audit.correction_reason

        status = next(a for a in _audits(session, "20260830LGLT0") if a.field_name == "game_status")
        assert status.extra_metadata["pre_image"]["game_status"] == "COMPLETED"
        assert status.extra_metadata["pre_image"]["away_score"] == "2"
        assert status.extra_metadata["evidence"] == DECLARATION

    def test_a_second_run_is_a_no_op_without_an_already_done_check(self, session) -> None:
        """Idempotency comes from the candidate set, not from a guard.

        Voiding removes the row from ``FINISHED_STATUSES``, so the next scan
        cannot see it. That is worth pinning: if a later change re-included
        cancelled rows, this run would void again and add five more audit rows
        per game, and the duplication would be invisible to the caller.
        """
        _game(session)
        _innings(session, "20260830LGLT0", away=3, home=3)
        _pbp(session, "20260830LGLT0", DECLARATION)

        repair_no_games(apply=True, session=session)
        second = repair_no_games(apply=True, session=session)

        assert second.scanned == 0
        assert second.repaired == []
        assert len(_audits(session, "20260830LGLT0")) == 5
        assert _row(session, "20260830LGLT0").game_status == "CANCELLED"


class TestTheCliSaysWhatItDid:
    def test_a_dry_run_prints_the_evidence_and_the_must_write_note(self, session, monkeypatch, capsys) -> None:
        """The default must stay a dry run, and say so where it cannot be missed.

        ``SessionLocal`` is pointed at this session rather than whatever the
        environment names: a CLI test that opens the real configured database
        tests the network, not the command, and the house rule is that tests
        touch local SQLite only.
        """
        import scripts.maintenance.repair_no_game_declared as cli_module

        _game(session)
        _innings(session, "20260830LGLT0", away=3, home=3)
        _pbp(session, "20260830LGLT0", DECLARATION)
        monkeypatch.setattr(cli_module, "SessionLocal", lambda: session)

        exit_code = main([])
        out = capsys.readouterr().out

        assert exit_code == 0
        assert "DRY RUN" in out
        assert "20260830LGLT0" in out
        assert DECLARATION in out
        assert _row(session, "20260830LGLT0").game_status == "COMPLETED"
        assert _audits(session, "20260830LGLT0") == []
