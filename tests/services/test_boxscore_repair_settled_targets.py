"""The repair must not re-chase a game whose dead letter is already settled.

The failure log has no memory. A game that failed five times is still in it, so a
later repair run selects it again and spends five more retries on a source that
already answered. One run on 2026-10-28 did exactly that 87 times -- 76 of them
Premier12 games from 2024-11, which carry a result and no body because no
boxscore was ever published for them, which the regression pack already grades as
a source limit rather than a gap.

The dead letter is the memory: ``exhausted`` says the retry budget is spent and
``ignored`` says an operator decided to stop. Re-selecting either is what refilled
``dlq:exhausted`` and reopened its incident. ``requeue`` remains the way back in,
so a game whose source later recovers is not locked out.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.game import Game
from src.services.boxscore_repair_service import gather_repair_candidates

GAME_A = "20241124USVE0"
GAME_B = "20261026HTSS0"


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    for table in (CrawlDeadLetter.__table__, Game.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)()


@pytest.fixture
def failure_log(tmp_path: Path) -> Path:
    """A failure log naming two games, in the format the logger writes."""
    path = tmp_path / "crawl_failures.log"
    path.write_text(
        f"2026-10-28 19:26:59\tgame\t{GAME_A}\thttp://example/\t5\tnaver_record_unavailable\n"
        f"2026-10-28 18:09:56\tgame\t{GAME_B}\thttp://example/\t5\tnaver_record_unavailable\n",
        encoding="utf-8",
    )
    return path


def _letter(
    session,
    *,
    status: str,
    game_id: str | None = GAME_A,
    target_id: str = GAME_A,
) -> None:
    session.add(
        CrawlDeadLetter(
            dlq_id=uuid4().hex,
            original_run_id=uuid4().hex,
            crawler="game_detail",
            target_type="game",
            target_id=target_id,
            game_id=game_id,
            status=status,
            failure_stage="fetch",
            error_code="FETCH_HTTP_ERROR",
            error_message="naver_record_unavailable",
            retry_count=5,
            max_retries=5,
        ),
    )
    session.commit()


def _candidates(session, log: Path) -> set[str]:
    return {c.game_id for c in gather_repair_candidates(session, source="log", log_path=log)}


class TestASettledLetterKeepsTheGameOut:
    def test_an_exhausted_letter_is_not_selected_again(self, session, failure_log) -> None:
        _letter(session, status=DlqStatus.EXHAUSTED.value, game_id=GAME_A, target_id=GAME_A)

        assert _candidates(session, failure_log) == {GAME_B}

    def test_an_ignored_letter_is_not_selected_again(self, session, failure_log) -> None:
        _letter(session, status=DlqStatus.IGNORED.value, game_id=GAME_A, target_id=GAME_A)

        assert _candidates(session, failure_log) == {GAME_B}

    def test_a_pending_letter_leaves_the_game_selectable(self, session, failure_log) -> None:
        """The repair's own normal path: a letter that still has retries left."""
        _letter(session, status=DlqStatus.PENDING.value, game_id=GAME_A, target_id=GAME_A)

        assert _candidates(session, failure_log) == {GAME_A, GAME_B}

    def test_a_letter_that_names_only_the_target_id_still_settles(self, session, failure_log) -> None:
        """The detail crawler's letters carry the game id in either column."""
        _letter(session, status=DlqStatus.EXHAUSTED.value, game_id=None, target_id=GAME_A)

        assert _candidates(session, failure_log) == {GAME_B}

    def test_another_game_s_letter_does_not_settle_this_one(self, session, failure_log) -> None:
        _letter(session, status=DlqStatus.EXHAUSTED.value, game_id="20241110PANL0", target_id="20241110PANL0")

        assert _candidates(session, failure_log) == {GAME_A, GAME_B}


class TestTheDoorsBackIn:
    def test_an_explicitly_requested_game_is_never_filtered(self, session, failure_log) -> None:
        """A person naming the game outranks the queue's memory of it."""
        _letter(session, status=DlqStatus.EXHAUSTED.value, game_id=GAME_A, target_id=GAME_A)

        candidates = gather_repair_candidates(session, source="all", specific_game_id=GAME_A)

        assert [c.game_id for c in candidates] == [GAME_A]
        assert candidates[0].source == "manual"

    def test_a_requeued_letter_is_selectable_again(self, session, failure_log) -> None:
        """``requeue`` returns the letter to pending, which this filter does not match."""
        _letter(session, status=DlqStatus.PENDING.value, game_id=GAME_A, target_id=GAME_A)

        candidates = gather_repair_candidates(session, source="log", log_path=failure_log)

        assert GAME_A in {c.game_id for c in candidates}
        # The candidate carries the date as the formatted string the crawler takes.
        assert next(c for c in candidates if c.game_id == GAME_A).game_date == "2024-11-24"
