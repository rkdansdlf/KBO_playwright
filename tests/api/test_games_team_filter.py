"""Tests for the era-spanning team filter on ``GET /api/games``.

``game.home_team`` is not a stable identifier. Two Lotte rows carry ``OB`` and
two Doosan rows carry ``DB``; two Kiwoom rows carry ``WO`` and ``KH``. Matching
the stored string therefore returned an arbitrary slice of a team's history --
for Kiwoom, 292 of 4,102 rows, so 93% of the record was unreachable by the code
the API's own description suggests.

The fix keys on ``team_franchises`` instead, and the part that matters is what it
must *not* merge: ``SK`` and ``SSG`` are one franchise, ``SS`` is a different one.
A blanket "normalise the string" would fold Samsung Lions into SSG Landers.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.api.routers.games import _team_matches
from src.models.franchise import Franchise
from src.models.game import Game
from src.utils.team_codes import franchise_ids_for_code


@pytest.fixture
def session() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Franchise.__table__.create(engine)
    Game.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as s:
        s.add_all(
            [
                Franchise(id=1, name="삼성 라이온즈", original_code="SS", current_code="SS"),
                Franchise(id=8, name="SSG 랜더스", original_code="SK", current_code="SSG"),
                Franchise(id=11, name="키움 히어로즈", original_code="WO", current_code="KH"),
                Franchise(id=4, name="두산 베어스", original_code="OB", current_code="DB"),
            ],
        )
        s.commit()
    return factory


def _game(game_id: str, home: str, away: str, home_fid: int | None, away_fid: int | None) -> Game:
    return Game(
        game_id=game_id,
        game_date=date(2026, 8, 5),
        home_team=home,
        away_team=away,
        home_franchise_id=home_fid,
        away_franchise_id=away_fid,
    )


def _ids(session: sessionmaker, code: str) -> set[str]:
    with session() as s:
        return {g.game_id for g in s.query(Game).filter(_team_matches(code, s)).all()}


class TestTheCodeResolvesToAWholeFranchise:
    def test_two_eras_of_one_code_resolve_to_one_id(self, session: sessionmaker) -> None:
        with session() as s:
            assert franchise_ids_for_code(s, "SK") == (8,)
            assert franchise_ids_for_code(s, "SSG") == (8,)

    def test_a_code_no_franchise_claims_resolves_to_nothing(self, session: sessionmaker) -> None:
        """The caller's cue to fall back to a literal match, not to return nothing."""
        with session() as s:
            assert franchise_ids_for_code(s, "ZZ") == ()
            assert franchise_ids_for_code(s, "") == ()

    def test_case_and_padding_do_not_change_the_answer(self, session: sessionmaker) -> None:
        with session() as s:
            assert franchise_ids_for_code(s, "  sk ") == (8,)


class TestOneQueryReturnsTheWholeHistory:
    def test_both_eras_of_a_franchise_come_back(self, session: sessionmaker) -> None:
        with session() as s:
            s.add_all(
                [
                    _game("G_OLD", "SK", "LG", 8, 3),
                    _game("G_NEW", "SSG", "LG", 8, 3),
                ],
            )
            s.commit()

        assert _ids(session, "SK") == {"G_OLD", "G_NEW"}
        assert _ids(session, "SSG") == {"G_OLD", "G_NEW"}

    def test_a_franchise_query_reaches_an_away_appearance(self, session: sessionmaker) -> None:
        """The old filter matched both columns; this must not quietly match only home."""
        with session() as s:
            s.add_all([_game("G_AWAY", "LG", "SSG", 3, 8)])
            s.commit()

        assert _ids(session, "SSG") == {"G_AWAY"}

    def test_an_unidentified_row_is_still_reachable_by_its_string(self, session: sessionmaker) -> None:
        """Rows with no franchise id are matched literally -- and only those."""
        with session() as s:
            s.add_all(
                [
                    _game("G_NO_FID", "SSG", "LG", None, 3),
                    _game("G_OTHER_FID", "SSG", "LG", 1, 3),
                ],
            )
            s.commit()

        # `G_OTHER_FID` is identified as Samsung Lions, so a SSG query must not
        # take its string at face value; the unguarded fallback would.
        assert _ids(session, "SSG") == {"G_NO_FID"}


class TestDifferentFranchisesStayDifferent:
    def test_samsung_lions_and_ssg_landers_are_not_one_team(self, session: sessionmaker) -> None:
        """The reason this cannot be a string normaliser.

        SSG Landers inherited the SK slot; Samsung Lions is a different club
        created in 2025. Merging them would hand one club the other's record.
        """
        with session() as s:
            s.add_all(
                [
                    _game("G_SAMSUNG", "SS", "LG", 1, 3),
                    _game("G_SSG", "SSG", "LG", 8, 3),
                    _game("G_H2H", "SS", "SSG", 1, 8),
                ],
            )
            s.commit()

        samsung = _ids(session, "SS")
        ssg = _ids(session, "SSG")

        assert "G_SAMSUNG" in samsung and "G_SAMSUNG" not in ssg
        assert "G_SSG" in ssg and "G_SSG" not in samsung
        # A game between them belongs to both, which is correct and is why the
        # two sets are not disjoint.
        assert "G_H2H" in samsung and "G_H2H" in ssg

    def test_an_unclaimed_code_falls_back_to_the_literal_match(self, session: sessionmaker) -> None:
        """A code the registry does not know still filters, rather than returning nothing."""
        with session() as s:
            s.add_all([_game("G_JP", "US", "JP", None, None)])
            s.commit()

        assert _ids(session, "JP") == {"G_JP"}
        assert _ids(session, "ZZ") == set()
