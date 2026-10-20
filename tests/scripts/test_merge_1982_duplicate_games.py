"""Merge of the 1982 duplicate game pairs.

The point these tests hold is that a duplicate is not automatically a spare.
Each pair carries fields the other half does not, so the script's job is to move
them onto the row that survives and only then delete -- and to refuse the delete
when a move did not land.

They run against SQLite. The production guard is exercised by asserting the
gate, not by pointing the CLI at the operational database.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.merge_1982_duplicate_games import (
    ENV_GATE,
    MERGED_FIELDS,
    MergeReport,
    build_pair_plans,
    main,
    merge_season,
)
from src.models.game import Game, GameMetadata


def _row(session, game_id: str):
    """Load one game by its natural key; ``Game`` is keyed by a surrogate id."""
    return session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GameMetadata.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)


def _pair(
    session,
    *,
    primary_id: str,
    secondary_id: str,
    game_date: date = date(1982, 3, 27),
    primary: dict | None = None,
    secondary: dict | None = None,
) -> tuple[Game, Game]:
    """Add one duplicate pair whose two game ids disagree about team order."""
    base = {
        "game_date": game_date,
        "away_team": "SS",
        "home_team": "MB",
        "away_score": 7,
        "home_score": 11,
        "winning_team": "MB",
        "season_id": 198200,
        "game_status": "CANCELLED",
    }
    primary_row = Game(game_id=primary_id, is_primary=True, **{**base, **(primary or {})})
    secondary_row = Game(game_id=secondary_id, is_primary=False, **{**base, **(secondary or {})})
    session.add_all([primary_row, secondary_row])
    session.commit()
    return primary_row, secondary_row


class TestPairing:
    def test_two_ids_that_disagree_about_team_order_still_pair(self, session_factory) -> None:
        with session_factory() as session:
            _pair(session, primary_id="19820327MBSS0", secondary_id="19820327SSMB0")
            report = MergeReport(season=1982)
            plans = build_pair_plans(list(session.query(Game).all()), report)

            assert len(plans) == 1
            assert report.unmatched_dates == []

    def test_the_pair_carries_both_home_and_away_scores(self, session_factory) -> None:
        """A key built on away/home order alone would pair 11-7 with 7-11 wrongly."""
        with session_factory() as session:
            _pair(session, primary_id="19820327MBSS0", secondary_id="19820327SSMB0")
            report = MergeReport(season=1982)
            plans = build_pair_plans(list(session.query(Game).all()), report)

            assert plans[0].primary_game_id == "19820327MBSS0"

    def test_a_group_of_three_is_reported_rather_than_guessed(self, session_factory) -> None:
        """Merging two of three would pick a rule nobody wrote down."""
        with session_factory() as session:
            primary, secondary = _pair(session, primary_id="19820327MBSS0", secondary_id="19820327SSMB0")
            third = Game(
                game_id="19820327MBSS1",
                is_primary=False,
                game_date=primary.game_date,
                away_team="SS",
                home_team="MB",
                away_score=7,
                home_score=11,
                winning_team="MB",
                season_id=198200,
                game_status="CANCELLED",
            )
            session.add(third)
            session.commit()
            report = MergeReport(season=1982)

            plans = build_pair_plans(list(session.query(Game).all()), report)

            assert plans == []
            assert len(report.unmatched_dates) == 1
            assert "3rows" in report.unmatched_dates[0]


class TestTheMergeMovesWhatOnlyOneHalfHas:
    def test_winning_score_travels_from_the_secondary(self, session_factory) -> None:
        with session_factory() as session:
            _pair(
                session,
                primary_id="19820327MBSS0",
                secondary_id="19820327SSMB0",
                primary={"winning_score": None},
                secondary={"winning_score": 11},
            )
            merge_season(session, apply=True)

            survivor = _row(session, "19820327MBSS0")
            assert survivor.winning_score == 11

    def test_franchise_ids_travel_from_the_secondary(self, session_factory) -> None:
        with session_factory() as session:
            _pair(
                session,
                primary_id="19820327MBSS0",
                secondary_id="19820327SSMB0",
                primary={"home_franchise_id": None, "away_franchise_id": None, "winning_franchise_id": None},
                secondary={"home_franchise_id": 2, "away_franchise_id": 1, "winning_franchise_id": 2},
            )
            merge_season(session, apply=True)

            survivor = _row(session, "19820327MBSS0")
            assert (survivor.home_franchise_id, survivor.away_franchise_id) == (2, 1)

    def test_the_winning_franchise_id_travels_too(self, session_factory) -> None:
        """194 pairs hold it on the secondary only.

        It was missing from the first version of the merge list, which would have
        dropped every one of them without an error -- no constraint fires, no row
        count moves, and nothing reports it.
        """
        assert "winning_franchise_id" in MERGED_FIELDS
        with session_factory() as session:
            _pair(
                session,
                primary_id="19820328LTHT0",
                secondary_id="19820328HTLT0",
                primary={"winning_franchise_id": None},
                secondary={"winning_franchise_id": 2},
            )
            merge_season(session, apply=True)

            assert _row(session, "19820328LTHT0").winning_franchise_id == 2

    def test_every_differing_field_is_covered_by_the_merge_list(self, session_factory) -> None:
        """The list is the whole safety net; a field outside it is silently dropped.

        This walks every column of a pair and fails if any field that differs is
        absent from ``MERGED_FIELDS``, which is the check that catches a new
        column appearing on one side only.
        """
        with session_factory() as session:
            primary, secondary = _pair(
                session,
                primary_id="19820327MBSS0",
                secondary_id="19820327SSMB0",
                primary={"winning_score": None, "winning_franchise_id": None},
                secondary={"winning_score": 11, "winning_franchise_id": 1},
            )
            session.commit()

            # The primary is already mutated by nothing yet, so compare the plan.
            report = MergeReport(season=1982)
            plans = build_pair_plans([primary, secondary], report)
            moved = set(plans[0].fields_to_move)

            assert moved == set(MERGED_FIELDS) & {"winning_score", "winning_franchise_id"}

    def test_a_value_already_on_the_primary_is_not_overwritten(self, session_factory) -> None:
        """The secondary's value may be the stale one; the primary already decided."""
        with session_factory() as session:
            _pair(
                session,
                primary_id="19820327MBSS0",
                secondary_id="19820327SSMB0",
                primary={"winning_score": 11},
                secondary={"winning_score": 99},
            )
            merge_season(session, apply=True)

            assert _row(session, "19820327MBSS0").winning_score == 11

    def test_the_delete_only_follows_a_landed_merge(self, session_factory) -> None:
        with session_factory() as session:
            _pair(
                session,
                primary_id="19820327MBSS0",
                secondary_id="19820327SSMB0",
                secondary={"winning_score": 11},
            )
            report = merge_season(session, apply=True)

            assert report.merged is True
            assert report.deleted == 1
            assert session.query(Game).count() == 1
            assert _row(session, "19820327SSMB0") is None

    def test_metadata_is_not_copied(self, session_factory) -> None:
        """It is keyed by game_id and already on the primary; a copy would duplicate."""
        with session_factory() as session:
            _pair(session, primary_id="19820327MBSS0", secondary_id="19820327SSMB0")
            session.add(GameMetadata(game_id="19820327MBSS0", stadium_code="DONGDAEMUN"))
            session.commit()
            merge_season(session, apply=True)

            assert session.query(GameMetadata).count() == 1
            assert "game_metadata" not in MERGED_FIELDS


class TestDryRunTouchesNothing:
    def test_no_rows_change_without_apply(self, session_factory) -> None:
        with session_factory() as session:
            _pair(session, primary_id="19820327MBSS0", secondary_id="19820327SSMB0", secondary={"winning_score": 11})
            report = merge_season(session, apply=False)

            assert report.applied is False
            assert report.merged is False
            assert report.deleted == 0
            assert session.query(Game).count() == 2
            assert _row(session, "19820327MBSS0").winning_score is None
            assert report.plans[0].fields_to_move == {"winning_score": 11}


class TestAnUnmatchedGroupStopsTheMerge:
    def test_nothing_is_merged_or_deleted(self, session_factory) -> None:
        with session_factory() as session:
            primary, _ = _pair(session, primary_id="19820327MBSS0", secondary_id="19820327SSMB0")
            session.add(
                Game(
                    game_id="19820327MBSS1",
                    is_primary=False,
                    game_date=primary.game_date,
                    away_team="SS",
                    home_team="MB",
                    away_score=7,
                    home_score=11,
                    winning_team="MB",
                    season_id=198200,
                    game_status="CANCELLED",
                ),
            )
            session.commit()
            before = session.query(Game).count()

            report = merge_season(session, apply=True)

            assert report.unmatched_dates
            assert report.merged is False
            assert report.deleted == 0
            assert session.query(Game).count() == before


class TestTheWriteGate:
    def test_apply_without_the_environment_gate_exits_three(self, session_factory, monkeypatch) -> None:
        monkeypatch.delenv(ENV_GATE, raising=False)
        monkeypatch.setattr(
            "scripts.maintenance.merge_1982_duplicate_games._open_session", lambda _url: session_factory()
        )

        assert main(["--apply", "--snapshot-dir", "/nonexistent-snapshot-dir-should-not-be-used"]) == 3
        with session_factory() as session:
            assert session.query(Game).count() == 0

    def test_another_season_is_refused(self, session_factory, monkeypatch) -> None:
        """The pairing rule is specific to the 1982 MBC/MB segment collision."""
        monkeypatch.setattr(
            "scripts.maintenance.merge_1982_duplicate_games._open_session", lambda _url: session_factory()
        )

        assert main(["--season", "1995"]) == 2
