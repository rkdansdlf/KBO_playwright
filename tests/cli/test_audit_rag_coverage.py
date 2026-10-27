"""Contract tests for the RAG season-coverage audit.

`audit_rag_index` compares the index against itself and `rag_identity_census`
compares it against the retired store. Neither asks whether what the season
contains is actually in the index, which is how every postseason game from 2018
to 2026 went missing with both gates green. These tests pin what the coverage
check counts and what it refuses to call a fault.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.cli.rag.audit_rag_coverage import (
    CoverageReport,
    audit_coverage,
    coverage_gaps,
)
from src.models.game import Game
from src.models.rag_chunk import RagChunk
from src.models.season import KboSeason


@pytest.fixture
def session() -> Iterator[Session]:
    """Give each test its own SQLite store so the counts are real."""
    engine = create_engine("sqlite://")
    for table in (Game.__table__, KboSeason.__table__, RagChunk.__table__):
        table.create(engine)
    with sessionmaker(bind=engine)() as session:
        yield session


def _season(session: Session, season_id: int, year: int, code: int = 0) -> KboSeason:
    row = KboSeason(
        season_id=season_id,
        season_year=year,
        league_type_code=code,
        league_type_name=f"code-{code}",
    )
    session.add(row)
    return row


def _game(session: Session, game_id: str, season_id: int, *, status: str = "COMPLETED") -> Game:
    row = Game(game_id=game_id, game_date=date(2026, 4, 1), season_id=season_id, game_status=status, is_primary=True)
    session.add(row)
    return row


def _chunk(session: Session, game_id: str) -> RagChunk:
    row = RagChunk(
        source_table="game",
        source_row_id=game_id,
        content="본문",
        index_status="ACTIVE",
        meta={},
    )
    session.add(row)
    return row


class TestWhatCountsAsACoverageGap:
    """The unit is one game chunk per completed game."""

    def test_a_season_whose_games_are_all_indexed_reports_nothing(self, session: Session) -> None:
        _season(session, 2026, 2026)
        _game(session, "20260401AAAA0", 2026)
        _game(session, "20260402AAAA0", 2026)
        _chunk(session, "20260401AAAA0")
        _chunk(session, "20260402AAAA0")
        session.commit()

        report = audit_coverage(session)

        assert report.seasons == {2026: 2}
        assert report.missing == {}

    def test_a_game_without_a_chunk_is_reported(self, session: Session) -> None:
        _season(session, 2026, 2026)
        _game(session, "20260401AAAA0", 2026)
        _game(session, "20260402AAAA0", 2026)
        _chunk(session, "20260401AAAA0")
        session.commit()

        report = audit_coverage(session)

        assert report.missing == {2026: ("20260402AAAA0",)}

    def test_a_never_indexed_season_reports_every_game(self, session: Session) -> None:
        # An unindexed season and a damaged one look the same in the count and
        # differ in intent; the report keeps them apart by showing the size of
        # the gap against the season, not a verdict.
        _season(session, 2015, 2015)
        _game(session, "20150401AAAA0", 2015)
        _game(session, "20150402AAAA0", 2015)
        session.commit()

        report = audit_coverage(session)

        assert report.seasons == {2015: 2}
        assert len(report.missing[2015]) == 2

    def test_an_unfinished_game_is_not_expected_in_the_index(self, session: Session) -> None:
        # The generator only writes completed games, so demanding a chunk for a
        # scheduled one would manufacture a gap that the build never promised.
        _season(session, 2026, 2026)
        _game(session, "20260401AAAA0", 2026, status="SCHEDULED")
        session.commit()

        report = audit_coverage(session)

        assert report.seasons == {}
        assert report.missing == {}

    def test_a_postseason_game_counts_toward_its_season_year(self, session: Session) -> None:
        # The season filter reads the whole season year, so the check has to
        # look at the same set: a postseason game missing from the index is the
        # exact fault this audit was written for.
        _season(session, 2026, 2026)
        _season(session, 313, 2026, code=4)
        _game(session, "20260401AAAA0", 2026)
        _game(session, "20261025HTSS0", 313)
        _chunk(session, "20260401AAAA0")
        session.commit()

        report = audit_coverage(session)

        assert report.seasons == {2026: 2}
        assert report.missing == {2026: ("20261025HTSS0",)}

    def test_exhibition_play_is_not_a_gap(self, session: Session) -> None:
        # The build excludes exhibition play on purpose (see
        # ``_INDEXED_SEASON_TYPE_CODES``), so counting it here would report the
        # same twenty-something spring-training games as missing forever -- a
        # gate that is always red is one nobody reads.
        _season(session, 2026, 2026)
        _season(session, 202601, 2026, code=1)
        _game(session, "20260312SKHT0", 202601)
        _game(session, "20260401AAAA0", 2026)
        _chunk(session, "20260401AAAA0")
        session.commit()

        report = audit_coverage(session)

        assert report.seasons == {2026: 1}
        assert report.missing == {}

    def test_a_chunk_for_another_source_is_not_a_cover(self, session: Session) -> None:
        # The identity is ``source_table`` plus key, so a row in another table
        # that happens to carry the same key must not stand in for the game
        # chunk. The key is deliberately identical here: with a different one the
        # check would pass even if it ignored the source table entirely.
        _season(session, 2026, 2026)
        _game(session, "20260401AAAA0", 2026)
        session.add(
            RagChunk(
                source_table="game_summary",
                source_row_id="20260401AAAA0",
                content="본문",
                index_status="ACTIVE",
                meta={},
            ),
        )
        session.commit()

        report = audit_coverage(session)

        assert report.missing == {2026: ("20260401AAAA0",)}


class TestTheGateIsScopedToRecentSeasons:
    """A historical backlog is not the same fault as a season that just lost rows."""

    def test_the_gap_list_is_ordered_newest_first(self, session: Session) -> None:
        for year in (2015, 2026):
            _season(session, year, year)
            _game(session, f"{year}0401AAAA0", year)
        session.commit()

        gaps = coverage_gaps(audit_coverage(session))

        assert [gap.year for gap in gaps] == [2026, 2015]

    def test_a_recent_limit_excludes_the_historical_backlog(self, session: Session) -> None:
        for year in (2015, 2025, 2026):
            _season(session, year, year)
            _game(session, f"{year}0401AAAA0", year)
        _chunk(session, "20260401AAAA0")
        session.commit()

        report = audit_coverage(session)
        recent = coverage_gaps(report, since=2025)

        assert [gap.year for gap in recent] == [2025]

    def test_a_fully_covered_recent_season_produces_no_gap(self, session: Session) -> None:
        _season(session, 2026, 2026)
        _game(session, "20260401AAAA0", 2026)
        _chunk(session, "20260401AAAA0")
        session.commit()

        assert coverage_gaps(audit_coverage(session), since=2026) == []


class TestTheReportShape:
    def test_the_summary_names_the_worst_season(self, session: Session) -> None:
        _season(session, 2015, 2015)
        _season(session, 2026, 2026)
        for index in range(3):
            _game(session, f"2015040{index}AAAA0", 2015)
        _game(session, "20260401AAAA0", 2026)
        session.commit()

        report = audit_coverage(session)

        assert isinstance(report, CoverageReport)
        assert report.summary_line().startswith("seasons=2")

    def test_an_empty_index_is_reported_as_a_gap_not_an_error(self, session: Session) -> None:
        _season(session, 2026, 2026)
        _game(session, "20260401AAAA0", 2026)
        session.commit()

        report = audit_coverage(session)

        assert report.seasons == {2026: 1}
        assert report.missing[2026] == ("20260401AAAA0",)
