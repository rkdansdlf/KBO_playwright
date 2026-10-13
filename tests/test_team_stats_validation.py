"""Tests for team batting/pitching consistency validation."""

import re

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateTable

import src.models
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.season import KboSeason
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.validators.quality_gate import QualityGate

#: The tables these tests read. Only these are created.
_TABLES = (
    KboSeason.__table__,
    TeamSeasonBatting.__table__,
    TeamSeasonPitching.__table__,
    PlayerSeasonBatting.__table__,
    PlayerSeasonPitching.__table__,
)

#: SQLite has no `ALTER COLUMN`, and the models declare `source` and `level` as
#: NOT NULL. The inserts below are raw SQL that omits both, so the DDL is
#: emitted from the models with the constraint stripped rather than hand-written.
_NOT_NULL = re.compile(r"\s+NOT NULL")


def _loosened(table) -> str:
    """Return the model's DDL for `table` with NOT NULL removed."""
    ddl = str(CreateTable(table).compile(dialect=create_engine("sqlite:///:memory:").dialect))
    return _NOT_NULL.sub("", ddl)


def _make_session():
    """Return a session over a schema derived from the ORM metadata.

    The columns come from the models rather than from DDL written out here. A
    hand-written schema has to be extended by hand whenever a column is added,
    and the omission stays silent until a query selects that column: `e9716962`
    added `extra_stats` to the season-team reads and all 17 tests in this file
    failed with `no such column` instead of one test reporting the drift.

    NOT NULL is dropped because the inserts below are raw SQL that omits
    `source` and `level`. Deriving the DDL keeps the column list honest while
    preserving the loose schema these tests were written against.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        for table in _TABLES:
            conn.execute(text(_loosened(table)))
    return sessionmaker(bind=engine)()


def _insert_regular_season(session):
    session.execute(text("INSERT INTO kbo_seasons (season_id, season_year, league_type_code) VALUES (202501, 2025, 0)"))
    session.commit()


class TestTeamBattingValidation:
    def test_data_source_alias_uses_source_precedence(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(text("ALTER TABLE player_season_batting RENAME COLUMN source TO data_source"))
            session.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, league, data_source, team_code, canonical_team_code,
                         plate_appearances, at_bats, runs, hits, home_runs)
                    VALUES (1, 2025, 'REGULAR', 'CRAWLER', 'SSG', 'SSG', 5, 3, 2, 2, 1),
                           (1, 2025, 'REGULAR', 'AGGREGATED', 'SSG', 'SSG', 99, 90, 20, 30, 9)
                    """,
                ),
            )
            session.execute(
                text(
                    """
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 5, 3, 2, 2, 1)
                    """,
                ),
            )
            session.commit()

            result = QualityGate(session).validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["player_source"] == "PER_KEY"
        finally:
            session.close()

    def test_prefers_crawler_source_and_excludes_unavailable_fields(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, source, team_code, canonical_team_code,
                         plate_appearances, at_bats, runs, hits, home_runs,
                         stolen_bases, caught_stealing)
                    VALUES (1, 2025, 'REGULAR', 'CRAWLER', 'SSG', 'SSG', 5, 3, 2, 2, 1, NULL, NULL),
                           (1, 2025, 'REGULAR', 'AGGREGATED', 'SSG', 'SSG', 99, 90, 20, 30, 9, 12, 4)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs, stolen_bases, caught_stealing)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 5, 3, 2, 2, 1, 12, 4)
                """),
            )
            session.commit()

            result = QualityGate(session).validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["player_source"] == "PER_KEY"
            assert "stolen_bases" in result["unavailable_fields"]
            assert "caught_stealing" in result["unavailable_fields"]
        finally:
            session.close()

    def test_treats_stolen_base_differences_as_non_blocking(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, source, team_code, canonical_team_code,
                         plate_appearances, at_bats, runs, hits, stolen_bases, caught_stealing)
                    VALUES (1, 2025, 'REGULAR', 'CRAWLER', 'SSG', 'SSG', 5, 3, 2, 2, 0, 0)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, stolen_bases, caught_stealing)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 5, 3, 2, 2, 12, 10)
                """),
            )
            session.commit()

            result = QualityGate(session).validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["semantics_exempt_diffs"] == [
                "SSG: stolen_bases: team=12 player_sum=0 diff=12",
                "SSG: caught_stealing: team=10 player_sum=0 diff=10",
            ]
        finally:
            session.close()

    def test_matches_when_player_sum_equals_team_record(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, team_code, canonical_team_code,
                         games, plate_appearances, at_bats, runs, hits, home_runs)
                    VALUES (1, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 5, 3, 2, 2, 1),
                           (2, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 4, 3, 1, 1, 0)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('SSG', 2025, 'REGULAR', 2, 9, 6, 3, 3, 1)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 1
            assert result["mismatches"] == []
        finally:
            session.close()

    def test_detects_mismatch(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, team_code, canonical_team_code,
                         games, plate_appearances, at_bats, runs, hits, home_runs)
                    VALUES (1, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 5, 3, 2, 2, 1)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 11, 5, 3, 3, 2)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is False
            assert len(result["mismatches"]) == 1
            m = result["mismatches"][0]
            assert m["team_id"] == "SSG"
            assert m["issue"] == "Team batting stats mismatch with player sum"
            assert len(m["diffs"]) > 0
        finally:
            session.close()

    def test_missing_team_record_skips_gracefully(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 0
            assert result["mismatches"] == []
        finally:
            session.close()

    def test_no_regular_season_returns_error(self):
        session = _make_session()
        try:
            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is False
            assert "No Regular Season IDs" in (result.get("error") or "")
        finally:
            session.close()

    def test_non_regular_league_skips(self):
        session = _make_session()
        try:
            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025, league="POSTSEASON")

            assert result["ok"] is True
            assert result["league"] == "POSTSEASON"
        finally:
            session.close()

    def test_multi_team_all_match(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, team_code, canonical_team_code,
                         games, plate_appearances, at_bats, runs, hits, home_runs)
                    VALUES (1, 2025, 'REGULAR', 'LG', 'LG', 1, 5, 4, 2, 2, 0),
                           (2, 2025, 'REGULAR', 'KT', 'KT', 1, 6, 5, 1, 1, 0)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('LG', 2025, 'REGULAR', 1, 5, 4, 2, 2, 0),
                           ('KT', 2025, 'REGULAR', 1, 6, 5, 1, 1, 0)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 2
            assert result["mismatches"] == []
        finally:
            session.close()

    def test_canonical_team_code_resolution(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, team_code, canonical_team_code,
                         games, plate_appearances, at_bats, runs, hits)
                    VALUES (1, 2025, 'REGULAR', 'SK', 'SSG',
                            1, 5, 4, 2, 2)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 5, 4, 2, 2)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 1
        finally:
            session.close()

    def test_detects_missing_player_records_for_team(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 5, 3, 2, 2, 1)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is False
            assert len(result["mismatches"]) == 1
            assert result["mismatches"][0]["issue"] == "No player season batting records for this team"
        finally:
            session.close()

    def test_ignores_special_event_team_codes(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, team_code, canonical_team_code,
                         games, plate_appearances, at_bats, runs, hits)
                    VALUES (1, 2025, 'REGULAR', 'EA', 'EA', 1, 5, 4, 2, 2),
                           (2, 2025, 'REGULAR', 'WE', 'WE', 1, 5, 4, 2, 2)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats, runs, hits)
                    VALUES ('EA', 2025, 'REGULAR', 1, 5, 4, 2, 2),
                           ('WE', 2025, 'REGULAR', 1, 5, 4, 2, 2)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 0
            assert result["mismatches"] == []
        finally:
            session.close()

    def test_includes_rows_with_special_raw_code_when_canonical_team_is_standard(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_batting
                        (player_id, season, league, team_code, canonical_team_code,
                         games, plate_appearances, at_bats, runs, hits)
                    VALUES (1, 2025, 'REGULAR', 'LG', 'LG', 1, 5, 4, 2, 2),
                           (2, 2025, 'REGULAR', 'WE', 'LG', 1, 7, 6, 1, 3)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats, runs, hits)
                    VALUES ('LG', 2025, 'REGULAR', 2, 12, 10, 3, 5)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 1
            assert result["mismatches"] == []
        finally:
            session.close()


class TestTeamPitchingValidation:
    def test_matches_when_player_sum_equals_team_record(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_pitching
                        (player_id, season, league, team_code, canonical_team_code,
                         games, wins, losses, innings_outs, strikeouts)
                    VALUES (1, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 1, 0, 21, 5),
                           (2, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 0, 1, 9, 3)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins, losses,
                         innings_pitched, innings_outs, strikeouts)
                    VALUES ('SSG', 2025, 'REGULAR', 2, 1, 1, 10.0, 30, 8)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 1
            assert result["mismatches"] == []
        finally:
            session.close()

    def test_detects_mismatch(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_pitching
                        (player_id, season, league, source, team_code, canonical_team_code,
                         games, wins, innings_outs, strikeouts, hits_allowed)
                    VALUES (1, 2025, 'REGULAR', 'official', 'SSG', 'SSG',
                            1, 1, 21, 5, 10)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins,
                         innings_pitched, strikeouts, hits_allowed)
                    VALUES ('SSG', 2025, 'REGULAR', 2, 7, 7.0, 10, 50)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is False
            assert len(result["mismatches"]) == 1
            m = result["mismatches"][0]
            assert m["team_id"] == "SSG"
            assert len(m["diffs"]) > 0
        finally:
            session.close()

    def test_missing_team_record_skips_gracefully(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 0
        finally:
            session.close()

    def test_no_regular_season_returns_error(self):
        session = _make_session()
        try:
            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is False
            assert "No Regular Season IDs" in (result.get("error") or "")
        finally:
            session.close()

    def test_non_regular_league_skips(self):
        session = _make_session()
        try:
            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025, league="POSTSEASON")

            assert result["ok"] is True
            assert result["league"] == "POSTSEASON"
        finally:
            session.close()

    def test_innings_pitched_tolerance(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_pitching
                        (player_id, season, league, team_code, canonical_team_code,
                         games, wins, innings_outs, strikeouts)
                    VALUES (1, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 1, 22, 5)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins,
                         innings_pitched, innings_outs, strikeouts)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 1, 7.1, 22, 5)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is True
        finally:
            session.close()

    def test_ignores_special_event_team_codes(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_pitching
                        (player_id, season, league, team_code, canonical_team_code,
                         games, wins, innings_outs, strikeouts)
                    VALUES (1, 2025, 'REGULAR', 'EA', 'EA', 1, 1, 21, 5),
                           (2, 2025, 'REGULAR', 'WE', 'WE', 1, 1, 21, 5)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins, innings_outs, strikeouts)
                    VALUES ('EA', 2025, 'REGULAR', 1, 1, 21, 5),
                           ('WE', 2025, 'REGULAR', 1, 1, 21, 5)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 0
            assert result["mismatches"] == []
        finally:
            session.close()

    def test_includes_pitching_rows_with_special_raw_code_when_canonical_team_is_standard(self):
        session = _make_session()
        try:
            _insert_regular_season(session)
            session.execute(
                text("""
                    INSERT INTO player_season_pitching
                        (player_id, season, league, team_code, canonical_team_code,
                         games, wins, losses, innings_outs, strikeouts)
                    VALUES (1, 2025, 'REGULAR', 'HH', 'HH', 1, 1, 0, 12, 4),
                           (2, 2025, 'REGULAR', 'EA', 'HH', 1, 0, 1, 9, 3)
                """),
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins, losses, innings_pitched, innings_outs, strikeouts)
                    VALUES ('HH', 2025, 'REGULAR', 2, 1, 1, 7.0, 21, 7)
                """),
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is True
            assert result["checked_players"] == 1
            assert result["mismatches"] == []
        finally:
            session.close()


class TestTheFixtureTracksTheModels:
    """The schema is derived from the models, so these assert the derivation.

    Without this the file goes back to hand-written DDL quietly, and the next
    added column surfaces as a wall of `no such column` failures whose only
    visible cause is a commit that did not touch this file at all.
    """

    def test_every_model_column_is_present_in_the_created_schema(self):
        """Each table offers every column its model declares.

        `e9716962` added `extra_stats` to the season-team reads. The fixture
        still lacked it, and 17 unrelated assertions failed on the missing
        column rather than on the behaviour they were written to check.
        """
        session = _make_session()
        try:
            inspector = inspect(session.get_bind())
            for table in _TABLES:
                created = {column["name"] for column in inspector.get_columns(table.name)}
                declared = {column.name for column in table.columns}
                assert declared <= created, f"{table.name} is missing {sorted(declared - created)}"
        finally:
            session.close()

    def test_the_schema_stays_loose_where_the_inserts_depend_on_it(self):
        """NOT NULL stays dropped, because the raw INSERTs omit those columns.

        The models mark `source` and `level` NOT NULL. Reintroducing the
        constraint would fail the inserts for a reason that has nothing to do
        with what these tests verify.
        """
        ddl = _loosened(PlayerSeasonBatting.__table__)
        assert "NOT NULL" not in ddl
        assert "source" in ddl
        assert "level" in ddl
