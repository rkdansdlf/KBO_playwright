"""Tests for team batting/pitching consistency validation."""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.validators.quality_gate import QualityGate


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE kbo_seasons (
                season_id INTEGER PRIMARY KEY,
                season_year INTEGER NOT NULL,
                league_type_code INTEGER NOT NULL
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE team_season_batting (
                team_id TEXT, season INTEGER, league TEXT,
                games INTEGER, plate_appearances INTEGER, at_bats INTEGER,
                runs INTEGER, hits INTEGER, doubles INTEGER, triples INTEGER,
                home_runs INTEGER, rbi INTEGER, stolen_bases INTEGER,
                caught_stealing INTEGER, walks INTEGER, strikeouts INTEGER
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE team_season_pitching (
                team_id TEXT, season INTEGER, league TEXT,
                games INTEGER, wins INTEGER, losses INTEGER, saves INTEGER,
                holds INTEGER, innings_pitched FLOAT, runs_allowed INTEGER,
                earned_runs INTEGER, hits_allowed INTEGER,
                home_runs_allowed INTEGER, walks_allowed INTEGER,
                strikeouts INTEGER
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_batting (
                player_id INTEGER, season INTEGER, league TEXT,
                team_code TEXT, canonical_team_code TEXT,
                games INTEGER, plate_appearances INTEGER, at_bats INTEGER,
                runs INTEGER, hits INTEGER, doubles INTEGER, triples INTEGER,
                home_runs INTEGER, rbi INTEGER, stolen_bases INTEGER,
                caught_stealing INTEGER, walks INTEGER, strikeouts INTEGER
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE player_season_pitching (
                player_id INTEGER, season INTEGER, league TEXT,
                team_code TEXT, canonical_team_code TEXT,
                games INTEGER, wins INTEGER, losses INTEGER, saves INTEGER,
                holds INTEGER, innings_outs INTEGER, innings_pitched FLOAT,
                runs_allowed INTEGER, earned_runs INTEGER, hits_allowed INTEGER,
                home_runs_allowed INTEGER, walks_allowed INTEGER,
                strikeouts INTEGER
            )
        """)
        )
    return sessionmaker(bind=engine)()


def _insert_regular_season(session):
    session.execute(text("INSERT INTO kbo_seasons (season_id, season_year, league_type_code) VALUES (202501, 2025, 0)"))
    session.commit()


class TestTeamBattingValidation:
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
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('SSG', 2025, 'REGULAR', 2, 9, 6, 3, 3, 1)
                """)
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
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 11, 5, 3, 3, 2)
                """)
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
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits, home_runs)
                    VALUES ('LG', 2025, 'REGULAR', 1, 5, 4, 2, 2, 0),
                           ('KT', 2025, 'REGULAR', 1, 6, 5, 1, 1, 0)
                """)
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
                    VALUES (1, 2025, 'REGULAR', 'OLD', 'NEW',
                            1, 5, 4, 2, 2)
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_batting
                        (team_id, season, league, games, plate_appearances, at_bats,
                         runs, hits)
                    VALUES ('NEW', 2025, 'REGULAR', 1, 5, 4, 2, 2)
                """)
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
                """)
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_batting(2025)

            assert result["ok"] is False
            assert len(result["mismatches"]) == 1
            assert result["mismatches"][0]["issue"] == "No player season batting records for this team"
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
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins, losses,
                         innings_pitched, strikeouts)
                    VALUES ('SSG', 2025, 'REGULAR', 2, 1, 1, 10.0, 8)
                """)
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
                        (player_id, season, league, team_code, canonical_team_code,
                         games, wins, innings_outs, strikeouts)
                    VALUES (1, 2025, 'REGULAR', 'SSG', 'SSG',
                            1, 1, 21, 5)
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins,
                         innings_pitched, strikeouts)
                    VALUES ('SSG', 2025, 'REGULAR', 2, 7, 7.0, 10)
                """)
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
                """)
            )
            session.execute(
                text("""
                    INSERT INTO team_season_pitching
                        (team_id, season, league, games, wins,
                         innings_pitched, strikeouts)
                    VALUES ('SSG', 2025, 'REGULAR', 1, 1, 7.1, 5)
                """)
            )
            session.commit()

            gate = QualityGate(session)
            result = gate.validate_season_team_pitching(2025)

            assert result["ok"] is True
        finally:
            session.close()
