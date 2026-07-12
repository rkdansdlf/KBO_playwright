"""Comprehensive tests for PA formula validation (PA = AB + BB + HBP + SH + SF)."""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.cli.generate_quality_report import get_pa_formula_integrity
from src.validators.quality_gate import QualityGate, run_quality_gate


def _make_session():
    from src.models.base import Base

    # Import all models to populate metadata
    from src.models.game import Game, GameBattingStat, GamePitchingStat
    from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
    from src.models.season import KboSeason
    from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching

    # Monkeypatch to make all columns nullable for legacy manual insert test compatibility, then restore
    original_nullables = {}
    for table in Base.metadata.tables.values():
        for column in table.columns:
            original_nullables[column] = column.nullable
            column.nullable = True

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    for column, val in original_nullables.items():
        column.nullable = val

    return sessionmaker(bind=engine)()


def _insert_regular_season(session, year: int = 2025):
    session.execute(
        text(
            """
            INSERT INTO kbo_seasons (season_id, season_year, league_type_code, league_type_name)
            VALUES (:sid, :year, 0, 'Regular Season')
            """,
        ),
        {"sid": year * 100 + 1, "year": year},
    )
    session.commit()


def test_pa_formula_matches_when_formula_holds():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        # PA=5 = AB(2)+BB(1)+HBP(0)+SH(1)+SF(1)
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, hits, runs, home_runs, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 5, 2, 1, 0, 1, 1, 1, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        assert result["ok"] is True
        assert result["checked_players"] == 1
        assert result["mismatches"] == []
    finally:
        session.close()


def test_pa_formula_detects_violation():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        # PA=10 but AB+BB+HBP+SH+SF = 4+0+0+0+0 = 4
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 10, 4, 0, 0, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        assert result["ok"] is False
        assert len(result["mismatches"]) == 1
        m = result["mismatches"][0]
        assert m["player_id"] == 10
        assert m["issue"] == "PA formula mismatch"
        assert m["expected_pa"] == 4
        assert m["actual_pa"] == 10
        assert m["difference"] == 6
    finally:
        session.close()


def test_pa_formula_null_coalesce():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        # PA=3, AB=3, all others NULL → expected = 3+0+0+0+0 = 3 → match
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 3, 3, NULL, NULL, NULL, NULL, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        assert result["ok"] is True
        assert result["checked_players"] == 1
    finally:
        session.close()


def test_pa_formula_excludes_non_completed():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'SCHEDULED', 202501, '2025-04-01'),
                       ('G2', 'CANCELLED', 202501, '2025-04-02')
                """,
            ),
        )
        # Both games have violations but neither is COMPLETED → should be ignored
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 10, 2, 0, 0, 0, 0, 'HOME', 1),
                       ('G2', 10, 'Player1', 5, 1, 0, 0, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        # No completed games → no players checked
        assert result["ok"] is True
        assert result["checked_players"] == 0
    finally:
        session.close()


def test_pa_formula_no_season_ids():
    session = _make_session()
    try:
        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        assert result["ok"] is False
        assert result["checked_players"] == 0
        assert "No Regular Season IDs" in (result.get("error") or "")
    finally:
        session.close()


def test_pa_formula_multiple_players_mixed():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        # Player 10: PA=5 = 2+1+0+1+1 → OK
        # Player 20: PA=8 ≠ 3+0+0+0+0=3 → violation
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Good', 5, 2, 1, 0, 1, 1, 'HOME', 1),
                       ('G1', 20, 'Bad', 8, 3, 0, 0, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        assert result["ok"] is False
        assert result["checked_players"] == 2
        assert len(result["mismatches"]) == 1
        assert result["mismatches"][0]["player_id"] == 20
    finally:
        session.close()


def test_pa_formula_all_zeros():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        # PA=0, all components 0 or NULL → expected = 0 → OK
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Zero', 0, 0, 0, 0, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)

        assert result["ok"] is True
        assert result["checked_players"] == 1
    finally:
        session.close()


def test_pa_formula_integrity_function():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 5, 2, 1, 0, 1, 1, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        result = get_pa_formula_integrity(session, 2025)

        assert result["ok"] is True
        assert result["violation_count"] == 0
    finally:
        session.close()


def test_pa_formula_integrity_function_detects_violations():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 10, 4, 0, 0, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.commit()

        result = get_pa_formula_integrity(session, 2025)

        assert result["ok"] is False
        assert result["violation_count"] == 1
        assert len(result["violations"]) == 1
        v = result["violations"][0]
        assert v["player_name"] == "Player1"
        assert v["pa"] == 10
        assert v["at_bats"] == 4
    finally:
        session.close()


def test_run_quality_gate_includes_pa_formula():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 5, 2, 1, 0, 1, 1, 'HOME', 1)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, league, team_code, plate_appearances, at_bats, hits, runs, home_runs)
                VALUES (10, 2025, 'REGULAR', 'SS', 5, 2, 1, 0, 0)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO team_season_batting (team_id, season, league, games, plate_appearances, at_bats, runs, hits)
                VALUES ('SS', 2025, 'REGULAR', 1, 5, 2, 0, 1)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_pitching
                    (player_id, season, league, team_code, games)
                VALUES (10, 2025, 'REGULAR', 'SS', 0)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO team_season_pitching (team_id, season, league, games, wins)
                VALUES ('SS', 2025, 'REGULAR', 0, 0)
                """,
            ),
        )
        session.commit()

        result = run_quality_gate(session, 2025)

        assert "pa_formula" in result
        assert result["pa_formula"]["ok"] is True
        assert result["ok"] is True
    finally:
        session.close()


def test_run_quality_gate_fails_on_pa_formula_violation():
    session = _make_session()
    try:
        _insert_regular_season(session)
        session.execute(
            text(
                """
                INSERT INTO game (game_id, game_status, season_id, game_date)
                VALUES ('G1', 'COMPLETED', 202501, '2025-04-01')
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, player_name, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, team_side, appearance_seq)
                VALUES ('G1', 10, 'Player1', 10, 4, 0, 0, 0, 0, 'HOME', 1)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, league, team_code, plate_appearances, at_bats, hits, runs, home_runs)
                VALUES (10, 2025, 'REGULAR', 'SS', 5, 4, 1, 0, 0)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO team_season_batting (team_id, season, league, games, plate_appearances, at_bats, runs, hits)
                VALUES ('SS', 2025, 'REGULAR', 1, 5, 4, 0, 1)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_pitching
                    (player_id, season, league, team_code, games)
                VALUES (10, 2025, 'REGULAR', 'SS', 0)
                """,
            ),
        )
        session.execute(
            text(
                """
                INSERT INTO team_season_pitching (team_id, season, league, games)
                VALUES ('SS', 2025, 'REGULAR', 0)
                """,
            ),
        )
        session.commit()

        result = run_quality_gate(session, 2025)

        assert result["pa_formula"]["ok"] is False
        assert result["ok"] is False
    finally:
        session.close()
