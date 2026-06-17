from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

from src.validators.data_quality_regression_pack import run_regression_pack

ROOT = Path(__file__).resolve().parents[1]


def _create_quality_tables(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE game_batting_stats (
                game_id TEXT,
                player_id INTEGER,
                plate_appearances INTEGER,
                at_bats INTEGER,
                walks INTEGER,
                hbp INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                hits INTEGER
            )
            """,
        ),
    )
    conn.execute(
        text(
            """
            CREATE TABLE game_pitching_stats (
                game_id TEXT,
                player_id INTEGER,
                runs_allowed INTEGER,
                earned_runs INTEGER
            )
            """,
        ),
    )
    conn.execute(
        text(
            """
            CREATE TABLE game_lineups (
                game_id TEXT,
                player_id INTEGER,
                player_name TEXT
            )
            """,
        ),
    )
    conn.execute(
        text(
            """
            CREATE TABLE player_season_batting (
                player_id INTEGER,
                plate_appearances INTEGER,
                at_bats INTEGER,
                walks INTEGER,
                hbp INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                league TEXT,
                source TEXT
            )
            """,
        ),
    )


def test_regression_pack_reports_core_data_quality_violations() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, hits)
                VALUES
                    ('G1', 1001, 5, 4, 1, 0, 0, 0, 2),
                    ('G2', 1002, 4, 4, 1, 0, 0, 0, 5),
                    ('G3', NULL, 3, 3, 0, 0, 0, 0, 1)
                """
            ),
        )
        conn.execute(
            text(
                """
                INSERT INTO game_pitching_stats (game_id, player_id, runs_allowed, earned_runs)
                VALUES ('G4', 2001, 2, 3)
                """
            ),
        )
        conn.execute(
            text(
                """
                INSERT INTO game_lineups (game_id, player_id, player_name)
                VALUES ('G5', NULL, 'Unknown Player')
                """
            ),
        )
        conn.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, league, source)
                VALUES
                    -- Case 1: REGULAR, AGGREGATED, matches formula (OK)
                    (3001, 5, 4, 1, 0, 0, 0, 'REGULAR', 'AGGREGATED'),
                    -- Case 2: REGULAR, AGGREGATED, violates formula (Violated)
                    (3002, 4, 4, 1, 0, 0, 0, 'REGULAR', 'AGGREGATED'),
                    -- Case 3: FUTURES (non-REGULAR), violates formula but should be ignored (OK)
                    (3003, 4, 4, 1, 0, 0, 0, 'FUTURES', 'AGGREGATED'),
                    -- Case 4: REGULAR, CRAWLER (non-AGGREGATED), violates formula but should be ignored (OK)
                    (3004, 4, 4, 1, 0, 0, 0, 'REGULAR', 'CRAWLER')
                """
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}
    assert report.ok is False
    assert results["game_batting_pa_formula"].status == "fail"
    assert results["game_batting_hits_not_gt_at_bats"].violation_count == 1
    assert results["game_pitching_earned_runs_not_gt_runs_allowed"].status == "fail"
    assert results["game_lineups_null_player_id"].violation_count == 1

    # Assert player_season_batting_pa_formula checks the correct target and reports exactly 1 violation (player_id 3002)
    assert results["player_season_batting_pa_formula"].status == "fail"
    assert results["player_season_batting_pa_formula"].violation_count == 1
    assert results["player_season_batting_pa_formula"].sample_ids == ("3002",)


def test_regression_pack_skips_missing_optional_tables() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        report = run_regression_pack(conn)

    assert report.ok is True
    assert {result.status for result in report.results} == {"skipped"}
    assert all("missing table" in result.message for result in report.results)


def test_data_quality_regression_pack_cli_emits_json(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, hits)
                VALUES ('G1', 1001, 5, 4, 1, 0, 0, 0, 2)
                """
            ),
        )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.cli.data_quality_regression_pack",
            "--database-url",
            f"sqlite:///{db_path}",
            "--json",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["check_count"] >= 4
