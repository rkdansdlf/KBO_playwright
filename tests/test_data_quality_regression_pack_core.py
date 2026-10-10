from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from src.cli.data_quality_regression_pack import main as data_quality_regression_pack_main
from src.validators.data_quality_regression_pack import (
    QualityRegressionReport,
    QualityRegressionResult,
    _adapt_limit_clause,
    _adapt_schema_aliases,
    render_regression_report,
    run_regression_pack,
)


def test_adapt_limit_clause_uses_oracle_fetch_first() -> None:
    sql = "SELECT CAST(game_id AS TEXT) FROM game_batting_stats LIMIT 5"

    assert _adapt_limit_clause(sql, "oracle") == (
        "SELECT TO_CHAR(game_id) FROM game_batting_stats\nFETCH FIRST 5 ROWS ONLY"
    )
    assert _adapt_limit_clause(sql, "sqlite") == sql


def test_adapt_schema_aliases_maps_source_to_data_source() -> None:
    sql = "SELECT * FROM player_season_batting WHERE source = 'AGGREGATED'"

    assert "data_source = 'AGGREGATED'" in _adapt_schema_aliases(sql, {"data_source"})
    assert _adapt_schema_aliases(sql, {"source"}) == sql


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
                """,
            ),
        )
        conn.execute(
            text(
                """
                INSERT INTO game_pitching_stats (game_id, player_id, runs_allowed, earned_runs)
                VALUES ('G4', 2001, 2, 3)
                """,
            ),
        )
        conn.execute(
            text(
                """
                INSERT INTO game_lineups (game_id, player_id, player_name)
                VALUES ('G5', NULL, 'Unknown Player')
                """,
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
                """,
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


def test_regression_pack_flags_season_rows_whose_sacrifices_were_never_recorded() -> None:
    """The gap the source filter was hiding, in the shape the real data has.

    ``player_season_batting_pa_formula`` only reads
    ``league = 'REGULAR' AND source = 'AGGREGATED'``, which is 882 of the 19,830
    season rows and reports no violation at all. The rows it never reads are the
    ones worth reading: every ``LEGACY_CRAWLER`` row has both sacrifice columns
    empty while ``plate_appearances`` is populated, so ``PA`` cannot be
    reconciled against ``AB + BB + HBP + SH + SF``. ``PA`` was supplied
    correctly by the source -- the sacrifice columns are simply unfilled, and the
    gate's own ``required_columns`` already names them.

    Counting those rows is what ``scripts/maintenance/audit_pa_formula.py``
    already calls ``FIXABLE_FORMULA``. The two tools read the same table and only
    one of them could see the problem.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, league, source)
                VALUES
                    -- The real shape: PA present, sacrifices never recorded, and
                    -- the row the existing check is scoped to ignore.
                    (4001, 15, 13, 1, 0, NULL, NULL, 'REGULAR', 'LEGACY_CRAWLER'),
                    -- Same gap outside REGULAR; a gate scoped to the regular
                    -- season would miss this one too.
                    (4002, 23, 22, 0, 0, NULL, NULL, 'KOREAN_SERIES', 'CRAWLER')
                """,
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}
    unaccounted = results["player_season_batting_unaccounted_pa"]

    assert unaccounted.status == "fail"
    assert unaccounted.violation_count == 2
    assert unaccounted.sample_ids == ("4001", "4002")

    # The scoped check still passes: these rows are outside its filter, and
    # widening that one would double-report what audit_pa_formula already owns.
    assert results["player_season_batting_pa_formula"].status == "pass"


def test_regression_pack_does_not_flag_a_row_that_reconciles_without_sacrifices() -> None:
    """A player with no sacrifices is not a gap.

    15,986 of the season rows satisfy ``PA = AB + BB + HBP`` with both sacrifice
    columns at zero. Reading "sacrifices are zero" as "sacrifices are missing"
    would fail every one of them and bury the 2,549 rows that are the actual
    finding.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, league, source)
                VALUES
                    -- Reconciles exactly, sacrifices genuinely zero.
                    (5001, 15, 13, 1, 1, 0, 0, 'REGULAR', 'LEGACY_CRAWLER'),
                    -- Reconciles including both sacrifices.
                    (5002, 16, 13, 1, 0, 1, 1, 'REGULAR', 'LEGACY_CRAWLER')
                """,
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}
    unaccounted = results["player_season_batting_unaccounted_pa"]

    assert unaccounted.status == "pass"
    assert unaccounted.violation_count == 0


def test_regression_pack_does_not_flag_a_row_whose_sacrifices_do_not_cover_the_gap() -> None:
    """Recorded sacrifices that still do not reconcile are a different defect.

    ``REGULAR/CRAWLER`` and ``REGULAR/OFFICIAL_ARCHIVE`` hold 23 rows where PA
    misses the formula even though both sacrifice columns are populated. Either
    the PA value is wrong or a sacrifice did not count as one, and neither
    explanation is "the sacrifices were never recorded" -- so this check must not
    claim them. They stay on the list for a fix that can tell the two apart.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, league, source)
                VALUES
                    (6001, 20, 18, 1, 0, 1, 0, 'REGULAR', 'CRAWLER'),
                    (6002, 19, 18, 1, 0, 0, 1, 'REGULAR', 'OFFICIAL_ARCHIVE')
                """,
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}

    assert results["player_season_batting_unaccounted_pa"].violation_count == 0


def test_regression_pack_scopes_the_unaccounted_pa_check_to_the_season() -> None:
    """CI scopes this pack to one season, so the new check has to honour it.

    Without the scope the gate would report every season's backlog on every run
    and the signal would be indistinguishable from the daily one.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text("ALTER TABLE player_season_batting ADD COLUMN season INTEGER"),
        )
        conn.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, plate_appearances, at_bats, walks, hbp,
                     sacrifice_hits, sacrifice_flies, league, source)
                VALUES
                    (7001, 2026, 15, 13, 1, 0, NULL, NULL, 'REGULAR', 'LEGACY_CRAWLER'),
                    (7002, 2000, 43, 30, 9, 2, NULL, NULL, 'REGULAR', 'LEGACY_CRAWLER')
                """,
            ),
        )

        scoped = run_regression_pack(conn, season=2026)
        unscoped = run_regression_pack(conn)

    scoped_result = next(r for r in scoped.results if r.check_id == "player_season_batting_unaccounted_pa")
    unscoped_result = next(r for r in unscoped.results if r.check_id == "player_season_batting_unaccounted_pa")

    assert scoped_result.violation_count == 1
    assert scoped_result.sample_ids == ("7001",)
    assert unscoped_result.violation_count == 2


def test_regression_pack_skips_missing_optional_tables() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        report = run_regression_pack(conn)

    assert report.ok is True
    assert {result.status for result in report.results} == {"skipped"}
    assert all("missing table" in result.message for result in report.results)


def test_regression_pack_allows_valid_high_era_but_flags_missing_innings() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE player_season_pitching (
                    player_id INTEGER,
                    era REAL,
                    innings_pitched REAL,
                    innings_outs INTEGER
                )
                """,
            ),
        )
        conn.execute(
            text(
                """
                INSERT INTO player_season_pitching
                    (player_id, era, innings_pitched, innings_outs)
                VALUES
                    (1, 135.0, 1.0, 3),
                    (2, 40.5, NULL, NULL)
                """,
            ),
        )

        report = run_regression_pack(conn)

    era_result = next(result for result in report.results if result.check_id == "era_range")
    assert era_result.violation_count == 1
    assert era_result.sample_ids == ("2",)


def test_regression_pack_requires_schema_when_requested() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        report = run_regression_pack(conn, require_schema=True)

    assert report.ok is False
    assert {result.status for result in report.results} == {"fail"}


def test_regression_pack_scopes_game_checks_to_target_date() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, hits)
                VALUES
                    ('20260624LGSS0', 1001, 4, 4, 1, 0, 0, 0, 2),
                    ('20260625LGSS0', 1002, 4, 4, 1, 0, 0, 0, 2)
                """,
            ),
        )

        report = run_regression_pack(conn, target_date="20260624")

    results = {result.check_id: result for result in report.results}
    assert results["game_batting_pa_formula"].violation_count == 1
    assert results["game_batting_pa_formula"].sample_ids == ("20260624LGSS0",)


def test_regression_pack_scopes_game_checks_to_target_season() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, player_id, plate_appearances, at_bats, walks, hbp, sacrifice_hits, sacrifice_flies, hits)
                VALUES
                    ('20010405LGSS0', 1001, 4, 4, 1, 0, 0, 0, 2),
                    ('20020405LGSS0', 1002, 4, 4, 1, 0, 0, 0, 2)
                """,
            ),
        )

        report = run_regression_pack(conn, season=2001)

    results = {result.check_id: result for result in report.results}
    assert results["game_batting_pa_formula"].violation_count == 1
    assert results["game_batting_pa_formula"].sample_ids == ("20010405LGSS0",)


def test_data_quality_regression_pack_cli_emits_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
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
                """,
            ),
        )

    exit_code = data_quality_regression_pack_main(
        [
            "--database-url",
            f"sqlite:///{db_path}",
            "--json",
        ],
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["check_count"] >= 4


def test_quality_regression_report_serializes_counts_and_samples() -> None:
    report = QualityRegressionReport(
        results=(
            QualityRegressionResult(
                check_id="ok_check",
                description="passing check",
                status="pass",
                violation_count=0,
                message="ok",
            ),
            QualityRegressionResult(
                check_id="fail_check",
                description="failing check",
                status="fail",
                violation_count=2,
                message="2 violation(s) found",
                sample_ids=("G1", "G2"),
            ),
        ),
    )

    payload = report.to_dict()

    assert payload["ok"] is False
    assert payload["check_count"] == 2
    assert payload["failure_count"] == 1
    assert payload["results"][1]["sample_ids"] == ["G1", "G2"]


def test_render_regression_report_includes_messages_and_samples() -> None:
    report = QualityRegressionReport(
        results=(
            QualityRegressionResult(
                check_id="sample_check",
                description="sample check",
                status="fail",
                violation_count=2,
                message="sample message",
                sample_ids=("A", "B"),
            ),
        ),
    )

    rendered = render_regression_report(report)

    assert "Data quality regression pack: FAIL" in rendered
    assert "Checks: 1" in rendered
    assert "Failures: 1" in rendered
    assert "- sample_check: fail (2 violation(s))" in rendered
    assert "  sample message" in rendered
    assert "  Samples: A, B" in rendered
