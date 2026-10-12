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
    _CHECKS,
    BaselineUnreadableError,
    compare_against_baseline,
    compare_unrecoverable,
    render_regression_report,
    run_regression_pack,
    write_baseline,
    write_unrecoverable,
)

#: The three checks that between them account for every season PA violation.
_PA_CHECK_IDS = (
    "player_season_batting_unaccounted_pa",
    "player_season_batting_unreconciled_pa",
    "player_season_batting_missing_pa",
)


def _count_sql_for(check_id: str) -> str:
    return next(check.count_sql for check in _CHECKS if check.check_id == check_id)


def _rows_matching(conn, check_id: str) -> set[str]:
    """Return the full set of player ids a check reports.

    ``run_regression_pack`` exposes a count and five samples, which cannot
    distinguish "these three rows" from "those three rows" and therefore cannot
    show that two checks are not claiming the same one. These assertions are
    about which rows land where, so they need the rows themselves.
    """
    sql = _count_sql_for(check_id).replace("SELECT COUNT(*)", "SELECT player_id")
    return {str(row[0]) for row in conn.execute(text(sql))}


def _season_rows(conn) -> set[str]:
    """Every season row whose PA fails the formula, under COALESCE.

    This is the defect the three checks exist to describe. It is written out here
    rather than imported so the checks are measured against the rule itself: a
    helper that drifted with the checks would let them agree on the wrong thing.
    """
    return {
        str(row[0])
        for row in conn.execute(
            text(
                """
                SELECT player_id FROM player_season_batting
                WHERE COALESCE(plate_appearances, 0) !=
                    COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                    + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                """,
            ),
        )
    }


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


def test_regression_pack_flags_reconciliation_failure_when_a_sacrifice_was_recorded() -> None:
    """The gap between the two checks that exist, in the rows it leaves behind.

    ``player_season_batting_pa_formula`` reads only REGULAR/AGGREGATED rows and
    found none. ``player_season_batting_unaccounted_pa`` reads rows whose
    sacrifices are both zero and found 2,549. Together they covered 2,572 of the
    2,603 violations and reported the pack as clean on the remaining 23.

    Those 23 are the interesting ones: a sacrifice *was* recorded, so the
    arithmetic could be done, and it does not balance. Most are the crawler's
    layout merge having erased the walk counts beside a real sacrifice total --
    ``walks`` NULL next to ``sacrifice_hits`` of 24 -- and a few have a plate
    appearance figure that disagrees with its own columns by more than a stray
    sacrifice would explain.

    A check that only looks at unrecorded sacrifices cannot see any of them,
    because the very thing that makes them legible is the sacrifice column.
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
                    -- The crawler merge erased the walk counts; the sacrifices survived.
                    (8001, 581, 487, NULL, NULL, 24, 2, 'REGULAR', 'CRAWLER'),
                    -- Recorded sacrifices, but PA does not reach their sum.
                    (8002, 18, 15, 1, 1, 2, 0, 'REGULAR', 'CRAWLER'),
                    -- No sacrifice at all, yet the columns outrun PA. The 2001
                    -- season row this mirrors carries an HBP of 400 against a PA
                    -- of 490, so the columns were filled from the wrong reading.
                    (8003, 490, 400, 125, 400, 0, 0, 'REGULAR', 'CRAWLER')
                """,
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}
    unreconciled = results["player_season_batting_unreconciled_pa"]

    assert unreconciled.status == "fail"
    assert unreconciled.violation_count == 3
    assert unreconciled.sample_ids == ("8001", "8002", "8003")

    # Neither of the two existing checks may claim these rows, or the new one
    # would be reporting something already covered.
    assert results["player_season_batting_pa_formula"].status == "pass"
    assert results["player_season_batting_unaccounted_pa"].violation_count == 0


def test_regression_pack_leaves_a_balanced_row_with_a_sacrifice_to_the_checks_that_have_it() -> None:
    """Recording a sacrifice does not by itself make a row suspicious.

    15,986 season rows balance as ``PA = AB + BB + HBP`` with no sacrifices, and
    far more balance while carrying some. Reading "sacrifices are recorded" as
    "check this row" would turn every completed season into a failure and bury
    the rows that genuinely do not add up.
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
                    (9001, 30, 26, 2, 1, 1, 0, 'REGULAR', 'CRAWLER'),
                    (9002, 40, 35, 2, 1, 2, 0, 'REGULAR', 'CRAWLER'),
                    (9003, 50, 44, 3, 1, 1, 1, 'REGULAR', 'CRAWLER')
                """,
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}

    assert results["player_season_batting_unreconciled_pa"].violation_count == 0


def test_regression_pack_flags_a_season_row_that_records_batting_but_no_plate_appearances() -> None:
    """A missing PA is not the same defect as an unreconciled one.

    Seven 2026 ROLLUP rows carry at-bats, walks and HBP but no
    ``plate_appearances``. ``COALESCE`` folds that NULL to zero, so the formula
    check reads it as 0 against a non-zero sum and the unaccounted check cannot
    see it at all -- there is nothing to compare against. Both report a
    violation, neither says why, and the row reads as a number that was simply
    never filled in.

    The other reconciliation check excludes a NULL PA on purpose so this stays
    the only place that reports it.
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
                    -- At-bats recorded, PA left empty: 0 out of 47, not a real season.
                    (7001, NULL, 47, 0, 0, 0, 0, 'REGULAR', 'ROLLUP'),
                    -- Nothing recorded at all: an empty row is not a defect.
                    (7002, NULL, 0, 0, 0, 0, 0, 'REGULAR', 'ROLLUP')
                """,
            ),
        )

        report = run_regression_pack(conn)

    results = {result.check_id: result for result in report.results}
    missing = results["player_season_batting_missing_pa"]

    assert missing.status == "fail"
    assert missing.violation_count == 1
    assert missing.sample_ids == ("7001",)
    assert results["player_season_batting_unreconciled_pa"].violation_count == 0


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


class TestComparingAgainstABaseline:
    """A count cannot tell a fixed backlog from a fresh break.

    2,603 violations today and 2,603 tomorrow reads as "no change" whether a
    player was repaired or a different one was damaged, and the pack reports
    ``ok=False`` either way -- so the operator cannot tell a quiet backlog from a
    bleeding one. This is why the whole season PA series is set, not summed:
    what matters is which rows moved.

    The backlog behind these rows is not repairable from here. The source is
    blocked by robots.txt and the game-level rows these seasons would be
    re-derived from are absent, so the rows stay. What can be stopped is the next
    one, and only a per-row comparison notices that.
    """

    def _baseline_file(self, tmp_path: Path, body: dict) -> Path:
        tmp_path.mkdir(parents=True, exist_ok=True)
        path = tmp_path / "pa_baseline.json"
        path.write_text(json.dumps(body, ensure_ascii=False))
        return path

    def test_a_row_the_baseline_does_not_have_is_reported_as_new(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (5001, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                    """,
                ),
            )
            baseline = self._baseline_file(
                Path(self._tmpdir.name),
                {"version": 1, "checks": {"player_season_batting_unaccounted_pa": {"player_ids": []}}},
            )
            verdict = compare_against_baseline(conn, baseline)

        assert verdict.new_violations == ("5001",)
        assert verdict.baseline_violations == ()
        assert verdict.is_new_damage is True

    def test_a_backlog_that_stays_the_size_is_not_new_damage(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            for pid in (5001, 5002, 5003):
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                        """,
                    ),
                    {"pid": pid},
                )
            baseline = self._baseline_file(
                Path(self._tmpdir.name),
                {
                    "version": 1,
                    "checks": {
                        "player_season_batting_unaccounted_pa": {"player_ids": ["5001", "5002", "5003"]},
                    },
                },
            )
            verdict = compare_against_baseline(conn, baseline)

        assert verdict.new_violations == ()
        assert verdict.baseline_violations == ("5001", "5002", "5003")
        assert verdict.is_new_damage is False

    def test_a_repair_and_a_break_in_one_run_are_told_apart(self) -> None:
        """The case a count cannot express at all.

        Three rows before, three rows after, and ``ok=False`` throughout -- but
        one was repaired and one is new. Reporting only the total would show no
        change and the new damage would ship.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            for pid in (5002, 5009):
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                        """,
                    ),
                    {"pid": pid},
                )
            baseline = self._baseline_file(
                Path(self._tmpdir.name),
                {
                    "version": 1,
                    "checks": {
                        "player_season_batting_unaccounted_pa": {"player_ids": ["5001", "5002", "5003"]},
                    },
                },
            )
            verdict = compare_against_baseline(conn, baseline)

        assert verdict.new_violations == ("5009",), "the new damage was not named"
        assert verdict.resolved_violations == ("5001", "5003"), "the repairs were not named"

    def test_a_violation_that_moved_between_checks_is_not_called_new(self) -> None:
        """The rows can legitimately change bucket without changing hands.

        ``unreconciled_pa`` and ``unaccounted_pa`` turn on whether a sacrifice was
        recorded, so a row whose sacrifices get backfilled moves out of the
        unaccounted bucket while still failing the formula. It is the same
        defect on the same player and must not raise new damage.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (5001, 581, 487, 63, 4, 24, 2, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            baseline = self._baseline_file(
                Path(self._tmpdir.name),
                {
                    "version": 1,
                    "checks": {
                        "player_season_batting_unaccounted_pa": {"player_ids": ["5001"]},
                        "player_season_batting_unreconciled_pa": {"player_ids": []},
                    },
                },
            )
            verdict = compare_against_baseline(conn, baseline)

        assert verdict.new_violations == ()
        assert verdict.is_new_damage is False

    def test_a_baseline_naming_a_check_that_no_longer_runs_is_reported(self) -> None:
        """A retired check cannot silently retire its rows.

        Without this, removing a check would read as "its violations are fixed"
        and the verdict would claim the backlog shrank while nobody looked at
        those players any more.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            baseline = self._baseline_file(
                Path(self._tmpdir.name),
                {"version": 1, "checks": {"player_season_batting_retired_check": {"player_ids": ["7001"]}}},
            )
            verdict = compare_against_baseline(conn, baseline)

        assert "player_season_batting_retired_check" in verdict.unknown_checks

    def test_an_unreadable_baseline_is_an_error_rather_than_an_all_clear(self) -> None:
        """A missing file must not read as "everything is new" or "nothing is".

        Both readings are wrong in the direction that hides a break: an absent
        baseline compared as empty reports the whole backlog as new damage,
        while a corrupt one compared as empty in the other direction would report
        nothing. The caller has to be told rather than handed a verdict.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            missing = Path(self._tmpdir.name) / "does_not_exist.json"
            corrupt = self._baseline_file(Path(self._tmpdir.name) / "bad", {"version": 1})
            corrupt.parent.mkdir(parents=True, exist_ok=True)
            corrupt.write_text("{not json")

        with pytest.raises(BaselineUnreadableError):
            compare_against_baseline(engine.connect(), missing)
        with pytest.raises(BaselineUnreadableError):
            compare_against_baseline(engine.connect(), corrupt)

    def test_the_baseline_records_the_rows_so_it_can_be_reused(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            for pid in (5001, 5002):
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                        """,
                    ),
                    {"pid": pid},
                )
            path = Path(self._tmpdir.name) / "written.json"
            written = write_baseline(conn, path)

        assert written["version"] == 1
        assert sorted(written["checks"]["player_season_batting_unaccounted_pa"]["player_ids"]) == ["5001", "5002"]
        assert json.loads(path.read_text()) == written

    def test_a_baseline_recorded_over_all_seasons_cannot_settle_a_scoped_run(self) -> None:
        """A scoped run must not read a whole-history baseline as mass repair.

        452 of the 760 recorded players fail in more than one season, so a
        baseline written without a scope holds 780 entries while ``--year 2026``
        sees 46. Reading the narrower run against the wider baseline would report
        714 players as repaired on every single run -- a verdict that is wrong,
        looks like progress, and would mask new damage arriving in a year that
        has just been backfilled.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(text("ALTER TABLE player_season_batting ADD COLUMN season INTEGER"))
            for pid, season in ((5001, 2026), (5002, 2025), (5003, 2024)):
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, season, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, :season, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                        """,
                    ),
                    {"pid": pid, "season": season},
                )
            whole = self._baseline_file(
                Path(self._tmpdir.name) / "whole",
                {
                    "version": 1,
                    "checks": {"player_season_batting_unaccounted_pa": {"player_ids": ["5001", "5002", "5003"]}},
                },
            )
            scoped = self._baseline_file(
                Path(self._tmpdir.name) / "scoped",
                {
                    "version": 1,
                    "scope": {"season": 2026},
                    "checks": {"player_season_batting_unaccounted_pa": {"player_ids": ["5001"]}},
                },
            )

            mismatch = compare_against_baseline(conn, whole, season=2026)
            matched = compare_against_baseline(conn, scoped, season=2026)

        assert mismatch.is_scope_mismatch is True, (
            "a whole-history baseline was compared against a single season without "
            "saying so, so the run would claim 2 players were repaired"
        )
        assert matched.is_scope_mismatch is False
        assert matched.new_violations == ()
        assert matched.resolved_violations == ()

    @pytest.fixture(autouse=True)
    def _tmpdir(self, tmp_path: Path):
        self._tmpdir = tmp_path


class TestRecordingRowsThatCannotBeRepaired:
    """A backlog nobody can fix still has to be distinguishable from a new break.

    Seventeen rows lost their walk and HBP counts to the crawler's per-layout
    merge. The values are not recoverable from here: the page they came from is
    blocked by robots.txt and the game-level rows these seasons would be
    re-derived from are absent, so `player_id` 51417's 2024 line has no
    `game_batting_stats` row behind it at all. Recomputing the missing columns
    from the PA identity would invent baseball statistics, which is the one
    outcome worse than a known gap.

    So they stay as they are and are recorded, because the only question that
    matters about a permanent backlog is whether it is still the same one.
    """

    @pytest.fixture(autouse=True)
    def _tmpdir(self, tmp_path: Path):
        self._tmpdir = tmp_path

    def _engine(self) -> object:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(text("ALTER TABLE player_season_batting ADD COLUMN season INTEGER"))
        return engine

    def test_a_row_missing_a_walk_count_is_recorded_with_its_reason(self) -> None:
        engine = self._engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (51417, 2024, 205, 183, NULL, NULL, 4, 3, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            body = write_unrecoverable(conn, Path(self._tmpdir.name) / "unrecoverable.json")

        entries = body["rows"]
        assert len(entries) == 1
        assert entries[0]["player_id"] == "51417"
        assert entries[0]["season"] == 2024
        assert entries[0]["reason"] == "SOURCE_UNAVAILABLE"
        assert "robots" in entries[0]["detail"].lower() or "re-collect" in entries[0]["detail"].lower()

    def test_a_row_that_is_only_null_written_is_not_recorded(self) -> None:
        """A NULL where a zero belongs costs nothing and should not be labelled lost.

        Of the rows the crawler merge touched, most reconcile once the absent
        column is read as zero. Recording those would describe a defect that
        changes no number anyone queries.
        """
        engine = self._engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                        (1, 17, 16, NULL, NULL, 0, 0, 'REGULAR', 'CRAWLER'),
                        (2, 205, 183, NULL, NULL, 4, 3, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            body = write_unrecoverable(conn, Path(self._tmpdir.name) / "u.json")

        assert [entry["player_id"] for entry in body["rows"]] == ["2"]

    def test_a_row_with_its_count_present_is_never_recorded(self) -> None:
        engine = self._engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (51302, 5, 5, 0, 0, 0, 0, 'REGULAR', 'ROLLUP')
                    """,
                ),
            )
            body = write_unrecoverable(conn, Path(self._tmpdir.name) / "u.json")

        assert body["rows"] == []

    def test_a_row_whose_absent_column_reads_as_zero_is_not_lost_counts(self) -> None:
        """A NULL beside a recorded sacrifice can still be a plain zero.

        The two shapes are indistinguishable from the missing column alone: a
        player who walked zero times has a NULL in the same place a lost count
        does. Only the formula separates them -- if folding the absent column to
        zero makes PA reconcile, nothing is missing and nothing was lost.

        This is the case a "walks IS NULL" test would wrongly claim. It carries a
        sacrifice, so it passes every condition the unfixable query has except
        the one that decides whether a value is actually gone.
        """
        engine = self._engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                        -- walks/hbp absent but PA reconciles once read as zero
                        (1, 187, 183, NULL, NULL, 3, 1, 'REGULAR', 'CRAWLER'),
                        -- walks/hbp absent and PA still short: a count is gone
                        (2, 205, 183, NULL, NULL, 3, 1, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            body = write_unrecoverable(conn, Path(self._tmpdir.name) / "u.json")

        assert [entry["player_id"] for entry in body["rows"]] == ["2"], (
            "a zero that reconciles was recorded as a lost count"
        )

    def test_the_record_distinguishes_a_growing_backlog_from_a_steady_one(self) -> None:
        engine = self._engine()
        with engine.begin() as conn:
            for pid in (51417, 72154):
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, 205, 183, NULL, NULL, 4, 3, 'REGULAR', 'CRAWLER')
                        """,
                    ),
                    {"pid": pid},
                )
            path = Path(self._tmpdir.name) / "u.json"
            first = write_unrecoverable(conn, path)
            steady = compare_unrecoverable(conn, path)

            # One row recovers while another is lost, so the total holds still.
            # A count-based comparison sees no change and reports nothing.
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (78413, 34, 28, NULL, NULL, 1, 1, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            conn.execute(
                text(
                    """
                    UPDATE player_season_batting SET walks = 0, hbp = 0
                    WHERE player_id = 51417
                    """,
                ),
            )
            grown = compare_unrecoverable(conn, path)

        assert len(first["rows"]) == 2
        assert steady.newly_unrecoverable == ()
        assert steady.still_unrecoverable == ("51417", "72154")
        assert grown.newly_unrecoverable == ("78413",), (
            "a newly lost row was not named; the total was unchanged, which is exactly the case a count cannot express"
        )
        assert grown.still_unrecoverable == ("72154",)


class TestTheSeasonPaChecksPartitionTheViolations:
    """Adding up to the right number is not the same as dividing correctly.

    Reporting 2,549 + 23 + 31 = 2,603 against a measured 2,603 is consistent with
    a partition and also with one check reporting a row twice while another
    misses a different one. Only the rows themselves settle it, and these
    assertions exist because the sum matched while nothing held the partition in
    place -- a future edit that tightened one condition could have moved rows
    between the buckets and kept the total intact.

    The rows below are the shapes that matter rather than the rows that happen to
    be in the database: the three buckets, a healthy row in each, and the empty
    row that must not be reported at all.
    """

    def _load(self, rows: list[tuple]) -> object:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                        -- unaccounted: PA exceeds the columns, no sacrifice recorded
                        (1, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER'),
                        -- unreconciled: a sacrifice was recorded and PA still misses
                        (2, 581, 487, NULL, NULL, 24, 2, 'REGULAR', 'CRAWLER'),
                        -- unreconciled: columns outrun PA with no sacrifice at all
                        (3, 490, 400, 125, 400, 0, 0, 'REGULAR', 'CRAWLER'),
                        -- missing_pa: a batting count with no PA to place it in
                        (4, NULL, 47, 0, 0, 0, 0, 'REGULAR', 'ROLLUP'),
                        -- healthy in each direction
                        (5, 16, 13, 1, 1, 1, 0, 'REGULAR', 'CRAWLER'),
                        (6, 10, 10, 0, 0, 0, 0, 'REGULAR', 'CRAWLER'),
                        -- nothing recorded: not a defect
                        (7, NULL, 0, 0, 0, 0, 0, 'REGULAR', 'ROLLUP')
                    """,
                ),
            )
        return engine

    def test_the_three_checks_cover_every_violation_and_nothing_else(self) -> None:
        engine = self._load([])
        with engine.connect() as conn:
            reported = {check_id: _rows_matching(conn, check_id) for check_id in _PA_CHECK_IDS}
            violations = _season_rows(conn)

        union = set().union(*reported.values())
        overlap = (
            (reported[_PA_CHECK_IDS[0]] & reported[_PA_CHECK_IDS[1]])
            | (reported[_PA_CHECK_IDS[0]] & reported[_PA_CHECK_IDS[2]])
            | (reported[_PA_CHECK_IDS[1]] & reported[_PA_CHECK_IDS[2]])
        )

        assert reported["player_season_batting_unaccounted_pa"] == {"1"}
        assert reported["player_season_batting_unreconciled_pa"] == {"2", "3"}
        assert reported["player_season_batting_missing_pa"] == {"4"}
        assert overlap == set(), f"two checks claimed the same row: {sorted(overlap)}"
        assert union == violations, (
            f"the buckets and the formula disagree: only the checks {sorted(union - violations)}, "
            f"only the formula {sorted(violations - union)}"
        )
        assert "5" not in union and "6" not in union, "a reconciled row was reported"
        assert "7" not in union, "a row with nothing in it was reported"

    def test_a_row_with_no_pa_cannot_reach_the_unaccounted_bucket(self) -> None:
        """Why the buckets stay apart even where the formula itself is degenerate.

        ``COALESCE(plate_appearances, 0) > AB + BB + HBP`` folds an absent PA
        into zero, so an absent PA reaches that check only if the sum is
        negative. On any row with non-negative counts -- every row a crawler
        writes -- the sum is at or above zero and zero is never greater than it,
        so a missing PA lands in ``missing_pa`` and nowhere else.

        This is why adding ``plate_appearances IS NOT NULL`` to that check would
        change nothing rather than fix an overlap: the disjointness is
        arithmetic, not a guard, and this pins the arithmetic down. The negative
        row is included to show where the reasoning stops, since a corrupted
        count is the one shape that could carry a missing PA into both buckets.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                        (1, NULL, 0, 0, 0, 0, 0, 'REGULAR', 'CRAWLER'),
                        (2, NULL, 47, 0, 0, 0, 0, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            ordinary = {
                cid: _rows_matching(conn, cid)
                for cid in ("player_season_batting_unaccounted_pa", "player_season_batting_missing_pa")
            }

            # Added afterwards so the reading above is not contaminated by it.
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (3, NULL, -5, 0, 0, 0, 0, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )
            degraded = {cid: _rows_matching(conn, cid) for cid in _PA_CHECK_IDS}

        assert ordinary["player_season_batting_unaccounted_pa"] == set(), (
            "a missing PA reached the unaccounted check, so the buckets are not "
            "disjoint by arithmetic and a guard would be load-bearing"
        )
        # Row 1 is empty, so it belongs to no check at all; row 2 has counts to
        # place and so reaches missing_pa.
        assert ordinary["player_season_batting_missing_pa"] == {"2"}

        # The negative row is the one shape that breaks the argument, and it
        # breaks it visibly rather than silently.
        assert degraded["player_season_batting_unaccounted_pa"] == {"3"}
        assert degraded["player_season_batting_missing_pa"] == {"2"}

    def test_a_row_lands_in_one_bucket_or_another_as_its_shape_changes(self) -> None:
        """Moving a single value across a boundary moves the row, not the count.

        Each pair below is one row differing only in the field the buckets turn
        on. If any two buckets ever claimed the same row, or a row moved out of
        every bucket, the partition would be wrong in the way a total cannot
        detect.
        """
        shapes = [
            # (description, row, the check that must claim it)
            (
                "PA over the columns with a sacrifice recorded",
                (2, 581, 487, 5, 5, 24, 2),
                "player_season_batting_unreconciled_pa",
            ),
            (
                "PA over the columns with none recorded",
                (2, 581, 487, 5, 5, 0, 0),
                "player_season_batting_unaccounted_pa",
            ),
            ("PA under the columns", (2, 100, 200, 5, 5, 0, 0), "player_season_batting_unreconciled_pa"),
            ("PA exactly on the columns", (2, 219, 200, 5, 5, 5, 4), None),
            ("PA absent from an otherwise full row", (2, None, 200, 5, 5, 5, 4), "player_season_batting_missing_pa"),
            ("PA absent from an empty row", (2, None, 0, 0, 0, 0, 0), None),
            (
                "a reported zero still counts toward the sum",
                (2, 207, 200, 0, 0, 5, 0),
                "player_season_batting_unreconciled_pa",
            ),
        ]
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            for label, row, expected in shapes:
                player_id, pa, ab, bb, hbp, sh, sf = row
                conn.execute(text("DELETE FROM player_season_batting"))
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, :pa, :ab, :bb, :hbp, :sh, :sf, 'REGULAR', 'CRAWLER')
                        """,
                    ),
                    {"pid": player_id, "pa": pa, "ab": ab, "bb": bb, "hbp": hbp, "sh": sh, "sf": sf},
                )
                claimed = [cid for cid in _PA_CHECK_IDS if _rows_matching(conn, cid)]
                if expected is None:
                    assert claimed == [], f"a clean row was reported by {claimed}: {label}"
                else:
                    assert claimed == [expected], f"{label} went to {claimed or ['nothing']}, expected {expected}"

    def test_the_partition_survives_a_scale_the_database_actually_reaches(self) -> None:
        """One row per bucket is not evidence the conditions cannot overlap.

        The real table holds 2,603 violations across 19 seasons and every source
        the crawlers write, including rows where PA is absent and rows that are
        otherwise empty. Building that mix and asserting the partition over it
        exercises the conditions against combinations the hand-picked rows above
        do not reach.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(text("ALTER TABLE player_season_batting ADD COLUMN season INTEGER"))
            sources = ("CRAWLER", "LEGACY_CRAWLER", "ROLLUP", "PROFILE", "AGGREGATED", "OFFICIAL_ARCHIVE")
            row = 0
            for season in range(2002, 2026):
                for source in sources:
                    for shape in range(4):
                        row += 1
                        pa, ab, bb, hbp, sh, sf = (
                            (15, 13, 1, 0, 0, 0),
                            (581, 487, None, None, 24, 2),
                            (490, 400, 125, 400, 0, 0),
                            (None, 47, 0, 0, 0, 0),
                        )[shape]
                        conn.execute(
                            text(
                                """
                                INSERT INTO player_season_batting
                                    (player_id, season, plate_appearances, at_bats, walks, hbp,
                                     sacrifice_hits, sacrifice_flies, league, source)
                                VALUES (:pid, :season, :pa, :ab, :bb, :hbp, :sh, :sf, 'REGULAR', :source)
                                """,
                            ),
                            {
                                "pid": row,
                                "season": season,
                                "pa": pa,
                                "ab": ab,
                                "bb": bb,
                                "hbp": hbp,
                                "sh": sh,
                                "sf": sf,
                                "source": source,
                            },
                        )

            reported = {cid: _rows_matching(conn, cid) for cid in _PA_CHECK_IDS}
            violations = _season_rows(conn)

        union = set().union(*reported.values())
        overlap = (
            (reported[_PA_CHECK_IDS[0]] & reported[_PA_CHECK_IDS[1]])
            | (reported[_PA_CHECK_IDS[0]] & reported[_PA_CHECK_IDS[2]])
            | (reported[_PA_CHECK_IDS[1]] & reported[_PA_CHECK_IDS[2]])
        )

        assert overlap == set(), f"two checks claimed {len(overlap)} row(s): {sorted(overlap)[:5]}"
        assert union == violations, (
            f"unreported {len(violations - union)}, unexpected {len(union - violations)} "
            f"out of {len(violations)} violations"
        )


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
