from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from src.cli.data_quality_regression_pack import main as data_quality_regression_pack_main
from src.validators.data_quality_regression_pack import (
    QualityRegressionReport,
    QualityRegressionResult,
    _adapt_limit_clause,
    _adapt_schema_aliases,
    _BASELINE_CHECK_IDS,
    _CHECKS,
    _violated_rows,
    BaselineUnreadableError,
    compare_against_baseline,
    compare_unrecoverable,
    render_regression_report,
    run_regression_pack,
    write_baseline,
    write_unrecoverable,
)

#: The four checks that between them account for every season PA violation.
#:
#: ``unaccounted_pa`` is split at 2001 because that is where
#: ``game_batting_stats`` starts, which is why the pair is listed as two: the
#: partition is only real if every row the formula fails lands in exactly one of
#: these, and the season is one of the boundaries it can land on.
_PA_CHECK_IDS = (
    "player_season_batting_unaccounted_pa",
    "player_season_batting_unaccounted_pa_pre2001",
    "player_season_batting_unreconciled_pa",
    "player_season_batting_missing_pa",
)

#: The two halves ``unaccounted_pa`` is split into, named once so a test about
#: the boundary cannot accidentally assert against only one side of it.
_UNACCOUNTED_IDS = (
    "player_season_batting_unaccounted_pa",
    "player_season_batting_unaccounted_pa_pre2001",
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


def _overlap_of(reported: dict[str, set[str]]) -> set[str]:
    """Return the rows that more than one check claimed.

    Written as a loop over the checks rather than as a fixed set of pairwise
    intersections: the previous expression named ``_PA_CHECK_IDS[0]`` through
    ``[2]`` explicitly, so adding a fourth check would have left the new one
    outside the partition assertion without failing anything -- the same shape of
    silent gap this test exists to catch.
    """
    seen: set[str] = set()
    claimed_twice: set[str] = set()
    for rows in reported.values():
        claimed_twice |= rows & seen
        seen |= rows
    return claimed_twice


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
    # ``season`` defaults to a modern year because the season PA checks are split
    # at 2001 and these fixtures are about the arithmetic, not about a year. A
    # test that cares which half a row lands in names the season explicitly
    # instead of relying on this.
    conn.execute(
        text(
            """
            CREATE TABLE player_season_batting (
                player_id INTEGER,
                season INTEGER NOT NULL DEFAULT 2025,
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
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, plate_appearances, at_bats, walks, hbp,
                     sacrifice_hits, sacrifice_flies, league, source)
                VALUES
                    (7001, 2026, 15, 13, 1, 0, NULL, NULL, 'REGULAR', 'LEGACY_CRAWLER'),
                    (7002, 2025, 43, 30, 9, 2, NULL, NULL, 'REGULAR', 'LEGACY_CRAWLER')
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
                self._tmpdir,
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
                self._tmpdir,
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
                self._tmpdir,
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
                self._tmpdir,
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
                self._tmpdir,
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
            missing = self._tmpdir / "does_not_exist.json"
            corrupt = self._baseline_file(self._tmpdir / "bad", {"version": 1})
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
            path = self._tmpdir / "written.json"
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
                self._tmpdir / "whole",
                {
                    "version": 1,
                    "checks": {"player_season_batting_unaccounted_pa": {"player_ids": ["5001", "5002", "5003"]}},
                },
            )
            scoped = self._baseline_file(
                self._tmpdir / "scoped",
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
            body = write_unrecoverable(conn, self._tmpdir / "unrecoverable.json")

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
            body = write_unrecoverable(conn, self._tmpdir / "u.json")

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
            body = write_unrecoverable(conn, self._tmpdir / "u.json")

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
            body = write_unrecoverable(conn, self._tmpdir / "u.json")

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
            path = self._tmpdir / "u.json"
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

    Reporting 2,122 + 427 + 23 + 31 = 2,603 against a measured 2,603 is consistent
    with a partition and also with one check reporting a row twice while another
    misses a different one. Only the rows themselves settle it, and these
    assertions exist because the sum matched while nothing held the partition in
    place -- a future edit that tightened one condition could have moved rows
    between the buckets and kept the total intact.

    The season is one of the boundaries a row can land on, not just a filter:
    splitting ``unaccounted_pa`` at 2001 turns one bucket into two, and a row that
    fell on neither side would leave the backlog understated without changing any
    other count.

    The rows below are the shapes that matter rather than the rows that happen to
    be in the database: the four buckets, a healthy row on each side of the split,
    and the empty row that must not be reported at all.
    """

    def _load(self, rows: list[tuple]) -> object:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                        -- unaccounted from 2001 on: PA exceeds the columns, no sacrifice recorded
                        (1, 2025, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER'),
                        -- unaccounted before 2001: the same shape, on the side of the
                        -- split where no game-level row exists to fill the sacrifices
                        (8, 1995, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER'),
                        -- unreconciled: a sacrifice was recorded and PA still misses
                        (2, 2025, 581, 487, NULL, NULL, 24, 2, 'REGULAR', 'CRAWLER'),
                        -- unreconciled: columns outrun PA with no sacrifice at all
                        (3, 2025, 490, 400, 125, 400, 0, 0, 'REGULAR', 'CRAWLER'),
                        -- missing_pa: a batting count with no PA to place it in
                        (4, 2025, NULL, 47, 0, 0, 0, 0, 'REGULAR', 'ROLLUP'),
                        -- healthy in each direction, one on each side of the split
                        (5, 2025, 16, 13, 1, 1, 1, 0, 'REGULAR', 'CRAWLER'),
                        (6, 1995, 10, 10, 0, 0, 0, 0, 'REGULAR', 'CRAWLER'),
                        -- nothing recorded: not a defect
                        (7, 2025, NULL, 0, 0, 0, 0, 0, 'REGULAR', 'ROLLUP')
                    """,
                ),
            )
        return engine

    def test_the_four_checks_cover_every_violation_and_nothing_else(self) -> None:
        engine = self._load([])
        with engine.connect() as conn:
            reported = {check_id: _rows_matching(conn, check_id) for check_id in _PA_CHECK_IDS}
            violations = _season_rows(conn)

        union = set().union(*reported.values())
        overlap = _overlap_of(reported)

        assert reported["player_season_batting_unaccounted_pa"] == {"1"}
        assert reported["player_season_batting_unaccounted_pa_pre2001"] == {"8"}
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

        The seasons run across the 2001 boundary on purpose: the split is a
        boundary a row can fall on, so the partition has to be asserted with rows
        on both sides rather than only with seasons the split admits.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            sources = ("CRAWLER", "LEGACY_CRAWLER", "ROLLUP", "PROFILE", "AGGREGATED", "OFFICIAL_ARCHIVE")
            row = 0
            for season in range(1995, 2026):
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
        overlap = _overlap_of(reported)

        assert overlap == set(), f"two checks claimed {len(overlap)} row(s): {sorted(overlap)[:5]}"
        assert union == violations, (
            f"unreported {len(violations - union)}, unexpected {len(union - violations)} "
            f"out of {len(violations)} violations"
        )


class TestThePre2001HalfIsReportedWithoutRaisingTheIncident:
    """A gap nothing can repair has to be graded apart from one that can.

    The pre-2001 seasons carry PA with both sacrifice columns empty and no source
    to fill them -- ``game_batting_stats`` holds no pre-2001 row, so a value
    written there would be invented rather than recovered. Counting that absence
    as a failure left the pack red from the day the check was added, and a gate
    that is always red is a gate nobody reads: the 481 rows a backfill or a
    correction can actually reach were reported at the same volume as the 2,122
    that cannot. These tests hold the two grades apart.
    """

    @staticmethod
    def _pack(values: str) -> QualityRegressionReport:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                    """
                    + values,
                ),
            )
        with engine.connect() as conn:
            return run_regression_pack(conn)

    @staticmethod
    def _by_id(report: QualityRegressionReport) -> dict[str, QualityRegressionResult]:
        return {result.check_id: result for result in report.results}

    def test_a_pre2001_row_warns_and_leaves_the_pack_ok(self) -> None:
        report = self._pack("(1, 1995, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')")
        results = self._by_id(report)

        limited = results["player_season_batting_unaccounted_pa_pre2001"]
        assert limited.status == "warn"
        assert limited.violation_count == 1, "the structural backlog is still counted"
        assert results["player_season_batting_unaccounted_pa"].status == "pass"
        assert report.ok is True, "a source limit must not raise the daily incident"
        assert report.failure_count == 0

    def test_a_modern_row_fails_while_the_pre2001_check_stays_quiet(self) -> None:
        report = self._pack("(1, 2025, 15, 13, 1, 0, 0, 0, 'REGULAR', 'CRAWLER')")
        results = self._by_id(report)

        assert results["player_season_batting_unaccounted_pa"].status == "fail"
        assert results["player_season_batting_unaccounted_pa"].violation_count == 1
        assert results["player_season_batting_unaccounted_pa_pre2001"].status == "pass"
        assert report.ok is False, "a repairable backlog still has to be visible"
        assert report.failure_count == 1

    @pytest.mark.parametrize(
        ("season", "claimant"),
        [
            (1982, "player_season_batting_unaccounted_pa_pre2001"),
            (2000, "player_season_batting_unaccounted_pa_pre2001"),
            (2001, "player_season_batting_unaccounted_pa"),
            (2026, "player_season_batting_unaccounted_pa"),
        ],
    )
    def test_the_split_sits_at_2001_where_the_game_rows_start(self, season: int, claimant: str) -> None:
        """2001 is the first season ``game_batting_stats`` covers, so it is the line.

        The two rows beside the boundary are the ones that matter: 2000 is
        unrecoverable and 2001 is not, so a split drawn anywhere else would grade
        one of them wrongly and the count alone would not show it.
        """
        report = self._pack(f"(1, {season}, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')")
        results = self._by_id(report)

        assert results[claimant].violation_count == 1, f"season {season} was not claimed by {claimant}"
        for other in _UNACCOUNTED_IDS:
            if other != claimant:
                assert results[other].violation_count == 0, f"season {season} was claimed by {claimant} and {other}"

    def test_a_schema_without_the_season_column_reports_both_halves_as_unmeasurable(self) -> None:
        """A database that cannot answer the check must say so, not guess a side.

        Without ``season`` in ``required_columns`` the split would silently count
        every row on one side of the boundary, and a count reads as a measurement.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
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
        with engine.connect() as conn:
            report = run_regression_pack(conn)
        results = self._by_id(report)

        for check_id in _UNACCOUNTED_IDS:
            assert results[check_id].status == "skipped"
            assert "season" in results[check_id].message

    def test_the_baseline_records_both_halves_with_their_own_seasons(self) -> None:
        """Both sides are watched, so an approved historical write stays visible.

        Recording only the modern half would leave the pre-2001 rows unwatched at
        exactly the moment a reviewed backfill is allowed to change them.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES
                        (1, 1995, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER'),
                        (2, 2025, 15, 13, 1, 0, 0, 0, 'REGULAR', 'CRAWLER')
                    """,
                ),
            )

            assert _violated_rows(conn, "player_season_batting_unaccounted_pa_pre2001") == ((1995, "1"),)
            assert _violated_rows(conn, "player_season_batting_unaccounted_pa") == ((2025, "2"),)

        assert set(_UNACCOUNTED_IDS) <= _BASELINE_CHECK_IDS

    def test_the_baseline_recorder_does_not_raise_on_a_lean_schema(self) -> None:
        """The recorder has to agree with the pack about an unanswerable check.

        The baseline path runs on the same database the pack just called
        unmeasurable, so without a guard a missing migration would surface as a
        traceback from the recorder instead of a note on the report.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE player_season_batting (player_id INTEGER, plate_appearances INTEGER)"))
            for check_id in _UNACCOUNTED_IDS:
                assert _violated_rows(conn, check_id) == ()


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


class TestTheBaselineSaysWhenItWasTaken:
    """A record with no timestamp cannot be read later.

    The backlog is still growing, and the whole measure is "what changed since
    this". Without a time on the file, a reader has two possible explanations for
    any new row -- it appeared after the baseline, or the baseline was taken after
    it -- and nothing in the file distinguishes them. The scope field settles the
    season question; nothing settles this one.
    """

    def test_the_record_carries_when_it_was_written(self, tmp_path: Path) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            body = write_baseline(conn, tmp_path / "b.json")

        assert "recorded_at" in body, sorted(body)
        # ISO-8601 with an offset, so it is not read as naive UTC and compared
        # against KST-stamped rows the way a bare timestamp would be.
        parsed = datetime.fromisoformat(body["recorded_at"])
        assert parsed.tzinfo is not None

    def test_re_recording_replaces_the_stamp_so_the_newer_reads_as_newer(self, tmp_path: Path) -> None:
        """Re-recording is the honest way to stop a backlog from reading as new.

        The season values carry no timestamp of their own, so the file's own is the
        only evidence available. Two records must not claim the same moment, or
        there is no way to tell which side of a comparison each one came from.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            path = tmp_path / "b.json"
            first = write_baseline(conn, path)
            later = write_baseline(conn, path)

        assert datetime.fromisoformat(later["recorded_at"]) >= datetime.fromisoformat(first["recorded_at"])
        assert json.loads(path.read_text())["recorded_at"] == later["recorded_at"]


class TestTheBaselineLivesInTheDatabase:
    """A file makes the comparison inert on the host that runs the gate.

    The scheduler does not run where this repository is checked out. Its last
    crawl landed twelve minutes before a local log stopped moving, so the daily
    workflow executes elsewhere -- a repository-relative file would exist on the
    machine that recorded it and nowhere else, and the gate would read "not
    compared" forever without anything failing to explain why.

    These keep the row-set contract and drop the path. The scope stays part of the
    key: a whole-history record read by a single-season run reports most of the
    backlog as repaired every night.
    """

    @staticmethod
    def _conn() -> object:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as setup:
            # The record tables come from the migration file itself. The code no
            # longer creates them, so a hand-written stand-in would let the
            # migration and the reader drift apart without anything failing.
            setup.connection.driver_connection.executescript(
                Path("migrations/sqlite/069_pa_violation_baseline.sql").read_text(),
            )
        # A connection with no transaction already open, so the tests can take one.
        conn = engine.connect()
        _create_quality_tables(conn)
        conn.rollback()
        return conn

    def test_recording_replaces_the_previous_record_for_the_same_scope(self) -> None:
        """A backlog that shrank must read as smaller, so the write is a replace."""
        from src.validators.data_quality_regression_pack import write_baseline_to_db

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
            for pid in (5001, 5002):
                conn.execute(
                    text(
                        """
                        INSERT INTO player_season_batting
                            (player_id, season, plate_appearances, at_bats, walks, hbp,
                             sacrifice_hits, sacrifice_flies, league, source)
                        VALUES (:pid, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                        """,
                    ),
                    {"pid": pid},
                )
            write_baseline_to_db(conn, season=2026)

            conn.execute(text("DELETE FROM player_season_batting WHERE player_id = 5002"))
            write_baseline_to_db(conn, season=2026)

            rows = [
                row[0]
                for row in conn.execute(text("SELECT player_id FROM pa_violation_baseline ORDER BY player_id")).all()
            ]
            conn.commit()

        assert rows == [5001], "the second recording did not replace the first"

    def test_a_season_run_reads_only_its_own_season_of_the_record(self) -> None:
        """A whole-history record must not answer a one-season question.

        452 of the 780 recorded players fail in more than one season. A record
        holding a bare set of player ids cannot tell a season run which rows are
        its own, so every row it does not name looks repaired -- 714 of them, on
        every run, and 2025's rows would be counted as this season's repairs.

        Storing the season with each row is what makes both sides of the
        comparison the same set of questions. Removing either the season from the
        query or the season filter from the comparison makes this fail, which is
        how the gap was found: both mutations passed the suite as it stood.
        """
        from src.validators.data_quality_regression_pack import (
            compare_against_baseline_db,
            write_baseline_to_db,
        )

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
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
            write_baseline_to_db(conn)
            verdict = compare_against_baseline_db(conn, season=2026)
            conn.commit()

        assert verdict.baseline_available is True
        assert verdict.new_violations == ()
        assert verdict.baseline_violations == ("5001",)
        assert verdict.resolved_violations == (), "another season's rows were counted as repairs"

    def test_a_record_covering_only_some_seasons_is_not_compared_over_all(self) -> None:
        """The one mismatch left, and it is the one that subtracts from the score.

        A record taken for one season cannot answer a whole-history run: every
        other season's violations would read as new damage. Those counts are
        deducted from the quality score, so the failure would be silent and the
        score would drop for no reason anyone can name.
        """
        from src.validators.data_quality_regression_pack import (
            compare_against_baseline_db,
            write_baseline_to_db,
        )

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
            for pid, season in ((5001, 2026), (5002, 2025)):
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
            write_baseline_to_db(conn, season=2026)
            verdict = compare_against_baseline_db(conn)
            conn.commit()

        assert verdict.is_scope_mismatch is True
        assert verdict.new_violations == ()
        assert verdict.baseline_violations == ()

    def test_a_season_scope_does_not_disturb_another_season(self) -> None:
        from src.validators.data_quality_regression_pack import write_baseline_to_db

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
            for pid, season in ((5001, 2026), (5002, 2025)):
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
            write_baseline_to_db(conn, season=2026)
            write_baseline_to_db(conn, season=2025)

            seasons = [
                row[0]
                for row in conn.execute(
                    text("SELECT DISTINCT scope_season FROM pa_violation_baseline ORDER BY 1")
                ).all()
            ]
            conn.commit()

        assert seasons == [2025, 2026]

    def test_reading_back_names_rows_the_record_did_not(self) -> None:
        from src.validators.data_quality_regression_pack import compare_against_baseline_db, write_baseline_to_db

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (5001, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                    """,
                ),
            )
            write_baseline_to_db(conn, season=2026)

            quiet = compare_against_baseline_db(conn, season=2026)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (5009, 2026, 20, 16, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                    """,
                ),
            )
            grown = compare_against_baseline_db(conn, season=2026)
            conn.commit()

        assert quiet.baseline_available is True
        assert quiet.new_violations == ()
        assert grown.new_violations == ("5009",)

    def test_no_record_at_all_is_reported_as_not_compared(self) -> None:
        from src.validators.data_quality_regression_pack import compare_against_baseline_db

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (5001, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                    """,
                ),
            )
            verdict = compare_against_baseline_db(conn, season=2026)
            conn.commit()

        assert verdict.baseline_available is False
        assert verdict.new_violations == ()

    def test_recording_stamps_when_it_was_taken(self) -> None:
        """A record with no time cannot be read later.

        The season rows carry no timestamp of their own, so this column is the
        only evidence of when the snapshot was taken -- without it, a row seen in
        a later run has two equally plausible explanations.
        """
        from src.validators.data_quality_regression_pack import write_baseline_to_db

        with self._conn() as conn:
            conn.rollback()
            conn.begin()
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, source)
                    VALUES (5001, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'LEGACY_CRAWLER')
                    """,
                ),
            )
            write_baseline_to_db(conn, season=2026)

            rows = conn.execute(text("SELECT recorded_at FROM pa_violation_baseline")).all()
            conn.commit()

        assert rows, "nothing was recorded"
        assert rows[0][0] is not None

    def test_a_missing_table_is_read_as_not_compared_rather_than_raised(self) -> None:
        """The daily stage reads this, so it must not fail on a fresh install.

        An unmigrated database is a normal state for a new deployment, and the
        pack runs inside the quality gate where an exception would take down the
        whole stage instead of one comparison. It has to come back saying it did
        not compare -- which the hub already turns into an operator hint.
        """
        from src.validators.data_quality_regression_pack import compare_against_baseline_db

        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            _create_quality_tables(conn)
            conn.rollback()
            verdict = compare_against_baseline_db(conn, season=2026)

        assert verdict.baseline_available is False
        assert verdict.new_violations == ()

    def test_recording_into_an_unmigrated_database_says_what_to_do(self) -> None:
        """Recording cannot be made to work by guessing at the schema.

        Writing either creates the table outside the migration history or raises
        something the operator cannot act on. The message names the cause, because
        the only fix is a migration run this command cannot do for them.
        """
        from src.validators.data_quality_regression_pack import (
            BaselineUnreadableError,
            write_baseline_to_db,
        )

        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            _create_quality_tables(conn)
            conn.rollback()
            with pytest.raises(BaselineUnreadableError, match="migrations"):
                write_baseline_to_db(conn, season=2026)


class TestCancelledGamesCarryingScores:
    """A cancelled game has no result, so a row holding both scores is false.

    7,995 rows of 1983-2000 were found in this state on 2026-10-12, all of them
    written inside half an hour on 2026-09-23 with nothing in the crawl ledger.
    Nothing in the status-derivation rules produces it -- every caller hands the
    database's own scores to the resolver, which returns COMPLETED or DRAW
    whenever both are present. The writer was never identified, so this check
    stands in for it: the observation is what was missing when it happened.
    """

    @staticmethod
    def _run(conn) -> tuple[object, object, object]:
        from src.validators.data_quality_regression_pack import _CHECKS, run_regression_pack

        check = next(item for item in _CHECKS if item.check_id == "game_cancelled_with_scores")
        # run_regression_pack first, so the finding is proved through the entry
        # point the scheduler and the daily workflow both call, not only through
        # the private helper that happens to know this one check.
        report = run_regression_pack(conn)
        reported = next(res for res in report.results if res.check_id == "game_cancelled_with_scores")
        return report, reported, check

    @staticmethod
    def _create_game_table(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT,
                    game_status TEXT,
                    home_score INTEGER,
                    away_score INTEGER,
                    is_primary BOOLEAN,
                    game_date TEXT
                )
                """,
            ),
        )

    def test_a_cancelled_game_holding_both_scores_is_reported(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._create_game_table(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game (game_id, game_status, home_score, away_score, is_primary, game_date)
                    VALUES ('G1', 'CANCELLED', 5, 4, 1, '2024-05-01')
                    """,
                ),
            )

            report, result, _ = self._run(conn)

        assert result.status == "fail"
        assert result.violation_count == 1
        assert result.sample_ids == ("G1",)
        assert report.ok is False, "the pack reports ok on a run that failed a check"

    def test_a_cancelled_game_without_scores_is_the_normal_shape(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._create_game_table(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game (game_id, game_status, home_score, away_score, is_primary, game_date)
                    VALUES ('G1', 'CANCELLED', NULL, NULL, 1, '2024-05-01'),
                           ('G2', 'COMPLETED', 5, 4, 1, '2024-05-02'),
                           ('G3', 'DRAW', 3, 3, 1, '2024-05-03')
                    """,
                ),
            )

            report, result, _ = self._run(conn)

        assert result.status == "pass"
        assert result.violation_count == 0
        assert report.ok is True

    def test_a_superseded_duplicate_row_is_not_reported(self) -> None:
        """A retired duplicate carries the score of a game that does exist.

        The 1982 dedup left 241 rows at ``is_primary = false`` holding exactly
        this shape -- CANCELLED with both scores. Counting them would put this
        check in failure before anything has happened again, and a gate that is
        red on arrival is a gate nobody reads.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._create_game_table(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game (game_id, game_status, home_score, away_score, is_primary, game_date)
                    VALUES ('DUP', 'CANCELLED', 5, 4, 0, '2024-05-01'),
                           ('PRIMARY', 'UNRESOLVED_MISSING', 5, 4, 1, '2024-05-01')
                    """,
                ),
            )

            report, result, _ = self._run(conn)

        assert result.status == "pass"
        assert result.violation_count == 0
        assert report.ok is True

    def test_the_check_is_skipped_rather_than_failed_without_a_game_table(self) -> None:
        """The other fixtures here have no ``game`` table, so they pass by absence.

        Without the table the check reports ``skipped``, which counts as
        not-failed. Proving that a green suite is not merely a suite where this
        check never ran is what keeps the observation from being decorative.
        """
        from sqlalchemy import inspect

        from src.validators.data_quality_regression_pack import _CHECKS, run_regression_pack

        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            assert "game" not in set(inspect(conn).get_table_names())

            report = run_regression_pack(conn)

        result = next(res for res in report.results if res.check_id == "game_cancelled_with_scores")
        assert result.status == "skipped"
        assert result.violation_count == 0
        assert next(c for c in _CHECKS if c.check_id == "game_cancelled_with_scores").table == "game"


class TestCancelledWithScoresAppliesInEveryEra:
    """The check has no era in its predicate, on purpose.

    It was written for a failure that happened entirely before 2001: on
    2026-09-23 a status sweep wrote CANCELLED over 7,995 rows that carried
    scores. Scoping it to 2001+ after those rows were relabelled would have been
    the opposite of a tripwire -- the same sweep, run again over 1983-2000,
    would pass silently. The two test rows here are one historical and one
    modern precisely so that dropping either half of the class fails.
    """

    @staticmethod
    def _schema(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT, game_status TEXT, home_score INTEGER,
                    away_score INTEGER, is_primary BOOLEAN, game_date TEXT
                )
                """,
            ),
        )

    @staticmethod
    def _reported(conn):
        from src.validators.data_quality_regression_pack import run_regression_pack

        return next(res for res in run_regression_pack(conn).results if res.check_id == "game_cancelled_with_scores")

    def test_a_historical_row_carrying_scores_is_still_a_violation(self) -> None:
        """Report the exact shape the 09-23 sweep produced, 7,995 times."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19880402LTOB0', 'CANCELLED', 0, 4, 1, '1988-04-02'),
                        ('20260711WEEA0',  'CANCELLED', 5, 4, 1, '2026-07-11')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "fail"
        assert result.violation_count == 2
        assert set(result.sample_ids) == {"19880402LTOB0", "20260711WEEA0"}, "the era must not decide"

    def test_a_retired_duplicate_is_not_reported(self) -> None:
        """The 241 superseded 1982 rows carry a real score and stay excluded."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19820327SSMB0', 'CANCELLED', 11, 7, 0, '1982-03-27'),
                        ('19820327MBSS0', 'UNRESOLVED_MISSING', 11, 7, 1, '1982-03-27')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0


class TestDuplicateGameIdentifiersAreReported:
    """One game under two identifiers is ours, not the source's.

    1,000 groups in a season share a date and both team codes and every one of
    them is a real doubleheader, so the key has to carry the suffix or the check
    reports a thousand ordinary games as duplicates. What is left is the real
    shape: the same game reached through the opposite team order, which is how
    the whole 1982 corpus existed twice.
    """

    @staticmethod
    def _schema(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT, game_status TEXT, home_score INTEGER,
                    away_score INTEGER, is_primary BOOLEAN, game_date TEXT,
                    home_team TEXT, away_team TEXT
                )
                """,
            ),
        )

    @staticmethod
    def _reported(conn):
        from src.validators.data_quality_regression_pack import run_regression_pack

        return next(res for res in run_regression_pack(conn).results if res.check_id == "game_duplicate_identifiers")

    def test_two_ids_for_one_game_is_one_pair(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19820327MBSS0', 'UNRESOLVED_MISSING', 11, 7, 1, '1982-03-27', 'MB', 'SS'),
                        ('19820327SSMB0', 'CANCELLED', 11, 7, 0, '1982-03-27', 'MB', 'SS')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "fail"
        assert result.violation_count == 1, "the unit is the pair, not the row"
        assert result.sample_ids == ("19820327MBSS0",)

    def test_a_doubleheader_is_not_a_duplicate(self) -> None:
        """Same date, same teams, different game -- the ordinary case, 1,000 times."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20100518LTHT0', 'COMPLETED', 3, 2, 1, '2010-05-18', 'HT', 'LT'),
                        ('20100518LTHT1', 'COMPLETED', 9, 1, 1, '2010-05-18', 'HT', 'LT'),
                        ('20100518OBHT0', 'COMPLETED', 3, 2, 1, '2010-05-18', 'OB', 'HT')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0

    def test_two_games_differing_only_by_date_are_not_duplicates(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20100310HTHH0', 'CANCELLED', NULL, NULL, 1, '2010-03-10', 'HH', 'KIA'),
                        ('20100311HTHH0', 'CANCELLED', NULL, NULL, 1, '2010-03-11', 'HH', 'KIA')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0

    def test_three_ids_for_one_game_is_still_one_pair(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19820327MBSS0', 'UNRESOLVED_MISSING', 11, 7, 1, '1982-03-27', 'MB', 'SS'),
                        ('19820327SSMB0', 'CANCELLED', 11, 7, 0, '1982-03-27', 'MB', 'SS'),
                        ('19820327SSMB9', 'CANCELLED', 11, 7, 0, '1982-03-27', 'MB', 'SS')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.violation_count == 1

    def test_the_suffix_expression_is_the_one_postgresql_agrees_with(self) -> None:
        """Pin the expression, because a SQLite test cannot catch this.

        ``SUBSTR(game_id, -1)`` is the last character on SQLite and the whole
        string on PostgreSQL, so the shorter spelling passes every test in this
        file and reports zero duplicates in production. The behaviour under test
        is dialect agreement, and the only way to assert it without a PostgreSQL
        fixture is to name the expression the two dialects share.
        """
        from src.validators.data_quality_regression_pack import _CHECKS

        check = next(item for item in _CHECKS if item.check_id == "game_duplicate_identifiers")
        for sql in (check.count_sql, check.sample_sql):
            assert "SUBSTR(game_id, LENGTH(game_id), 1)" in sql
            assert "SUBSTR(game_id, -1)" not in sql

    def test_a_row_missing_teams_is_not_grouped(self) -> None:
        """Grouping on a NULL would either skip it or fold unrelated games together."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20180801WOSK0', 'COMPLETED', 14, 8, 1, '2018-08-01', NULL, NULL),
                        ('20180801LGOB0', 'COMPLETED', 3, 1, 1, '2018-08-01', 'LG', 'OB')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0


class TestDuplicateDetectionFoldsEraTeamCodesIntoFranchises:
    """A franchise changes its code; the matchup does not.

    2011-2018 belongs to NX (넥센), which is franchise 11, and franchise 11 is
    also WO and KH. Every Heroes game in those eight seasons is stored twice:
    once under the era code and once under the current one, carrying no score
    and no body the second time. Grouping on the raw code keeps the two rows
    apart, so the check reported 22 pairs against a database holding 1,161.

    The behaviour under test is that the key is the matchup, not the spelling.
    """

    @staticmethod
    def _schema(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT, game_status TEXT, home_score INTEGER,
                    away_score INTEGER, is_primary BOOLEAN, game_date TEXT,
                    home_team TEXT, away_team TEXT
                )
                """,
            ),
        )

    @staticmethod
    def _reported(conn):
        from src.validators.data_quality_regression_pack import run_regression_pack

        return next(res for res in run_regression_pack(conn).results if res.check_id == "game_duplicate_identifiers")

    def test_one_game_under_two_era_codes_of_one_franchise_is_one_pair(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20110312HTWO0', 'COMPLETED', 4, 3, 1, '2011-03-12', 'KH', 'KIA'),
                        ('20110312HTNX0', 'COMPLETED', NULL, NULL, 1, '2011-03-12', 'NX', 'KIA')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.status == "fail"
        assert result.violation_count == 1, "NX and KH name one franchise, so this is one game twice"

    def test_two_games_between_the_same_franchises_on_different_days_are_separate(self) -> None:
        """Folding codes must not fold distinct games into one."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20110312HTWO0', 'COMPLETED', 4, 3, 1, '2011-03-12', 'KH', 'KIA'),
                        ('20110313HTNX0', 'COMPLETED', 4, 3, 1, '2011-03-13', 'NX', 'KIA')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.violation_count == 0

    def test_an_all_star_side_is_not_folded_onto_a_club(self) -> None:
        """EA and WE name the two halves of one game, not two franchises.

        Neither maps to a franchise, so each keeps its own key and the All-Star
        fixture stays exactly one group. Folding them onto a shared key would
        report 13 real All-Star games as duplicates once a club game shared the
        date; folding them onto a club's franchise would merge an exhibition with
        a league game. Both are wrong and neither is caught by a test that only
        uses eras with a code mapping.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20150718EAWE0', 'COMPLETED', 5, 4, 1, '2015-07-18', 'WE', 'EA'),
                        ('20150718EAHH0', 'COMPLETED', 3, 1, 1, '2015-07-18', 'EA', 'HH')
                    """,
                ),
            )

            result = self._reported(conn)

        assert result.violation_count == 0, "an All-Star side is not a club, so nothing folds"

    def test_the_case_expression_covers_every_code_the_history_declares(self) -> None:
        """Pin the SQL to the source of truth.

        The mapping is generated from ``iter_team_history``. A code that is
        added there and missing here would silently stop being folded, which is
        the exact failure this change was made to fix, and a per-era sample test
        would not notice.
        """
        from src.utils.team_history import iter_team_history
        from src.validators.data_quality_regression_pack import _franchise_case

        sql = _franchise_case("home_team")

        for entry in iter_team_history():
            assert f"WHEN '{entry.team_code.upper()}' THEN 'F{entry.franchise_id}'" in sql, entry.team_code

    def test_every_generated_branch_maps_back_to_its_own_franchise(self) -> None:
        """A shared spelling must not claim two franchises.

        ``SK`` and ``SSG`` are one franchise and ``SS`` is a different club that
        inherited the same two letters in 2025, so a fold that reached the wrong
        entry would merge a Samsung Lions game with an SSG Landers game.
        """
        from src.utils.team_history import franchise_id_for_team_code
        from src.validators.data_quality_regression_pack import _franchise_case

        import re

        sql = _franchise_case("home_team")
        parsed = dict(re.findall(r"WHEN '([A-Z]+)' THEN 'F(\d+)'", sql))

        assert parsed
        for code, franchise_id in parsed.items():
            assert franchise_id_for_team_code(code, None) == int(franchise_id), code


class TestTheBodyGapIsReportedWithoutFailing:
    """A structural limit is not a regression, in any decade.

    namuwiki publishes the result for 1982-2000 but no boxscore bodies, so those
    games carry scores and nothing else. The same shape exists in the modern era:
    50 games between 2011 and 2026 -- all-star fixtures, Korean Series games and
    the 2024 World Baseball Classic -- have a score and no body because no
    boxscore was ever collected for them. Reporting either as a failure produces a
    gate that is red on arrival, which is a gate nobody reads. The number still
    has to be visible: it is the gap a 09-23 status sweep reached 2,848 rows into
    without anything recording it.

    The era is not in the predicate. An era-scoped check is how the 50 fell out of
    the report the moment they were relabelled UNRESOLVED_MISSING, which is how a
    gate loses the rows it exists to watch.
    """

    @staticmethod
    def _schema(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT, game_status TEXT, home_score INTEGER,
                    away_score INTEGER, is_primary BOOLEAN, game_date TEXT
                )
                """,
            ),
        )
        # The four body tables carry the columns the other checks in the pack
        # read. Creating them without those columns turns their checks from
        # skipped into errors, which is a different test failing for an
        # unrelated reason.
        conn.execute(
            text("CREATE TABLE player_game_batting (game_id TEXT, player_id TEXT, hits INTEGER, at_bats INTEGER)")
        )
        conn.execute(text("CREATE TABLE game_lineups (game_id TEXT, player_id TEXT)"))
        conn.execute(
            text("CREATE TABLE game_inning_scores (game_id TEXT, inning INTEGER, team_side TEXT, runs INTEGER)"),
        )
        conn.execute(
            text("CREATE TABLE game_batting_stats (game_id TEXT, player_id TEXT, hits INTEGER, at_bats INTEGER)")
        )

    @staticmethod
    def _reported(conn):
        from src.validators.data_quality_regression_pack import run_regression_pack

        report = run_regression_pack(conn)
        return report, next(res for res in report.results if res.check_id == "game_result_without_body")

    def test_the_gap_is_warned_about_and_leaves_the_pack_ok(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19880402LTOB0', 'UNRESOLVED_MISSING', 0, 4, 1, '1988-04-02'),
                        ('20110723EAWE0', 'UNRESOLVED_MISSING', 5, 4, 1, '2011-07-23'),
                        ('20260711WEEA0', 'COMPLETED', 5, 4, 1, '2026-07-11')
                    """,
                ),
            )
            # The modern COMPLETED row gets a body, so the two rows that remain
            # are the gaps and this one is the ordinary case.
            conn.execute(text("INSERT INTO player_game_batting (game_id, player_id) VALUES ('20260711WEEA0', 'p1')"))

            report, result = self._reported(conn)

        assert result.status == "warn"
        assert result.violation_count == 2
        assert set(result.sample_ids) == {"19880402LTOB0", "20110723EAWE0"}, "the modern gap is reported too"
        assert report.ok is True, "a source limit must not raise the daily incident"
        assert report.failure_count == 0

    def test_a_game_that_does_have_a_body_is_not_counted(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19880402LTOB0', 'COMPLETED', 0, 4, 1, '1988-04-02')
                    """,
                ),
            )
            conn.execute(text("INSERT INTO player_game_batting (game_id, player_id) VALUES ('19880402LTOB0', 'p1')"))

            _, result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0

    @pytest.mark.parametrize(
        "table",
        ["player_game_batting", "game_lineups", "game_inning_scores", "game_batting_stats"],
    )
    def test_a_body_in_any_one_of_the_four_tables_counts_as_a_body(self, table: str) -> None:
        """Otherwise a game carrying only lineups is reported as a gap that is not there."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(text("INSERT INTO game VALUES ('20180801WOSK0', 'COMPLETED', 14, 8, 1, '2018-08-01')"))
            conn.execute(text(f"INSERT INTO {table} (game_id) VALUES ('20180801WOSK0')"))

            _, result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0

    def test_a_retired_duplicate_is_not_counted(self) -> None:
        """Otherwise retiring the 241 1982 duplicates moves a source-limit figure.

        The figure is supposed to describe what the source does not publish.
        Our own housekeeping changing it would make the two indistinguishable.
        """
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('19820327MBSS0', 'UNRESOLVED_MISSING', 11, 7, 1, '1982-03-27'),
                        ('19820327SSMB0', 'CANCELLED', 11, 7, 0, '1982-03-27')
                    """,
                ),
            )

            _, result = self._reported(conn)

        assert result.status == "warn"
        assert result.violation_count == 1, "only the surviving row counts"
        assert result.sample_ids == ("19820327MBSS0",)

    def test_a_game_without_scores_is_not_a_body_gap(self) -> None:
        """No result means nothing was collected, which is the normal shape of a fixture list."""
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(text("INSERT INTO game VALUES ('20260820HTHH0', 'COMPLETED', NULL, NULL, 1, '2026-08-20')"))

            _, result = self._reported(conn)

        assert result.status == "pass"
        assert result.violation_count == 0
