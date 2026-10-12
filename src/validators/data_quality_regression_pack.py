"""Small invariant pack for crawler data quality regressions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path  # noqa: TC003 - a runtime dependency: read_text/write_text/mkdir
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.reflection import Inspector


@dataclass(frozen=True)
class QualityRegressionResult:
    """QualityRegressionResult class."""

    check_id: str
    description: str
    status: str
    violation_count: int
    message: str
    sample_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "check_id": self.check_id,
            "description": self.description,
            "status": self.status,
            "violation_count": self.violation_count,
            "message": self.message,
            "sample_ids": list(self.sample_ids),
        }


@dataclass(frozen=True)
class QualityRegressionReport:
    """QualityRegressionReport class."""

    results: tuple[QualityRegressionResult, ...]

    @property
    def ok(self) -> bool:
        """Handle the ok operation.

        Returns:
            True if successful, False otherwise.

        """
        return all(result.status != "fail" for result in self.results)

    @property
    def check_count(self) -> int:
        """Check count.

        Returns:
            Integer result.

        """
        return len(self.results)

    @property
    def failure_count(self) -> int:
        """Handle the failure count operation.

        Returns:
            Integer result.

        """
        return sum(1 for result in self.results if result.status == "fail")

    def to_dict(self) -> dict[str, Any]:
        """Handle the to dict operation.

        Returns:
            Dictionary result.

        """
        return {
            "ok": self.ok,
            "check_count": self.check_count,
            "failure_count": self.failure_count,
            "results": [result.to_dict() for result in self.results],
        }


@dataclass(frozen=True)
class BaselineVerdict:
    """What changed since a recorded baseline, by row rather than by total.

    A total answers "is anything wrong" and both a steady backlog and a bleeding
    one answer yes. Naming the rows answers "is this getting worse", which is
    the only question a backlog nobody can repair can still act on.
    """

    #: Rows failing now that no baseline entry claimed.
    new_violations: tuple[str, ...]
    #: Rows failing now that a baseline entry already claimed.
    baseline_violations: tuple[str, ...]
    #: Baseline rows that no longer fail.
    resolved_violations: tuple[str, ...]
    #: Checks the baseline names that this pack does not run.
    unknown_checks: tuple[str, ...]
    #: The baseline was recorded over a different scope than this run.
    is_scope_mismatch: bool = False

    @property
    def is_new_damage(self) -> bool:
        """Whether anything failed that was not already known to fail."""
        return bool(self.new_violations) or bool(self.unknown_checks) or self.is_scope_mismatch


class BaselineUnreadableError(RuntimeError):
    """Raised when a baseline cannot be read, rather than being treated as empty.

    An absent or corrupt baseline compared against nothing reports the entire
    backlog as new damage, and compared the other way reports none of it. Both
    readings hide a real break in opposite directions, so the caller is told
    instead of being handed a verdict.
    """


def _violated_player_ids(conn: Connection, check_id: str, *, season: int | None = None) -> tuple[str, ...]:
    """Return every player id a check reports, not a sample of them.

    The pack's own result carries a count and five samples, which cannot say
    *which* rows are at fault, and a baseline comparison is entirely about which
    rows those are.

    The season scope is applied here for the same reason the pack applies it
    through ``_scope_predicate``: a check's own SQL has no notion of which run it
    belongs to, so a caller that filtered the result but not the query would be
    comparing a narrow run against a wide baseline.
    """
    check = next((item for item in _CHECKS if item.check_id == check_id), None)
    if check is None:
        return ()
    sql = check.count_sql.replace("SELECT COUNT(*)", "SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')")
    predicate, params, _ = _scope_predicate(check.table, target_date=None, season=season)
    sql = _append_scope_predicate(sql, predicate)
    return tuple(sorted({str(row[0]) for row in conn.execute(text(sql), params)}))


def _load_baseline(path: Path) -> dict[str, Any]:
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        msg = f"baseline is unreadable: {path}"
        raise BaselineUnreadableError(msg) from exc
    if not isinstance(body, dict) or not isinstance(body.get("checks"), dict):
        msg = f"baseline has no checks section: {path}"
        raise BaselineUnreadableError(msg)
    return body


def write_baseline(conn: Connection, path: Path, *, season: int | None = None) -> dict[str, Any]:
    """Record the rows currently failing so a later run can diff against them.

    Only the season PA checks are recorded. The others report game-level rows
    keyed by game id and are compared by count elsewhere, and mixing them in
    would make a game's absence look like a player being repaired.

    The scope is written into the file rather than left implicit. 452 of the 760
    players in the current set fail in more than one season, so a baseline
    recorded over all seasons and read by a ``--year`` run would call most of
    them repaired every time.
    """
    body: dict[str, Any] = {
        "version": 1,
        "scope": {"season": season},
        "checks": {
            check_id: {"player_ids": list(_violated_player_ids(conn, check_id, season=season))}
            for check_id in sorted(_BASELINE_CHECK_IDS)
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return body


def compare_against_baseline(conn: Connection, path: Path, *, season: int | None = None) -> BaselineVerdict:
    """Diff the current violations against a baseline, row by row.

    A row is compared by player id across all recorded checks rather than per
    check, because a row legitimately changes bucket: the unaccounted and
    unreconciled checks turn on whether a sacrifice was recorded, so backfilling
    one moves a failing row between them while the defect is unchanged. Keying on
    the check would report that move as new damage.

    A scope disagreement is reported rather than resolved. Reading a whole-history
    baseline from a single-season run would mark hundreds of players repaired, and
    that verdict reads like progress.
    """
    baseline = _load_baseline(path)
    recorded = baseline["checks"]
    recorded_season = (baseline.get("scope") or {}).get("season")

    now: set[str] = set()
    for check_id in sorted(set(recorded) | set(_BASELINE_CHECK_IDS)):
        now.update(_violated_player_ids(conn, check_id, season=season))

    before: set[str] = set()
    for entry in recorded.values():
        ids = entry.get("player_ids") if isinstance(entry, dict) else None
        if isinstance(ids, list):
            before.update(str(value) for value in ids)

    return BaselineVerdict(
        new_violations=tuple(sorted(now - before)),
        baseline_violations=tuple(sorted(now & before)),
        resolved_violations=tuple(sorted(before - now)),
        unknown_checks=tuple(sorted(set(recorded) - set(_BASELINE_CHECK_IDS))),
        is_scope_mismatch=recorded_season != season,
    )


#: The season PA checks a baseline is recorded for, named explicitly so that
#: retiring one is visible in ``unknown_checks`` rather than silent.
_BASELINE_CHECK_IDS = frozenset(
    {
        "player_season_batting_unaccounted_pa",
        "player_season_batting_unreconciled_pa",
        "player_season_batting_missing_pa",
    },
)


@dataclass(frozen=True)
class UnrecoverableVerdict:
    """How the unfixable rows stand against what was recorded before.

    Separate from ``BaselineVerdict`` because the question is different. A
    baseline asks whether the defect is growing; this asks whether the rows nobody
    can repair are still the same ones, which is the only thing left to check
    about a permanent backlog.
    """

    #: Rows now unfixable that the record did not name.
    newly_unrecoverable: tuple[str, ...]
    #: Rows the record named that are still unfixable.
    still_unrecoverable: tuple[str, ...]


#: Season rows whose counting columns are absent *and* whose totals no longer
#: reconcile. A NULL where a zero belongs is excluded: reading it as zero
#: satisfies the formula, so no queried number is wrong and there is nothing lost.
#:
#: A recorded sacrifice is also required, which separates this from the
#: unaccounted shape. When both sacrifice columns are zero the formula failing
#: only says the sacrifices were never written -- the identity holds once they are
#: filled in, and no value is missing. Where a sacrifice *was* recorded the
#: arithmetic was possible and still came out short, so a count really is gone.
_UNRECOVERABLE_SQL = """
    SELECT COALESCE(CAST(player_id AS TEXT), 'NULL'), season, league, source
    FROM player_season_batting
    WHERE (walks IS NULL OR hbp IS NULL)
      AND (COALESCE(sacrifice_hits, 0) > 0 OR COALESCE(sacrifice_flies, 0) > 0)
      AND COALESCE(plate_appearances, 0) !=
        COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
        + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
"""

#: Why these rows are not repaired, stated so a reader does not assume they were.
#: The source page is disallowed by robots.txt, and the game-level rows these
#: seasons would be re-derived from are absent, so there is no lawful path back to
#: the values. Recomputing them from the PA identity would fabricate statistics.
_UNRECOVERABLE_REASON = "SOURCE_UNAVAILABLE"
_UNRECOVERABLE_DETAIL = (
    "walk/HBP columns lost to the crawler's per-layout merge; cannot be re-collected (source disallowed by robots.txt)"
)


def _unrecoverable_rows(conn: Connection) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "player_id": str(row[0]),
            "season": row[1],
            "league": row[2],
            "source": row[3],
            "reason": _UNRECOVERABLE_REASON,
            "detail": _UNRECOVERABLE_DETAIL,
        }
        for row in conn.execute(text(_UNRECOVERABLE_SQL))
    )


def write_unrecoverable(conn: Connection, path: Path) -> dict[str, Any]:
    """Record the season rows that cannot be repaired, and why.

    The rows are left exactly as they are. This writes a record beside them, not
    a correction over them: the missing values came from a source that is now
    disallowed and from game-level rows that are not there, so any number derived
    from the PA identity would be an invention rather than a recovery.
    """
    body: dict[str, Any] = {
        "version": 1,
        "note": "recorded, not repaired: these values cannot be recovered from any lawful source",
        "rows": [dict(row) for row in _unrecoverable_rows(conn)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return body


def compare_unrecoverable(conn: Connection, path: Path) -> UnrecoverableVerdict:
    """Report rows that became unfixable since the record was written.

    A row that can no longer be repaired is a different event from one that never
    could be. A crawler that starts losing values is still running and still
    writing, so this is where new damage of that shape surfaces.
    """
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        msg = f"unrecoverable record is unreadable: {path}"
        raise BaselineUnreadableError(msg) from exc

    recorded = {str(entry.get("player_id")) for entry in body.get("rows", []) if isinstance(entry, dict)}
    now = {str(entry["player_id"]) for entry in _unrecoverable_rows(conn)}

    return UnrecoverableVerdict(
        newly_unrecoverable=tuple(sorted(now - recorded)),
        still_unrecoverable=tuple(sorted(now & recorded)),
    )


@dataclass(frozen=True)
class _SqlCheck:
    check_id: str
    description: str
    table: str
    required_columns: tuple[str, ...]
    count_sql: str
    sample_sql: str


@dataclass(frozen=True)
class _PackOptions:
    target_date: str | None
    season: int | None
    require_schema: bool


_CHECKS: tuple[_SqlCheck, ...] = (
    _SqlCheck(
        check_id="game_batting_pa_formula",
        description="Game batting PA equals AB + BB + HBP + SH + SF",
        table="game_batting_stats",
        required_columns=(
            "game_id",
            "player_id",
            "plate_appearances",
            "at_bats",
            "walks",
            "hbp",
            "sacrifice_hits",
            "sacrifice_flies",
        ),
        count_sql="""
            SELECT COUNT(*)
            FROM game_batting_stats
            WHERE COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_batting_stats
            WHERE COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="player_season_batting_pa_formula",
        description="Season batting PA equals AB + BB + HBP + SH + SF",
        table="player_season_batting",
        required_columns=(
            "player_id",
            "plate_appearances",
            "at_bats",
            "walks",
            "hbp",
            "sacrifice_hits",
            "sacrifice_flies",
            "league",
            "source",
        ),
        count_sql="""
            SELECT COUNT(*)
            FROM player_season_batting
            WHERE league = 'REGULAR' AND source = 'AGGREGATED'
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE league = 'REGULAR' AND source = 'AGGREGATED'
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
            LIMIT 5
        """,
    ),
    # Deliberately wider than the check above. That one is scoped to
    # REGULAR/AGGREGATED, which is 882 of 19,830 season rows, and it reported no
    # violation while the table held 2,603. The rows it cannot see are
    # LEGACY_CRAWLER, and every one of those has both sacrifice columns empty
    # with PA populated -- PA arrived correct from the source and the sacrifices
    # were simply never written, so the formula cannot be checked at all rather
    # than being wrong. audit_pa_formula.py calls the same shape FIXABLE_FORMULA.
    #
    # No league or source filter here, because the gap is not a regular-season
    # phenomenon: KOREAN_SERIES and PLAYOFF rows carry it too. Scoping the
    # verdict to one league would hide those the same way.
    #
    # PA strictly greater than AB+BB+HBP, and both sacrifice columns at zero.
    # Both halves are load-bearing. 15,986 rows reconcile as
    # PA = AB+BB+HBP+0+0 because nobody sacrificed, and calling that missing
    # data would bury the finding. The other side -- sacrifices recorded but
    # still short of PA, 23 rows in REGULAR/CRAWLER and REGULAR/OFFICIAL_ARCHIVE --
    # is a different defect: either PA is wrong or a sacrifice did not count as
    # one. This check does not claim those.
    _SqlCheck(
        check_id="player_season_batting_unaccounted_pa",
        description="Season batting PA exceeds AB + BB + HBP with no sacrifice recorded",
        table="player_season_batting",
        required_columns=(
            "player_id",
            "plate_appearances",
            "at_bats",
            "walks",
            "hbp",
            "sacrifice_hits",
            "sacrifice_flies",
        ),
        count_sql="""
            SELECT COUNT(*)
            FROM player_season_batting
            WHERE COALESCE(plate_appearances, 0) >
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                AND COALESCE(sacrifice_hits, 0) = 0
                AND COALESCE(sacrifice_flies, 0) = 0
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE COALESCE(plate_appearances, 0) >
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                AND COALESCE(sacrifice_hits, 0) = 0
                AND COALESCE(sacrifice_flies, 0) = 0
            LIMIT 5
        """,
    ),
    # The two checks above covered 2,572 of 2,603 season violations and reported
    # the pack clean on the remaining 31 without naming them. This one and the
    # next divide those 31 by what the row is actually missing, and together with
    # unaccounted_pa the three split the whole 2,603 -- 2,549 / 23 / 31 -- so no
    # row is counted twice and none goes unreported.
    #
    # PA <= AB+BB+HBP appears beside "a sacrifice was recorded" because both
    # directions are defects. A check written only for "PA exceeds the columns"
    # cannot see the 2001 row carrying an HBP of 400 against a PA of 490, which
    # is a column filled from the wrong reading rather than a missing one.
    #
    # plate_appearances IS NOT NULL on purpose: a row with no PA is the next
    # check's finding, so COALESCE folding NULL to zero cannot land it here too.
    _SqlCheck(
        check_id="player_season_batting_unreconciled_pa",
        description="Season batting PA does not reconcile against a recorded row",
        table="player_season_batting",
        required_columns=(
            "player_id",
            "plate_appearances",
            "at_bats",
            "walks",
            "hbp",
            "sacrifice_hits",
            "sacrifice_flies",
        ),
        count_sql="""
            SELECT COUNT(*)
            FROM player_season_batting
            WHERE plate_appearances IS NOT NULL
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
              AND (
                COALESCE(sacrifice_hits, 0) > 0
                OR COALESCE(sacrifice_flies, 0) > 0
                OR COALESCE(plate_appearances, 0)
                   <= COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
              )
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE plate_appearances IS NOT NULL
              AND COALESCE(plate_appearances, 0) !=
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
              AND (
                COALESCE(sacrifice_hits, 0) > 0
                OR COALESCE(sacrifice_flies, 0) > 0
                OR COALESCE(plate_appearances, 0)
                   <= COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
              )
            LIMIT 5
        """,
    ),
    # 31 rows carry a batting count but no plate_appearances: 18 PROFILE from
    # 2025, 9 ROLLUP from 2026, 4 CRAWLER from 2001. COALESCE reads that NULL as
    # zero, so the scoped formula check calls it a violation without saying why
    # and unaccounted_pa cannot compare a missing figure against anything at all.
    #
    # An entirely empty row is excluded on purpose. Nothing recorded is not a
    # defect, it is a row with nothing in it -- there are 30 of those, and
    # reporting them would bury the 31.
    _SqlCheck(
        check_id="player_season_batting_missing_pa",
        description="Season batting row records a batting count but no plate appearances",
        table="player_season_batting",
        required_columns=("player_id", "plate_appearances", "at_bats"),
        count_sql="""
            SELECT COUNT(*)
            FROM player_season_batting
            WHERE plate_appearances IS NULL
              AND (
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
              ) > 0
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE plate_appearances IS NULL
              AND (
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
              ) > 0
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_batting_hits_not_gt_at_bats",
        description="Game batting hits do not exceed at-bats",
        table="game_batting_stats",
        required_columns=("game_id", "player_id", "hits", "at_bats"),
        count_sql="""
            SELECT COUNT(*)
            FROM game_batting_stats
            WHERE COALESCE(hits, 0) > COALESCE(at_bats, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_batting_stats
            WHERE COALESCE(hits, 0) > COALESCE(at_bats, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_pitching_earned_runs_not_gt_runs_allowed",
        description="Game pitching earned runs do not exceed runs allowed",
        table="game_pitching_stats",
        required_columns=("game_id", "player_id", "earned_runs", "runs_allowed"),
        count_sql="""
            SELECT COUNT(*)
            FROM game_pitching_stats
            WHERE COALESCE(earned_runs, 0) > COALESCE(runs_allowed, 0)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_pitching_stats
            WHERE COALESCE(earned_runs, 0) > COALESCE(runs_allowed, 0)
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_batting_null_player_id",
        description="Game batting stat rows should resolve player_id",
        table="game_batting_stats",
        required_columns=("game_id", "player_id"),
        count_sql="SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL",
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_batting_stats
            WHERE player_id IS NULL
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_pitching_null_player_id",
        description="Game pitching stat rows should resolve player_id",
        table="game_pitching_stats",
        required_columns=("game_id", "player_id"),
        count_sql="SELECT COUNT(*) FROM game_pitching_stats WHERE player_id IS NULL",
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_pitching_stats
            WHERE player_id IS NULL
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="game_lineups_null_player_id",
        description="Lineup rows should resolve player_id",
        table="game_lineups",
        required_columns=("game_id", "player_id"),
        count_sql="SELECT COUNT(*) FROM game_lineups WHERE player_id IS NULL",
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game_lineups
            WHERE player_id IS NULL
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="batting_avg_range",
        description="Season batting average should be between 0.0 and 1.0",
        table="player_season_batting",
        required_columns=("avg",),
        count_sql="SELECT COUNT(*) FROM player_season_batting WHERE avg < 0 OR avg > 1.0",
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), ''), avg
            FROM player_season_batting
            WHERE avg < 0 OR avg > 1.0
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="era_range",
        description="Season ERA should be non-negative and have a positive innings basis",
        table="player_season_pitching",
        required_columns=("era", "innings_pitched", "innings_outs"),
        count_sql="""
            SELECT COUNT(*)
            FROM player_season_pitching
            WHERE era < 0
               OR (
                   era > 30.0
                   AND COALESCE(innings_outs, 0) <= 0
                   AND COALESCE(innings_pitched, 0) <= 0
               )
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), ''), era
            FROM player_season_pitching
            WHERE era < 0
               OR (
                   era > 30.0
                   AND COALESCE(innings_outs, 0) <= 0
                   AND COALESCE(innings_pitched, 0) <= 0
               )
            LIMIT 5
        """,
    ),
    _SqlCheck(
        check_id="hits_lte_at_bats_season",
        description="Season hits should not exceed at-bats",
        table="player_season_batting",
        required_columns=("hits", "at_bats"),
        count_sql="SELECT COUNT(*) FROM player_season_batting WHERE hits > at_bats AND at_bats > 0",
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), ''), hits, at_bats
            FROM player_season_batting
            WHERE hits > at_bats AND at_bats > 0
            LIMIT 5
        """,
    ),
)


def run_regression_pack(
    conn: Connection,
    checks: Sequence[_SqlCheck] = _CHECKS,
    *,
    target_date: str | None = None,
    season: int | None = None,
    require_schema: bool = False,
) -> QualityRegressionReport:
    """Run data quality invariants against a SQLAlchemy connection.

    Args:
        conn: Conn.
        checks: Checks.
        target_date: Optional YYYYMMDD target date for game-level checks.
        season: Optional season for season-level checks.
        require_schema: Treat missing tables or columns as failures.

    """
    inspector = inspect(conn)

    table_names = set(inspector.get_table_names())
    options = _PackOptions(target_date=target_date, season=season, require_schema=require_schema)
    results = tuple(_run_check(conn, inspector, table_names, check, options) for check in checks)
    return QualityRegressionReport(results=results)


def render_regression_report(report: QualityRegressionReport) -> str:
    """Report render regression.

    Args:
        report: Report.
        report: Report.
        report: Report.

    Returns:
        String result.

    """
    lines = [
        f"Data quality regression pack: {'PASS' if report.ok else 'FAIL'}",
        f"Checks: {report.check_count}",
        f"Failures: {report.failure_count}",
    ]
    for result in report.results:
        lines.append(f"- {result.check_id}: {result.status} ({result.violation_count} violation(s))")
        if result.message:
            lines.append(f"  {result.message}")
        if result.sample_ids:
            lines.append(f"  Samples: {', '.join(result.sample_ids)}")
    return "\n".join(lines)


def report_to_json(report: QualityRegressionReport) -> str:
    """Report to json.

    Args:
        report: Report.
        report: Report.
        report: Report.

    Returns:
        String result.

    """
    return json.dumps(report.to_dict(), ensure_ascii=False)


def _run_check(
    conn: Connection,
    inspector: Inspector,
    table_names: set[str],
    check: _SqlCheck,
    options: _PackOptions,
) -> QualityRegressionResult:
    missing_status = "fail" if options.require_schema else "skipped"
    if check.table not in table_names:
        return QualityRegressionResult(
            check_id=check.check_id,
            description=check.description,
            status=missing_status,
            violation_count=0,
            message=f"missing table: {check.table}",
        )

    columns = {column["name"] for column in inspector.get_columns(check.table)}
    predicate, params, scope_columns = _scope_predicate(
        check.table,
        target_date=options.target_date,
        season=options.season,
    )
    missing_columns = [
        column
        for column in (*check.required_columns, *scope_columns)
        if column not in columns and not (column == "source" and "data_source" in columns)
    ]
    if missing_columns:
        return QualityRegressionResult(
            check_id=check.check_id,
            description=check.description,
            status=missing_status,
            violation_count=0,
            message=f"missing columns on {check.table}: {', '.join(missing_columns)}",
        )

    count_sql = _adapt_schema_aliases(check.count_sql, columns)
    sample_sql = _adapt_schema_aliases(check.sample_sql, columns)
    count_sql = _append_scope_predicate(count_sql, predicate)
    sample_sql = _adapt_limit_clause(
        _append_scope_predicate(sample_sql, predicate),
        conn.dialect.name,
    )
    violation_count = int(conn.execute(text(count_sql), params).scalar_one())
    sample_ids = tuple(str(row[0]) for row in conn.execute(text(sample_sql), params).all())
    status = "pass" if violation_count == 0 else "fail"
    message = "ok" if status == "pass" else f"{violation_count} violation(s) found"
    return QualityRegressionResult(
        check_id=check.check_id,
        description=check.description,
        status=status,
        violation_count=violation_count,
        message=message,
        sample_ids=sample_ids,
    )


def _scope_predicate(
    table: str,
    *,
    target_date: str | None,
    season: int | None,
) -> tuple[str | None, dict[str, object], tuple[str, ...]]:
    game_tables = {"game_batting_stats", "game_pitching_stats", "game_lineups"}
    season_tables = {"player_season_batting", "player_season_pitching"}
    if table in game_tables:
        if target_date:
            return "game_id LIKE :target_game_prefix", {"target_game_prefix": f"{target_date}%"}, ("game_id",)
        if season is not None:
            return "game_id LIKE :target_game_prefix", {"target_game_prefix": f"{season}%"}, ("game_id",)
    if table in season_tables and season is not None:
        return "season = :target_season", {"target_season": season}, ("season",)
    return None, {}, ()


def _append_scope_predicate(sql: str, predicate: str | None) -> str:
    if predicate is None:
        return sql
    query, separator, limit = sql.rpartition("LIMIT")
    if separator:
        return f"{query.rstrip()} AND {predicate}\nLIMIT{limit}"
    return f"{sql.rstrip()} AND {predicate}"


def _adapt_limit_clause(sql: str, dialect: str) -> str:
    """Translate SQLite/PostgreSQL sample SQL syntax for Oracle."""
    if dialect != "oracle":
        return sql
    sql = sql.replace("CAST(game_id AS TEXT)", "TO_CHAR(game_id)")
    sql = sql.replace("CAST(player_id AS TEXT)", "TO_CHAR(player_id)")
    query, separator, limit = sql.rpartition("LIMIT")
    if separator and limit.strip().isdigit():
        return f"{query.rstrip()}\nFETCH FIRST {limit.strip()} ROWS ONLY"
    return sql


def _adapt_schema_aliases(sql: str, columns: set[str]) -> str:
    """Adapt known local/OCI column aliases in invariant SQL."""
    if "source" not in columns and "data_source" in columns:
        return sql.replace("source", "data_source")
    return sql
