"""Small invariant pack for crawler data quality regressions."""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path  # noqa: TC003 - a runtime dependency: read_text/write_text/mkdir
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text

from src.constants import KST
from src.utils.team_history import iter_team_history

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
    #: Whether a record for this scope was found at all.
    #:
    #: The file-backed reader raises instead, because a missing path there means
    #: the operator has not run the command yet. In the database, absence is a
    #: normal state a fresh install is in, so it has to be representable without
    #: being an error and without being mistaken for an empty record.
    baseline_available: bool = True

    @property
    def is_new_damage(self) -> bool:
        """Whether anything failed that was not already known to fail."""
        return (
            bool(self.new_violations)
            or bool(self.unknown_checks)
            or self.is_scope_mismatch
            or not self.baseline_available
        )


class BaselineUnreadableError(RuntimeError):
    """Raised when a baseline cannot be read, rather than being treated as empty.

    An absent or corrupt baseline compared against nothing reports the entire
    backlog as new damage, and compared the other way reports none of it. Both
    readings hide a real break in opposite directions, so the caller is told
    instead of being handed a verdict.
    """


def _violated_rows(conn: Connection, check_id: str, *, season: int | None = None) -> tuple[tuple[int, str], ...]:
    """Return every ``(season, player_id)`` a check reports, not a sample of them.

    The pack's own result carries a count and five samples, which cannot say
    *which* rows are at fault, and a baseline comparison is entirely about which
    rows those are.

    The season comes back with each row rather than being applied to the query.
    452 of the 760 recorded players fail in more than one season, so a bare set
    of player ids cannot answer "is 2026 getting worse" -- the other seasons'
    rows are indistinguishable from repairs. Selecting the season here lets the
    comparison narrow both sides to the same set of rows.

    A run with no season filter reads every season, and each row keeps its own.
    """
    check = next((item for item in _CHECKS if item.check_id == check_id), None)
    if check is None:
        return ()
    if not _table_exists(conn, check.table):
        return ()
    columns = {column["name"] for column in inspect(conn).get_columns(check.table)}
    # A schema that cannot answer the check has no row to record. Raising here
    # instead would make the baseline recorder crash on the same lean database the
    # pack is built to report as "unmeasurable", which turns a missing migration
    # into a traceback rather than a note on the report.
    if _missing_columns(columns, check.required_columns):
        return ()
    # A table with no season column cannot say which run its rows belong to, so
    # the season falls back to the one the caller asked about. Only the season
    # tables carry one, and those are the ones being compared.
    has_season = season is not None or "season" in columns
    if has_season:
        sql = check.count_sql.replace(
            "SELECT COUNT(*)",
            "SELECT COALESCE(CAST(season AS TEXT), '0'), COALESCE(CAST(player_id AS TEXT), 'NULL')",
        )
    else:
        sql = check.count_sql.replace(
            "SELECT COUNT(*)",
            "SELECT '0', COALESCE(CAST(player_id AS TEXT), 'NULL')",
        )
    predicate, params, _ = _scope_predicate(check.table, target_date=None, season=season)
    sql = _append_scope_predicate(sql, predicate)
    found = {(str(row[0]), str(row[1])) for row in conn.execute(text(sql), params).all()}
    if season is not None:
        # The predicate already narrowed the query, so the season it filtered on
        # is the season every row belongs to.
        return tuple(sorted((season, player_id) for _, player_id in found))
    return tuple(sorted((int(row_season), player_id) for row_season, player_id in found if row_season.isdigit()))


def _violated_player_ids(conn: Connection, check_id: str, *, season: int | None = None) -> tuple[str, ...]:
    """Return the player ids a check reports, without their seasons.

    Used where only membership matters. The comparison itself uses
    ``_violated_rows``, because dropping the season is what makes a record look
    like repairs.
    """
    return tuple(sorted({player_id for _, player_id in _violated_rows(conn, check_id, season=season)}))


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
        # The season rows carry no timestamp of their own, so this is the only
        # evidence of when the snapshot was taken. Without it a later reader
        # cannot tell whether a new row appeared after this or whether the file
        # was taken after it, and the comparison it enables has no anchor.
        "recorded_at": datetime.now(KST).isoformat(),
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
#:
#: The pre-2001 half is included even though it reports as ``warn``. Its rows are
#: static only while nobody writes them, and approved historical backfills are
#: exactly the case where somebody does -- so a baseline is what notices those
#: rows changing, which is a different question from whether they should block
#: the daily run.
_BASELINE_CHECK_IDS = frozenset(
    {
        "player_season_batting_unaccounted_pa",
        "player_season_batting_unaccounted_pa_pre2001",
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


#: Season scope written into the record when a run covers every season. Zero
#: rather than NULL because the scope is part of the primary key, and no KBO
#: season is shorter than four digits.
ALL_SEASONS_SCOPE = 0

BASELINE_TABLE = "pa_violation_baseline"
UNRECOVERABLE_TABLE = "pa_unrecoverable"

#: Explains the one way an operator can fix this, which is running the migrations.
BASELINE_TABLE_MISSING = f"{BASELINE_TABLE} is absent; run the migrations"


def _missing_columns(columns: set[str], required: tuple[str, ...]) -> list[str]:
    """Name the required columns a table does not have.

    ``source`` is matched against ``data_source`` because the local schema and the
    deployed one spell it differently and the check's SQL is rewritten for
    whichever is present, so its absence from ``columns`` is not a gap.
    """
    return [
        column for column in required if column not in columns and not (column == "source" and "data_source" in columns)
    ]


def _table_exists(conn: Connection, table: str) -> bool:
    """Return whether a record table is present.

    Both tables come from the migrations. Creating them here would split the
    schema between the migration history and the code that reads it, and a
    read-only connection cannot do it anyway -- so an absent table is reported as
    "not compared" rather than papered over.
    """
    return table in set(inspect(conn).get_table_names())


def _delete_baseline_scope(conn: Connection, *, season: int | None) -> None:
    """Remove a record so a later one can be written in its place.

    A season-scoped write removes only that season's rows; a whole-history write
    removes every row. Merging instead would keep naming rows that have been
    repaired, which makes the comparison unable to notice repairs at all.
    """
    if season is None:
        conn.execute(text("DELETE FROM pa_violation_baseline"))
        return
    conn.execute(
        text("DELETE FROM pa_violation_baseline WHERE scope_season = :scope"),
        {"scope": season},
    )


def write_baseline_to_db(conn: Connection, *, season: int | None = None) -> dict[str, Any]:
    """Record the rows currently failing, replacing any record for this scope.

    The write replaces rather than merges so a backlog that shrank reads as
    smaller; a merge would keep reporting rows that have been repaired and make
    the comparison incapable of noticing the other direction either.

    Each row keeps the season it was found in. Recording a scope and storing a
    bare player id inside it cannot answer a narrower question: 452 of the 760
    players fail in more than one season, so a ``--year`` run would see every
    other season's row as a repair. The season is what lets a season run select
    exactly the rows it is being compared against.

    Storing the record rather than writing a file is what makes this usable: the
    scheduler runs on a different host from the one holding the repository, so a
    repository-relative file would leave the daily gate permanently uncompared.
    """
    if not _table_exists(conn, BASELINE_TABLE):
        raise BaselineUnreadableError(BASELINE_TABLE_MISSING)
    stamp = datetime.now(KST)
    recorded: dict[str, Any] = {
        "version": 2,
        "scope": {"season": season},
        "recorded_at": stamp.isoformat(),
        "checks": {},
    }

    _delete_baseline_scope(conn, season=season)
    for check_id in sorted(_BASELINE_CHECK_IDS):
        rows = _violated_rows(conn, check_id, season=season)
        recorded["checks"][check_id] = {
            "rows": [{"season": row_season, "player_id": player_id} for row_season, player_id in rows],
        }
        for row_season, player_id in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO pa_violation_baseline (check_id, scope_season, player_id, recorded_at)
                    VALUES (:check_id, :scope, :player_id, :recorded_at)
                    """,
                ),
                {
                    "check_id": check_id,
                    "scope": row_season,
                    "player_id": int(player_id),
                    "recorded_at": stamp,
                },
            )
    return recorded


def compare_against_baseline_db(conn: Connection, *, season: int | None = None) -> BaselineVerdict:
    """Diff the current violations against the recorded ones, in the database.

    An absent table or an absent scope is reported as "not compared". Reading a
    missing record as an empty one would report the whole historical backlog as
    new damage on the first night, and those counts are subtracted from a score.
    """
    if not _table_exists(conn, BASELINE_TABLE):
        return BaselineVerdict((), (), (), unknown_checks=(), baseline_available=False)

    current: dict[str, set[int]] = {}
    now: set[str] = set()
    for check_id in sorted(_BASELINE_CHECK_IDS):
        rows_for_check = _violated_rows(conn, check_id, season=season)
        current[check_id] = {row_season for row_season, _ in rows_for_check}
        now.update(player_id for _, player_id in rows_for_check)

    recorded: list[tuple[str, int, int]] = []
    with contextlib.suppress(Exception):  # a missing table means not compared
        recorded = [
            (str(row[0]), int(row[1]), int(row[2]))
            for row in conn.execute(text("SELECT check_id, scope_season, player_id FROM pa_violation_baseline")).all()
        ]

    if not recorded:
        # No rows is an absent record, not an empty one. Reading it as empty
        # would report every historical violation as new damage on the first run,
        # and these counts are subtracted from a score.
        return BaselineVerdict(
            new_violations=(),
            baseline_violations=(),
            resolved_violations=(),
            unknown_checks=(),
            baseline_available=False,
        )

    if season is None:
        # A record taken for one season cannot answer a whole-history run: every
        # other season's violations would read as new damage, and those counts are
        # deducted from the quality score.
        recorded_seasons = {row_season for _, row_season, _ in recorded}
        failing_now = set().union(*current.values()) if current else set()
        if not failing_now <= recorded_seasons:
            return BaselineVerdict(
                new_violations=(),
                baseline_violations=(),
                resolved_violations=(),
                unknown_checks=(),
                is_scope_mismatch=True,
                baseline_available=False,
            )

    before_rows = {str(player_id) for _, row_season, player_id in recorded if season is None or row_season == season}
    recorded_checks = {check_id for check_id, _, _ in recorded}
    return BaselineVerdict(
        new_violations=tuple(sorted(now - before_rows)),
        baseline_violations=tuple(sorted(now & before_rows)),
        resolved_violations=tuple(sorted(before_rows - now)),
        unknown_checks=tuple(sorted(recorded_checks - set(_BASELINE_CHECK_IDS))),
    )


def write_unrecoverable_to_db(conn: Connection) -> dict[str, Any]:
    """Record the season rows that cannot be repaired, in the database.

    Stored beside the baseline for the same reason: the scheduler does not run
    where the repository is checked out, so a file would make the record exist on
    the machine that wrote it and nowhere else.
    """
    if not _table_exists(conn, UNRECOVERABLE_TABLE):
        return {"version": 1, "rows": []}
    rows = _unrecoverable_rows(conn)
    conn.execute(text("DELETE FROM pa_unrecoverable"))
    for row in rows:
        conn.execute(
            text(
                """
                INSERT INTO pa_unrecoverable
                    (player_id, season, league, source, reason, detail, recorded_at)
                VALUES (:player_id, :season, :league, :source, :reason, :detail, :recorded_at)
                """,
            ),
            {
                "player_id": int(row["player_id"]),
                "season": row["season"],
                "league": row["league"],
                "source": row["source"],
                "reason": row["reason"],
                "detail": row["detail"],
                "recorded_at": datetime.now(KST),
            },
        )
    return {"version": 1, "rows": [dict(row) for row in rows]}


def compare_unrecoverable_db(conn: Connection) -> UnrecoverableVerdict:
    """Report rows that became unrecoverable since the last record.

    A row that can no longer be repaired is a different event from one that never
    could be, and the crawler that loses counts is still running, so this is where
    damage of that shape surfaces.
    """
    recorded: set[str] = set()
    with contextlib.suppress(Exception):  # a missing table means nothing was recorded
        recorded = {str(row[0]) for row in conn.execute(text("SELECT player_id FROM pa_unrecoverable")).all()}
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
    #: Other tables the SQL reads, which must exist for it to run at all.
    #:
    #: ``required_columns`` covers the owning table only. A check that joins
    #: another one -- to compare against, say -- fails with "no such table" when
    #: that table is absent, which turns a partial schema into a collection
    #: error for every test and every partially migrated database. Naming the
    #: table here lets the pack report the check as unmeasurable instead, which
    #: is what every other missing-column case already does.
    referenced_tables: tuple[str, ...] = ()

    #: When true, a nonzero violation count is reported as ``warn`` rather than
    #: ``fail``.
    #:
    #: A structural limit is not the same thing as a regression. The pre-2001
    #: seasons have scores but no boxscore bodies at all -- namuwiki publishes
    #: only the result -- so counting that absence as a failure produces a gate
    #: that is red from the moment it is added, which is a gate nobody reads.
    #: ``warn`` keeps the number visible and reported while leaving ``ok`` --
    #: and therefore the incident that ``crawl_daily_games`` raises -- to the
    #: checks that describe something that can go wrong again.
    informational: bool = False


@dataclass(frozen=True)
class _PackOptions:
    target_date: str | None
    season: int | None
    require_schema: bool


def _franchise_case(column: str) -> str:
    """Build a dialect-portable CASE that folds a team code to its franchise.

    Two games are the same game when they are the same matchup, and a matchup is
    named by a franchise rather than by a code. ``NX`` (넥센, 2010-2018), ``WO``
    (우리, 2008-2009) and ``KH`` (키움, 2019-) are one franchise under three
    codes, and so are ``HT``/``KIA`` and ``OB``/``DB``. Grouping on the raw code
    therefore reports two copies of one game as two different matchups, which is
    how this check passed against a database holding 1,161 duplicate pairs while
    reporting 22.

    The mapping is read from ``_TEAM_HISTORY`` rather than written out here, so
    the SQL cannot drift from ``src/utils/team_history.py``. Codes that name no
    franchise -- ``EA`` and ``WE``, the All-Star sides -- deliberately fall to
    ``code:<value>``: they are two halves of one exhibition, not two clubs, and
    folding them onto a shared key would make an All-Star game collide with a
    club game. A code with no mapping is left distinct rather than grouped.

    Args:
        column: The team-code column to normalize.

    Returns:
        A SQL expression yielding ``F<franchise id>`` or ``code:<value>``.

    """
    branches: dict[int, list[str]] = {}
    for entry in iter_team_history():
        branches.setdefault(entry.franchise_id, []).append(entry.team_code.upper())
    whens = " ".join(
        f"WHEN '{code}' THEN 'F{franchise_id}'"
        for franchise_id, codes in sorted(branches.items())
        for code in sorted(codes)
    )
    return f"(CASE UPPER({column}) {whens} ELSE 'code:' || UPPER({column}) END)"


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
    #
    # Scoped to ``season >= 2001`` because before that year no lawful source for
    # the sacrifices exists at all: ``game_batting_stats`` holds no pre-2001 row,
    # so nothing on this side can be re-derived and the subset is a structural
    # limit rather than a defect. Those rows are reported by the ``_pre2001``
    # check below as a warning instead. Leaving them here kept the whole pack red
    # from the day it was added, which is how a gate stops being read -- and the
    # red was standing backlog, not the thing a gate is for.
    #
    # What remains is work that can actually be done: postseason and exhibition
    # rows written by the ``CRAWLER`` lineage, most of them with game-level rows
    # a backfill could derive the sacrifices from. Keeping the season in the
    # predicate is also what lets a baseline watch this half for growth without
    # 2,122 permanent rows drowning the signal.
    #
    # ``season`` is in ``required_columns`` on both halves so a database without
    # the column reports them as unmeasurable rather than counting every row on
    # one side of the split.
    _SqlCheck(
        check_id="player_season_batting_unaccounted_pa",
        description="Season batting PA exceeds AB + BB + HBP with no sacrifice recorded (2001 or later)",
        table="player_season_batting",
        required_columns=(
            "player_id",
            "season",
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
            WHERE season >= 2001
              AND COALESCE(plate_appearances, 0) >
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                AND COALESCE(sacrifice_hits, 0) = 0
                AND COALESCE(sacrifice_flies, 0) = 0
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE season >= 2001
              AND COALESCE(plate_appearances, 0) >
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                AND COALESCE(sacrifice_hits, 0) = 0
                AND COALESCE(sacrifice_flies, 0) = 0
            LIMIT 5
        """,
    ),
    # The pre-2001 half of the check above, split out rather than repaired. The
    # rows are real -- PA is populated and both sacrifice columns are empty -- but
    # the source that would fill them is gone: the season pages carry team totals
    # only and ``game_batting_stats`` has no pre-2001 row to re-derive from, so a
    # value written here would be invented rather than recovered. ``warn`` keeps
    # the number on the report without letting a permanent backlog raise the daily
    # incident, the same treatment ``game_result_without_body`` already gets for
    # the pre-2001 boxscore gap.
    #
    # It is a separate check rather than an exemption inside the one above so the
    # two halves stay disjoint: ``unaccounted_pa`` plus this check plus
    # ``unreconciled_pa`` and ``missing_pa`` split the whole season backlog, and no
    # row is counted on both sides of the ``season`` boundary.
    _SqlCheck(
        check_id="player_season_batting_unaccounted_pa_pre2001",
        description="Season batting PA exceeds AB + BB + HBP with no sacrifice recorded (pre-2001, source-limited)",
        table="player_season_batting",
        required_columns=(
            "player_id",
            "season",
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
            WHERE season < 2001
              AND COALESCE(plate_appearances, 0) >
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                AND COALESCE(sacrifice_hits, 0) = 0
                AND COALESCE(sacrifice_flies, 0) = 0
        """,
        sample_sql="""
            SELECT COALESCE(CAST(player_id AS TEXT), 'NULL')
            FROM player_season_batting
            WHERE season < 2001
              AND COALESCE(plate_appearances, 0) >
                COALESCE(at_bats, 0) + COALESCE(walks, 0) + COALESCE(hbp, 0)
                AND COALESCE(sacrifice_hits, 0) = 0
                AND COALESCE(sacrifice_flies, 0) = 0
            LIMIT 5
        """,
        informational=True,
    ),
    # The two checks above covered 2,572 of 2,603 season violations and reported
    # the pack clean on the remaining 31 without naming them. This one and the
    # next divide those 31 by what the row is actually missing, and together with
    # the two ``unaccounted_pa`` halves they split the whole 2,603 -- 2,122
    # source-limited pre-2001 rows and 427 / 23 / 31 that can still be worked --
    # so no row is counted twice and none goes unreported.
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
    # A finished game with inning lines and no team total is a total that was
    # missed, not one that does not exist. The innings are written once per
    # inning, so the total is arithmetic on data already stored -- and the save
    # path fills it on the next crawl.
    #
    # 2026-08-20 produced five of these at once, all COMPLETED, all with complete
    # inning lines: away 10 home 6, away 16 home 4, away 4 home 5, away 6 home 4,
    # away 1 home 7. The batch that wrote the innings on 2026-08-26 wrote no
    # totals and reported success. 365 stored games are in this state.
    #
    # Cancelled games are excluded: they legitimately have no total even when
    # innings were recorded before the call-off. A game with no innings is
    # excluded too -- it has not been crawled, so there is no evidence that a
    # total should exist.
    #
    # The innings must also look like a whole game, for the same reason the save
    # path refuses a short line. The away side always bats nine innings and the
    # home side bats nine or one fewer -- it does not bat the bottom ninth when it
    # already leads, and extra innings stay equal. A line shorter than that is a
    # partial crawl, so the innings do not prove what the final total was, and
    # counting the game here would claim evidence the rows do not carry. Exactly
    # one stored game is in that state (20230404LTSK0, seven innings each side);
    # its innings are the defect, and audit_historical_invariants reports that.
    _SqlCheck(
        check_id="game_score_missing_with_innings",
        description="Finished games with inning lines should carry both team totals",
        table="game",
        required_columns=("game_id", "game_status", "home_score", "away_score"),
        referenced_tables=("game_inning_scores",),
        count_sql="""
            SELECT COUNT(*)
            FROM game
            WHERE is_primary
              AND game_status IN ('COMPLETED', 'FINAL', 'DRAW', '종료')
              AND (home_score IS NULL OR away_score IS NULL)
              AND EXISTS (
                  SELECT 1 FROM game_inning_scores s
                  WHERE s.game_id = game.game_id
                  GROUP BY s.game_id
                  HAVING COUNT(DISTINCT s.team_side) = 2
                     AND COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END) >= 9
                     AND COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END)
                       - COUNT(DISTINCT CASE WHEN s.team_side = 'home' THEN s.inning END) IN (0, 1)
              )
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game.game_id AS TEXT), ''), game.game_status
            FROM game
            WHERE game.is_primary
              AND game.game_status IN ('COMPLETED', 'FINAL', 'DRAW', '종료')
              AND (game.home_score IS NULL OR game.away_score IS NULL)
              AND EXISTS (
                  SELECT 1 FROM game_inning_scores s
                  WHERE s.game_id = game.game_id
                  GROUP BY s.game_id
                  HAVING COUNT(DISTINCT s.team_side) = 2
                     AND COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END) >= 9
                     AND COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END)
                       - COUNT(DISTINCT CASE WHEN s.team_side = 'home' THEN s.inning END) IN (0, 1)
              )
            LIMIT 5
        """,
    ),
    # The other direction, and the reason the save path only fills a blank rather
    # than overwriting: a stated total that contradicts its own innings is the
    # only trace that two parts of the source page disagreed. Overwriting it with
    # a sum would remove the evidence and record a number nobody read.
    _SqlCheck(
        check_id="game_score_disagrees_with_innings",
        description="A team total should equal the sum of that team's innings",
        table="game",
        required_columns=("game_id", "home_score", "away_score"),
        referenced_tables=("game_inning_scores",),
        count_sql="""
            SELECT COUNT(*)
            FROM game
            WHERE is_primary
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM game_inning_scores s
                  WHERE s.game_id = game.game_id
                  GROUP BY s.game_id
                  HAVING COUNT(DISTINCT s.team_side) = 2
              )
              AND (
                    home_score <> (
                        SELECT SUM(s.runs) FROM game_inning_scores s
                        WHERE s.game_id = game.game_id AND s.team_side = 'home'
                    )
                 OR away_score <> (
                        SELECT SUM(s.runs) FROM game_inning_scores s
                        WHERE s.game_id = game.game_id AND s.team_side = 'away'
                    )
              )
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game.game_id AS TEXT), ''), game.home_score, game.away_score
            FROM game
            WHERE game.is_primary
              AND game.home_score IS NOT NULL
              AND game.away_score IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM game_inning_scores s
                  WHERE s.game_id = game.game_id
                  GROUP BY s.game_id
                  HAVING COUNT(DISTINCT s.team_side) = 2
              )
              AND (
                    game.home_score <> (
                        SELECT SUM(s.runs) FROM game_inning_scores s
                        WHERE s.game_id = game.game_id AND s.team_side = 'home'
                    )
                 OR game.away_score <> (
                        SELECT SUM(s.runs) FROM game_inning_scores s
                        WHERE s.game_id = game.game_id AND s.team_side = 'away'
                    )
              )
            LIMIT 5
        """,
    ),
    # A game cannot be finished in fewer than five innings. Five is the
    # league's own minimum for a result to exist: below it there is no winner to
    # record, which is why such a game is declared a no-game and replayed.
    #
    # This check exists because the database has no vocabulary for that verdict.
    # 20260830LGLT0 was declared a no-game by the umpire after three innings of
    # rain -- the declaration is stored in its own play-by-play, "3회말 1번타순
    # 3구 후 우천으로 인한 노게임 선언" -- yet it sits as COMPLETED carrying the
    # partial 0-2 line as a result, and both teams count a game that produced
    # none. Neither game_status nor the schedule parser knows what a no-game is,
    # so nothing upstream can stop the next one.
    #
    # The check reads the shape rather than the verdict, and that is the point:
    # a wrong score, a wrong status and a lost body all end in the same
    # impossible shape, so none of them hides. It also catches the one row's
    # repair being undone -- see scripts/maintenance/repair_no_game_declared.py,
    # which records why that reversal is possible -- and that is what makes this
    # a tripwire instead of a statement about one game.
    #
    # Both sides must have innings. A game with no innings at all is a
    # different gap, and game_result_without_body already reports it; counting
    # it here as well would report the same rows under two headings.
    #
    # The floor is five rather than nine. A game called by weather between five
    # and eight innings is a real result, and 23 stored games sit in that range
    # as legitimate called games, so requiring nine would report them all as
    # defects. Only below five is a result structurally impossible, which is
    # also why the threshold cannot be triggered by a valid game.
    #
    # The difference clause covers what the floor alone cannot. The away side
    # always bats at least as many innings as the home side, and the home side
    # at most one fewer, so six away against three home is impossible however
    # long the line is. No stored game has that shape, so the branch is covered
    # synthetically rather than by production data.
    #
    # Years 2024 and 2025 pad every game to twelve innings with three zero
    # extra slots, so a truncated line there reads exactly like a complete one
    # and this check is blind for those two seasons. That blindness is not
    # fixable by a predicate: a game called after five and a crawl that lost the
    # ninth are the same twelve rows.
    #
    # The status list carries the two dead literals 'FINAL' and '종료' that its
    # neighbouring checks also carry. Neither appears on a stored row today, but
    # the set is the shape a future writer would most plausibly introduce, and a
    # check that does not recognise it would report zero while the defect it
    # exists for was happening.
    _SqlCheck(
        check_id="game_final_with_too_few_innings",
        description="Finished games should have at least five innings per side",
        table="game",
        required_columns=("game_id", "game_status", "is_primary"),
        referenced_tables=("game_inning_scores",),
        count_sql="""
            SELECT COUNT(*)
            FROM game
            WHERE is_primary
              AND game_status IN ('COMPLETED', 'FINAL', 'DRAW', '종료')
              AND EXISTS (
                  SELECT 1 FROM game_inning_scores s
                  WHERE s.game_id = game.game_id
                  GROUP BY s.game_id
                  HAVING COUNT(DISTINCT s.team_side) = 2
                     AND NOT (
                         COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END) >= 5
                         AND COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END)
                           - COUNT(DISTINCT CASE WHEN s.team_side = 'home' THEN s.inning END) IN (0, 1)
                     )
              )
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game.game_id AS TEXT), ''), game.game_status
            FROM game
            WHERE game.is_primary
              AND game.game_status IN ('COMPLETED', 'FINAL', 'DRAW', '종료')
              AND EXISTS (
                  SELECT 1 FROM game_inning_scores s
                  WHERE s.game_id = game.game_id
                  GROUP BY s.game_id
                  HAVING COUNT(DISTINCT s.team_side) = 2
                     AND NOT (
                         COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END) >= 5
                         AND COUNT(DISTINCT CASE WHEN s.team_side = 'away' THEN s.inning END)
                           - COUNT(DISTINCT CASE WHEN s.team_side = 'home' THEN s.inning END) IN (0, 1)
                     )
              )
            LIMIT 5
        """,
    ),
    # A cancelled game has no result, so a row carrying both scores is asserting
    # something that did not happen. The 2001+ rows that do this are the small
    # residue of finished matches whose terminal state was overwritten, and the
    # 1983-2000 rows were 7,995 of them at once -- written in half an hour on
    # 2026-09-23 with no entry in the crawl ledger, so no crawler produced them
    # and no status-derivation rule explains them.
    #
    # This check is the observation that was missing when that happened. It does
    # not prevent it and cannot repair it: the three derive_stable_game_status
    # callers already hand the database's own scores to the resolver, which
    # returns COMPLETED or DRAW whenever both are present, so a CANCELLED row
    # carrying scores did not come through that path. The writer was never found,
    # and guessing at one would add a guard with no verified failure behind it.
    #
    # is_primary is scoped on purpose. The 241 superseded 1982 duplicate rows
    # were left CANCELLED on purpose when they were retired -- they carry the
    # score of a game that exists, which is exactly this shape. Reporting them
    # would mean this check starts failing before anything has happened again,
    # and a gate that is red on arrival is a gate nobody reads.
    # A cancelled game has no result, so a row carrying both scores is asserting
    # something that did not happen. The 2001+ rows that did this were the small
    # residue of fixtures whose terminal state was written as CANCELLED at load
    # time: all-star games, Korean Series games and the 2024 World Baseball
    # Classic, none of which ever had a boxscore collected.
    #
    # The 1983-2000 rows that once made this gate 7,995 violations wide are no
    # longer here either. Both sets were the same fact -- result present, body
    # absent -- and were relabelled UNRESOLVED_MISSING on 2026-10-16, which is the
    # state the status means and the one derive_stable_game_status cannot produce
    # because it only sees scores.
    #
    # The era is deliberately absent from the predicate. Scoping this check to
    # 2001+ while the historical rows were mislabelled was a way to expose the
    # 50; with both sets relabelled it would be the opposite of a tripwire, since
    # the failure this check was written for happened entirely before 2001 and a
    # recurrence there would pass unnoticed. A row that carries both scores is a
    # result, and a result cannot be cancelled, in any decade.
    #
    # is_primary is scoped on purpose. The 241 superseded 1982 duplicate rows
    # were left CANCELLED on purpose when they were retired -- they carry the
    # score of a game that exists, which is exactly this shape. Reporting them
    # would mean this check starts failing before anything has happened again,
    # and a gate that is red on arrival is a gate nobody reads.
    _SqlCheck(
        check_id="game_cancelled_with_scores",
        description="Cancelled games should not carry both team scores",
        table="game",
        required_columns=("game_id", "game_status", "home_score", "away_score", "is_primary"),
        count_sql="""
            SELECT COUNT(*)
            FROM game
            WHERE is_primary
              AND game_status = 'CANCELLED'
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game
            WHERE is_primary
              AND game_status = 'CANCELLED'
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
            LIMIT 5
        """,
    ),
    # The result is known but the boxscore body was never collected, so lineups,
    # innings and boxscore rows are all 0. This is a property of the source, not
    # a regression, and it is exactly the gap that let a 09-23 status sweep reach
    # 2,848 rows without anything recording it.
    #
    # The era is deliberately not part of the predicate. It began as a pre-2001
    # check because namuwiki publishes only the result for 1982-2000, but the
    # same shape exists in the modern era and is the same fact: 50 games between
    # 2011 and 2026 -- all-star fixtures, Korean Series games and the 2024 World
    # Baseball Classic -- carry a score and no body because no boxscore was ever
    # collected for them. Scoping this to 2001 would have let those 50 fall out of
    # the report the moment they were relabelled UNRESOLVED_MISSING, which is how
    # a gate loses the rows it exists to watch. A structural limit is not a
    # failure in any decade, so the era does not change the grade.
    #
    # The body test spans four tables rather than one. A game can carry lineups
    # or innings with no batting rows, and calling that "no body" would report a
    # gap that is not there -- the four-table test is the same one the 2026-10-16
    # classification of these rows used.
    #
    # is_primary is scoped for the same reason as on the check above, and because
    # it keeps this a stable number. Without it the 241 retired 1982 duplicates
    # are counted here as well, so retiring them would move a monitoring figure
    # that is supposed to describe a source limit rather than our housekeeping.
    _SqlCheck(
        check_id="game_result_without_body",
        description="Games carry a result but no boxscore body (source limit)",
        table="game",
        required_columns=("game_id", "game_status", "home_score", "away_score", "is_primary"),
        referenced_tables=("player_game_batting", "game_lineups", "game_inning_scores", "game_batting_stats"),
        informational=True,
        count_sql="""
            SELECT COUNT(*)
            FROM game
            WHERE is_primary
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM player_game_batting b WHERE b.game_id = game.game_id)
              AND NOT EXISTS (SELECT 1 FROM game_lineups l WHERE l.game_id = game.game_id)
              AND NOT EXISTS (SELECT 1 FROM game_inning_scores i WHERE i.game_id = game.game_id)
              AND NOT EXISTS (SELECT 1 FROM game_batting_stats s WHERE s.game_id = game.game_id)
        """,
        sample_sql="""
            SELECT COALESCE(CAST(game_id AS TEXT), '')
            FROM game
            WHERE is_primary
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM player_game_batting b WHERE b.game_id = game.game_id)
              AND NOT EXISTS (SELECT 1 FROM game_lineups l WHERE l.game_id = game.game_id)
              AND NOT EXISTS (SELECT 1 FROM game_inning_scores i WHERE i.game_id = game.game_id)
              AND NOT EXISTS (SELECT 1 FROM game_batting_stats s WHERE s.game_id = game.game_id)
            LIMIT 5
        """,
    ),
    # Two rows describing one game under two different game_id values.
    #
    # The doubleheader suffix is part of the key, not an afterthought. A season
    # contains 1,000 groups that share a date and both team codes, and every one
    # of them is a legitimate doubleheader: 2010-05-18 played LG-HT twice, at 잠실
    # and at 대전, and those are two games. Keying on date and teams alone would
    # report 1,000 violations that are all real baseball. Keying on the suffix
    # as well leaves the genuine duplicate, which is the same game reached through
    # a different team order -- 19820327MBSS0 and 19820327SSMB0.
    #
    # That shape is not a hypothetical. The 1982 corpus was loaded once with the
    # team order the answer set carries and once with the order
    # build_standard_game_id produces, which is the opposite convention, so all
    # 241 games of the season existed twice. The two halves agree on date, teams
    # and score and differ only in the identifier. Any tool that recomputes a
    # game_id from the teams will therefore re-create them, which is why this
    # check exists next to the data rather than inside the data.
    #
    # The unit is the pair, not the row, so the count is comparable against the
    # known backlog. Grouping on the raw team code reported 22 groups; grouping on
    # the franchise reports 1,161 -- 32 from 2010 and the rest from 2011-2018,
    # where every Heroes game exists twice, once under the era code and once
    # under the current one.
    #
    # That gap is the reason the keys are folded through _franchise_case. NX and
    # KH are one franchise, so a group whose two rows differ only in that column
    # is one game reached twice, and a check that cannot see it reports 22
    # against a database holding 1,161. The stored home_franchise_id and
    # away_franchise_id columns cannot be used for the same purpose: they are
    # NULL on 165 rows and wrong on others (20100310KIAHH0 carries
    # away_franchise_id 11, which is the Heroes franchise, while its away_team is
    # KIA). The code is the value the loader actually wrote, so the code is what
    # gets normalized.
    #
    # The last character is taken as SUBSTR(game_id, LENGTH(game_id), 1) and not
    # as SUBSTR(game_id, -1). The negative two-argument form returns the whole
    # string on PostgreSQL and only the last character on SQLite, so the shorter
    # spelling reads correctly against the SQLite the unit tests run on and
    # silently groups by nothing in production. This check was written that way,
    # passed its tests, and reported 0 duplicate pairs against a database that
    # has 22. Any cross-dialect function in this file needs the same care.
    #
    # This one is a failure rather than a warning. Unlike a missing boxscore,
    # which is what the source does not publish and no crawl can change, a
    # duplicate is ours: it was created by loading, it inflates every count that
    # groups by game, and deleting it loses nothing.
    _SqlCheck(
        check_id="game_duplicate_identifiers",
        description="Games present twice under different game_id values (counted per pair)",
        table="game",
        required_columns=("game_id", "game_date", "home_team", "away_team"),
        count_sql=f"""
            SELECT COUNT(*)
            FROM (
                SELECT 1
                FROM game
                WHERE game_date IS NOT NULL
                  AND home_team IS NOT NULL
                  AND away_team IS NOT NULL
                GROUP BY game_date, {_franchise_case("home_team")}, {_franchise_case("away_team")},
                         SUBSTR(game_id, LENGTH(game_id), 1)
                HAVING COUNT(*) > 1 AND COUNT(DISTINCT game_id) > 1
            ) duplicated
        """,  # noqa: S608 - fragment is generated from iter_team_history, not input
        sample_sql=f"""
            SELECT MIN(game_id)
            FROM game
            WHERE game_date IS NOT NULL
              AND home_team IS NOT NULL
              AND away_team IS NOT NULL
            GROUP BY game_date, {_franchise_case("home_team")}, {_franchise_case("away_team")},
                     SUBSTR(game_id, LENGTH(game_id), 1)
            HAVING COUNT(*) > 1 AND COUNT(DISTINCT game_id) > 1
            LIMIT 5
        """,  # noqa: S608 - fragment is generated from iter_team_history, not input
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
    missing_columns = _missing_columns(columns, (*check.required_columns, *scope_columns))
    if not missing_columns and check.referenced_tables:
        available = set(inspect(conn).get_table_names())
        missing_columns = [f"{name} (table)" for name in check.referenced_tables if name not in available]
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
    status = "pass" if violation_count == 0 else ("warn" if check.informational else "fail")
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
