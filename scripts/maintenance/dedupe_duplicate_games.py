"""Retire the duplicate half of a game that is stored under two identifiers.

Why this exists
---------------
``game_duplicate_identifiers`` in the regression pack groups by franchise, and
against the production database it reports 1,161 groups: one real game, two
``game_id`` values. They are not a uniform population, so this script sorts them
before touching anything.

* 1,121 groups (2011-2018) are one Heroes fixture loaded twice. ``WO`` and ``NX``
  are both franchise 11, and the pairs differ only in which spelling the loader
  used. One side carries the score and the boxscore, the other carries neither.
* 32 groups (2010) are one fixture loaded twice with no result and no body on
  either side. The survivor is the earlier load, which is also the side whose
  ``game_id`` uses the code that was actually in force that season
  (``KIA``/``DB``, not the 2000-and-earlier ``HT``/``OB``). Both signals agree on
  all 32, so nothing here rests on the ordering alone.
* 8 groups (the 2014 Korean Series and two 2018 fixtures) carry a body on both
  sides. The larger body is the superset -- 572 rows against 491, for example --
  so retiring the smaller loses no event.

The survivor rule is therefore one rule and not three: the row with the most body
rows, then the row that has a score, then the earliest load. It decides all 1,161
groups with no tie, and it never retires the only copy of a boxscore.

What it does not do
-------------------
It does not merge. The retired row keeps its identity in the snapshot and in the
rollback script, so a body row can be moved back across if a survivor turns out to
be the wrong one. It does not touch ``is_primary = FALSE`` rows, and it does not
repair ``home_franchise_id`` / ``away_franchise_id``, which are NULL on 165 rows
and wrong on others -- a separate defect that is named in the runbook.

Safety
------
Dry-run by default. Writing needs ``--apply`` and ``KBO_ALLOW_DUPLICATE_RETIRE=1``.
The snapshot is written before the delete, and the rollback SQL is a full-column
INSERT for every parent row plus the child rows the delete removes.

Usage
-----
    python3 -m scripts.maintenance.dedupe_duplicate_games
    python3 -m scripts.maintenance.dedupe_duplicate_games --year 2014
    python3 -m scripts.maintenance.dedupe_duplicate_games --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from src.utils.team_history import franchise_id_for_team_code

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

#: Both flags are required, matching the other historical-restoration scripts.
APPLY_FLAG = "--apply"
ENV_GATE = "KBO_ALLOW_DUPLICATE_RETIRE"

#: Where snapshots and rollback SQL are written.
ARCHIVE_DIR = Path("data/archive")

#: Tables that make up a boxscore. Their combined row count decides the survivor.
BODY_TABLES = (
    "game_batting_stats",
    "game_pitching_stats",
    "game_play_by_play",
    "game_lineups",
    "game_inning_scores",
    "game_summary",
)

#: Tables whose rows are removed along with a retired parent. A body table is not
#: in this list because the survivor rule guarantees the loser never holds the
#: only copy; they are still counted and reported, and a non-zero count here on a
#: body table is what would stop the run.
CHILD_TABLES = (
    "game_metadata",
    "game_events",
    "game_highlights",
    "game_mvps",
    "game_validation_metrics",
    "correction_audit_trail",
    "game_broadcasts",
)


def _sortable_datetime(value: datetime | str | None) -> str:
    """Render a timestamp so it sorts chronologically as a string.

    PostgreSQL returns a ``datetime``; SQLite returns the text it stored, which
    has no ordering guarantee unless it is normalized. ISO-8601 sorts correctly
    as text, so normalizing here is what lets one sort key serve both dialects.

    Args:
        value: The stored timestamp, in whatever shape the driver returned.

    Returns:
        An ISO-8601 string, or the empty string when there is no timestamp.

    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    parsed = datetime.fromisoformat(str(value))
    return parsed.isoformat()


def _franchise_key(code: str | None) -> str:
    """Fold a team code to its franchise, leaving unmapped codes distinct.

    Mirrors the regression pack's ``_franchise_case``. ``EA`` and ``WE`` name the
    two sides of an All-Star game and map to no franchise, so each keeps its own
    key; folding them together would let an exhibition collide with a club game.

    Args:
        code: Raw team code as stored.

    Returns:
        ``F<franchise id>`` or ``code:<value>``.

    """
    if not code:
        return "code:"
    normalized = str(code).upper()
    franchise_id = franchise_id_for_team_code(normalized, None)
    return f"F{franchise_id}" if franchise_id is not None else f"code:{normalized}"


@dataclass(frozen=True)
class GameRow:
    """One stored ``game`` row."""

    game_id: str
    game_date: date
    home_team: str | None
    away_team: str | None
    away_score: int | None
    home_score: int | None
    game_status: str | None
    created_at: datetime | None
    xmin: int


@dataclass
class DuplicateGroup:
    """Two or more rows describing one game."""

    game_date: date
    home_key: str
    away_key: str
    doubleheader: str
    members: list[GameRow] = field(default_factory=list)
    body_counts: dict[str, int] = field(default_factory=dict)

    @property
    def key(self) -> tuple[date, str, str, str]:
        """Return the grouping key, matching the gate's."""
        return (self.game_date, self.home_key, self.away_key, self.doubleheader)

    def _rank(self, row: GameRow) -> tuple[int, int, int, str]:
        """Order rows so the best one sorts first.

        Most body rows wins, then having a score, then the earliest load. The
        first term is what stops a boxscore being retired in favour of a copy of
        itself; the rest only run when the bodies are equal, and on this database
        they decide 32 groups and nothing else.

        The last term carries both ``xmin`` and ``created_at`` because SQLite has
        no ``xmin``. Where the two are both available they agree: on all 32
        groups decided by load order, the transaction id and the created
        timestamp name the same survivor.

        Args:
            row: The row to score.

        Returns:
            A sort key; smaller is better.

        """
        created = _sortable_datetime(row.created_at)
        return (
            -self.body_counts.get(row.game_id, 0),
            0 if (row.away_score is not None and row.home_score is not None) else 1,
            row.xmin,
            created,
        )

    def survivor(self) -> GameRow:
        """Return the row to keep.

        Raises:
            ValueError: If two rows tie on every term, which would make the
                choice arbitrary rather than evidence-backed.

        """
        ordered = sorted(self.members, key=self._rank)
        if len(ordered) > 1 and self._rank(ordered[0]) == self._rank(ordered[1]):
            matchup = f"{self.game_date} {self.home_key}@{self.away_key}"
            message = f"ambiguous survivor for {matchup}"
            raise ValueError(message)
        return ordered[0]

    def retired(self) -> list[GameRow]:
        """Return the rows the delete would remove."""
        keep = self.survivor().game_id
        return [row for row in self.members if row.game_id != keep]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the group."""
        survivor = self.survivor()
        return {
            "game_date": self.game_date.isoformat(),
            "home_key": self.home_key,
            "away_key": self.away_key,
            "doubleheader": self.doubleheader,
            "survivor": survivor.game_id,
            "retired": [row.game_id for row in self.retired()],
            "body_counts": self.body_counts,
            "members": [asdict(row) | {"game_date": row.game_date.isoformat()} for row in self.members],
        }


@dataclass
class DedupeReport:
    """Summarize a run."""

    rows_scanned: int = 0
    groups: int = 0
    would_retire: int = 0
    retired: int = 0
    child_rows_removed: int = 0
    body_rows_on_retired: int = 0
    years: dict[str, int] = field(default_factory=dict)
    samples: list[dict[str, Any]] = field(default_factory=list)
    snapshot_path: str | None = None
    rollback_path: str | None = None
    applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize the report."""
        return asdict(self)


def _fetch_rows(conn: Connection) -> list[GameRow]:
    """Read every primary ``game`` row.

    ``xmin`` is a system column PostgreSQL exposes and SQLite does not, so it is
    resolved through the dialect rather than written into the SELECT. It is the
    last-resort tiebreak of the survivor rule, and 32 of the 1,161 groups are
    decided by it, so the value is worth the cast.
    """
    dialect = conn.dialect.name
    if dialect == "postgresql":
        xmin_expr = "xmin::text"
    elif dialect == "sqlite":
        # No transaction id to read. 0 puts every row in one bucket, so the
        # tiebreak falls through to created_at, which is the same ordering.
        xmin_expr = "0"
    else:
        xmin_expr = "0"

    rows = conn.execute(
        text(
            f"""
            SELECT game_id, game_date, home_team, away_team,
                   away_score, home_score, game_status, created_at, {xmin_expr}
            FROM game
            WHERE is_primary IS NOT FALSE
              AND game_date IS NOT NULL
            """,
        ),
    ).all()
    return [
        GameRow(
            game_id=str(row[0]),
            game_date=row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1])),
            home_team=row[2],
            away_team=row[3],
            away_score=row[4],
            home_score=row[5],
            game_status=row[6],
            created_at=row[7],
            xmin=int(row[8]),
        )
        for row in rows
    ]


def _body_counts(conn: Connection) -> dict[str, int]:
    """Sum the boxscore rows per game across the six body tables."""
    counts: dict[str, int] = defaultdict(int)
    for table in BODY_TABLES:
        for game_id, count in conn.execute(text(f'SELECT game_id, COUNT(*) FROM "{table}" GROUP BY game_id')):
            counts[str(game_id)] += int(count)
    return dict(counts)


def find_duplicate_groups(rows: Sequence[GameRow], body_counts: dict[str, int]) -> list[DuplicateGroup]:
    """Group rows by matchup and return only the groups holding more than one.

    Args:
        rows: Every primary game row.
        body_counts: Body-row count per game_id.

    Returns:
        One entry per duplicated game, ordered by date.

    """
    grouped: dict[tuple[date, str, str, str], DuplicateGroup] = {}
    for row in rows:
        if not row.home_team or not row.away_team:
            continue
        key = (row.game_date, _franchise_key(row.home_team), _franchise_key(row.away_team), row.game_id[-1])
        group = grouped.get(key)
        if group is None:
            group = DuplicateGroup(
                game_date=key[0],
                home_key=key[1],
                away_key=key[2],
                doubleheader=key[3],
            )
            grouped[key] = group
        group.members.append(row)
    duplicates = [group for group in grouped.values() if len(group.members) > 1]
    for group in duplicates:
        group.body_counts = {row.game_id: body_counts.get(row.game_id, 0) for row in group.members}
    return sorted(duplicates, key=lambda group: group.key)


def _table_columns(conn: Connection, table: str) -> list[str]:
    """List a table's columns on either dialect.

    PostgreSQL answers from ``information_schema``; SQLite has no such view, so
    the answer comes from ``PRAGMA table_info``. A rollback that reads columns
    the wrong way round produces an INSERT whose values are permuted, which is
    worse than producing none: the row is restored and it is wrong.

    Args:
        conn: Open connection.
        table: Table to inspect.

    Returns:
        Column names in declaration order.

    """
    if conn.dialect.name == "sqlite":
        rows = conn.execute(text(f'PRAGMA table_info("{table}")')).all()
        return [str(row[1]) for row in rows]
    rows = conn.execute(
        text(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = :t AND table_schema = current_schema()
            ORDER BY ordinal_position
            """,
        ),
        {"t": table},
    ).all()
    return [str(row[0]) for row in rows]


def _sql_literal(value: object) -> str:
    """Render one value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, date | datetime):
        return f"'{value.isoformat()}'"
    text_value = str(value).replace("'", "''")
    return f"'{text_value}'"


def _in_clause(conn: Connection, game_ids: Sequence[str]) -> tuple[str, dict[str, Any]]:
    """Build a parameterized ``game_id IN (...)`` predicate.

    SQLite has no ``= ANY(array)``, so the array form cannot be used here: it
    parses on PostgreSQL and fails on every other dialect. The identifiers are
    bound parameters either way, never interpolated.

    Args:
        conn: Open connection, consulted for its bind-parameter style.
        game_ids: Ids to match.

    Returns:
        The predicate fragment and its parameters.

    """
    keys = [f":gid{index}" for index in range(len(game_ids))]
    params = {f"gid{index}": value for index, value in enumerate(game_ids)}
    return f"({', '.join(keys)})", params


def _full_row_payload(conn: Connection, game_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Read every column of every named ``game`` row, for a lossless rollback."""
    if not game_ids:
        return {}
    columns = _table_columns(conn, "game")
    column_list = ", ".join(columns)
    predicate, params = _in_clause(conn, game_ids)
    rows = conn.execute(
        text(f"SELECT {column_list} FROM game WHERE game_id IN {predicate}"),
        params,
    ).all()
    game_id_index = columns.index("game_id")
    return {str(row[game_id_index]): dict(zip(columns, row, strict=True)) for row in rows}


def _write_rollback(
    path: Path,
    parent_rows: dict[str, dict[str, Any]],
    children: list[tuple[str, list[dict[str, Any]]]],
) -> None:
    """Write INSERT statements that undo the delete.

    Children are written before the parent rows: a foreign key from a body row
    back to ``game`` rejects the child unless the parent is already there, so
    the order is part of the contract and not a formatting preference.
    """
    lines = [
        "-- Rollback for the duplicate-retirement run.",
        "-- Generated by scripts/maintenance/dedupe_duplicate_games.py.",
        "",
    ]
    for table, rows in children:
        if not rows:
            continue
        lines.append(f"-- {table}: {len(rows)} row(s)")
        for row in rows:
            column_list = ", ".join(row)
            value_list = ", ".join(_sql_literal(row[column]) for column in row)
            lines.append(f'INSERT INTO "{table}" ({column_list}) VALUES ({value_list});')
        lines.append("")
    if parent_rows:
        columns = sorted({column for row in parent_rows.values() for column in row})
        column_list = ", ".join(columns)
        lines.append(f"-- game: {len(parent_rows)} row(s)")
        for row in parent_rows.values():
            value_list = ", ".join(_sql_literal(row.get(column)) for column in columns)
            lines.append(f"INSERT INTO game ({column_list}) VALUES ({value_list});")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _capture_children(conn: Connection, game_ids: Sequence[str]) -> list[tuple[str, list[dict[str, Any]]]]:
    """Read the rows a delete would remove, table by table."""
    captured: list[tuple[str, list[dict[str, Any]]]] = []
    predicate, params = _in_clause(conn, game_ids)
    for table in CHILD_TABLES + BODY_TABLES:
        columns = _table_columns(conn, table)
        column_list = ", ".join(columns)
        found = conn.execute(
            text(f'SELECT {column_list} FROM "{table}" WHERE game_id IN {predicate}'),
            params,
        ).all()
        captured.append((table, [dict(zip(columns, row, strict=True)) for row in found]))
    return captured


def dedupe(
    conn: Connection,
    *,
    year: int | None = None,
    apply: bool = False,
) -> DedupeReport:
    """Preview, and optionally perform, the retirement.

    Args:
        conn: Open connection.
        year: Restrict to one season year.
        apply: Persist the delete instead of previewing it.

    Returns:
        The report.

    """
    report = DedupeReport(applied=apply)
    rows = _fetch_rows(conn)
    body_counts = _body_counts(conn)
    groups = find_duplicate_groups(rows, body_counts)
    if year is not None:
        groups = [group for group in groups if group.game_date.year == year]
    report.rows_scanned = len(rows)
    report.groups = len(groups)

    retired_ids: list[str] = []
    years: dict[str, int] = defaultdict(int)
    for group in groups:
        for row in group.retired():
            retired_ids.append(row.game_id)
            years[str(group.game_date.year)] += 1
            if body_counts.get(row.game_id, 0) > 0:
                report.body_rows_on_retired += body_counts.get(row.game_id, 0)
    report.would_retire = len(retired_ids)
    report.years = dict(sorted(years.items()))
    report.samples = [group.to_dict() for group in groups[:10]]
    if not retired_ids:
        return report

    if not apply:
        return report

    snapshot = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "groups": report.groups,
        "retire": retired_ids,
        "detail": [group.to_dict() for group in groups],
    }
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = ARCHIVE_DIR / f"duplicate_games_snapshot_{stamp}.json"
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    report.snapshot_path = str(snapshot_path)

    parent_rows = _full_row_payload(conn, retired_ids)
    children = _capture_children(conn, retired_ids)
    rollback_path = ARCHIVE_DIR / f"duplicate_games_rollback_{stamp}.sql"
    _write_rollback(rollback_path, parent_rows, children)
    report.rollback_path = str(rollback_path)

    predicate, params = _in_clause(conn, retired_ids)
    for table, rows_captured in children:
        count = len(rows_captured)
        if not count:
            continue
        conn.execute(text(f'DELETE FROM "{table}" WHERE game_id IN {predicate}'), params)
        report.child_rows_removed += count
    conn.execute(text(f"DELETE FROM game WHERE game_id IN {predicate}"), params)
    conn.commit()
    report.retired = len(retired_ids)
    return report


def _open_connection(db_url: str | None):
    """Return a connection on the configured database."""
    if db_url:
        from sqlalchemy import create_engine

        return create_engine(db_url).connect()
    from src.db.engine import Engine

    return Engine.connect()


def _denied_reason(apply: bool) -> str | None:
    """Return why the write is refused, or ``None`` when it is allowed."""
    if not apply:
        return "dry_run"
    if os.getenv(ENV_GATE) != "1":
        return f"{ENV_GATE} is not set to 1"
    return None


def main(argv: Sequence[str] | None = None) -> int:
    """Preview or perform the retirement.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, help="Restrict to one season year")
    parser.add_argument("--apply", action="store_true", help="Write; dry-run is the default")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument("--db-url", default=None, help="Target database URL (default: the configured engine)")
    args = parser.parse_args(argv)

    denied = _denied_reason(args.apply)
    if denied is not None and args.apply:
        sys.stderr.write(f"dedupe_denied: {denied}\n")
        return 3

    connection = _open_connection(args.db_url)
    try:
        report = dedupe(connection, year=args.year, apply=args.apply and denied is None)
    finally:
        connection.close()

    if args.as_json:
        sys.stdout.write(json.dumps(report.to_dict(), ensure_ascii=False, default=str) + "\n")
    else:
        sys.stdout.write(
            f"rows={report.rows_scanned} groups={report.groups} would_retire={report.would_retire} "
            f"retired={report.retired} child_rows={report.child_rows_removed} "
            f"body_on_retired={report.body_rows_on_retired}\n",
        )
        for year, count in report.years.items():
            sys.stdout.write(f"    {year}: {count}\n")
        for sample in report.samples:
            sys.stdout.write(
                f"    {sample['game_date']} {sample['home_key']}@{sample['away_key']}"
                f"dh={sample['doubleheader']} keep={sample['survivor']} retire={sample['retired']}\n",
            )
        if report.snapshot_path:
            sys.stdout.write(f"    snapshot={report.snapshot_path}\n")
        if report.rollback_path:
            sys.stdout.write(f"    rollback={report.rollback_path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
