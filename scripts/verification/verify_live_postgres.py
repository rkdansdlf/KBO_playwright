"""Read-only PostgreSQL acceptance check for the weekly live verification.

Replaces ``verify_oci_live_sync.py`` in ``.github/workflows/live_db_verification.yml``.
Oracle ADB has refused connections since 2026-09-27 (DPY-6005) and is being phased
out; production data lands in the PostgreSQL database on the Windows host reached
over Tailscale.

The check never writes. Every check runs in its own transaction that starts with
``SET TRANSACTION READ ONLY``, the engine also opens each session with
``default_transaction_read_only=on``, and nothing is committed. A write attempted
from inside this process fails with ``ReadOnlySqlTransaction`` instead of changing
data.

Checks: connection, role_privileges, migrations, schema, freshness_sync,
freshness_games, core_tables. Exit 0 when no check fails, 1 when a check fails, and
2 on a configuration error (missing URL, or a URL that is not PostgreSQL).

Usage:
    LIVE_DB_URL='postgresql+psycopg://kbo_ci_ro:...@100.81.73.13:5432/bega_prod' python3 scripts/verification/verify_live_postgres.py --json --require-readonly-role

The global engine in ``src.db.engine`` is never used: this script builds its own
engine from the URL it was given. Importing ``apply_postgres_migrations`` still
imports that module, so the workflow also sets ``DATABASE_URL`` to the same
read-only URL to keep it from creating a local SQLite file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, SQLAlchemyError
from sqlalchemy.pool import NullPool

import src.models  # noqa: F401  registers every ORM table on Base.metadata
from src.cli.sync.apply_postgres_migrations import apply_migrations
from src.constants import KST
from src.db.drift_detector import SchemaDriftDetector
from src.db.drift_dto import DriftType
from src.models.base import Base
from src.models.crawl_execution import RUN_STATUS_PARTIAL, RUN_STATUS_SUCCESS
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_POSTPONED,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

ENV_VAR = "LIVE_DB_URL"
EXIT_PASSED = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2
CONNECT_TIMEOUT_SECONDS = 10
READ_ONLY_OPTION = "-c default_transaction_read_only=on"
DEFAULT_MAX_SYNC_AGE_HOURS = 72.0
DEFAULT_MAX_GAME_AGE_DAYS = 10
#: A game that finished on the day it was played can still be waiting on the
#: crawler for this many days; a larger gap means completed results are missing.
COMPLETION_GRACE_DAYS = 2
CORE_TABLES = ("game", "player_basic", "teams", "kbo_seasons", "game_batting_stats", "game_pitching_stats")
MIGRATION_TABLE = "schema_migrations"
WRITE_PRIVILEGES = "INSERT,UPDATE,DELETE,TRUNCATE"
_RECOVERABLE = (SQLAlchemyError, RuntimeError, OSError, ValueError)


class ConfigError(ValueError):
    """The database URL is missing or is not a PostgreSQL URL."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Outcome of one named check.

    ``ok`` is False only for a failure. A ``warning`` leaves ``ok`` True, and a
    ``skipped`` check is also ``ok`` so it cannot fail the run.
    """

    name: str
    ok: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    warning: str | None = None
    skipped: bool = False


@dataclass(frozen=True, slots=True)
class LiveDbReport:
    """Every check result plus the redacted database target."""

    database: str
    timestamp: str
    status: str
    checks: list[CheckResult]


def resolve_url(cli_url: str | None, environ: Mapping[str, str]) -> str:
    """Return the PostgreSQL URL to check, refusing any silent default.

    Args:
        cli_url: Value of ``--url``, if given.
        environ: Environment mapping consulted when ``cli_url`` is empty.

    Returns:
        The stripped URL.

    Raises:
        ConfigError: If no URL is set, it does not parse, or it is not PostgreSQL.

    """
    url = (cli_url or environ.get(ENV_VAR) or "").strip()
    if not url:
        msg = f"no database URL: pass --url or set {ENV_VAR}; this script never falls back to a default database"
        raise ConfigError(msg)
    try:
        backend = make_url(url).get_backend_name()
    except ArgumentError, ValueError:
        # The parser error echoes the URL, and the URL carries the password.
        msg = "the database URL does not parse as a SQLAlchemy URL"
        raise ConfigError(msg) from None
    if backend != "postgresql":
        hint = (
            " (use postgresql:// or postgresql+psycopg://; SQLAlchemy no longer accepts postgres://)"
            if backend == "postgres"
            else ""
        )
        msg = f"expected a postgresql URL, got backend {backend!r}{hint}"
        raise ConfigError(msg)
    return url


def redact(url: str) -> str:
    """Return the URL with its password replaced, safe for logs and reports."""
    return make_url(url).render_as_string(hide_password=True)


def build_engine(url: str) -> Engine:
    """Create an engine whose every session is read-only and time-bounded.

    The server-side default makes each connection read-only even before a check
    starts its own transaction, so a missed ``SET TRANSACTION`` still cannot write.
    """
    return create_engine(
        url,
        poolclass=NullPool,
        connect_args={"connect_timeout": CONNECT_TIMEOUT_SECONDS, "options": READ_ONLY_OPTION},
    )


@contextmanager
def read_only_transaction(engine: Engine) -> Iterator[Connection]:
    """Yield a connection whose transaction is read-only and always rolled back."""
    with engine.connect() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        try:
            yield connection
        finally:
            connection.rollback()


def _attempt(name: str, body: Callable[[], CheckResult]) -> CheckResult:
    """Run one check, turning an exception into a failed result instead of a crash."""
    try:
        return body()
    except _RECOVERABLE as exc:
        first_line = str(exc).splitlines()[0] if str(exc) else ""
        return CheckResult(
            name=name, ok=False, message=f"{name} could not complete: {type(exc).__name__}: {first_line}"
        )


# --- connection and privileges -------------------------------------------------


def check_connection(engine: Engine) -> CheckResult:
    """Confirm the server answers and that the session is read-only."""
    with read_only_transaction(engine) as connection:
        row = connection.execute(
            text(
                "SELECT 1 AS probe, current_setting('server_version') AS server_version, "
                "current_user AS db_user, current_database() AS db_name",
            ),
        ).one()
        read_only = connection.execute(text("SHOW transaction_read_only")).scalar()
    ok = row.probe == 1 and read_only == "on"
    message = (
        f"connected to {row.db_name} as {row.db_user} (PostgreSQL {row.server_version}); "
        f"transaction_read_only={read_only}"
    )
    details = {
        "server_version": row.server_version,
        "current_user": row.db_user,
        "current_database": row.db_name,
        "transaction_read_only": read_only,
    }
    return CheckResult(name="connection", ok=ok, message=message, details=details)


_ROLE_FLAGS_SQL = text(
    """
    SELECT
        COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false) AS is_superuser,
        has_schema_privilege(current_user, 'public', 'CREATE') AS can_create_in_public
    """,
)
_WRITABLE_TABLES_SQL = text(
    """
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
      AND has_table_privilege(current_user, format('%I.%I', schemaname, tablename), :privileges)
    ORDER BY tablename
    """,
)


def evaluate_role(
    *,
    is_superuser: bool,
    can_create_in_public: bool,
    writable_tables: Sequence[str],
    require_readonly_role: bool,
) -> CheckResult:
    """Classify whether the connecting role can write, and whether that is allowed.

    Args:
        is_superuser: The role bypasses privilege checks.
        can_create_in_public: The role may create objects in schema ``public``.
        writable_tables: Tables in ``public`` the role may modify.
        require_readonly_role: Treat a writable role as a failure rather than a warning.

    Returns:
        The role_privileges result.

    """
    reasons: list[str] = []
    if is_superuser:
        reasons.append("superuser")
    if can_create_in_public:
        reasons.append("CREATE on schema public")
    if writable_tables:
        reasons.append(f"write privilege on {len(writable_tables)} public table(s)")
    details = {
        "is_superuser": is_superuser,
        "can_create_in_public": can_create_in_public,
        "writable_tables_count": len(writable_tables),
        "writable_tables_sample": list(writable_tables[:5]),
    }
    if not reasons:
        return CheckResult(
            name="role_privileges",
            ok=True,
            message="connecting role has no write privilege in schema public",
            details=details,
        )
    message = "connecting role can write (" + "; ".join(reasons) + ")"
    if require_readonly_role:
        return CheckResult(
            name="role_privileges",
            ok=False,
            message=(
                f"{message}; create a read-only role (for example "
                "`CREATE ROLE kbo_ci_ro LOGIN PASSWORD '...'; GRANT pg_read_all_data TO kbo_ci_ro;`) "
                "and point LIVE_DB_READONLY_URL at it"
            ),
            details=details,
        )
    return CheckResult(
        name="role_privileges",
        ok=True,
        message=message,
        details=details,
        warning=f"{message}; the session is still read-only, but the URL should use a read-only role",
    )


def check_role(engine: Engine, *, require_readonly_role: bool) -> CheckResult:
    """Read the connecting role's privileges without changing anything."""
    with read_only_transaction(engine) as connection:
        flags = connection.execute(_ROLE_FLAGS_SQL).one()
        writable = [row[0] for row in connection.execute(_WRITABLE_TABLES_SQL, {"privileges": WRITE_PRIVILEGES})]
    return evaluate_role(
        is_superuser=bool(flags.is_superuser),
        can_create_in_public=bool(flags.can_create_in_public),
        writable_tables=writable,
        require_readonly_role=require_readonly_role,
    )


# --- migrations and schema -----------------------------------------------------


def check_migrations(engine: Engine) -> CheckResult:
    """Report migration files that the database has not recorded yet (read-only)."""
    pending = apply_migrations(engine, check=True)
    if pending:
        message = f"{len(pending)} migration file(s) pending; latest pending: {pending[-1]}"
    else:
        message = "all PostgreSQL migration files are recorded as applied"
    return CheckResult(name="migrations", ok=not pending, message=message, details={"pending": pending})


def evaluate_schema(
    *,
    missing_tables: Sequence[str],
    missing_columns: Sequence[str],
    extra_tables: Sequence[str],
    extra_columns: Sequence[str],
    missing_index_count: int,
) -> CheckResult:
    """Fail on ORM objects absent from the live schema; report extras as information."""
    ok = not missing_tables and not missing_columns
    if ok:
        message = "every ORM table and column exists in the live schema"
    else:
        message = f"{len(missing_tables)} missing table(s), {len(missing_columns)} missing column(s)"
    details = {
        "missing_tables": list(missing_tables),
        "missing_columns": list(missing_columns),
        "extra_tables": list(extra_tables),
        "extra_columns": list(extra_columns),
        "missing_index_count": missing_index_count,
    }
    return CheckResult(name="schema", ok=ok, message=message, details=details)


def check_schema(engine: Engine) -> CheckResult:
    """Compare ORM metadata with the live catalog through the existing drift detector."""
    with read_only_transaction(engine) as connection:
        drift = SchemaDriftDetector(connection).detect_drift()
        live = inspect(connection)
        live_tables = set(live.get_table_names())
        orm_tables = Base.metadata.tables
        extra_columns: list[str] = []
        for table_name, orm_table in orm_tables.items():
            if table_name not in live_tables:
                continue
            live_columns = {column["name"] for column in live.get_columns(table_name)}
            extra_columns.extend(
                f"{table_name}.{name}" for name in sorted(live_columns - {column.name for column in orm_table.columns})
            )
    return evaluate_schema(
        missing_tables=[item.table_name for item in drift.drifts if item.drift_type is DriftType.MISSING_TABLE],
        missing_columns=[
            f"{item.table_name}.{item.object_name}"
            for item in drift.drifts
            if item.drift_type is DriftType.MISSING_COLUMN
        ],
        extra_tables=sorted(live_tables - set(orm_tables) - {MIGRATION_TABLE}),
        extra_columns=extra_columns,
        missing_index_count=sum(1 for item in drift.drifts if item.drift_type is DriftType.MISSING_INDEX),
    )


# --- freshness and core tables -------------------------------------------------


def evaluate_sync_freshness(
    latest_finished_at: datetime | None,
    *,
    now: datetime,
    max_age_hours: float,
) -> CheckResult:
    """Require a successful crawl run finished within ``max_age_hours``.

    The ledger stores naive UTC, so a naive value is read as UTC.
    """
    if latest_finished_at is None:
        return CheckResult(
            name="freshness_sync",
            ok=False,
            message="no successful or partial crawl run is recorded in crawl_execution_runs",
        )
    latest_utc = latest_finished_at if latest_finished_at.tzinfo else latest_finished_at.replace(tzinfo=UTC)
    age_hours = (now - latest_utc).total_seconds() / 3600
    ok = age_hours <= max_age_hours
    return CheckResult(
        name="freshness_sync",
        ok=ok,
        message=f"latest successful crawl finished {age_hours:.1f}h ago (limit {max_age_hours:g}h)",
        details={"latest_finished_at_utc": latest_utc.isoformat(), "age_hours": round(age_hours, 2)},
    )


def evaluate_game_freshness(
    latest_past: date | None,
    latest_completed: date | None,
    *,
    now: datetime,
    max_age_days: int,
) -> CheckResult:
    """Require recent past games to have completed results, judged from the schedule.

    ``latest_past`` is the latest game dated before today (KST) that was not
    cancelled or postponed; ``latest_completed`` is the latest COMPLETED or DRAW
    game dated before today. Off-season and breaks have no recent past game, so
    the check is skipped instead of failing every Sunday in November.
    """
    today = now.astimezone(KST).date()
    if latest_past is None or (today - latest_past).days > max_age_days:
        return CheckResult(
            name="freshness_games",
            ok=True,
            message=f"skipped: no games scheduled in the last {max_age_days} days (off-season or break)",
            details={"today_kst": today.isoformat(), "latest_past_game_date": _iso(latest_past)},
            skipped=True,
        )
    details = {
        "today_kst": today.isoformat(),
        "latest_past_game_date": latest_past.isoformat(),
        "latest_completed_game_date": _iso(latest_completed),
    }
    if latest_completed is None or (latest_past - latest_completed).days > COMPLETION_GRACE_DAYS:
        return CheckResult(
            name="freshness_games",
            ok=False,
            message=(
                f"latest scheduled past game {latest_past.isoformat()} but latest completed "
                f"{_iso(latest_completed) or 'none'} (grace {COMPLETION_GRACE_DAYS} days)"
            ),
            details=details,
        )
    return CheckResult(
        name="freshness_games",
        ok=True,
        message=f"latest scheduled past game {latest_past.isoformat()} has a completed result",
        details=details,
    )


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def check_sync_freshness(engine: Engine, *, now: datetime, max_age_hours: float) -> CheckResult:
    """Read the most recent successful crawl finish time."""
    with read_only_transaction(engine) as connection:
        latest = connection.execute(
            text("SELECT max(finished_at) FROM crawl_execution_runs WHERE status IN (:success, :partial)"),
            {"success": RUN_STATUS_SUCCESS, "partial": RUN_STATUS_PARTIAL},
        ).scalar()
    return evaluate_sync_freshness(latest, now=now, max_age_hours=max_age_hours)


def check_game_freshness(engine: Engine, *, now: datetime, max_age_days: int) -> CheckResult:
    """Read the latest past scheduled and completed game dates in one transaction.

    NULL statuses are kept on purpose: ``game_status NOT IN (...)`` alone drops
    NULL rows, which would hide games whose status was never recorded.
    """
    today = now.astimezone(KST).date()
    with read_only_transaction(engine) as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    max(game_date) FILTER (
                        WHERE game_date < :today
                          AND (game_status IS NULL OR game_status NOT IN (:cancelled, :postponed))
                    ) AS latest_past,
                    max(game_date) FILTER (
                        WHERE game_date < :today AND game_status IN (:completed, :draw)
                    ) AS latest_completed
                FROM game
                """,
            ),
            {
                "today": today,
                "cancelled": GAME_STATUS_CANCELLED,
                "postponed": GAME_STATUS_POSTPONED,
                "completed": GAME_STATUS_COMPLETED,
                "draw": GAME_STATUS_DRAW,
            },
        ).one()
    return evaluate_game_freshness(
        row.latest_past,
        row.latest_completed,
        now=now,
        max_age_days=max_age_days,
    )


def evaluate_core_tables(states: Mapping[str, str]) -> CheckResult:
    """Require every core table to be present and populated."""
    not_populated = {table: state for table, state in states.items() if state != "populated"}
    ok = not not_populated
    if ok:
        message = f"all {len(states)} core tables are populated"
    else:
        message = "core tables not populated: " + ", ".join(f"{t} ({s})" for t, s in not_populated.items())
    return CheckResult(name="core_tables", ok=ok, message=message, details=dict(states))


def check_core_tables(engine: Engine) -> CheckResult:
    """Confirm each core table exists and holds at least one row."""
    states: dict[str, str] = {}
    with read_only_transaction(engine) as connection:
        inspector = inspect(connection)
        for table in CORE_TABLES:
            if not inspector.has_table(table):
                states[table] = "missing"
                continue
            has_rows = connection.execute(text(f"SELECT EXISTS (SELECT 1 FROM {table})")).scalar()
            states[table] = "populated" if has_rows else "empty"
    return evaluate_core_tables(states)


# --- orchestration -------------------------------------------------------------


def run_checks(
    engine: Engine,
    *,
    now: datetime,
    max_sync_age_hours: float,
    max_game_age_days: int,
    require_readonly_role: bool,
) -> list[CheckResult]:
    """Run every check in order; a failed check never stops the next one."""
    return [
        _attempt("connection", lambda: check_connection(engine)),
        _attempt(
            "role_privileges",
            lambda: check_role(engine, require_readonly_role=require_readonly_role),
        ),
        _attempt("migrations", lambda: check_migrations(engine)),
        _attempt("schema", lambda: check_schema(engine)),
        _attempt(
            "freshness_sync",
            lambda: check_sync_freshness(engine, now=now, max_age_hours=max_sync_age_hours),
        ),
        _attempt(
            "freshness_games",
            lambda: check_game_freshness(engine, now=now, max_age_days=max_game_age_days),
        ),
        _attempt("core_tables", lambda: check_core_tables(engine)),
    ]


def build_report(url: str, checks: list[CheckResult], *, now: datetime) -> LiveDbReport:
    """Assemble the report; the database is recorded only in redacted form."""
    status = "PASSED" if all(check.ok for check in checks) else "FAILED"
    return LiveDbReport(
        database=redact(url),
        timestamp=now.isoformat(),
        status=status,
        checks=checks,
    )


def _print_text(report: LiveDbReport) -> None:
    print(f"Live PostgreSQL acceptance: {report.status} ({report.database})")
    for check in report.checks:
        if check.warning and check.ok:
            print(f"[WARN] {check.name}: {check.warning}")
            continue
        label = "SKIP" if check.skipped else ("PASS" if check.ok else "FAIL")
        print(f"[{label}] {check.name}: {check.message}")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only acceptance checks and return the process exit code."""
    parser = argparse.ArgumentParser(description="Read-only PostgreSQL live acceptance check")
    parser.add_argument("--url", help=f"PostgreSQL URL (defaults to ${ENV_VAR})")
    parser.add_argument("--json", action="store_true", help="Print the report as JSON")
    parser.add_argument(
        "--require-readonly-role",
        action="store_true",
        help="Fail when the connecting role can write (the session is read-only either way)",
    )
    parser.add_argument("--max-sync-age-hours", type=float, default=DEFAULT_MAX_SYNC_AGE_HOURS)
    parser.add_argument("--max-game-age-days", type=int, default=DEFAULT_MAX_GAME_AGE_DAYS)
    args = parser.parse_args(argv)
    if args.max_sync_age_hours <= 0 or args.max_game_age_days <= 0:
        parser.error("freshness limits must be positive")

    try:
        url = resolve_url(args.url, os.environ)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    now = datetime.now(UTC)
    engine = build_engine(url)
    try:
        checks = run_checks(
            engine,
            now=now,
            max_sync_age_hours=args.max_sync_age_hours,
            max_game_age_days=args.max_game_age_days,
            require_readonly_role=args.require_readonly_role,
        )
    finally:
        engine.dispose()

    report = build_report(url, checks, now=now)
    if args.json:
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str))
    else:
        _print_text(report)
    return EXIT_PASSED if report.status == "PASSED" else EXIT_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
