"""Bootstrap and verify the PostgreSQL schema and incremental migrations."""

from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn
from urllib.parse import urlparse

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import DATABASE_URL, create_engine_for_url

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)
MIGRATION_DIR = Path(__file__).resolve().parents[3] / "migrations" / "postgresql"
MIGRATION_TABLE = "schema_migrations"
MIGRATION_NAME_RE = re.compile(r"^(\d+)_.*\.sql$")
_NUMERIC_TYPE_PREFIXES = ("INT", "BIGINT", "SMALLINT", "SERIAL", "BIGSERIAL", "SMALLSERIAL")
ORM_BASELINE_TABLES = ("game", "kbo_seasons")
ADOPTABLE_MIGRATIONS = frozenset(
    {
        "047_remove_redundant_phase1_indexes.sql",
        "048_add_award_player_id.sql",
        "049_quality_gate_and_projection_tables.sql",
        "050_external_season_stats.sql",
        "051_rag_index_consistency.sql",
    },
)


def _migration_paths(directory: Path = MIGRATION_DIR) -> list[Path]:
    """Return PostgreSQL migration files ordered by numeric version."""
    return sorted(
        (path for path in directory.glob("*.sql") if MIGRATION_NAME_RE.match(path.name)),
        key=lambda path: (int(MIGRATION_NAME_RE.match(path.name).group(1)), path.name),  # type: ignore[union-attr]
    )


def _ensure_tracking_table(connection: Connection) -> None:
    """Create the migration tracking table when applying migrations."""
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version VARCHAR(128) PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        ),
    )


def _ensure_orm_baseline(connection: Connection) -> None:
    """Require the ORM-created baseline schema before incremental migrations."""
    inspector = inspect(connection)
    missing = [table for table in ORM_BASELINE_TABLES if not inspector.has_table(table)]
    if missing:
        msg = (
            "PostgreSQL migrations require the ORM baseline schema; "
            f"missing tables: {', '.join(missing)}. Run init_db() first."
        )
        raise RuntimeError(msg)


def _bootstrap_orm_schema(engine: Engine) -> None:
    """Create the SQLAlchemy baseline for a new PostgreSQL database."""
    import src.models  # noqa: F401
    from src.db.engine import _ensure_stat_recalc_view
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    _ensure_stat_recalc_view(engine)


def _tracking_table_exists(connection: Connection) -> bool:
    return inspect(connection).has_table(MIGRATION_TABLE)


def _numeric_tracking(connection: Connection) -> bool:
    """Return whether ``schema_migrations.version`` stores numbers rather than names.

    An older runner recorded only the numeric prefix in an INTEGER column with a
    separate ``filename`` column, so comparing a migration file name against
    ``version`` never matches and every migration looks pending forever.

    Args:
        connection: Connection whose tracking table already exists.

    Returns:
        True for the numeric shape, False for the file-name shape.

    Raises:
        RuntimeError: If the tracking table has no ``version`` column.

    """
    for column in inspect(connection).get_columns(MIGRATION_TABLE):
        if column["name"] == "version":
            return str(column["type"]).upper().startswith(_NUMERIC_TYPE_PREFIXES)
    msg = f"{MIGRATION_TABLE} has no version column; migration history is unusable"
    raise RuntimeError(msg)


def _leading_version(value: object) -> int:
    """Return the numeric version of a migration file name or stored value.

    Args:
        value: A file name (``047_name.sql``) or an already-numeric value.

    Returns:
        The leading version as an integer.

    Raises:
        RuntimeError: If a numeric tracking column holds a non-numeric value.

    """
    raw = str(value).strip()
    match = MIGRATION_NAME_RE.match(raw)
    if match:
        return int(match.group(1))
    try:
        return int(raw)
    except ValueError as exc:
        msg = f"numeric {MIGRATION_TABLE}.version holds a non-numeric value: {raw!r}"
        raise RuntimeError(msg) from exc


def _applied_versions(connection: Connection, *, numeric: bool) -> set[object]:
    """Return the applied migration identifiers for the detected tracking shape.

    Args:
        connection: Connection whose tracking table already exists.
        numeric: Whether the version column stores numbers.

    Returns:
        Integers for the numeric shape, file names for the name shape.

    """
    rows = connection.execute(text(f"SELECT version FROM {MIGRATION_TABLE}"))  # noqa: S608
    if numeric:
        return {_leading_version(row[0]) for row in rows}
    return {str(row[0]) for row in rows}


def _is_applied(path_name: str, applied: set[object], *, numeric: bool) -> bool:
    """Return whether one migration file is already recorded.

    Args:
        path_name: Migration file name.
        applied: Applied identifiers from :func:`_applied_versions`.
        numeric: Whether the version column stores numbers.

    Returns:
        True when the file was already applied.

    """
    if numeric:
        return _leading_version(path_name) in applied
    return path_name in applied


def _record_version(connection: Connection, path_name: str, *, numeric: bool) -> None:
    """Record one applied migration in whichever tracking shape exists.

    Args:
        connection: Connection whose tracking table already exists.
        path_name: Migration file name being recorded.
        numeric: Whether the version column stores numbers.

    """
    if numeric:
        connection.execute(
            text(f"INSERT INTO {MIGRATION_TABLE} (version, filename) VALUES (:version, :filename)"),  # noqa: S608
            {"version": _leading_version(path_name), "filename": path_name},
        )
    else:
        connection.execute(
            text(f"INSERT INTO {MIGRATION_TABLE} (version) VALUES (:version)"),  # noqa: S608
            {"version": path_name},
        )


def _raise_adoption_error(message: str) -> NoReturn:
    raise RuntimeError(message)


def _validate_existing_schema_for_adoption(connection: Connection) -> None:
    """Validate the schema shape before adopting current migration metadata."""
    inspector = inspect(connection)
    if inspector.has_table("_schema_migrations"):
        msg = "Legacy _schema_migrations found; OCI/Oracle history requires separate review"
        _raise_adoption_error(msg)
    if not inspector.has_table("awards"):
        _raise_adoption_error("Existing schema adoption requires the awards table")

    award_columns = {column["name"] for column in inspector.get_columns("awards")}
    missing_columns = {"player_id", "team_code"} - award_columns
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        _raise_adoption_error(f"Existing schema adoption requires awards columns: {missing}")

    index_names = {index.get("name") for index in inspector.get_indexes("awards")}
    if "idx_award_player_id" not in index_names:
        _raise_adoption_error("Existing schema adoption requires idx_award_player_id")

    if inspector.has_table("rag_chunks"):
        rag_columns = {column["name"] for column in inspector.get_columns("rag_chunks")}
        missing_rag_columns = {"content_hash", "index_version", "index_status", "indexed_at"} - rag_columns
        if missing_rag_columns:
            missing = ", ".join(sorted(missing_rag_columns))
            _raise_adoption_error(f"Existing schema adoption requires rag_chunks columns: {missing}")

    missing_tables = {
        "quarantined_records",
        "correction_audit_trail",
        "player_projections",
    } - set(inspector.get_table_names())
    if missing_tables:
        missing = ", ".join(sorted(missing_tables))
        _raise_adoption_error(f"Existing schema adoption requires tables: {missing}")


def adopt_existing_schema(engine: Engine, *, directory: Path = MIGRATION_DIR) -> list[str]:
    """Record the current PostgreSQL migration baseline without executing DDL."""
    del directory
    if engine.dialect.name == "oracle":
        msg = "PostgreSQL schema adoption does not support Oracle engines"
        raise RuntimeError(msg)

    with engine.begin() as connection:
        _validate_existing_schema_for_adoption(connection)
        _ensure_tracking_table(connection)
        numeric = _numeric_tracking(connection)
        applied = _applied_versions(connection, numeric=numeric)
        to_record = sorted(
            version for version in ADOPTABLE_MIGRATIONS if not _is_applied(version, applied, numeric=numeric)
        )
        for version in to_record:
            _record_version(connection, version, numeric=numeric)
        return to_record


def apply_migrations(engine: Engine, *, directory: Path = MIGRATION_DIR, check: bool = False) -> list[str]:
    """Apply incremental PostgreSQL migrations or return pending versions."""
    if engine.dialect.name == "oracle":
        msg = "PostgreSQL migrations do not support Oracle engines"
        raise RuntimeError(msg)
    paths = _migration_paths(directory)

    if check:
        with engine.connect() as connection:
            _ensure_orm_baseline(connection)
            if not _tracking_table_exists(connection):
                return [path.name for path in paths]
            numeric = _numeric_tracking(connection)
            applied = _applied_versions(connection, numeric=numeric)
            return [path.name for path in paths if not _is_applied(path.name, applied, numeric=numeric)]

    with engine.begin() as connection:
        _ensure_orm_baseline(connection)
        _ensure_tracking_table(connection)
        numeric = _numeric_tracking(connection)
        applied = _applied_versions(connection, numeric=numeric)
        pending = [path for path in paths if not _is_applied(path.name, applied, numeric=numeric)]
        for path in pending:
            for statement in split_sql_statements(path.read_text(encoding="utf-8")):
                connection.exec_driver_sql(statement)
            _record_version(connection, path.name, numeric=numeric)
    return [path.name for path in pending]


def split_sql_statements(source: str) -> list[str]:
    """Split a migration file into executable statements.

    Splitting on ``;`` alone is wrong, and it failed in production. The header
    comment on ``060_parking_fee_kinds.sql`` reads "these kinds never had a
    table of their own; they lived only in the raw snapshot text", so the naive
    split cut the file inside that comment and welded its tail onto the next
    statement as though it were code. Postgres rejected the result with
    ``syntax error at or near ...`` on the *following* statement -- the table
    had already been created by the ORM baseline, so the run left schema and
    bookkeeping disagreeing, with the table present and the version never
    recorded.

    The comment-only leading chunk is not itself the failure: measured against
    PostgreSQL 15 with psycopg, a comment-only statement is accepted. It is
    dropped here because a chunk that carries no SQL is not a statement, and
    because the empty-query rejection belongs to PL/pgSQL ``EXECUTE`` rather
    than to this path -- an operator chasing that error would be chasing the
    wrong one.

    The scanner is deliberately small -- enough to know where a ``;`` is real
    data and where it is text. Line comments, block comments and single-quoted
    strings are all it tracks, which covers every migration in the PostgreSQL
    and SQLite chains; neither uses dollar quoting. The Oracle chain is not read
    by this runner, and its PL/SQL blocks would need real block tracking.

    Args:
        source: The migration file's text.

    Returns:
        The statements to execute, in file order, without comment-only chunks.

    """
    statements: list[str] = []
    current: list[str] = []
    has_code = False
    index = 0
    length = len(source)

    while index < length:
        char = source[index]
        pair = source[index : index + 2]

        if pair == "--":
            end = source.find("\n", index)
            end = length if end == -1 else end
            current.append(source[index:end])
            index = end
            continue

        if pair == "/*":
            end = source.find("*/", index + 2)
            end = length if end == -1 else end + 2
            current.append(source[index:end])
            index = end
            continue

        if char == "'":
            end = index + 1
            while end < length:
                if source[end] == "'":
                    if source[end + 1 : end + 2] == "'":
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            current.append(source[index:end])
            has_code = True
            index = end
            continue

        if char == ";":
            _append_statement(statements, "".join(current), has_code=has_code)
            current = []
            has_code = False
            index += 1
            continue

        current.append(char)
        if not char.isspace():
            has_code = True
        index += 1

    _append_statement(statements, "".join(current), has_code=has_code)
    return statements


def _append_statement(statements: list[str], chunk: str, *, has_code: bool) -> None:
    """Keep a chunk only when it holds SQL that is not a comment.

    A chunk of comments alone reaches Postgres as an empty query, which it
    rejects -- the failure that motivated this scanner. `has_code` is tracked by
    the scanner rather than recomputed here so the two cannot disagree about
    what counts as a comment.
    """
    sql = chunk.strip()
    if sql and has_code:
        statements.append(sql)


DEFAULT_PROD_DB_NAMES = frozenset({"bega_prod"})
PROD_DB_NAMES_ENV = "KBO_PROD_DB_NAMES"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", "127.0.0.0", ""})


def _prod_db_names() -> frozenset[str]:
    """Return the database names treated as production.

    Args:
        None.

    Returns:
        Lower-cased database names. Falls back to the built-in default when the
        environment override is unset or blank.

    """
    raw = os.getenv(PROD_DB_NAMES_ENV, "")
    names = {token.strip().lower() for token in raw.replace(",", " ").split() if token.strip()}
    return frozenset(names) if names else DEFAULT_PROD_DB_NAMES


def _looks_like_prod(url: str) -> bool:
    """Return whether a database URL points at a production target.

    Only a known production database name on a non-loopback host counts. A
    loopback target is always treated as local even if the name matches, because
    the risk being guarded against is remote DDL, not the name itself.

    Args:
        url: Database URL to classify.

    Returns:
        True when the URL should be treated as production.

    """
    parsed = urlparse(url)
    if (parsed.hostname or "").strip().lower() in LOOPBACK_HOSTS:
        return False
    return (parsed.path or "").lstrip("/").lower() in _prod_db_names()


def main(argv: Sequence[str] | None = None) -> int:
    """Apply PostgreSQL incremental migrations or check pending versions."""
    parser = argparse.ArgumentParser(description="Apply PostgreSQL incremental migrations")
    parser.add_argument("--url", help="Override DATABASE_URL")
    parser.add_argument("--check", action="store_true", help="Return non-zero when migrations are pending")
    parser.add_argument(
        "--adopt-existing",
        action="store_true",
        help="Validate the current schema and record the current migration baseline without executing DDL",
    )
    parser.add_argument(
        "--allow-prod",
        action="store_true",
        help=f"Allow a schema-writing run against a production database (see {PROD_DB_NAMES_ENV})",
    )
    args = parser.parse_args(argv)
    if args.check and args.adopt_existing:
        parser.error("--check and --adopt-existing cannot be combined")
    url = args.url or os.getenv("DATABASE_URL") or DATABASE_URL
    if not url:
        msg = "DATABASE_URL is required"
        raise SystemExit(msg)

    # ``--check`` only reads, so it stays unguarded; anything that writes schema
    # to a production target must be opted into explicitly.
    targets_prod = _looks_like_prod(url)
    if targets_prod and not args.allow_prod and not args.check:
        logger.error(
            "Refusing to write schema to a production target (%s). "
            "Re-run with --allow-prod once the change is intended and verified.",
            urlparse(url).path.lstrip("/"),
        )
        return 2
    if targets_prod:
        logger.warning("Running against PRODUCTION target: %s", urlparse(url).path.lstrip("/"))

    engine = create_engine_for_url(url)
    try:
        if args.adopt_existing:
            pending = adopt_existing_schema(engine)
        else:
            if not args.check:
                _bootstrap_orm_schema(engine)
            pending = apply_migrations(engine, check=args.check)
    except (SQLAlchemyError, RuntimeError, OSError):
        logger.exception("PostgreSQL migration failed")
        return 1
    finally:
        engine.dispose()
    if pending:
        logger.info("PostgreSQL migrations pending/applied: %s", pending)
    return 1 if args.check and pending else 0


if __name__ == "__main__":
    raise SystemExit(main())
