#!/usr/bin/env python3
"""Reset a disposable Oracle verification schema after explicit confirmation."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import create_engine_for_url

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

OBJECT_DROP_ORDER: tuple[tuple[str, str], ...] = (
    ("MATERIALIZED VIEW", "DROP MATERIALIZED VIEW"),
    ("VIEW", "DROP VIEW"),
    ("TRIGGER", "DROP TRIGGER"),
    ("TABLE", "DROP TABLE"),
    ("SEQUENCE", "DROP SEQUENCE"),
    ("INDEX", "DROP INDEX"),
    ("PACKAGE", "DROP PACKAGE"),
    ("PROCEDURE", "DROP PROCEDURE"),
    ("FUNCTION", "DROP FUNCTION"),
    ("TYPE", "DROP TYPE"),
    ("SYNONYM", "DROP SYNONYM"),
)
ORACLE_IDENTIFIER_RE = re.compile(r"^[A-Z][A-Z0-9_$#]*$")
PRESERVED_OBJECT_NAMES = frozenset(
    {
        "DBTOOLS$EXECUTION_HISTORY",
        "DBTOOLS$EXECUTION_HISTORY_SEQ",
        "DBTOOLS$EXECUTION_HISTORY_PK",
    },
)
PRESERVED_OBJECT_PREFIXES = ("SYS_IL",)


def _quote_oracle_identifier(name: str) -> str:
    """Quote a normal Oracle user-object identifier after validation."""
    normalized = name.upper()
    if not ORACLE_IDENTIFIER_RE.fullmatch(normalized):
        message = f"Unsafe Oracle object identifier: {name!r}"
        raise ValueError(message)
    return f'"{normalized}"'


def _drop_statement(object_type: str, object_name: str, ddl: str) -> str:
    """Build the drop statement for one verified Oracle object."""
    quoted_name = _quote_oracle_identifier(object_name)
    if object_type == "TABLE":
        return f"{ddl} {quoted_name} CASCADE CONSTRAINTS PURGE"
    if object_type == "TYPE":
        return f"{ddl} {quoted_name} FORCE"
    return f"{ddl} {quoted_name}"


def _current_identity(connection: Connection) -> tuple[str, str]:
    """Return the current Oracle session user and schema."""
    row = connection.execute(
        text(
            "SELECT SYS_CONTEXT('USERENV', 'SESSION_USER'), SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual",
        ),
    ).one()
    return str(row[0]), str(row[1])


def _object_names(connection: Connection, object_type: str) -> list[str]:
    """Return current-user object names for one Oracle object type."""
    rows = connection.execute(
        text(
            "SELECT object_name FROM user_objects WHERE object_type = :object_type ORDER BY object_name",
        ),
        {"object_type": object_type},
    )
    return [
        str(row[0])
        for row in rows
        if str(row[0]).upper() not in PRESERVED_OBJECT_NAMES
        and not str(row[0]).upper().startswith(PRESERVED_OBJECT_PREFIXES)
    ]


def _inventory(connection: Connection) -> dict[str, list[str]]:
    """Return the disposable schema inventory grouped by object type."""
    inventory = {object_type: _object_names(connection, object_type) for object_type, _ in OBJECT_DROP_ORDER}
    inventory["PRESERVED"] = sorted(PRESERVED_OBJECT_NAMES) + list(PRESERVED_OBJECT_PREFIXES)
    return inventory


def _verify_identity(connection: Connection, expected_user: str) -> tuple[str, str]:
    """Require the reset target to be the explicitly approved verification user."""
    session_user, current_schema = _current_identity(connection)
    expected = expected_user.upper()
    if session_user.upper() != expected or current_schema.upper() != expected:
        message = f"Refusing reset outside {expected}: session_user={session_user}, current_schema={current_schema}"
        raise RuntimeError(message)
    return session_user, current_schema


def reset_schema(engine: Engine, *, expected_user: str = "ADMIN") -> list[str]:
    """Drop all disposable objects owned by the verified Oracle user."""
    dropped: list[str] = []
    with engine.begin() as connection:
        _verify_identity(connection, expected_user)
        for object_type, ddl in OBJECT_DROP_ORDER:
            for object_name in _object_names(connection, object_type):
                connection.exec_driver_sql(_drop_statement(object_type, object_name, ddl))
                dropped.append(f"{object_type}:{object_name}")
    return dropped


def _resolve_url(explicit_url: str | None) -> str:
    """Resolve only the explicit verification URL, never the primary URL."""
    url = explicit_url or os.getenv("OCI_DB_URL")
    if not url or not url.startswith("oracle"):
        raise RuntimeError("An Oracle OCI_DB_URL or --url is required; DATABASE_URL is never used for reset")
    return url


def main(argv: list[str] | None = None) -> int:
    """Reset a disposable Oracle verification schema after an explicit confirmation."""
    load_dotenv(PROJECT_ROOT / ".env")
    parser = argparse.ArgumentParser(description="Reset a disposable Oracle verification schema")
    parser.add_argument("--url", help="Explicit Oracle verification URL; defaults to OCI_DB_URL")
    parser.add_argument("--expected-user", default="ADMIN")
    parser.add_argument("--confirm", help="Must exactly match --expected-user for a destructive reset")
    parser.add_argument("--dry-run", action="store_true", help="Print the inventory without dropping objects")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        url = _resolve_url(args.url)
        engine = create_engine_for_url(url, tns_admin=os.getenv("TNS_ADMIN"))
        try:
            with engine.connect() as connection:
                session_user, current_schema = _verify_identity(connection, args.expected_user)
                inventory = _inventory(connection)
            if args.dry_run:
                payload = {
                    "session_user": session_user,
                    "current_schema": current_schema,
                    "inventory": inventory,
                    "dry_run": True,
                }
            else:
                if args.confirm != args.expected_user:
                    raise RuntimeError("Destructive reset requires --confirm matching --expected-user")
                dropped = reset_schema(engine, expected_user=args.expected_user)
                payload = {
                    "session_user": session_user,
                    "current_schema": current_schema,
                    "dropped_count": len(dropped),
                    "dropped": dropped,
                    "dry_run": False,
                }
        finally:
            engine.dispose()
    except (SQLAlchemyError, RuntimeError, ValueError, OSError) as exc:
        sys.stderr.write(f"Oracle verification schema reset failed: {exc}\n")
        return 1

    if args.json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    else:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
