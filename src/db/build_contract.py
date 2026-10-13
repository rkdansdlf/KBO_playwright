"""What this build contains, so a run can say which one wrote it.

Three questions today could only be answered by inferring from behaviour, and each
inference was wrong:

* `source_limited` was searched for in `status`, found nothing, and read as "the
  fix is not deployed" -- it was in `checkpoint.outcome`, on 90 rows.
* The refusal code was hardcoded as the retired `FETCH_BLOCKED`, missing the live
  spelling entirely.
* `parking_fee_kinds` sat empty while its crawler reported success every day, and
  nothing could say whether the running build was the one that writes it.

Inferring from behaviour is how each went wrong. A missing value is not evidence
of absence, and a build cannot be identified by what it fails to do.

The marker is the highest migration this build carries. That is the sharpest
available identity for one specific reason: a migration can be applied while the
code that uses it is still not deployed, and that gap is exactly what was
unanswerable for `parking_fee_kinds`. Once a run records what its build carried,
"which build wrote these rows" is a group-by.

Two limits are stated here rather than discovered later. The marker says what a
build **contains**, not what it **runs** -- true unless a deployment is selective,
and even then it narrows the question rather than settling it. And it does not
exist on rows written before this, which must read as an older build rather than
as a missing value.
"""

from __future__ import annotations

import logging
from functools import cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

#: Dialect whose migrations this build's identity is read from.
#:
#: PostgreSQL is the operational database, so a run's identity is what it carried
#: for that schema. SQLite and Oracle chains are development and legacy, and a
#: number from either would say nothing about the production run it appears beside.
CONTRACT_DIALECT = "postgresql"

#: Key the contract is stored under inside `checkpoint`.
#:
#: Namespaced under `contract` so it cannot collide with a crawler's own
#: checkpoint keys, of which `outcome` and `reason` are already spoken for.
CONTRACT_KEY = "contract"


@cache
def highest_migration(dialect: str = CONTRACT_DIALECT) -> str | None:
    """Return the highest migration version this build carries.

    Args:
        dialect: Migration chain to read.

    Returns:
        The zero-padded version string, or None when the chain is unreadable. None
        is not "no migrations" -- it is "this build cannot say", and the contract
        records it as such rather than as an empty identity.

    """
    from src.db.migration_engine import MigrationEngine

    try:
        available = MigrationEngine().get_available_migrations(dialect)
    except OSError:
        logger.warning("Could not read the %s migration chain for the build contract", dialect, exc_info=True)
        return None
    if not available:
        return None
    return f"{max(item.version for item in available):03d}"


@cache
def build_contract() -> dict[str, Any]:
    """Return what this build carries, in the shape a run records.

    Returns:
        The contract. Cheap to compute once and read anywhere.

    """
    from src.crawlers.run_vocabulary import CONTRACT_VERSION

    return {
        "migrations": highest_migration(),
        "vocabulary": CONTRACT_VERSION,
    }


def stamp(checkpoint: object) -> dict[str, Any]:
    """Return a checkpoint with this build's contract added.

    Merged rather than replaced: a crawler's own keys -- `outcome`, `reason` -- are
    the reason the column exists, and a marker that overwrote them would fix the
    identification problem by destroying the record that identified the first one.

    Args:
        checkpoint: The checkpoint the caller supplied, or None.

    Returns:
        A new mapping. The caller's own value is never mutated.

    """
    merged: dict[str, Any] = dict(checkpoint) if isinstance(checkpoint, dict) else {}
    existing = merged.get(CONTRACT_KEY)
    merged[CONTRACT_KEY] = {**(existing if isinstance(existing, dict) else {}), **build_contract()}
    return merged


def parse_contract(checkpoint: object) -> dict[str, Any]:
    """Return the contract recorded on a checkpoint, if any.

    Args:
        checkpoint: The raw `checkpoint` column.

    Returns:
        The contract, or an empty mapping for a row written before this existed.
        Empty rather than None so that callers do not have to distinguish "older
        build" from "no row", which is the distinction that produced the
        misdiagnosis.

    """
    from src.crawlers.run_vocabulary import parse_checkpoint

    contract = parse_checkpoint(checkpoint).get(CONTRACT_KEY)
    return dict(contract) if isinstance(contract, dict) else {}


def migration_of(checkpoint: object) -> str | None:
    """Return the migration version the build that wrote this row carried.

    Args:
        checkpoint: The raw `checkpoint` column.

    Returns:
        The version string, or None for a row written before the marker existed.

    """
    value = parse_contract(checkpoint).get("migrations")
    return str(value) if value is not None else None


def describe_observation(value: object) -> str:
    """Return a human reading of a recorded contract value.

    Args:
        value: The recorded value.

    Returns:
        A sentence. A row with no marker is called an older build rather than a
        broken one, because that is what it is.

    """
    if value is None or value == "":
        return "no marker on this row: written before builds were identified"
    return f"a build carrying migrations through {value}"


def rows_root(migrations_root: Path | None = None) -> Path:
    """Return the migrations root, for callers that need to point elsewhere.

    Args:
        migrations_root: An override.

    Returns:
        The root the engine reads.

    """
    from src.db.migration_engine import MIGRATIONS_ROOT

    return migrations_root or MIGRATIONS_ROOT


__all__ = [
    "CONTRACT_DIALECT",
    "CONTRACT_KEY",
    "build_contract",
    "describe_observation",
    "highest_migration",
    "migration_of",
    "parse_contract",
    "rows_root",
    "stamp",
]
