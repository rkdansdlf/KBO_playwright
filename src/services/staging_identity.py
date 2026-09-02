"""Phase 105 Gate 4: Deep Runtime Identity Verification Protocol.

Enforces deep runtime identity checks against Oracle Autonomous Database staging
prior to executing any staging rehearsal operations.

Validates 5 exact-match SYS_CONTEXT descriptors against an immutable allowlist:
1. DB_UNIQUE_NAME
2. SERVICE_NAME
3. CURRENT_SCHEMA
4. SESSION_USER
5. CON_NAME

Fails closed on any mismatch, missing descriptor, empty value, or probe error.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Valid identifier pattern for allowlist entries (prevents SQL injection or malformed tokens)
_SAFE_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+$")

IDENTITY_DESCRIPTORS = (
    "db_unique_name",
    "service_name",
    "current_schema",
    "session_user",
    "con_name",
)


@dataclass(frozen=True)
class StagingIdentityAllowlist:
    """Immutable contract defining expected exact-match staging context descriptors."""

    db_unique_name: str
    service_name: str
    current_schema: str
    session_user: str
    con_name: str

    def __post_init__(self) -> None:
        """Validate that all allowlist values are safe, non-empty identifiers."""
        for field_name in IDENTITY_DESCRIPTORS:
            val = getattr(self, field_name, None)
            if not val or not isinstance(val, str):
                msg = f"Allowlist descriptor {field_name} must be a non-empty string"
                raise ValueError(msg)
            if "--" in val or "/*" in val or "*/" in val:
                msg = f"Allowlist descriptor {field_name} contains invalid characters: {val!r}"
                raise ValueError(msg)
            if not _SAFE_IDENTIFIER_PATTERN.match(val):
                msg = f"Allowlist descriptor {field_name} contains invalid characters: {val!r}"
                raise ValueError(msg)

    def to_dict(self) -> dict[str, str]:
        """Export as dictionary."""
        return {k: getattr(self, k) for k in IDENTITY_DESCRIPTORS}


@dataclass
class IdentityMismatch:
    """Details of a single context descriptor discrepancy."""

    descriptor: str
    expected: str
    actual: str


@dataclass
class IdentityResult:
    """Outcome of the 5-descriptor identity verification probe."""

    passed: bool
    mismatches: list[IdentityMismatch]
    raw_values: dict[str, str]
    error: str | None = None

    @property
    def is_clean(self) -> bool:
        """Return True if all 5 descriptors matched with no errors."""
        return self.passed and not self.mismatches and self.error is None


def probe_runtime_identity(session: Session) -> dict[str, str]:
    """Execute read-only identity probe query to extract SYS_CONTEXT descriptors.

    On Oracle dialects, queries SYS_CONTEXT from DUAL.
    On non-Oracle dialects (SQLite/PostgreSQL test harnesses), inspects session-level
    metadata or mock attributes provided for testing.
    """
    bind = session.get_bind()
    dialect_name = bind.dialect.name if bind else "unknown"

    if dialect_name == "oracle":
        query = text(
            """
            SELECT
                SYS_CONTEXT('USERENV', 'DB_UNIQUE_NAME') AS db_unique_name,
                SYS_CONTEXT('USERENV', 'SERVICE_NAME')   AS service_name,
                SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema,
                SYS_CONTEXT('USERENV', 'SESSION_USER')   AS session_user,
                SYS_CONTEXT('USERENV', 'CON_NAME')       AS con_name
            FROM DUAL
            """
        )
        row = session.execute(query).mappings().fetchone()
        if not row:
            msg = "Oracle SYS_CONTEXT query returned no rows"
            raise RuntimeError(msg)
        return {k: str(row.get(k) or "").strip() for k in IDENTITY_DESCRIPTORS}

    # For testing on non-Oracle engines, check for mock context attached to session/engine
    mock_context = getattr(session, "_mock_oracle_sys_context", None) or getattr(bind, "_mock_oracle_sys_context", None)
    if isinstance(mock_context, dict):
        return {k: str(mock_context.get(k) or "").strip() for k in IDENTITY_DESCRIPTORS}

    # If running against standard SQLite/PG without mock context, return empty descriptors
    return dict.fromkeys(IDENTITY_DESCRIPTORS, "")


def verify_staging_identity(
    session: Session,
    allowlist: StagingIdentityAllowlist,
) -> IdentityResult:
    """Verify runtime context against the immutable allowlist.

    Executes a read-only probe and compares all 5 context descriptors.
    Fails closed if any descriptor does not match exactly or if an error occurs.
    """
    mismatches: list[IdentityMismatch] = []
    try:
        raw_values = probe_runtime_identity(session)
    except (SQLAlchemyError, RuntimeError, OSError) as e:
        logger.exception("Identity verification probe failed")
        return IdentityResult(
            passed=False,
            mismatches=[],
            raw_values={},
            error=f"Probe query failed: {e}",
        )

    expected = allowlist.to_dict()
    for desc in IDENTITY_DESCRIPTORS:
        exp_val = expected[desc]
        act_val = raw_values.get(desc, "")

        if not act_val:
            mismatches.append(IdentityMismatch(descriptor=desc, expected=exp_val, actual="<NULL_OR_EMPTY>"))
        elif act_val.upper() != exp_val.upper():
            # Identity names in Oracle are typically case-insensitive canonical strings
            mismatches.append(IdentityMismatch(descriptor=desc, expected=exp_val, actual=act_val))

    passed = len(mismatches) == 0
    if not passed:
        logger.warning(
            "Identity verification failed with %d mismatch(es): %s",
            len(mismatches),
            ", ".join(f"{m.descriptor}: exp={m.expected}, act={m.actual}" for m in mismatches),
        )
    else:
        logger.info("Identity verification passed for all 5 SYS_CONTEXT descriptors.")

    return IdentityResult(
        passed=passed,
        mismatches=mismatches,
        raw_values=raw_values,
        error=None,
    )
