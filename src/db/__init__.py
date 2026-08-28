"""Database and migration management package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .drift_detector import (
    SchemaDriftDetector as SchemaDriftDetector,
)
from .drift_dto import (
    DriftSeverity as DriftSeverity,
)
from .drift_dto import (
    DriftType as DriftType,
)
from .drift_dto import (
    SchemaDriftItem as SchemaDriftItem,
)
from .drift_dto import (
    SchemaDriftReport as SchemaDriftReport,
)
from .dto import (
    MigrationDialect as MigrationDialect,
)
from .dto import (
    MigrationExecutionResult as MigrationExecutionResult,
)
from .dto import (
    MigrationFileMeta as MigrationFileMeta,
)
from .dto import (
    MigrationStatusReport as MigrationStatusReport,
)
from .migration_engine import (
    MigrationEngine as MigrationEngine,
)

if TYPE_CHECKING:
    from .engine import (
        Engine as Engine,
    )
    from .engine import (
        SessionLocal as SessionLocal,
    )
    from .engine import (
        get_db_session as get_db_session,
    )

__all__ = [
    "DriftSeverity",
    "DriftType",
    "Engine",
    "MigrationDialect",
    "MigrationEngine",
    "MigrationExecutionResult",
    "MigrationFileMeta",
    "MigrationStatusReport",
    "SchemaDriftDetector",
    "SchemaDriftItem",
    "SchemaDriftReport",
    "SessionLocal",
    "get_db_session",
]


def __getattr__(name: str) -> Any:  # noqa: ANN401
    if name in {"Engine", "SessionLocal", "get_db_session"}:
        from . import engine

        return getattr(engine, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
