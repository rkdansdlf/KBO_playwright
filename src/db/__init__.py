"""db 패키지."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
from .migration_engine import MigrationEngine as MigrationEngine

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
    "Engine",
    "MigrationDialect",
    "MigrationEngine",
    "MigrationExecutionResult",
    "MigrationFileMeta",
    "MigrationStatusReport",
    "SessionLocal",
    "get_db_session",
]


def __getattr__(name: str) -> Any:  # noqa: ANN401
    if name in {"Engine", "SessionLocal", "get_db_session"}:
        from . import engine

        return getattr(engine, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
