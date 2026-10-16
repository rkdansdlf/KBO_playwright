"""Shared pgvector type import with a fallback for environments without pgvector."""

from __future__ import annotations

try:
    from pgvector.sqlalchemy import Vector
except ImportError:
    from sqlalchemy.types import UserDefinedType

    class Vector(UserDefinedType):  # type: ignore[no-redef]
        """Fallback Vector type when the pgvector package is not installed."""

        def __init__(self, dim: int = 1536) -> None:
            """Initialize the fallback Vector with dimension."""
            self.dim = dim

        def get_col_spec(self, **kw: object) -> str:  # noqa: ARG002
            """Return the column specification string."""
            return f"vector({self.dim})"


__all__ = ["Vector"]
