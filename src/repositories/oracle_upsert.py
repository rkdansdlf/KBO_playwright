"""Dialect-neutral fallback upsert used by Oracle-backed repositories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class MissingUniqueKeyValuesError(ValueError):
    """Raised when an upsert payload lacks a business-key value."""

    def __init__(self, keys: list[str]) -> None:
        """Initialize the error with the missing business keys.

        Args:
            keys: Missing business-key names.

        """
        super().__init__(f"Missing unique-key values: {', '.join(keys)}")


def upsert_model_by_unique_keys(
    session: Session,
    model: type[object],
    payload: dict[str, object],
    unique_keys: tuple[str, ...],
) -> object:
    """Insert a model row or update the row matching its business keys.

    This path is used for dialects without a SQLAlchemy-specific conflict
    clause. Database-generated identity and creation timestamps are preserved
    when an existing row is updated.
    """
    missing = [key for key in unique_keys if key not in payload or payload[key] is None]
    if missing:
        raise MissingUniqueKeyValuesError(missing)

    columns = set(model.__table__.columns.keys())  # type: ignore[attr-defined]
    values = {key: value for key, value in payload.items() if key in columns}
    filters = [getattr(model, key) == values[key] for key in unique_keys]
    existing = session.execute(select(model).where(*filters)).scalars().first()

    if existing is None:
        row = model(**values)  # type: ignore[call-arg]
        session.add(row)
        return row

    immutable = set(unique_keys) | {"id", "created_at"}
    for key, value in values.items():
        if key not in immutable:
            setattr(existing, key, value)
    return existing
