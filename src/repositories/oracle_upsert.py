"""Portable upsert helpers for databases without native SQLAlchemy upsert APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.orm import Session


def upsert_model_by_unique_keys(
    session: Session,
    model: type[object],
    payload: Mapping[str, object],
    unique_keys: Sequence[str],
) -> None:
    """Insert or update a model using its business-key columns.

    ``Session.merge`` only matches primary-key values. This helper also works
    for models whose id is an auto-increment column and whose actual upsert
    key is a composite unique constraint.

    Args:
        session: Active database session.
        model: SQLAlchemy model class.
        payload: Column values to insert or update.
        unique_keys: Columns that identify an existing row.

    """
    missing_keys = [key for key in unique_keys if key not in payload]
    if missing_keys:
        message = f"Missing unique-key values: {', '.join(missing_keys)}"
        raise ValueError(message)

    criteria = [getattr(model, key) == payload[key] for key in unique_keys]
    existing = session.execute(select(model).where(*criteria)).scalars().first()
    if existing is None:
        session.add(model(**dict(payload)))  # type: ignore[call-arg]
        return

    for key, value in payload.items():
        if key not in {*unique_keys, "id", "created_at"}:
            setattr(existing, key, value)
