from __future__ import annotations

from sqlalchemy import create_engine

import pytest


@pytest.fixture
def _db_engine():
    """Shared in-memory SQLite fixture for sync tests that need DB schema."""
    from src.models.base import Base as ModelsBase

    engine = create_engine("sqlite:///:memory:", echo=False)
    ModelsBase.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()
