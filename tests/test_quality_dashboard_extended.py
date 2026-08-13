"""Tests for extended quality dashboard CLI and JSON generation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.cli.quality_dashboard import (
    generate_daily_quality_dashboard,
    save_quality_dashboard,
)
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    yield session
    session.close()


def test_generate_daily_quality_dashboard(db_session: Session, tmp_path: Path) -> None:
    """Test generate_daily_quality_dashboard combines gap reports & health check."""
    with (
        patch("src.cli.gap_report.SessionLocal", return_value=db_session),
        patch("src.cli.health_check.SessionLocal", return_value=db_session),
    ):
        dashboard = generate_daily_quality_dashboard(tmp_path)

        assert "generated_at" in dashboard
        assert "gap_report" in dashboard
        assert "NOTICES" in dashboard["gap_report"]
        assert "health_check" in dashboard

        output_file = tmp_path / "dashboard.json"
        save_quality_dashboard(dashboard, output_file)
        assert output_file.exists()
