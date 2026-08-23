"""Tests for extended platform health check CLI."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.cli.health_check import _table_issue_counts, run_health_check
from src.models.base import Base
from src.models.kbo_press_release import KboPressRelease
from src.models.rag_chunk import RagChunk

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


def test_run_health_check_json(db_session: Session) -> None:
    """Test run_health_check returns complete 5-layer diagnostic JSON."""
    db_session.add(
        KboPressRelease(
            notice_id="101",
            category="공시",
            title="올스타전 안내",
            published_date=date(2026, 8, 9),
            source_url="https://example.com/101",
        )
    )
    db_session.add(
        RagChunk(
            source_table="kbo_press_releases",
            source_row_id="101",
            title="KBO 공시 - 올스타전 안내",
            content="내용",
            meta={"category": "press_release"},
        )
    )
    db_session.commit()

    with patch("src.cli.health_check.SessionLocal", return_value=db_session):
        report = run_health_check(json_format=True)

        assert "timestamp" in report
        assert "overall_healthy" in report
        assert "datasources" in report
        assert "freshness_issue_count" in report["datasources"]
        assert "tables" in report
        assert "rag_chunks" in report
        assert report["rag_chunks"]["total_chunks"] == 1
        assert "telegram_bot" in report
        assert "api_routers" in report
        assert report["api_routers"]["total_endpoints"] == 8


def test_table_issue_counts_separate_optional_tables() -> None:
    rows = [
        {"table": "game", "rows": 1},
        {"table": "kbo_press_releases", "rows": 0},
        {"table": "player_milestones", "rows": 0},
        {"table": "stadium_congestion", "rows": "ERR"},
    ]

    assert _table_issue_counts(rows) == (0, 3)
