"""Unit tests for SchemaDriftDetector."""

from __future__ import annotations

from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)

from src.db.drift_detector import SchemaDriftDetector
from src.db.drift_dto import DriftSeverity, DriftType


def test_detect_missing_table() -> None:
    """Test detecting table defined in metadata but missing in database."""
    engine = create_engine("sqlite:///:memory:")

    custom_meta = MetaData()
    Table(
        "sample_missing_table",
        custom_meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(50), nullable=False),
    )

    detector = SchemaDriftDetector(engine, metadata=custom_meta, dialect="sqlite")
    report = detector.detect_drift()

    assert report.is_synced is False
    assert report.drift_count == 1
    drift = report.drifts[0]
    assert drift.drift_type == DriftType.MISSING_TABLE
    assert drift.table_name == "sample_missing_table"
    assert drift.severity == DriftSeverity.HIGH


def test_detect_missing_column_and_generate_oracle_ddl() -> None:
    """Test detecting missing column and generating Oracle ALTER TABLE ADD DDL."""
    engine = create_engine("sqlite:///:memory:")

    # Create table in DB with only 'id'
    db_meta = MetaData()
    Table(
        "player_test",
        db_meta,
        Column("id", Integer, primary_key=True),
    )
    db_meta.create_all(engine)

    # ORM metadata has 'id' and 'korean_name' and 'homeruns'
    orm_meta = MetaData()
    Table(
        "player_test",
        orm_meta,
        Column("id", Integer, primary_key=True),
        Column("korean_name", String(100), nullable=True),
        Column("homeruns", Integer, nullable=False),
    )

    detector = SchemaDriftDetector(engine, metadata=orm_meta, dialect="oracle")
    report = detector.detect_drift()

    assert report.is_synced is False
    assert report.drift_count == 2

    # Check generated Oracle DDL
    ddls = report.generated_ddl
    assert len(ddls) == 2
    assert any("ALTER TABLE player_test ADD (korean_name VARCHAR2(100));" in d for d in ddls)
    assert any("ALTER TABLE player_test ADD (homeruns NUMBER(10) NOT NULL);" in d for d in ddls)


def test_detect_missing_index_and_apply_remediation() -> None:
    """Test detecting missing index, generating DDL, and applying remediation."""
    engine = create_engine("sqlite:///:memory:")

    # Create table in DB without index
    db_meta = MetaData()
    Table(
        "game_test",
        db_meta,
        Column("game_id", String(30), primary_key=True),
        Column("season", Integer, nullable=False),
        Column("status", String(20), nullable=True),
    )
    db_meta.create_all(engine)

    # ORM metadata has index on (season, status)
    orm_meta = MetaData()
    t_orm = Table(
        "game_test",
        orm_meta,
        Column("game_id", String(30), primary_key=True),
        Column("season", Integer, nullable=False),
        Column("status", String(20), nullable=True),
    )
    Index("ix_game_season_status", t_orm.c.season, t_orm.c.status)

    detector = SchemaDriftDetector(engine, metadata=orm_meta, dialect="sqlite")
    report = detector.detect_drift()

    assert report.drift_count == 1
    assert report.drifts[0].drift_type == DriftType.MISSING_INDEX
    assert len(report.generated_ddl) == 1

    # Apply remediation
    applied = detector.apply_remediation(report)
    assert applied == 1

    # Second check should now be in sync
    report_after = detector.detect_drift()
    assert report_after.is_synced is True
    assert report_after.drift_count == 0
