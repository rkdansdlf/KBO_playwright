"""Tests for OCISync schema-drift resilience and 0-row dynamic column mapping."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.sync.sync_base import BulkCopyUpsertOptions, OCISyncBase

from sqlalchemy.pool import StaticPool

Base = declarative_base()


class DummyModel(Base):
    """Dummy model with an extra local column."""

    __tablename__ = "dummy_table"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    local_only_column = Column(String(50))


@pytest.fixture
def sync_engine_pair():
    """Create source and target SQLite engines and sessions for testing."""
    source_engine = create_engine("sqlite:///:memory:")
    target_engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    Base.metadata.create_all(bind=source_engine)

    # Create target table WITHOUT local_only_column (simulating delayed OCI migration)
    with target_engine.connect() as conn:
        conn.execute(text("CREATE TABLE dummy_table (id INTEGER PRIMARY KEY, name VARCHAR(50))"))
        conn.commit()

    source_session = sessionmaker(bind=source_engine)()
    target_session = sessionmaker(bind=target_engine)()

    sync_base = OCISyncBase.__new__(OCISyncBase)
    sync_base.sqlite_session = source_session
    sync_base.target_session = target_session
    sync_base.oci_engine = target_engine
    sync_base.concurrency = 1
    sync_base._target_columns_cache = {}
    sync_base._oracle_columns_cache = {}

    yield sync_base, source_session, target_session, source_engine, target_engine

    source_session.close()
    target_session.close()
    source_engine.dispose()
    target_engine.dispose()


class TestOCISyncSchemaDrift:
    """Test suite for OCISync schema-drift resilience."""

    def test_get_target_table_columns_returns_lowercase_keys(self, sync_engine_pair):
        sync_base, _, target_session, _, _ = sync_engine_pair

        cols = sync_base._get_target_table_columns("dummy_table")
        assert cols == {"id", "name"}
        assert "local_only_column" not in cols

        # Test caching
        assert "dummy_table" in sync_base._target_columns_cache
        assert sync_base._get_target_table_columns("dummy_table") == {"id", "name"}

    def test_resolve_sync_columns_excludes_missing_remote_columns(self, sync_engine_pair):
        sync_base, _, _, _, _ = sync_engine_pair

        resolved = sync_base._resolve_sync_columns(DummyModel, exclude_cols=[])
        assert "id" in resolved
        assert "name" in resolved
        assert "local_only_column" not in resolved

    def test_bulk_copy_upsert_filters_missing_target_columns(self, sync_engine_pair):
        sync_base, _, target_session, _, _ = sync_engine_pair

        records = [
            {"id": 1, "name": "Alice", "local_only_column": "value1"},
            {"id": 2, "name": "Bob", "local_only_column": "value2"},
        ]

        with patch.object(sync_base, "_do_bulk_copy_upsert") as mock_bulk_copy:
            sync_base._bulk_copy_upsert(
                "dummy_table",
                BulkCopyUpsertOptions(
                    records=records,
                    unique_cols=["id", "local_only_column"],
                ),
            )

            assert mock_bulk_copy.called
            passed_records = mock_bulk_copy.call_args[0][1]
            passed_unique = mock_bulk_copy.call_args[0][2]

            assert passed_records == [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
            assert passed_unique == ["id"]

    def test_sync_simple_table_with_schema_drift(self, sync_engine_pair):
        sync_base, source_session, target_session, source_engine, target_engine = sync_engine_pair
        from src.sync.sync_base import SimpleTableSyncOptions

        # Populate source DB with extra column data
        source_session.add(DummyModel(id=100, name="TestPlayer", local_only_column="ShouldBeSkipped"))
        source_session.commit()

        def mock_do_bulk_copy(table_name, records, unique_cols, update_timestamp=False, connection=None):
            with target_engine.connect() as conn:
                for record in records:
                    assert "local_only_column" not in record
                    cols = ", ".join(record.keys())
                    vals = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in record.values()])
                    conn.execute(text(f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"))
                conn.commit()

        with patch.object(sync_base, "_do_bulk_copy_upsert", side_effect=mock_do_bulk_copy):
            synced_count = sync_base.sync_simple_table(
                DummyModel,
                SimpleTableSyncOptions(conflict_keys=["id"]),
            )
            assert synced_count == 1

        # Verify row exists in target DB with valid fields
        with target_engine.connect() as conn:
            res = conn.execute(text("SELECT id, name FROM dummy_table WHERE name = 'TestPlayer'")).fetchone()
            assert res is not None
            assert res[1] == "TestPlayer"
