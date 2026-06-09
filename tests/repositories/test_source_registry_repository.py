from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.source_registry_repository import (
    DataSourceRepository,
    RawSourceSnapshotRepository,
    save_raw_snapshots,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    DataSource.__table__.create(engine)
    RawSourceSnapshot.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


class TestDataSourceRepository:
    def test_save_new(self, session):
        repo = DataSourceRepository(session)
        ds = repo.save({
            "source_key": "kbo_schedule",
            "source_type": "official_kbo",
            "target_domain": "schedule",
            "reliability": "high",
        })
        assert ds.source_key == "kbo_schedule"
        assert ds.source_type == "official_kbo"

    def test_save_existing(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "kbo_schedule", "source_type": "official_kbo", "target_domain": "schedule",
                    "reliability": "high", "base_url": "http://old"})
        session.flush()

        updated = repo.save({"source_key": "kbo_schedule", "base_url": "http://new"})
        assert updated.base_url == "http://new"

    def test_get_by_key(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "test", "source_type": "a", "target_domain": "schedule", "reliability": "high"})
        session.flush()

        ds = repo.get_by_key("test")
        assert ds is not None
        assert ds.source_key == "test"
        assert repo.get_by_key("nonexistent") is None

    def test_get_by_domain(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "a", "source_type": "t", "target_domain": "ticket", "reliability": "high"})
        repo.save({"source_key": "b", "source_type": "t", "target_domain": "ticket", "reliability": "high"})
        repo.save({"source_key": "c", "source_type": "t", "target_domain": "seat", "reliability": "high"})
        session.flush()

        results = repo.get_by_domain("ticket")
        assert len(results) == 2

    def test_get_active_by_domain(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "a", "source_type": "t", "target_domain": "ticket", "reliability": "high", "is_active": True})
        repo.save({"source_key": "b", "source_type": "t", "target_domain": "ticket", "reliability": "high", "is_active": False})
        session.flush()

        results = repo.get_active_by_domain("ticket")
        assert len(results) == 1
        assert results[0].source_key == "a"

    def test_get_all_active(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "a", "source_type": "t", "target_domain": "t", "reliability": "h", "is_active": True})
        repo.save({"source_key": "b", "source_type": "t", "target_domain": "t", "reliability": "h", "is_active": False})
        session.flush()

        results = repo.get_all_active()
        assert len(results) == 1

    def test_mark_success(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "test", "source_type": "a", "target_domain": "d", "reliability": "h"})
        session.flush()

        ds = repo.mark_success("test", "abc123")
        assert ds is not None
        assert ds.last_content_hash == "abc123"
        assert ds.last_success_at is not None

    def test_get_stale_sources(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "stale", "source_type": "t", "target_domain": "d", "reliability": "h", "is_active": True})
        session.flush()
        ds = repo.get_by_key("stale")
        ds.last_success_at = datetime(2020, 1, 1)
        session.flush()

        results = repo.get_stale_sources(max_hours=1)
        assert len(results) == 1

        repo2 = DataSourceRepository(session)
        fresh = repo2.get_by_key("stale")
        fresh.last_success_at = datetime.now(UTC).replace(tzinfo=None)
        session.flush()

        results = repo2.get_stale_sources(max_hours=48)
        assert len(results) == 0


class TestRawSourceSnapshotRepository:
    def test_save_and_get_by_source_id(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "kbo_schedule", "source_type": "official_kbo", "target_domain": "schedule",
                           "reliability": "high"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap = snap_repo.save({
            "data_source_id": ds.id,
            "raw_html_or_json_path": "http://example.com",
            "content_hash": "abc123",
            "fetched_at": datetime.now(UTC).replace(tzinfo=None),
            "status_code": 200,
        })
        assert snap.id is not None

        results = snap_repo.get_by_source_id(ds.id)
        assert len(results) == 1

    def test_get_by_hash(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save({"data_source_id": ds.id, "raw_html_or_json_path": "u", "content_hash": "abc",
                        "fetched_at": datetime.now(UTC).replace(tzinfo=None)})
        session.flush()

        found = snap_repo.get_by_hash(ds.id, "abc")
        assert found is not None
        assert snap_repo.get_by_hash(ds.id, "xyz") is None

    def test_get_unparsed(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save({"data_source_id": ds.id, "raw_html_or_json_path": "u", "content_hash": "a",
                        "fetched_at": datetime.now(UTC).replace(tzinfo=None), "parse_status": "pending"})
        snap_repo.save({"data_source_id": ds.id, "raw_html_or_json_path": "u", "content_hash": "b",
                        "fetched_at": datetime.now(UTC).replace(tzinfo=None), "parse_status": "done"})
        session.flush()

        results = snap_repo.get_unparsed()
        assert len(results) == 1

    def test_get_failed_for_retry(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save({"data_source_id": ds.id, "raw_html_or_json_path": "u", "content_hash": "a",
                        "fetched_at": datetime(2020, 1, 1), "parse_status": "failed"})
        session.flush()

        results = snap_repo.get_failed_for_retry(retry_after_hours=0)
        assert len(results) == 1

    def test_get_reprocess_pending(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save({"data_source_id": ds.id, "raw_html_or_json_path": "u", "content_hash": "a",
                        "fetched_at": datetime.now(UTC).replace(tzinfo=None), "reprocess_status": "pending"})
        session.flush()

        results = snap_repo.get_reprocess_pending()
        assert len(results) == 1

    def test_update_parse_status(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap = snap_repo.save({"data_source_id": ds.id, "raw_html_or_json_path": "u", "content_hash": "a",
                               "fetched_at": datetime.now(UTC).replace(tzinfo=None)})
        session.flush()

        snap_repo.update_parse_status(snap.id, "done", parser_version="v1", error_message=None)
        session.flush()

        updated = session.get(RawSourceSnapshot, snap.id)
        assert updated.parse_status == "done"
        assert updated.parser_version == "v1"


class TestSaveRawSnapshots:
    def test_save_raw_snapshots(self, session):
        DataSource.__table__.create(create_engine("sqlite:///:memory:"), checkfirst=True)

        ds_repo = DataSourceRepository(session)
        ds_repo.save({"source_key": "kbo_schedule", "source_type": "official_kbo", "target_domain": "schedule",
                           "reliability": "high"})
        session.commit()

        raw_pages = [
            {"source_key": "kbo_schedule", "url": "http://example.com", "html": "<html>test</html>",
             "status_code": 200},
        ]
        saved = save_raw_snapshots(session, raw_pages)
        assert saved == 1

        saved = save_raw_snapshots(session, raw_pages)
        assert saved == 0
