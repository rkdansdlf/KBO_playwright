import hashlib

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
def session():
    engine = create_engine("sqlite:///:memory:")
    DataSource.__table__.create(engine)
    RawSourceSnapshot.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


class TestDataSourceRepository:
    def test_save_and_get(self, session):
        repo = DataSourceRepository(session)
        data = {"source_key": "test_key", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        ds = repo.save(data)
        assert ds.source_key == "test_key"
        assert ds.is_active is True

        fetched = repo.get_by_key("test_key")
        assert fetched is not None
        assert fetched.source_key == "test_key"

    def test_save_updates_existing(self, session):
        repo = DataSourceRepository(session)
        repo.save(
            {"source_key": "test_key", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        )
        repo.save(
            {"source_key": "test_key", "source_type": "official_team", "target_domain": "event", "is_active": False}
        )
        ds = repo.get_by_key("test_key")
        assert ds.source_type == "official_team"
        assert ds.is_active is False

    def test_get_all_active(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "a1", "source_type": "official_kbo", "target_domain": "event", "is_active": True})
        repo.save({"source_key": "a2", "source_type": "official_kbo", "target_domain": "event", "is_active": False})
        repo.save({"source_key": "a3", "source_type": "official_kbo", "target_domain": "event", "is_active": True})
        active = repo.get_all_active()
        assert len(active) == 2

    def test_get_stale_sources(self, session):
        from datetime import datetime, timedelta, timezone

        repo = DataSourceRepository(session)
        ds = repo.save(
            {"source_key": "stale", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        )
        ds.last_success_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=72)
        session.commit()

        stale = repo.get_stale_sources(max_hours=48)
        keys = [s.source_key for s in stale]
        assert "stale" in keys

    def test_get_stale_sources_excludes_fresh(self, session):
        from datetime import datetime, timezone

        repo = DataSourceRepository(session)
        repo.save({"source_key": "fresh", "source_type": "official_kbo", "target_domain": "event", "is_active": True})
        ds = repo.get_by_key("fresh")
        ds.last_success_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.commit()

        stale = repo.get_stale_sources(max_hours=48)
        keys = [s.source_key for s in stale]
        assert "fresh" not in keys

    def test_mark_success(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "ms", "source_type": "official_kbo", "target_domain": "event", "is_active": True})
        repo.mark_success("ms", "abc123")
        ds = repo.get_by_key("ms")
        assert ds.last_content_hash == "abc123"
        assert ds.last_success_at is not None

    def test_get_by_domain(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "k1", "source_type": "official_kbo", "target_domain": "ticket", "is_active": True})
        repo.save({"source_key": "k2", "source_type": "official_kbo", "target_domain": "event", "is_active": True})
        result = repo.get_by_domain("ticket")
        assert len(result) == 1
        assert result[0].source_key == "k1"

    def test_get_active_by_domain_excludes_inactive(self, session):
        repo = DataSourceRepository(session)
        repo.save({"source_key": "k1", "source_type": "official_kbo", "target_domain": "ticket", "is_active": False})
        result = repo.get_active_by_domain("ticket")
        assert len(result) == 0


class TestRawSourceSnapshotRepository:
    def test_save_and_get_by_source(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save(
            {"source_key": "s1", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        )
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        from datetime import datetime, timezone

        snap = snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "/tmp/test.json",
                "content_hash": "hash1",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )
        assert snap.data_source_id == ds.id
        assert snap.parse_status == "pending"

        snaps = snap_repo.get_by_source_id(ds.id)
        assert len(snaps) == 1

    def test_get_by_hash(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save(
            {"source_key": "s2", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        )
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        from datetime import datetime, timezone

        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "/tmp/a.json",
                "content_hash": "abc",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "/tmp/b.json",
                "content_hash": "def",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )

        found = snap_repo.get_by_hash(ds.id, "abc")
        assert found is not None
        assert found.content_hash == "abc"

        not_found = snap_repo.get_by_hash(ds.id, "xyz")
        assert not_found is None

    def test_get_unparsed(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save(
            {"source_key": "s3", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        )
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        from datetime import datetime, timezone

        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "/tmp/a.json",
                "content_hash": "h1",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "/tmp/b.json",
                "content_hash": "h2",
                "parse_status": "done",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )
        session.commit()

        unparsed = snap_repo.get_unparsed()
        assert len(unparsed) == 1
        assert unparsed[0].content_hash == "h1"

    def test_update_parse_status(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save(
            {"source_key": "s4", "source_type": "official_kbo", "target_domain": "event", "is_active": True}
        )
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        from datetime import datetime, timezone

        snap = snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "/tmp/a.json",
                "content_hash": "h1",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        )
        session.commit()

        snap_repo.update_parse_status(snap.id, "done", parser_version="v1")
        updated = session.get(RawSourceSnapshot, snap.id)
        assert updated.parse_status == "done"
        assert updated.parser_version == "v1"


class TestSaveRawSnapshots:
    def test_new_snapshot_marks_data_source_success(self, session):
        ds_repo = DataSourceRepository(session)
        ds_repo.save(
            {"source_key": "p0_event", "source_type": "official_team", "target_domain": "event", "is_active": True}
        )
        session.flush()

        html = "<html>fresh event page</html>"
        saved = save_raw_snapshots(
            session,
            [
                {
                    "source_key": "p0_event",
                    "url": "https://example.com/events",
                    "html": html,
                    "status_code": 200,
                }
            ],
        )

        ds = ds_repo.get_by_key("p0_event")
        assert saved == 1
        assert ds.last_success_at is not None
        assert ds.last_content_hash == hashlib.sha256(html.encode()).hexdigest()

    def test_duplicate_snapshot_still_marks_data_source_success(self, session):
        from datetime import datetime, timedelta, timezone

        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save(
            {"source_key": "p1_seat", "source_type": "official_team", "target_domain": "seat", "is_active": True}
        )
        session.flush()

        html = "<html>unchanged seat page</html>"
        content_hash = hashlib.sha256(html.encode()).hexdigest()
        old_success_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7)
        ds.last_success_at = old_success_at
        ds.last_content_hash = content_hash

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "https://example.com/seat",
                "content_hash": content_hash,
                "fetched_at": old_success_at,
                "status_code": 200,
            }
        )
        session.commit()

        saved = save_raw_snapshots(
            session,
            [
                {
                    "source_key": "p1_seat",
                    "url": "https://example.com/seat",
                    "html": html,
                    "status_code": 200,
                }
            ],
        )
        session.commit()

        updated = ds_repo.get_by_key("p1_seat")
        assert saved == 0
        assert updated.last_content_hash == content_hash
        assert updated.last_success_at is not None
        assert updated.last_success_at > old_success_at
