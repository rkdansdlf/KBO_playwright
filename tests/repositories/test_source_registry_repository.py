from __future__ import annotations

from datetime import UTC, datetime

import pytest
import contextlib
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
        ds = repo.save(
            {
                "source_key": "kbo_schedule",
                "source_type": "official_kbo",
                "target_domain": "schedule",
                "reliability": "high",
            },
        )
        assert ds.source_key == "kbo_schedule"
        assert ds.source_type == "official_kbo"

    def test_save_existing(self, session):
        repo = DataSourceRepository(session)
        repo.save(
            {
                "source_key": "kbo_schedule",
                "source_type": "official_kbo",
                "target_domain": "schedule",
                "reliability": "high",
                "base_url": "http://old",
            },
        )
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
        repo.save(
            {
                "source_key": "a",
                "source_type": "t",
                "target_domain": "ticket",
                "reliability": "high",
                "is_active": True,
            },
        )
        repo.save(
            {
                "source_key": "b",
                "source_type": "t",
                "target_domain": "ticket",
                "reliability": "high",
                "is_active": False,
            },
        )
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
        repo.save(
            {"source_key": "stale", "source_type": "t", "target_domain": "d", "reliability": "h", "is_active": True},
        )
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
        ds = ds_repo.save(
            {
                "source_key": "kbo_schedule",
                "source_type": "official_kbo",
                "target_domain": "schedule",
                "reliability": "high",
            },
        )
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap = snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "http://example.com",
                "content_hash": "abc123",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "status_code": 200,
            },
        )
        assert snap.id is not None

        results = snap_repo.get_by_source_id(ds.id)
        assert len(results) == 1

    def test_get_by_hash(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "abc",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
            },
        )
        session.flush()

        found = snap_repo.get_by_hash(ds.id, "abc")
        assert found is not None
        assert snap_repo.get_by_hash(ds.id, "xyz") is None

    def test_get_unparsed(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "a",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "parse_status": "pending",
            },
        )
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "b",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "parse_status": "done",
            },
        )
        session.flush()

        results = snap_repo.get_unparsed()
        assert len(results) == 1

    def test_get_failed_for_retry(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "a",
                "fetched_at": datetime(2020, 1, 1),
                "parse_status": "failed",
            },
        )
        session.flush()

        results = snap_repo.get_failed_for_retry(retry_after_hours=0)
        assert len(results) == 1

    def test_get_reprocess_pending(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "a",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "reprocess_status": "pending",
            },
        )
        session.flush()

        results = snap_repo.get_reprocess_pending()
        assert len(results) == 1

    def test_update_parse_status(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap = snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "a",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
            },
        )
        session.flush()

        snap_repo.update_parse_status(snap.id, "done", parser_version="v1", error_message=None)
        session.flush()

        updated = session.get(RawSourceSnapshot, snap.id)
        assert updated.parse_status == "done"
        assert updated.parser_version == "v1"

    def test_update_parse_status_persists_failure_message(self, session):
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save({"source_key": "k", "source_type": "t", "target_domain": "d", "reliability": "h"})
        session.flush()

        snap_repo = RawSourceSnapshotRepository(session)
        snap = snap_repo.save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": "u",
                "content_hash": "a",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
            },
        )
        session.flush()

        snap_repo.update_parse_status(snap.id, "failed", parser_version="v2", error_message="selector missing")
        session.flush()

        updated = session.get(RawSourceSnapshot, snap.id)
        assert updated.parse_status == "failed"
        assert updated.parser_version == "v2"
        assert updated.error_message == "selector missing"


class TestSaveRawSnapshots:
    def test_save_raw_snapshots(self, session):
        DataSource.__table__.create(create_engine("sqlite:///:memory:"), checkfirst=True)

        ds_repo = DataSourceRepository(session)
        ds_repo.save(
            {
                "source_key": "kbo_schedule",
                "source_type": "official_kbo",
                "target_domain": "schedule",
                "reliability": "high",
            },
        )
        session.commit()

        raw_pages = [
            {
                "source_key": "kbo_schedule",
                "url": "http://example.com",
                "html": "<html>test</html>",
                "status_code": 200,
            },
        ]
        saved = save_raw_snapshots(session, raw_pages)
        assert saved == 1

        saved = save_raw_snapshots(session, raw_pages)
        assert saved == 0


class TestARecrawledArtifactKeepsItsParserOutcome:
    """``capture_metadata`` has to survive the path a re-crawl takes.

    The artifact is content-addressed, so crawling the same bytes twice leaves one
    row: the second crawl finds it by hash and lands in ``update_parse_status``
    rather than inserting. That path used to carry only ``parse_status``,
    ``parser_version`` and ``error_message`` -- so when its parser had produced
    ``parsed_records``, the baseline was dropped. ``validate_snapshot`` compares a
    re-parse against exactly that value, which is why the drift check reported
    ``with_baseline: 0`` for the most recent 100 snapshots: the number existed at
    crawl time and never reached the row.

    Three edges are pinned here: the value arrives, a crawl that has no metadata
    does not erase what is stored, and a merge keeps keys another producer wrote.
    """

    def _source(self, session, key: str = "lg_twins_events"):
        DataSourceRepository(session).save(
            {
                "source_key": key,
                "source_type": "club_site",
                "target_domain": "events",
                "reliability": "medium",
            },
        )
        session.commit()

    def _page(self, **extra):
        return {
            "source_key": "lg_twins_events",
            "url": "https://example.test/events",
            "html": "<html>같은 내용</html>",
            "status_code": 200,
            **extra,
        }

    @pytest.fixture(autouse=True)
    def _artifacts_in_a_temp_dir(self, tmp_path, monkeypatch):
        """Keep the captured artifacts out of the repository's data directory."""
        monkeypatch.setattr("src.repositories.source_registry_repository.evidence_root", lambda: tmp_path)
        return tmp_path

    def test_the_baseline_from_a_recrawl_reaches_the_row(self, session):
        self._source(session)
        save_raw_snapshots(session, [self._page()])
        session.commit()

        save_raw_snapshots(
            session,
            [self._page(parse_status="done", capture_metadata={"parsed_records": 5})],
        )
        session.commit()

        stored = session.query(RawSourceSnapshot).one()
        assert stored.capture_metadata == {"parsed_records": 5}

    def test_metadata_alone_is_enough_to_reach_the_row(self, session):
        """A crawler that records only the parser outcome must still be stored.

        Every producer today sets ``parse_status`` alongside it, so a condition that
        omits ``capture_metadata`` passes all of these tests -- measured: dropping it
        from the branch left 21/21 green. The branch names it because the next
        producer may not, and a silently ignored baseline is invisible: the drift
        check would just keep saying ``with_baseline: 0``.
        """
        self._source(session)
        save_raw_snapshots(session, [self._page()])
        session.commit()

        save_raw_snapshots(session, [self._page(capture_metadata={"parsed_records": 7})])
        session.commit()

        stored = session.query(RawSourceSnapshot).one()
        assert stored.capture_metadata == {"parsed_records": 7}

    def test_a_recrawl_without_metadata_does_not_erase_it(self, session):
        """A crawl that only renews the status must not clear the baseline."""
        self._source(session)
        save_raw_snapshots(
            session,
            [self._page(parse_status="done", capture_metadata={"parsed_records": 5})],
        )
        session.commit()

        save_raw_snapshots(session, [self._page(parse_status="failed", error_message="selector moved")])
        session.commit()

        stored = session.query(RawSourceSnapshot).one()
        assert stored.capture_metadata == {"parsed_records": 5}
        assert stored.parse_status == "failed"

    def test_metadata_is_merged_rather_than_replaced(self, session):
        """Nothing guarantees this dictionary has a single producer."""
        self._source(session)
        save_raw_snapshots(session, [self._page(parse_status="done", capture_metadata={"a": 1})])
        session.commit()

        save_raw_snapshots(session, [self._page(parse_status="done", capture_metadata={"b": 2})])
        session.commit()

        stored = session.query(RawSourceSnapshot).one()
        assert stored.capture_metadata == {"a": 1, "b": 2}

    def test_update_parse_status_merges_into_what_is_there(self, session):
        self._source(session)
        DataSourceRepository(session).get_by_key("lg_twins_events")
        repo = RawSourceSnapshotRepository(session)
        snapshot = repo.save(
            {
                "data_source_id": session.query(DataSource).one().id,
                "raw_html_or_json_path": "u",
                "content_hash": "h",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "capture_metadata": {"kept": True},
            },
        )
        session.flush()

        repo.update_parse_status(snapshot.id, "done", capture_metadata={"parsed_records": 3})
        session.flush()

        stored = session.get(RawSourceSnapshot, snapshot.id)
        assert stored.capture_metadata == {"kept": True, "parsed_records": 3}

    def test_update_parse_status_without_metadata_leaves_it_alone(self, session):
        self._source(session)
        repo = RawSourceSnapshotRepository(session)
        snapshot = repo.save(
            {
                "data_source_id": session.query(DataSource).one().id,
                "raw_html_or_json_path": "u",
                "content_hash": "h",
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "capture_metadata": {"kept": True},
            },
        )
        session.flush()

        repo.update_parse_status(snapshot.id, "failed", error_message="boom")
        session.flush()

        stored = session.get(RawSourceSnapshot, snapshot.id)
        assert stored.capture_metadata == {"kept": True}
