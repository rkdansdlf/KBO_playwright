from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base

from src.sync.oci_sync import OCISync

_Base = declarative_base()


class _SampleModel(_Base):
    __tablename__ = "sample_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    value = Column(Integer, nullable=True)
    payload = Column(Text, nullable=True)
    created_at = Column(String(30), nullable=True)
    updated_at = Column(String(30), nullable=True)


# ── helpers ───────────────────────────────────────────────────────────


def _build_session(db_path: str):
    engine = create_engine(f"sqlite:///{db_path}")
    _Base.metadata.create_all(bind=engine)
    return Session(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _build_syncer(session):
    syncer = object.__new__(OCISync)
    syncer.sqlite_session = session
    syncer.oci_engine = None
    syncer.target_session = None
    syncer.concurrency = 1  # Force sequential path: concurrent path requires a real OCI engine
    return syncer


def _spy_bulk(syncer):
    spy = {}
    syncer._bulk_copy_upsert = lambda tbl, recs, keys, **kw: spy.update(tbl=tbl, recs=recs, keys=keys, kw=kw)
    return spy


def _target_exists(syncer, exists: bool = True):
    syncer._target_table_exists = lambda _m: exists


def _seed_sample(session, records: list[dict]):
    for rec in records:
        session.add(_SampleModel(**rec))
    session.flush()


# ── fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def session(tmp_path):
    return _build_session(Path(tmp_path, "test.db"))


@pytest.fixture
def syncer(session):
    return _build_syncer(session)


# ── sync_simple_table ─────────────────────────────────────────────────


class TestSyncSimpleTable:
    def test_syncs_all_records(self, syncer, session):
        _seed_sample(session, [{"name": "a", "value": 1}, {"name": "b", "value": 2}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        result = syncer.sync_simple_table(_SampleModel, ["name"])
        assert result == 2
        assert spy["tbl"] == "sample_table"
        assert spy["keys"] == ["name"]
        assert len(spy["recs"]) == 2

    def test_returns_0_when_table_missing(self, syncer, session):
        _target_exists(syncer, False)
        assert syncer.sync_simple_table(_SampleModel, ["name"]) == 0

    def test_returns_0_when_no_records(self, syncer, session):
        _target_exists(syncer)
        _spy_bulk(syncer)
        assert syncer.sync_simple_table(_SampleModel, ["name"]) == 0

    def test_excludes_id_by_default(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"])
        assert "id" not in spy["recs"][0]

    def test_exclude_cols_extended(self, syncer, session):
        _seed_sample(session, [{"name": "a", "value": 99}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"], exclude_cols=["value"])
        record = spy["recs"][0]
        assert "id" not in record
        assert "value" not in record
        assert record["name"] == "a"

    def test_exclude_cols_auto_adds_id(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"], exclude_cols=[])
        assert "id" not in spy["recs"][0]

    def test_applies_filters(self, syncer, session):
        _seed_sample(session, [{"name": "keep", "value": 1}, {"name": "skip", "value": 2}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"], filters=[_SampleModel.value == 1])
        assert len(spy["recs"]) == 1
        assert spy["recs"][0]["name"] == "keep"

    def test_null_created_at_filled(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"])
        assert isinstance(spy["recs"][0]["created_at"], datetime)

    def test_existing_created_at_preserved(self, syncer, session):
        dt = "2025-01-01T00:00:00"
        _seed_sample(session, [{"name": "a", "created_at": dt}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"])
        assert spy["recs"][0]["created_at"] == dt

    def test_null_updated_at_filled(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"])
        assert isinstance(spy["recs"][0]["updated_at"], datetime)

    def test_transform_fn_applied(self, syncer, session):
        _seed_sample(session, [{"name": "hello", "value": 5}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)

        def upper_name(data):
            data["name"] = data["name"].upper()
            return data

        syncer.sync_simple_table(_SampleModel, ["name"], transform_fn=upper_name)
        assert spy["recs"][0]["name"] == "HELLO"

    def test_transform_fn_adding_keys(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)

        def add_key(data):
            data["extra"] = "x"
            return data

        syncer.sync_simple_table(_SampleModel, ["name"], transform_fn=add_key)
        assert spy["recs"][0].get("extra") == "x"

    def test_json_serialization_of_dict_objects(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)

        def inject_dict(data):
            data["payload"] = {"hello": "world"}
            return data

        syncer.sync_simple_table(_SampleModel, ["name"], transform_fn=inject_dict)
        assert spy["recs"][0]["payload"] == '{"hello": "world"}'

    def test_json_serialization_of_list_objects(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)

        def inject_list(data):
            data["payload"] = [1, 2, 3]
            return data

        syncer.sync_simple_table(_SampleModel, ["name"], transform_fn=inject_list)
        assert spy["recs"][0]["payload"] == "[1, 2, 3]"

    def test_passes_conflict_keys_to_bulk_copy(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"])
        assert spy["keys"] == ["name"]

    def test_update_timestamp_true(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"], update_timestamp=True)
        assert spy["kw"].get("update_timestamp") is True

    def test_update_timestamp_false(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"], update_timestamp=False)
        assert spy["kw"].get("update_timestamp") is False

    def test_update_timestamp_default_depends_on_exclude_cols(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        spy = _spy_bulk(syncer)
        _target_exists(syncer)
        syncer.sync_simple_table(_SampleModel, ["name"], exclude_cols=["updated_at"])
        assert spy["kw"].get("update_timestamp") is False

    def test_batch_splits_records(self, syncer, session):
        _seed_sample(session, [{"name": f"n{i}"} for i in range(7)])
        batches = []
        _target_exists(syncer)
        syncer._bulk_copy_upsert = lambda tbl, recs, keys, **kw: batches.append(len(recs))
        syncer.sync_simple_table(_SampleModel, ["name"], batch_size=3)
        assert batches == [3, 3, 1]

    def test_no_compatible_columns_returns_0(self, syncer, session):
        _seed_sample(session, [{"name": "a"}])
        _target_exists(syncer)
        _spy_bulk(syncer)
        result = syncer.sync_simple_table(
            _SampleModel,
            ["name"],
            exclude_cols=["id", "name", "value", "payload", "created_at", "updated_at"],
        )
        assert result == 0
