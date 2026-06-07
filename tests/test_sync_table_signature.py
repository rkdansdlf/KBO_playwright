from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base

from src.sync.oci_sync import OCISync

_SigBase = declarative_base()


class _SeasonModel(_SigBase):
    __tablename__ = "sig_season"
    id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=True)
    value = Column(String(50), nullable=True)
    updated_at = Column(String(30), nullable=True)


class _YearModel(_SigBase):
    __tablename__ = "sig_year"
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=True)
    value = Column(String(50), nullable=True)
    updated_at = Column(String(30), nullable=True)


# ── helpers ───────────────────────────────────────────────────────────


def _build_dual(path_local: str, path_remote: str):
    local_engine = create_engine(f"sqlite:///{path_local}")
    remote_engine = create_engine(f"sqlite:///{path_remote}")
    for e in (local_engine, remote_engine):
        _SigBase.metadata.create_all(bind=e)
    local_session = Session(bind=local_engine)
    remote_session = Session(bind=remote_engine)
    syncer = object.__new__(OCISync)
    syncer.sqlite_session = local_session
    syncer.target_session = remote_session
    syncer.oci_engine = None
    return syncer, local_session, remote_session


def _seed(session, model, records: list[dict]):
    for rec in records:
        session.add(model(**rec))
    session.commit()
    session.close()


def _now_ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")


# ── fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def dual(tmp_path):
    return _build_dual(
        os.path.join(tmp_path, "local.db"),
        os.path.join(tmp_path, "remote.db"),
    )


# ── _get_table_signature ─────────────────────────────────────────────


class TestGetTableSignature:
    def test_match_identical_data(self, dual):
        syncer, local, remote = dual
        ts = _now_ts()
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": ts}])
        _seed(remote, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": ts}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is True
        assert sig["local"]["count"] == 1
        assert sig["remote"]["count"] == 1

    def test_mismatch_different_count(self, dual):
        syncer, local, remote = dual
        ts = _now_ts()
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": ts}])
        _seed(
            remote,
            _SeasonModel,
            [
                {"season": 2025, "value": "a", "updated_at": ts},
                {"season": 2025, "value": "b", "updated_at": ts},
            ],
        )
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is False

    def test_mismatch_different_updated_at(self, dual):
        syncer, local, remote = dual
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-01-01T00:00:00"}])
        _seed(remote, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-01T00:00:00"}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is False

    def test_with_year_filter(self, dual):
        syncer, local, remote = dual
        _seed(
            local,
            _SeasonModel,
            [
                {"season": 2024, "value": "old", "updated_at": "2025-01-01"},
                {"season": 2025, "value": "cur", "updated_at": "2025-06-01"},
            ],
        )
        _seed(
            remote,
            _SeasonModel,
            [
                {"season": 2025, "value": "cur", "updated_at": "2025-06-01"},
            ],
        )
        sig = syncer._get_table_signature(_SeasonModel, year=2025)
        assert sig["match"] is True

    def test_custom_year_col(self, dual):
        syncer, local, remote = dual
        _seed(local, _YearModel, [{"year": 2025, "value": "a", "updated_at": "2025-06-01"}])
        _seed(remote, _YearModel, [{"year": 2025, "value": "a", "updated_at": "2025-06-01"}])
        sig = syncer._get_table_signature(_YearModel, year=2025, year_col="year")
        assert sig["match"] is True

    def test_no_rows(self, dual):
        syncer, local, remote = dual
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is True
        assert sig["local"]["count"] == 0
        assert sig["remote"]["count"] == 0
        assert sig["local"]["max_updated_at"] is None
        assert sig["remote"]["max_updated_at"] is None

    def test_timestamp_truncated_to_seconds(self, dual):
        syncer, local, remote = dual
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-07T12:34:56.789"}])
        _seed(remote, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-07T12:34:56"}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is True

    def test_t_t_space_normalization(self, dual):
        syncer, local, remote = dual
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-07T12:34:56"}])
        _seed(remote, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-07 12:34:56"}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is True

    def test_table_missing_locally(self, dual):
        syncer, local, remote = dual
        ts = _now_ts()
        _seed(remote, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": ts}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is False
        assert sig["remote"]["count"] == 1

    def test_table_missing_remotely(self, dual):
        syncer, local, remote = dual
        ts = _now_ts()
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": ts}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert sig["match"] is False
        assert sig["local"]["count"] == 1

    def test_return_structure(self, dual):
        syncer, local, remote = dual
        sig = syncer._get_table_signature(_SeasonModel)
        assert set(sig.keys()) == {"local", "remote", "match"}
        assert set(sig["local"].keys()) == {"count", "max_updated_at"}
        assert set(sig["remote"].keys()) == {"count", "max_updated_at"}
        assert isinstance(sig["match"], bool)

    def test_serialize_scalar_used(self, dual):
        syncer, local, remote = dual
        _seed(local, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-07T12:34:56"}])
        _seed(remote, _SeasonModel, [{"season": 2025, "value": "a", "updated_at": "2025-06-07T12:34:56"}])
        sig = syncer._get_table_signature(_SeasonModel)
        assert isinstance(sig["local"]["max_updated_at"], str)
