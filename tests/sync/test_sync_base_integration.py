from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session, sessionmaker

from src.sync.sync_base import (
    GameSyncEligibility,
    _build_composite_signature_query,
    _dedupe_records_for_conflict_keys,
    _is_game_dirty,
    _row_to_record,
    _serialize_scalar,
    build_game_sync_eligibility,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
    load_game_sync_signatures,
)

pytestmark = pytest.mark.usefixtures("_db_engine")


class TestGameSyncEligibility:
    def test_empty_eligibility(self):
        e = GameSyncEligibility()
        assert e.parent_game_ids == []
        assert e.detail_game_ids == []
        assert e.counts() == {
            "skipped_schedule_only": 0,
            "skipped_incomplete_detail": 0,
            "skipped_empty_relay": 0,
            "skipped_cancelled": 0,
        }


class TestSerializeScalar:
    def test_none(self):
        assert _serialize_scalar(None) is None

    def test_datetime(self):
        dt = datetime(2025, 6, 1, 12, 30, 0)
        assert _serialize_scalar(dt) == "2025-06-01T12:30:00"

    def test_int(self):
        assert _serialize_scalar(42) == 42

    def test_str(self):
        assert _serialize_scalar("hello") == "hello"


class TestDedupeRecords:
    def test_empty_keys(self):
        records = [{"a": 1}, {"a": 1}]
        assert _dedupe_records_for_conflict_keys(records, []) == records

    def test_dedupe_by_key(self):
        records = [{"id": 1, "val": "a"}, {"id": 1, "val": "b"}, {"id": 2, "val": "c"}]
        result = _dedupe_records_for_conflict_keys(records, ["id"])
        assert len(result) == 2

    def test_null_in_conflict_key_preserved(self):
        records = [{"id": None, "val": "a"}, {"id": None, "val": "b"}]
        result = _dedupe_records_for_conflict_keys(records, ["id"])
        assert len(result) == 2


class TestRowToRecord:
    def test_with_mapping(self):
        mock = MagicMock()
        mock._mapping = {"a": 1, "b": 2}
        result = _row_to_record(mock, ["a", "b"])
        assert result["a"] == 1
        assert result["b"] == 2

    def test_with_attributes(self):
        class FakeRow:
            a = 10
            b = 20

        result = _row_to_record(FakeRow(), ["a", "b"])
        assert result["a"] == 10

    def test_dict_list_serialized_to_json(self):
        mock = MagicMock()
        mock._mapping = {"data": {"key": "val"}, "items": [1, 2]}
        result = _row_to_record(mock, ["data", "items"])
        assert result["data"] == '{"key": "val"}'
        assert result["items"] == "[1, 2]"

    def test_created_at_defaulted(self):
        mock = MagicMock()
        mock._mapping = {"a": 1}
        result = _row_to_record(mock, ["a", "created_at"])
        assert result["created_at"] is not None

    def test_updated_at_defaulted(self):
        mock = MagicMock()
        mock._mapping = {"a": 1}
        result = _row_to_record(mock, ["a", "updated_at"])
        assert result["updated_at"] is not None


class TestFilterGameIdsByYear:
    def test_none_year_returns_all(self):
        assert filter_game_ids_by_year(["20240101", "20250101"], None) == ["20240101", "20250101"]

    def test_filters_by_prefix(self):
        assert filter_game_ids_by_year(["20240101", "20250101", "20231201"], 2024) == ["20240101"]

    def test_empty_input(self):
        assert filter_game_ids_by_year([], 2025) == []


class TestBuildCompositeSignatureQuery:
    def test_returns_string(self):
        sql = _build_composite_signature_query(None)
        assert isinstance(sql, str)
        assert "game_id" in sql
        assert "COUNT(*)" in sql

    def test_with_game_ids_filter(self):
        sql = _build_composite_signature_query(["g1"])
        assert "WHERE g.game_id IN :game_ids" in sql


class TestLoadGameSyncSignatures:
    def test_empty_db(self, _db_engine):
        session = sessionmaker(bind=_db_engine)()
        sigs = load_game_sync_signatures(session)
        assert sigs == {}

    def test_with_game_data(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.commit()

        sigs = load_game_sync_signatures(session)
        assert "20250601_01" in sigs
        assert sigs["20250601_01"]["game"]["home_team"] == "SSG"
        session.close()


class TestIsGameDirty:
    def test_identical_signatures_not_dirty(self):
        local = {"game": {"game_status": "COMPLETED", "home_score": 5, "updated_at": "2025-01-01"}}
        remote = {"game": {"game_status": "COMPLETED", "home_score": 5, "updated_at": "2025-01-01"}}
        assert not _is_game_dirty("g1", local, remote)

    def test_different_status_dirty(self):
        local = {"game": {"game_status": "COMPLETED", "home_score": 5}}
        remote = {"game": {"game_status": "LIVE", "home_score": 5}}
        assert _is_game_dirty("g1", local, remote)

    def test_newer_local_updated_at_dirty(self):
        local = {"game": {"game_status": "COMPLETED", "updated_at": "2025-06-02"}}
        remote = {"game": {"game_status": "COMPLETED", "updated_at": "2025-06-01"}}
        assert _is_game_dirty("g1", local, remote)

    def test_missing_remote_dirty(self):
        local = {"game": {"game_status": "COMPLETED"}}
        assert _is_game_dirty("g1", local, {})


class TestDetectDirtyGameIds:
    def test_no_local_signatures(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch("src.sync.sync_base.load_game_sync_signatures", return_value={}):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == []

    def test_missing_in_remote_is_dirty(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch(
            "src.sync.sync_base.load_game_sync_signatures",
            side_effect=[
                {"g1": {"game": {"game_status": "COMPLETED"}}},
                {},
            ],
        ):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == ["g1"]


class TestBuildGameSyncEligibility:
    def test_empty_input(self):
        session = MagicMock(spec=Session)
        e = build_game_sync_eligibility(session, [])
        assert e.parent_game_ids == []

    def test_cancelled_game_skipped(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="CANCELLED",
        )
        session.add(g)
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_cancelled
        session.close()

    def test_scheduled_no_detail_skipped(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="SCHEDULED",
        )
        session.add(g)
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_schedule_only
        session.close()
