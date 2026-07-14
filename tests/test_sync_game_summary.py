from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.game import GameSummary
from src.sync.oci_sync import OCISync

# ── helpers ───────────────────────────────────────────────────────────


def _build_env(tmp_path: str):
    local_path = Path(tmp_path, "local.db")
    target_path = Path(tmp_path, "target.db")
    local_engine = create_engine(f"sqlite:///{local_path}")
    target_engine = create_engine(f"sqlite:///{target_path}")
    GameSummary.__table__.create(bind=local_engine, checkfirst=True)
    GameSummary.__table__.create(bind=target_engine, checkfirst=True)
    local_session = Session(bind=local_engine)
    target_session = Session(bind=target_engine)

    syncer = object.__new__(OCISync)
    syncer.sqlite_session = local_session
    syncer.target_session = target_session
    syncer.oci_engine = None
    syncer._chunked = lambda items, size: ([items[i : i + size] for i in range(0, len(items), size)])
    return syncer, local_session, target_session


def _seed(session, records: list[dict]):
    for rec in records:
        session.add(GameSummary(**rec))
    session.flush()


def _spy_bulk_copy(syncer):
    calls = []

    def _fake_insert(table_name, options):
        calls.append((table_name, options))
        for rec in options.records:
            syncer.target_session.add(GameSummary(**rec))
        syncer.target_session.flush()

    syncer._bulk_copy_upsert = _fake_insert
    return calls


def _spy_reset_sequence(syncer):
    calls = []
    syncer._reset_target_sequence_for_table = lambda name: calls.append(name)
    return calls


# ── fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def env(tmp_path):
    return _build_env(tmp_path)


# ── _sync_game_summary_rows ──────────────────────────────────────────


class TestSyncGameSummaryRows:
    def test_deletes_before_insert(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "리뷰", "player_name": "A"}])
        target.add(GameSummary(game_id="G001", summary_type="리뷰", player_name="OLD"))
        target.commit()

        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()

        remaining = target.query(GameSummary).all()
        assert len(remaining) == 1
        assert remaining[0].player_name == "A"
        assert len(calls) == 1

    def test_delete_batched_by_500(self, env):
        syncer, local, target = env
        game_ids = [f"G{i:03d}" for i in range(1001)]
        for gid in game_ids:
            local.add(GameSummary(game_id=gid, summary_type="T"))
        local.flush()

        for gid in game_ids:
            target.add(GameSummary(game_id=gid, summary_type="T"))
        target.commit()

        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()

        remaining = target.query(GameSummary).count()
        assert remaining == 1001
        assert len(calls) == 1
        assert len(calls[0][1].records) == 1001

    def test_delete_scoped_by_summary_type(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "리뷰", "player_name": "A"}])

        target.add(GameSummary(game_id="G001", summary_type="리뷰", player_name="OLD"))
        target.add(GameSummary(game_id="G001", summary_type="프리뷰", player_name="PRE"))
        target.commit()

        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows(summary_type="리뷰")

        assert len(calls) == 1
        assert len(calls[0][1].records) == 1
        assert calls[0][1].records[0]["summary_type"] == "리뷰"

        remaining = target.query(GameSummary).order_by(GameSummary.summary_type).all()
        assert len(remaining) == 2
        types = [r.summary_type for r in remaining]
        assert types == ["리뷰", "프리뷰"]

    def test_skips_when_no_rows(self, env):
        syncer, local, target = env
        calls = _spy_bulk_copy(syncer)
        result = syncer._sync_game_summary_rows()
        assert result == 0
        assert len(calls) == 0

    def test_deduplicates_by_key(self, env):
        syncer, local, target = env
        _seed(
            local,
            [
                {"game_id": "G001", "summary_type": "리뷰", "player_id": 1, "player_name": "A", "detail_text": "x"},
                {"game_id": "G001", "summary_type": "리뷰", "player_id": 1, "player_name": "A", "detail_text": "x"},
            ],
        )
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()
        assert len(calls[0][1].records) == 1

    def test_deduplicates_by_full_text(self, env):
        syncer, local, target = env
        _seed(
            local,
            [
                {"game_id": "G001", "summary_type": "리뷰", "player_name": "A", "detail_text": "hello"},
                {"game_id": "G001", "summary_type": "리뷰", "player_name": "A", "detail_text": "world"},
            ],
        )
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()
        assert len(calls[0][1].records) == 2

    def test_replace_game_ids_scopes_delete(self, env):
        syncer, local, target = env
        _seed(
            local,
            [
                {"game_id": "G001", "summary_type": "A", "player_name": "X"},
                {"game_id": "G002", "summary_type": "A", "player_name": "Y"},
            ],
        )
        target.add(GameSummary(game_id="G001", summary_type="A", player_name="OLD1"))
        target.add(GameSummary(game_id="G002", summary_type="A", player_name="OLD2"))
        target.commit()

        target.commit = MagicMock(wraps=target.commit)
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows(replace_game_ids=["G001"])

        assert len(calls) == 1
        assert len(calls[0][1].records) == 2

        old_rows = target.query(GameSummary).filter(GameSummary.player_name == "OLD1").count()
        assert old_rows == 0

    def test_excludes_system_columns(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "T", "player_name": "A"}])
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()
        record = calls[0][1].records[0]
        assert "id" not in record
        assert "created_at" not in record
        assert "updated_at" not in record

    def test_resets_sequence(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "T", "player_name": "A"}])
        reset_calls = _spy_reset_sequence(syncer)
        _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()
        assert "game_summary" in reset_calls

    def test_passes_to_bulk_copy_upsert(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "T", "player_name": "A"}])
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()
        tbl, options = calls[0]
        assert tbl == "game_summary"
        assert options.unique_cols == []
        assert options.update_timestamp is False

    def test_commits_delete(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "T", "player_name": "A"}])
        target.add(GameSummary(game_id="G001", summary_type="T", player_name="OLD"))
        target.commit()

        target.commit = MagicMock(wraps=target.commit)
        _spy_bulk_copy(syncer)
        _spy_reset_sequence(syncer)
        syncer._sync_game_summary_rows()
        target.commit.assert_called()

    def test_summary_type_filter_in_query(self, env):
        syncer, local, target = env
        _seed(
            local,
            [
                {"game_id": "G001", "summary_type": "리뷰", "player_name": "A"},
                {"game_id": "G001", "summary_type": "프리뷰", "player_name": "B"},
            ],
        )
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows(summary_type="프리뷰")
        assert len(calls[0][1].records) == 1
        assert calls[0][1].records[0]["summary_type"] == "프리뷰"

    def test_filters_passthrough(self, env):
        syncer, local, target = env
        _seed(
            local,
            [
                {"game_id": "G001", "summary_type": "T", "player_name": "A"},
                {"game_id": "G002", "summary_type": "T", "player_name": "B"},
            ],
        )
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows(filters=[GameSummary.game_id == "G001"])
        assert len(calls[0][1].records) == 1
        assert calls[0][1].records[0]["game_id"] == "G001"

    def test_returns_inserted_count(self, env):
        syncer, local, target = env
        _seed(
            local,
            [
                {"game_id": "G001", "summary_type": "T", "player_name": "A"},
                {"game_id": "G002", "summary_type": "T", "player_name": "B"},
            ],
        )
        _spy_bulk_copy(syncer)
        result = syncer._sync_game_summary_rows()
        assert result == 2

    def test_handle_null_player_id_name_text(self, env):
        syncer, local, target = env
        _seed(local, [{"game_id": "G001", "summary_type": "T"}])
        calls = _spy_bulk_copy(syncer)
        syncer._sync_game_summary_rows()
        record = calls[0][1].records[0]
        assert record["game_id"] == "G001"
        assert record["summary_type"] == "T"
