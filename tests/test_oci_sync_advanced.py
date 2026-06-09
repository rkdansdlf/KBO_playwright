from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameLineup,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.sync.oci_sync import OCISync
from src.sync.sync_base import OCISyncBase

#
# ─── _bulk_copy_upsert retry ──────────────────────────────────────────────────
#


def _build_minimal_base():
    """Build an OCISyncBase that has enough plumbing for retry tests."""
    engine = create_engine("sqlite:///:memory:")
    local_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    base = OCISyncBase.__new__(OCISyncBase)
    base.sqlite_session = local_factory()
    base.target_session = local_factory()
    base.oci_engine = engine
    return base


def test_bulk_copy_upsert_retries_on_transient_failure(monkeypatch):
    syncer = _build_minimal_base()
    attempt_log = []
    reconnect_log = []

    def _fake_do_copy(_self, table_name, records, unique_cols, update_timestamp, **kwargs):
        attempt_log.append(table_name)
        if len(attempt_log) < 3:
            raise ConnectionError("connection lost (transient)")
        return 42

    monkeypatch.setattr(OCISyncBase, "_do_bulk_copy_upsert", _fake_do_copy)
    monkeypatch.setattr(OCISyncBase, "_reconnect_oci", lambda _self: reconnect_log.append("reconnect"))

    syncer._bulk_copy_upsert("game_play_by_play", [{"id": 1}], ["id"])

    assert len(attempt_log) == 3
    assert len(reconnect_log) == 2


def test_bulk_copy_upsert_raises_on_persistent_failure(monkeypatch):
    syncer = _build_minimal_base()
    attempt_log = []
    reconnect_log = []

    def _fake_do_copy(_self, table_name, records, unique_cols, update_timestamp, **kwargs):
        attempt_log.append(table_name)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(OCISyncBase, "_do_bulk_copy_upsert", _fake_do_copy)
    monkeypatch.setattr(OCISyncBase, "_reconnect_oci", lambda _self: reconnect_log.append("reconnect"))

    with pytest.raises(RuntimeError, match="connection lost"):
        syncer._bulk_copy_upsert("game_play_by_play", [{"id": 1}], ["id"])

    assert len(attempt_log) == 3
    assert len(reconnect_log) == 2


def test_bulk_copy_upsert_skips_when_no_records(monkeypatch):
    syncer = _build_minimal_base()
    called = False

    def _fake_do_copy(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(OCISyncBase, "_do_bulk_copy_upsert", _fake_do_copy)

    syncer._bulk_copy_upsert("game_play_by_play", [], ["id"])
    assert not called


def test_bulk_copy_upsert_reconnect_on_each_retry(monkeypatch):
    syncer = _build_minimal_base()
    reconnect_calls = 0
    first_session = syncer.target_session

    def _fake_do_copy(*_args, **_kwargs):
        nonlocal reconnect_calls
        raise RuntimeError("connection lost")

    def _fake_reconnect(_self):
        nonlocal reconnect_calls
        reconnect_calls += 1
        _self.target_session = _build_minimal_base().target_session

    monkeypatch.setattr(OCISyncBase, "_do_bulk_copy_upsert", _fake_do_copy)
    monkeypatch.setattr(OCISyncBase, "_reconnect_oci", _fake_reconnect)

    with pytest.raises(RuntimeError, match="connection lost"):
        syncer._bulk_copy_upsert("game_play_by_play", [{"id": 1}], ["id"])

    assert reconnect_calls == 2
    assert syncer.target_session is not first_session


#
# ─── Pre-sync health check ────────────────────────────────────────────────────
#


def _build_minimal_games_mixin():
    """Build an OCISync that only has a sqlite session for games tests."""
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    local_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    syncer = OCISync.__new__(OCISync)
    syncer.sqlite_session = local_factory()
    syncer.target_session = None
    return syncer


def test_sync_game_details_connection_failure_aborts(monkeypatch):
    syncer = _build_minimal_games_mixin()
    monkeypatch.setattr(syncer, "test_connection", lambda: False)

    result = syncer.sync_game_details()

    assert result == {}


def test_sync_specific_game_connection_failure_aborts(monkeypatch):
    syncer = _build_minimal_games_mixin()
    monkeypatch.setattr(syncer, "test_connection", lambda: False)

    result = syncer.sync_specific_game("20260514NCLT0")

    assert result == {}


def test_sync_pregame_game_connection_failure_aborts(monkeypatch):
    syncer = _build_minimal_games_mixin()
    monkeypatch.setattr(syncer, "test_connection", lambda: False)

    result = syncer.sync_pregame_game("20260514NCLT0")

    assert result == {}


#
# ─── _sync_referenced_player_basic_for_games partial missing  ────────────────
#


def _build_player_game_env():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameSummary.__table__,
        GameEvent.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_sync_referenced_player_basic_for_games_partial_missing(caplog, monkeypatch):
    factory = _build_player_game_env()

    with factory() as session:
        session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG", status="Active"))
        session.add(
            GameLineup(
                game_id="20260514NCLT0",
                team_side="away",
                team_code="NC",
                player_id=1001,
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
            )
        )
        session.add(
            GameLineup(
                game_id="20260514NCLT0",
                team_side="home",
                team_code="LT",
                player_id=2002,
                player_name="없는선수",
                batting_order=2,
                appearance_seq=2,
            )
        )
        session.commit()

        syncer = OCISync.__new__(OCISync)
        syncer.sqlite_session = session

        synced_player_ids = []

        def _fake_sync_by_ids(_self, player_ids):
            synced_player_ids.extend(player_ids)
            return len(player_ids)

        monkeypatch.setattr(OCISync, "sync_player_basic_by_ids", _fake_sync_by_ids)

        with caplog.at_level("WARNING", logger="src.sync.sync_players"):
            result = syncer._sync_referenced_player_basic_for_games(["20260514NCLT0"])

        assert result == 1
        assert synced_player_ids == [1001]
        assert "Skipping 1 missing player_ids" in caplog.text
        assert "2002" in caplog.text


def test_sync_referenced_player_basic_for_games_all_missing(caplog, monkeypatch):
    factory = _build_player_game_env()

    with factory() as session:
        session.add(
            GameLineup(
                game_id="20260514NCLT0",
                team_side="away",
                team_code="NC",
                player_id=2002,
                player_name="없는선수",
                batting_order=1,
                appearance_seq=1,
            )
        )
        session.commit()

        syncer = OCISync.__new__(OCISync)
        syncer.sqlite_session = session
        called = False

        def _fake_sync_by_ids(*_args, **_kwargs):
            nonlocal called
            called = True
            return 0

        monkeypatch.setattr(OCISync, "sync_player_basic_by_ids", _fake_sync_by_ids)

        with caplog.at_level("WARNING", logger="src.sync.sync_players"):
            result = syncer._sync_referenced_player_basic_for_games(["20260514NCLT0"])

        assert result == 0
        assert not called
        assert "Skipping 1 missing player_ids" in caplog.text


def test_sync_referenced_player_basic_for_games_no_game_ids():
    factory = _build_player_game_env()

    with factory() as session:
        syncer = OCISync.__new__(OCISync)
        syncer.sqlite_session = session

        result = syncer._sync_referenced_player_basic_for_games([])
        assert result == 0

        result = syncer._sync_referenced_player_basic_for_games([""])
        assert result == 0
