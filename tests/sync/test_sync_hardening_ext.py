from __future__ import annotations

from datetime import datetime, time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.sync.sync_base import OCISyncBase, SimpleTableSyncOptions
from src.sync.sync_games import GameSyncMixin


@pytest.mark.parametrize(
    "message",
    ["closed the connection", "connection closed", "DPY-4011: connection lost"],
)
def test_new_oci_connection_failure_markers_are_transient(message: str) -> None:
    assert OCISyncBase._is_transient_oci_error(RuntimeError(message)) is True


def test_ensure_table_returns_without_oci_engine() -> None:
    syncer = object.__new__(OCISyncBase)

    with patch("src.models.base.Base.metadata.create_all") as create_all:
        syncer._ensure_table(object())

    create_all.assert_not_called()


def test_ensure_table_returns_for_non_table_model() -> None:
    syncer = object.__new__(OCISyncBase)
    syncer.oci_engine = MagicMock()

    with patch("src.models.base.Base.metadata.create_all") as create_all:
        syncer._ensure_table(object())

    create_all.assert_not_called()


def test_sync_simple_table_ensures_table_before_target_check() -> None:
    syncer = object.__new__(OCISyncBase)
    syncer.oci_engine = None
    syncer._ensure_table = MagicMock()
    syncer._target_table_exists = MagicMock(return_value=False)
    model = SimpleNamespace(__tablename__="sync_fixture")

    result = syncer.sync_simple_table(model, SimpleTableSyncOptions(conflict_keys=[]))

    assert result == 0
    syncer._ensure_table.assert_called_once_with(model)
    syncer._target_table_exists.assert_called_once_with(model)


def test_oracle_metadata_time_values_become_datetimes() -> None:
    syncer = GameSyncMixin()
    syncer.oci_engine = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
    syncer._cached_game_metadata_source_payload_limit = None
    data = {"start_time": time(9, 30), "end_time": time(12, 45)}

    result = syncer._transform_game_metadata_for_target(data)

    assert result["start_time"] == datetime(1970, 1, 1, 9, 30)
    assert result["end_time"] == datetime(1970, 1, 1, 12, 45)


def test_sync_game_details_ensures_game_tables_before_empty_scope() -> None:
    syncer = GameSyncMixin()
    syncer.test_connection = MagicMock(return_value=True)
    syncer._ensure_table = MagicMock()
    syncer._game_detail_parent_scope = MagicMock(return_value=([], []))

    result = syncer.sync_game_details(unsynced_only=True)

    assert result == {}
    assert syncer._ensure_table.call_count == 12
